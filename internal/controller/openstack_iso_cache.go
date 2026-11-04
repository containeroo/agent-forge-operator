package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"

	api "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	osapi "github.com/containeroo/agent-forge-operator/internal/openstack"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const imageCacheOwnedValue = "true"

// This phase is a durable cleanup journal, not just a progress label. A restart
// must finish releasing the old reference before acquiring a new image identity.
const openStackPhaseRefreshingImage = "RefreshingDiscoveryImage"

const imageCacheLabel = "agent-forge.containeroo.ch/openstack-image-cache"
const imageCachePhase = "agent-forge.containeroo.ch/cache-phase"

// imageCacheKey includes actual authentication scope and InfraEnv UID. Equal ISO
// bytes are shared only within the same namespace, InfraEnv, project, and region.
func imageCacheKey(scope, infraEnvUID, sha string) string {
	sum := sha256.Sum256([]byte(scope + "/" + infraEnvUID + "/" + sha))
	return "os-image-" + hex.EncodeToString(sum[:])[:48]
}
func imageIDForCache(cache *corev1.ConfigMap) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(string(cache.UID))).String()
}

func (r *OpenStackAgentReconciler) downloadDiscovery(ctx context.Context, a *api.OpenStackAgent) (string, isoDownloadResult, error) {
	available, url, _ := discoveryISOAvailable(ctx, r.reader(), a.Namespace, a.Spec.Configuration.InfraEnvRef.Name)
	if !available {
		return "", isoDownloadResult{}, &osapi.Error{Reason: "InfraEnvUnavailable", Retryable: true, Rejected: true}
	}
	file, err := os.CreateTemp("", "agent-forge-openstack-*.iso")
	if err != nil {
		return "", isoDownloadResult{}, &osapi.Error{Reason: "ISOTemporaryStorageFailed", Retryable: true}
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", isoDownloadResult{}, err
	}
	// Reuse the bounded streaming downloader and content digest used by vSphere.
	result, err := downloadISO(ctx, url, path, "")
	if err != nil {
		_ = os.Remove(path)
		return "", isoDownloadResult{}, &osapi.Error{Reason: "ISODownloadFailed", Retryable: true}
	}
	if result.SizeBytes == 0 {
		_ = os.Remove(path)
		return "", isoDownloadResult{}, &osapi.Error{Reason: "EmptyDiscoveryImage", Rejected: true}
	}
	return path, result, nil
}

// acquireCache uses resourceVersion updates for reference acquisition. Once the
// last releaser sets Deleting, new consumers cannot race image garbage collection.
func (r *OpenStackAgentReconciler) acquireCache(ctx context.Context, a *api.OpenStackAgent) (*corev1.ConfigMap, error) {
	key := client.ObjectKey{Namespace: a.Namespace, Name: a.Status.ImageCacheName}
	cache := &corev1.ConfigMap{}
	err := r.reader().Get(ctx, key, cache)
	if apierrors.IsNotFound(err) {
		cache = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name,
			Labels: map[string]string{imageCacheLabel: imageCacheOwnedValue}, Annotations: map[string]string{imageCachePhase: "Active"}},
			Data: map[string]string{string(a.UID): "reference"}}
		if err := r.Create(ctx, cache); err != nil {
			return nil, err
		}
		return cache, nil
	}
	if err != nil {
		return nil, err
	}
	if cache.Labels[imageCacheLabel] != imageCacheOwnedValue {
		return nil, osapi.ErrOwnership
	}
	if cache.Annotations[imageCachePhase] != "Active" || !cache.DeletionTimestamp.IsZero() {
		return nil, &osapi.Error{Reason: "ImageCacheDeleting", Retryable: true}
	}
	if cache.Data[string(a.UID)] == "" {
		if cache.Data == nil {
			cache.Data = map[string]string{}
		}
		cache.Data[string(a.UID)] = "reference"
		if err := r.Update(ctx, cache); err != nil {
			return nil, err
		}
	}
	return cache, nil
}

func (r *OpenStackAgentReconciler) prepareDiscoveryImage(ctx context.Context, p osapi.Client, a *api.OpenStackAgent) (bool, error) {
	var path string
	if a.Status.ImageCacheName == "" {
		infra := infraEnvWatchObject()
		if err := r.reader().Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: a.Spec.Configuration.InfraEnvRef.Name}, infra); err != nil {
			return false, err
		}
		var result isoDownloadResult
		var err error
		path, result, err = r.downloadDiscovery(ctx, a)
		if err != nil {
			return false, err
		}
		defer func() { _ = os.Remove(path) }()
		a.Status.InfraEnvUID = string(infra.GetUID())
		a.Status.ISOSHA256 = result.SHA256
		a.Status.ISOSizeBytes = result.SizeBytes
		a.Status.ImageCacheName = imageCacheKey(p.Scope(), a.Status.InfraEnvUID, result.SHA256)
		// Checkpoint before acquiring any external resources, but keep this exact file
		// alive until upload. Independent downloads need not be byte-identical.
		if err := r.status(ctx, a); err != nil {
			return false, err
		}
	}
	return r.ensureDiscoveryImage(ctx, p, a, path)
}

func (r *OpenStackAgentReconciler) ensureDiscoveryImage(ctx context.Context, p osapi.Client, a *api.OpenStackAgent, path string) (bool, error) {
	cache, err := r.acquireCache(ctx, a)
	if err != nil {
		return false, err
	}
	id := imageIDForCache(cache)
	if a.Status.ImageID == "" {
		a.Status.ImageID = id
		if err := r.status(ctx, a); err != nil {
			return false, err
		}
	}
	if a.Status.ImageID != id {
		return false, &osapi.Error{Reason: "ImageCacheIdentityChanged", Rejected: true}
	}
	owner := string(cache.UID)
	image, err := p.Get(ctx, osapi.Image, id, owner)
	if errors.Is(err, osapi.ErrNotFound) {
		// A supplied Glance UUID makes this create idempotent across process crashes
		// and concurrent consumers. A concurrent 409 is retried by observation.
		image, err = p.Create(ctx, osapi.Image, a.Status.ImageCacheName, owner, a)
	}
	if err != nil {
		return false, err
	}
	switch image.State {
	case "active":
		if image.SizeBytes != a.Status.ISOSizeBytes {
			return false, &osapi.Error{Reason: "ImageSizeMismatch", Rejected: true}
		}
		return true, nil
	case "saving", "importing":
		return false, nil
	case "queued":
		return false, r.uploadDiscoverySnapshot(ctx, p, a, path, owner)

	default:
		return false, &osapi.Error{Reason: "DiscoveryImageFailed", Rejected: true}
	}
}

// uploadDiscoverySnapshot uploads the hashed file from this reconcile. If a
// restart lost that file, observe Glance first; only queued images need a new
// download. Different bytes get a fresh identity, never an upload to the old ID.
func (r *OpenStackAgentReconciler) uploadDiscoverySnapshot(ctx context.Context, p osapi.Client, a *api.OpenStackAgent, path, owner string) error {
	if path == "" {
		downloaded, result, err := r.downloadDiscovery(ctx, a)
		if err != nil {
			return err
		}
		defer func() { _ = os.Remove(downloaded) }()
		path = downloaded
		if result.SHA256 != a.Status.ISOSHA256 {
			if !canRefreshDiscoveryImage(a) {
				return &osapi.Error{Reason: "DiscoveryImageRefreshBlocked", Rejected: true}
			}
			a.Status.Phase = openStackPhaseRefreshingImage
			return r.status(ctx, a) // journal before releasing the old reference
		}
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return p.UploadImage(ctx, a.Status.ImageID, owner, file)
}

func canRefreshDiscoveryImage(a *api.OpenStackAgent) bool {
	return a.Status.ServerID == "" && a.Status.DiscoveryVolumeID == "" && a.Status.PendingCreate == "" && a.Status.AgentRef == nil && a.Status.InstanceCreatedAt == nil
}

func (r *OpenStackAgentReconciler) refreshDiscoveryImage(ctx context.Context, p osapi.Client, a *api.OpenStackAgent) error {
	if !canRefreshDiscoveryImage(a) {
		return &osapi.Error{Reason: "DiscoveryImageRefreshBlocked", Rejected: true}
	}
	done, err := r.releaseDiscoveryImage(ctx, p, a)
	if err != nil || !done {
		return err
	}
	// The old reference is gone (and last-reference GC has completed). Clearing
	// the journal and identity together is crash-safe; the next reconcile downloads
	// a fresh snapshot. The port and blank OS volume remain unchanged.
	a.Status.ImageCacheName = ""
	a.Status.ImageID = ""
	a.Status.ISOSHA256 = ""
	a.Status.ISOSizeBytes = 0
	a.Status.InfraEnvUID = ""
	a.Status.Phase = "PreparingDiscoveryImage"
	return nil
}

func (r *OpenStackAgentReconciler) releaseDiscoveryImage(ctx context.Context, p osapi.Client, a *api.OpenStackAgent) (bool, error) {
	if a.Status.ImageCacheName == "" {
		return true, nil
	}
	cache := &corev1.ConfigMap{}
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: a.Status.ImageCacheName}, cache); err != nil {
		if apierrors.IsNotFound(err) && a.Status.ImageID == "" {
			return true, nil
		}
		// Missing bookkeeping cannot authorize deleting an image whose ownership is
		// no longer known. Already collected images are verified by their pinned ID.
		if apierrors.IsNotFound(err) {
			_, err = p.Get(ctx, osapi.Image, a.Status.ImageID, "missing-cache-owner")
			if errors.Is(err, osapi.ErrNotFound) {
				return true, nil
			}
		}
		return false, err
	}
	if cache.Labels[imageCacheLabel] != imageCacheOwnedValue {
		return false, osapi.ErrOwnership
	}
	if a.Status.ImageID != "" && a.Status.ImageID != imageIDForCache(cache) {
		// A previous release may have completed before its status checkpoint. If a
		// new cache now occupies this key, never delete it or any of its references.
		_, err := p.Get(ctx, osapi.Image, a.Status.ImageID, "missing-cache-owner")
		if errors.Is(err, osapi.ErrNotFound) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, osapi.ErrOwnership
	}
	if cache.Annotations[imageCachePhase] != openStackPhaseDeleting {
		delete(cache.Data, string(a.UID))
		if len(cache.Data) == 0 {
			cache.Annotations[imageCachePhase] = openStackPhaseDeleting
		}
		if err := r.Update(ctx, cache); err != nil {
			return false, err
		}
		if len(cache.Data) > 0 {
			return true, nil
		}
		return false, nil // durable tombstone excludes acquisition before deletion
	}
	id := imageIDForCache(cache)
	_, err := p.Get(ctx, osapi.Image, id, string(cache.UID))
	if !errors.Is(err, osapi.ErrNotFound) {
		if err != nil {
			return false, err
		}
		return false, p.Delete(ctx, osapi.Image, id, string(cache.UID))
	}
	uid, version := cache.UID, cache.ResourceVersion
	if err := r.Delete(ctx, cache, client.Preconditions{UID: &uid, ResourceVersion: &version}); err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	return true, nil
}
