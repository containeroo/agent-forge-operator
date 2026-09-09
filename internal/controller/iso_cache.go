/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

func (r *VsphereAgentReconciler) ensureISOCache(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, provider VMProvider, isoDownloadURL string) (string, error) {
	now := metav1.Now()
	token := pool.GetAnnotations()[forceISORefreshAnnotation]
	if !isoCacheDue(pool, isoDownloadURL, token, now.Time) {
		return pool.Status.ISO.Path, nil
	}

	result, err := provider.EnsureISO(ctx, pool, isoDownloadURL)
	if err != nil {
		recordISOOperation("ensure", err)
		return "", err
	}
	recordISOOperation("ensure", nil)

	previousPath := pool.Status.ISO.Path
	previousHistory := append([]agentforgev1alpha1.ISOCacheHistoryEntry(nil), pool.Status.ISO.History...)
	previousHistory = ensureISOHistoryEntry(previousHistory, agentforgev1alpha1.ISOCacheHistoryEntry{
		Path:       previousPath,
		SHA256:     pool.Status.ISO.SHA256,
		SizeBytes:  pool.Status.ISO.SizeBytes,
		UploadedAt: pool.Status.ISO.UploadedAt,
	})
	uploadedAt := pool.Status.ISO.UploadedAt
	if result.Uploaded || uploadedAt.IsZero() || result.Path != previousPath {
		uploadedAt = now
	}

	pool.Status.ISO.URL = redactedDownloadURL(isoDownloadURL)
	pool.Status.ISO.Path = result.Path
	pool.Status.ISO.SHA256 = result.SHA256
	pool.Status.ISO.SizeBytes = result.SizeBytes
	pool.Status.ISO.CheckedAt = now
	pool.Status.ISO.UploadedAt = uploadedAt
	pool.Status.ISO.ForceRefreshToken = token
	retainVersions := isoRetainVersions(pool)
	pool.Status.ISO.History = updatedISOHistory(previousHistory, agentforgev1alpha1.ISOCacheHistoryEntry{
		Path:       result.Path,
		SHA256:     result.SHA256,
		SizeBytes:  result.SizeBytes,
		UploadedAt: uploadedAt,
	}, retainVersions)

	for _, stalePath := range staleISOPaths(previousHistory, previousPath, result.Path, retainVersions) {
		if err := provider.DeleteISO(ctx, pool, stalePath); err != nil {
			recordISOOperation("delete", err)
			if r.Recorder != nil {
				recordEvent(r.Recorder, pool, corev1.EventTypeWarning, "ISOPruneFailed", stableErrorMessage(err))
			}
			if entry, found := findISOHistoryEntry(previousHistory, stalePath); found {
				pool.Status.ISO.History = ensureISOHistoryEntry(pool.Status.ISO.History, entry)
			}
		} else {
			recordISOOperation("delete", nil)
		}
	}

	reason := "Reused"
	message := fmt.Sprintf("Reused cached ISO %s", result.Path)
	if result.Uploaded {
		reason = "Uploaded"
		message = fmt.Sprintf("Uploaded cached ISO %s", result.Path)
	}
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               conditionISOReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: pool.Generation,
		Reason:             reason,
		Message:            message,
	})
	if r.Recorder != nil {
		recordEvent(r.Recorder, pool, corev1.EventTypeNormal, "ISO"+reason, message)
	}

	return result.Path, nil
}
