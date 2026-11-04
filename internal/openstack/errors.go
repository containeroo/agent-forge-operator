package openstack

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"syscall"

	"github.com/gophercloud/gophercloud/v2"
)

// Error contains only safe diagnostics, never the original error, response body,
// URL, headers, or credential values. Rejected means the request is known not to
// have created a resource; diagnostic classification does not change this decision.
type Error struct {
	Reason              string
	Retryable, Rejected bool
	service, operation  string
	category, requestID string
	statusCode          int
}

func (e *Error) Error() string {
	message := e.Reason
	switch e.Reason {
	case reasonHTTPSRequired:
		message += "; HTTP is disabled; use HTTPS or explicitly set allow-insecure-http=true in the credentials Secret (plaintext credentials and tokens)"
	case "InvalidHTTPPolicy":
		message += "; allow-insecure-http must be exactly true or false when present in the credentials Secret"
	case "InvalidAuthenticationURL":
		message += "; auth-url must be an absolute HTTP(S) URL without user information, query, or fragment"
	}
	if e.service != "" {
		message += fmt.Sprintf("; service=%s; operation=%s", e.service, e.operation)
	}
	if e.category != "" {
		message += "; category=" + e.category
	}
	if e.statusCode != 0 {
		message += fmt.Sprintf("; HTTP %d %s", e.statusCode, http.StatusText(e.statusCode))
	}
	if e.requestID != "" {
		message += "; request_id=" + e.requestID
	}
	return message
}

// Is preserves sentinel matching when operation diagnostics have been added.
// There is deliberately no Unwrap: callers must never recover an unsafe cause.
func (e *Error) Is(target error) bool {
	return (target == ErrNotFound && e.Reason == ErrNotFound.Reason) ||
		(target == ErrOwnership && e.Reason == ErrOwnership.Reason) ||
		(target == ErrThrottle && e.Reason == ErrThrottle.Reason)
}

var (
	ErrNotFound  = &Error{Reason: "ResourceNotFound", Rejected: true}
	ErrOwnership = &Error{Reason: "ResourceOwnershipMismatch", Rejected: true}
	ErrThrottle  = &Error{Reason: "RateLimited", Retryable: true, Rejected: true}
	// Do not echo arbitrary header values, even from a header named request ID.
	requestIDPattern = regexp.MustCompile(`^req-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

// SafeError converts SDK errors before controllers can log or persist them.
// Classification uses error types, never parsing or forwarding Error() text.
func SafeError(err error) error {
	if err == nil {
		return nil
	}
	var safe *Error
	if errors.As(err, &safe) {
		return safe
	}
	result := &Error{Reason: "OpenStackRequestFailed", Retryable: true}
	var response gophercloud.ErrUnexpectedResponseCode
	var responsePointer *gophercloud.ErrUnexpectedResponseCode
	if errors.As(err, &response) {
		return responseError(response)
	}
	if errors.As(err, &responsePointer) {
		return responseError(*responsePointer)
	}
	result.category = errorCategory(err)
	return result
}

func responseError(response gophercloud.ErrUnexpectedResponseCode) *Error {
	result := &Error{Reason: "OpenStackRequestFailed", Retryable: true, category: "HTTPResponse"}
	switch response.Actual {
	case 404:
		*result = *ErrNotFound
	case 400, 401, 403, 405, 413, 422:
		result.Reason, result.Retryable, result.Rejected = "ConfigurationRejected", false, true
	case 429:
		*result = *ErrThrottle
	}
	result.category = "HTTPResponse"
	if response.Actual >= 100 && response.Actual <= 599 {
		result.statusCode = response.Actual
	}
	for _, header := range []string{"X-Openstack-Request-Id", "X-Compute-Request-Id"} {
		if value := response.ResponseHeader.Get(header); requestIDPattern.MatchString(value) {
			result.requestID = value
			break
		}
	}
	return result
}

func errorCategory(err error) string {
	var unknownCA x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var certificate x509.CertificateInvalidError
	var verification *tls.CertificateVerificationError
	var record tls.RecordHeaderError
	var alert tls.AlertError
	var dns *net.DNSError
	var network net.Error
	switch {
	case errors.Is(err, http.ErrSchemeMismatch):
		return "HTTPResponseToHTTPSClient"
	case errors.Is(err, context.Canceled):
		return "RequestCanceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "RequestTimeout"
	case errors.As(err, &unknownCA):
		return "TLSUnknownAuthority"
	case errors.As(err, &hostname):
		return "TLSHostnameMismatch"
	case errors.As(err, &certificate):
		return "TLSCertificateInvalid"
	case errors.As(err, &verification), errors.As(err, &record), errors.As(err, &alert):
		return "TLSHandshakeFailed"
	case errors.As(err, &dns):
		return "DNSResolutionFailed"
	case errors.As(err, &network) && network.Timeout():
		return "RequestTimeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "ConnectionRefused"
	case errors.Is(err, syscall.ECONNRESET):
		return "ConnectionReset"
	case errors.Is(err, syscall.ENETUNREACH), errors.Is(err, syscall.EHOSTUNREACH):
		return "NetworkUnreachable"
	default:
		return clientErrorCategory(err)
	}
}

func clientErrorCategory(err error) string {
	var endpoint gophercloud.ErrEndpointNotFound
	var endpointPointer *gophercloud.ErrEndpointNotFound
	var service gophercloud.ErrServiceNotFound
	var servicePointer *gophercloud.ErrServiceNotFound
	var input gophercloud.ErrInvalidInput
	var inputPointer *gophercloud.ErrInvalidInput
	var missing gophercloud.ErrMissingInput
	var missingPointer *gophercloud.ErrMissingInput
	var syntax *json.SyntaxError
	var mismatch *json.UnmarshalTypeError
	switch {
	case errors.As(err, &endpoint), errors.As(err, &endpointPointer):
		return "EndpointNotFound"
	case errors.As(err, &service), errors.As(err, &servicePointer):
		return "ServiceNotFound"
	case errors.As(err, &input), errors.As(err, &inputPointer), errors.As(err, &missing), errors.As(err, &missingPointer):
		return "InvalidSDKInput"
	case errors.As(err, &syntax), errors.As(err, &mismatch):
		return "InvalidAPIResponse"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "UnexpectedEndOfResponse"
	default:
		return "UnclassifiedClientError"
	}
}

// These helpers accept only constant operation labels from provider code. Never
// pass a URL, resource name, SDK error string, or user configuration as a label.
func safeErrorAt(err error, service, operation string) error {
	if err == nil {
		return nil
	}
	safe := *SafeError(err).(*Error)
	if safe.service == "" {
		safe.service, safe.operation = service, operation
	}
	return &safe
}

func safeResourceError(err error, kind Kind, action string) error {
	service, resource := "openstack", "resource"
	switch kind {
	case Port:
		service, resource = serviceNeutron, "port"
	case OSVolume:
		service, resource = serviceCinder, "os-volume"
	case DiscoveryVolume:
		service, resource = serviceCinder, "discovery-volume"
	case Server:
		service, resource = serviceNova, "server"
	case Image:
		service, resource = serviceGlance, "image"
	}
	return safeErrorAt(err, service, action+"-"+resource)
}
