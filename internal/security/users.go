package security

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// ErrUserMutationUnknown means a request may have committed. The caller must
// not blindly retry or interpret an account existence check as proof of the
// password value. The provider does not expose a compare-and-set interface.
var ErrUserMutationUnknown = errors.New("account mutation outcome is unknown")

var accountName = regexp.MustCompile(`^[A-Za-z0-9_.@-]{1,64}$`)

// UserAdmin calls the built-in shell account API. It must be configured with
// the exact approved HTTPS management path and bootstrap credentials. This is
// authentication management only, not fine-grained authorization or policy
// activation. The caller must coordinate external administrators separately.
type UserAdmin struct {
	BaseURL  string
	Username string
	Password string
	Client   *http.Client
}

func (admin UserAdmin) request(ctx context.Context, method, user, password string) (bool, error) {
	mutating := method != http.MethodGet
	if ctx == nil || !ValidAccountName(user) || (method != http.MethodGet && method != http.MethodPut && method != http.MethodDelete) || (method == http.MethodPut && !validPassword(password)) {
		return false, fmt.Errorf("valid HTTPS account endpoint, credentials and request are required")
	}
	// The shell accepts an indexed account with the bootstrap name in addition
	// to the configured bootstrap password. Mutating that indexed record does
	// not rotate or revoke the configured credential.
	if user == admin.Username {
		return false, fmt.Errorf("bootstrap account requires startup credential rotation")
	}
	endpoint, httpClient, err := admin.verifiedClient()
	if err != nil {
		return false, err
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/_serverless/security/users/" + user
	endpoint.RawPath = ""
	var body []byte
	if method == http.MethodPut {
		body, err = json.Marshal(struct {
			Password string `json:"password"`
		}{Password: password})
		if err != nil {
			return false, fmt.Errorf("account request cannot be encoded")
		}
		defer clear(body)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("account request cannot be constructed")
	}
	request.SetBasicAuth(admin.Username, admin.Password)
	if method == http.MethodPut {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := httpClient.Do(request)
	if err != nil {
		if mutating {
			return false, ErrUserMutationUnknown
		}
		return false, fmt.Errorf("account read failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && !((method == http.MethodGet || method == http.MethodDelete) && response.StatusCode == http.StatusNotFound) {
		if mutating {
			return false, fmt.Errorf("%w: HTTP %d", ErrUserMutationUnknown, response.StatusCode)
		}
		return false, fmt.Errorf("account read returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		if mutating {
			return false, ErrUserMutationUnknown
		}
		return false, fmt.Errorf("account read response is unavailable")
	}
	var result struct {
		User         string `json:"user"`
		Exists       *bool  `json:"exists"`
		Found        *bool  `json:"found"`
		Acknowledged *bool  `json:"acknowledged"`
	}
	if json.Unmarshal(data, &result) != nil || result.User != user {
		if mutating {
			return false, ErrUserMutationUnknown
		}
		return false, fmt.Errorf("account read response is invalid")
	}
	switch method {
	case http.MethodGet:
		if result.Exists == nil || *result.Exists != (response.StatusCode == http.StatusOK) {
			return false, fmt.Errorf("account read response is inconsistent")
		}
		return *result.Exists, nil
	case http.MethodDelete:
		if result.Found == nil || *result.Found != (response.StatusCode == http.StatusOK) {
			return false, ErrUserMutationUnknown
		}
		return *result.Found, nil
	default:
		if result.Acknowledged == nil || !*result.Acknowledged {
			return false, ErrUserMutationUnknown
		}
		return true, nil
	}
}

// ValidAccountName reports whether a name can identify an account in the
// shell's HTTP API. The caller separately reserves the bootstrap username.
func ValidAccountName(user string) bool {
	return accountName.MatchString(user) && user != "_version" && user != "." && user != ".."
}

func validPassword(password string) bool {
	return password != "" && len(password) <= 16384
}

func (admin UserAdmin) verifiedClient() (*url.URL, http.Client, error) {
	endpoint, err := url.Parse(admin.BaseURL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.Opaque != "" ||
		!ValidAccountName(admin.Username) || !validPassword(admin.Password) || admin.Client == nil {
		return nil, http.Client{}, fmt.Errorf("valid HTTPS account endpoint and bootstrap credentials are required")
	}
	transport, ok := admin.Client.Transport.(*http.Transport)
	if !ok || transport == nil || transport.Proxy != nil || transport.DialTLS != nil || transport.DialTLSContext != nil ||
		(transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify) {
		return nil, http.Client{}, fmt.Errorf("explicit certificate-verifying transport without a proxy or custom TLS dialer is required")
	}
	httpClient := *admin.Client
	httpClient.Jar = nil
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return endpoint, httpClient, nil
}

// Exists reads only account existence; it cannot establish a password value.
func (admin UserAdmin) Exists(ctx context.Context, user string) (bool, error) {
	return admin.request(ctx, http.MethodGet, user, "")
}

// Put submits an account creation or password replacement. Callers must not
// retry an unknown outcome without first reconciling it.
func (admin UserAdmin) Put(ctx context.Context, user, password string) error {
	_, err := admin.request(ctx, http.MethodPut, user, password)
	return err
}

// Delete submits an account removal and reports whether the account existed.
func (admin UserAdmin) Delete(ctx context.Context, user string) (bool, error) {
	return admin.request(ctx, http.MethodDelete, user, "")
}
