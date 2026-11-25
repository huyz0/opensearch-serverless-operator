package controller

import (
	"context"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/netip"
	"sync"
	"time"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/security"
)

type securityAccountInputs struct {
	sources           v1alpha1.SecurityAccountSources
	previous, desired string
	bootstrap         string
	roots             *x509.CertPool
	domain            string
}

const accountPasswordSecretType = "serverless.opensearch.org/account-password"

type securityAccountBackend interface {
	resolve(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.SecurityAccountChange) (securityAccountInputs, error)
	verify(context.Context, *v1alpha1.OpenSearchServerless, securityAccountInputs, string, bool) (map[string]string, error)
	apply(context.Context, *v1alpha1.OpenSearchServerless, securityAccountInputs, string) error
}

func (r *OperationReconciler) ConfigureDevelopmentSecurityAccounts(instance *InstanceReconciler, reader client.Reader) {
	r.accountBackend = developmentSecurityAccountBackend{runtime: instance.podHTTP(), reader: reader}
}

type developmentSecurityAccountBackend struct {
	runtime podHTTPAccess
	reader  client.Reader
}

func (backend developmentSecurityAccountBackend) resolve(ctx context.Context, instance *v1alpha1.OpenSearchServerless, account *v1alpha1.SecurityAccountChange) (securityAccountInputs, error) {
	var input securityAccountInputs
	r := backend.runtime
	if instance.Status.PrimaryStore == nil || instance.Status.HTTPTLSBaseline == nil || instance.Status.BasicAuthBaseline == nil || r.core == nil || instance.Status.ServingFleetIntentByRole["ingest"] == "" {
		return input, fmt.Errorf("security changes require a pinned store and approved managed HTTPS Basic Auth")
	}
	_, password, err := r.resolveBasicAuth(ctx, instance)
	if err != nil || len(password) == 0 {
		return input, fmt.Errorf("startup account credentials failed validation")
	}
	domain, err := r.peerDNSDomain(instance)
	if err != nil || domain == "" {
		return input, fmt.Errorf("approved per-Pod HTTPS identity is unavailable")
	}
	input.roots, err = r.developmentHTTPTrustPool(ctx, instance, domain, instance.Status.ServingFleetIntentByRole["ingest"])
	if err != nil {
		return input, fmt.Errorf("pinned HTTPS material is unavailable")
	}
	input.domain, input.bootstrap = domain, string(password)
	read := func(ref *v1alpha1.SecretKeyReference) (string, string, error) {
		if ref == nil {
			return "", "", nil
		}
		if ref.Name == instance.Status.BasicAuthBaseline.SecretName || ref.Name == instance.Status.HTTPTLSBaseline.CertificateName || ref.Name == instance.Status.HTTPTLSBaseline.TrustName {
			return "", "", fmt.Errorf("application passwords must use dedicated account Secrets")
		}
		secret, err := r.core.Secrets(instance.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil || secret.UID == "" || secret.DeletionTimestamp != nil || secret.Immutable == nil || !*secret.Immutable || secret.Type != accountPasswordSecretType {
			return "", "", fmt.Errorf("application password Secret must be live, immutable and approved for account changes")
		}
		value := secret.Data[ref.Key]
		if len(value) == 0 || len(value) > 4096 || !utf8.Valid(value) || string(value) == input.bootstrap {
			return "", "", fmt.Errorf("invalid application account credential")
		}
		return string(secret.UID), string(value), nil
	}
	input.sources.PreviousSecretUID, input.previous, err = read(account.PreviousPasswordRef)
	if err != nil {
		return securityAccountInputs{}, err
	}
	input.sources.DesiredSecretUID, input.desired, err = read(account.DesiredPasswordRef)
	if err != nil || input.previous == input.desired {
		return securityAccountInputs{}, fmt.Errorf("application account transition requires distinct approved credential inputs")
	}
	return input, nil
}

func (backend developmentSecurityAccountBackend) admin(ctx context.Context, instance *v1alpha1.OpenSearchServerless, input securityAccountInputs) (security.UserAdmin, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	pods := backend.runtime.core.Pods(instance.Namespace)
	cohort, err := listRuntimeCohort(ctx, pods, instance)
	if err != nil {
		return security.UserAdmin{}, err
	}
	for uid, item := range cohort {
		if err := ctx.Err(); err != nil {
			return security.UserAdmin{}, err
		}
		if item.Role != "ingest" || item.Fleet != instance.Status.ServingFleetIntentByRole["ingest"] {
			continue
		}
		pod, err := pods.Get(ctx, item.Name, metav1.GetOptions{})
		if err != nil || pod.UID != uid || pod.ResourceVersion != item.Version || pod.DeletionTimestamp != nil ||
			pod.Annotations[keystoreUIDAnnotation] != instance.Status.BasicAuthBaseline.SecretUID || pod.Annotations[peerDomainAnnotation] != input.domain {
			continue
		}
		identity, err := httpPodIdentity(instance, item.Name, input.domain)
		if err != nil {
			return security.UserAdmin{}, err
		}
		tlsConfig, err := managedPodTLSConfig(instance, item.Fleet, identity, input.roots)
		if err != nil {
			return security.UserAdmin{}, err
		}
		transport := &http.Transport{TLSClientConfig: tlsConfig}
		endpoint := "https://" + netip.AddrPortFrom(item.IP, 9200).String()
		base := &http.Client{Transport: transport, Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		health := *base
		if err := java.ConfigureHealthBasicAuth(&health, netip.AddrPortFrom(item.IP, 9200).String(), runtimeconfig.BasicAuthUser, []byte(input.bootstrap)); err != nil {
			return security.UserAdmin{}, err
		}
		if err := java.CheckServing(ctx, &health, endpoint, item.Name, "ingest", 5*time.Second); err != nil {
			base.CloseIdleConnections()
			continue
		}
		fresh, err := pods.Get(ctx, item.Name, metav1.GetOptions{})
		if err != nil || fresh.ResourceVersion != pod.ResourceVersion || fresh.UID != pod.UID {
			base.CloseIdleConnections()
			continue
		}
		return security.UserAdmin{BaseURL: endpoint, Username: runtimeconfig.BasicAuthUser, Password: input.bootstrap, Client: base}, nil
	}
	return security.UserAdmin{}, fmt.Errorf("no live qualified ingest account endpoint")
}

func (backend developmentSecurityAccountBackend) verify(ctx context.Context, instance *v1alpha1.OpenSearchServerless, input securityAccountInputs, user string, desired bool) (map[string]string, error) {
	admin, err := backend.admin(ctx, instance, input)
	if err != nil {
		return nil, err
	}
	defer admin.Client.CloseIdleConnections()
	rejected, accepted := input.desired, input.previous
	if desired {
		rejected, accepted = input.previous, input.desired
	}
	checkRecord := func(ctx context.Context) error {
		exists, err := admin.Exists(ctx, user)
		if err != nil {
			return err
		}
		if exists != (accepted != "") {
			return fmt.Errorf("indexed account presence does not match the expected state")
		}
		return nil
	}
	observer := accountCohortObserver{Core: backend.runtime.core, Reader: backend.reader}
	var mu sync.Mutex
	cache := map[string]*x509.CertPool{instance.Status.ServingFleetIntentByRole["ingest"]: input.roots}
	roots := func(ctx context.Context, fleet string) (*x509.CertPool, error) {
		mu.Lock()
		defer mu.Unlock()
		if pool := cache[fleet]; pool != nil {
			return pool, nil
		}
		pool, err := backend.runtime.developmentHTTPTrustPool(ctx, instance, input.domain, fleet)
		if err == nil {
			cache[fleet] = pool
		}
		return pool, err
	}
	consumers, err := observer.verifyAccount(ctx, instance, roots, input.domain, admin, user, rejected, accepted, checkRecord)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, len(consumers))
	for _, consumer := range consumers {
		result[consumer.PodUID] = consumer.ProcessID
	}
	return result, nil
}

func (backend developmentSecurityAccountBackend) apply(ctx context.Context, instance *v1alpha1.OpenSearchServerless, input securityAccountInputs, user string) error {
	admin, err := backend.admin(ctx, instance, input)
	if err != nil {
		return err
	}
	defer admin.Client.CloseIdleConnections()
	if input.desired == "" {
		_, err = admin.Delete(ctx, user)
		return err
	}
	return admin.Put(ctx, user, input.desired)
}
