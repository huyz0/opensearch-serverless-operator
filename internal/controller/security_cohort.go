package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"maps"
	"net/http"
	"net/netip"
	"time"

	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/security"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

type runtimeCohortPod struct {
	Name, Role, Fleet, Version, ContainerID string
	IP                                      netip.Addr
	RestartCount                            int32
}

// accountCohortObserver binds direct HTTPS evidence to the current inventory.
// Reader must bypass the controller cache.
type accountCohortObserver struct {
	Reader client.Reader
	Core   corev1client.CoreV1Interface
}

func (observer accountCohortObserver) verifyAccount(ctx context.Context, instance *v1alpha1.OpenSearchServerless, roots func(context.Context, string) (*x509.CertPool, error), domain string, admin security.UserAdmin, user, previous, desired string, checkRecord func(context.Context) error) ([]security.Consumer, error) {
	if roots == nil || (domain != "" && !validDNSHost(domain)) {
		return nil, fmt.Errorf("HTTP trust roots and a valid optional Pod DNS domain are required")
	}
	peer, err := workload.PeerDNSService(instance)
	if err != nil {
		return nil, err
	}
	return observer.observe(ctx, instance, checkRecord, func(ctx context.Context, pod runtimeCohortPod) (security.AccountProcess, error) {
		serverName := pod.IP.String()
		if domain != "" {
			host, err := workload.PeerHostname(pod.Name)
			if err != nil {
				return security.AccountProcess{}, err
			}
			serverName = fmt.Sprintf("%s.%s.%s.svc.%s", host, peer.Name, instance.Namespace, domain)
			if !validDNSHost(serverName) {
				return security.AccountProcess{}, fmt.Errorf("invalid Pod certificate identity")
			}
		}
		process := security.AccountProcess{Name: pod.Name, Role: pod.Role}
		pool, err := roots(ctx, pod.Fleet)
		if err != nil {
			return security.AccountProcess{}, err
		}
		config, err := managedPodTLSConfig(instance, pod.Fleet, serverName, pool)
		if err != nil {
			return security.AccountProcess{}, err
		}
		return observeAccountOnPod(ctx, netip.AddrPortFrom(pod.IP, 9200), config, admin, process, user, previous, desired)
	})
}

func observeAccountOnPod(ctx context.Context, address netip.AddrPort, config *tls.Config, admin security.UserAdmin, process security.AccountProcess, user, previous, desired string) (security.AccountProcess, error) {
	admin.BaseURL = "https://" + address.String()
	admin.Client = &http.Client{Transport: &http.Transport{TLSClientConfig: config}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer admin.Client.CloseIdleConnections()
	observed, verified, err := admin.ObserveAccount(ctx, process, user, previous, desired)
	if err == nil && !verified {
		err = fmt.Errorf("credentials have not converged")
	}
	return observed, err
}

// observe brackets record reads and credential probes with the same Pod and
// instance inventory. The final record read can take long enough for a Pod
// to join or restart, so inventory must be checked after that read too.
func (observer accountCohortObserver) observe(ctx context.Context, instance *v1alpha1.OpenSearchServerless, checkRecord func(context.Context) error, probe func(context.Context, runtimeCohortPod) (security.AccountProcess, error)) ([]security.Consumer, error) {
	if ctx == nil || probe == nil || observer.Core == nil || observer.Reader == nil || instance == nil || instance.UID == "" || instance.Namespace == "" || instance.Name == "" || instance.ResourceVersion == "" || instance.DeletionTimestamp != nil {
		return nil, fmt.Errorf("a live instance and uncached namespaced clients are required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	checkInstance := func() error {
		var current v1alpha1.OpenSearchServerless
		if err := observer.Reader.Get(ctx, client.ObjectKeyFromObject(instance), &current); err != nil {
			return err
		}
		if current.UID != instance.UID || current.ResourceVersion != instance.ResourceVersion || current.DeletionTimestamp != nil {
			return fmt.Errorf("instance changed during account observation")
		}
		return nil
	}
	if err := checkInstance(); err != nil {
		return nil, err
	}
	pods := observer.Core.Pods(instance.Namespace)
	first, err := listRuntimeCohort(ctx, pods, instance)
	if err != nil {
		return nil, err
	}
	if checkRecord != nil {
		if err := checkRecord(ctx); err != nil {
			return nil, fmt.Errorf("check account record before credential probes: %w", err)
		}
	}
	observed := make([]security.Consumer, len(first))
	group, work := errgroup.WithContext(ctx)
	group.SetLimit(8)
	index := 0
	for uid, pod := range first {
		if work.Err() != nil {
			break
		}
		slot := index
		index++
		group.Go(func() error {
			process, err := probe(work, pod)
			if err != nil {
				return fmt.Errorf("observe account on Pod %s: %w", pod.Name, err)
			}
			if process.Name != pod.Name || process.Role != pod.Role || process.Incarnation == "" {
				return fmt.Errorf("account observation did not identify Pod %s", pod.Name)
			}
			observed[slot] = security.Consumer{PodUID: string(uid), ProcessID: process.Incarnation}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	incarnations := make(map[string]bool, len(observed))
	for _, consumer := range observed {
		if incarnations[consumer.ProcessID] {
			return nil, fmt.Errorf("multiple Pods reported the same Java process incarnation")
		}
		incarnations[consumer.ProcessID] = true
	}
	if checkRecord != nil {
		if err := checkRecord(ctx); err != nil {
			return nil, fmt.Errorf("check account record after credential probes: %w", err)
		}
	}
	last, err := listRuntimeCohort(ctx, pods, instance)
	if err != nil {
		return nil, err
	}
	if !maps.Equal(first, last) {
		return nil, fmt.Errorf("Pod membership or identity changed during account observation")
	}
	if err := checkInstance(); err != nil {
		return nil, err
	}
	return observed, nil
}

func listRuntimeCohort(ctx context.Context, pods corev1client.PodInterface, instance *v1alpha1.OpenSearchServerless) (map[types.UID]runtimeCohortPod, error) {
	cohort := make(map[types.UID]runtimeCohortPod)
	addresses := make(map[netip.Addr]bool)
	serving := make(map[string]bool)
	// Use owner references as the authority. Filtering by instance label
	// could silently omit an owned, reachable Pod with a drifted label.
	options := metav1.ListOptions{Limit: 200}
	seenContinue := make(map[string]struct{})
	for {
		page, err := pods.List(ctx, options)
		if err != nil {
			return nil, fmt.Errorf("list namespace Pods: %w", err)
		}
		for i := range page.Items {
			pod := &page.Items[i]
			owner := metav1.GetControllerOf(pod)
			owned := owner != nil && owner.UID == instance.UID && owner.Name == instance.Name && owner.Kind == "OpenSearchServerless" && owner.APIVersion == v1alpha1.GroupVersion.String()
			labelMatches := pod.Labels[workload.InstanceLabel] == string(instance.UID)
			referencesInstance := false
			for _, reference := range pod.OwnerReferences {
				if reference.UID == instance.UID {
					referencesInstance = true
					break
				}
			}
			if !referencesInstance && !labelMatches {
				continue
			}
			if !owned || !labelMatches || pod.Namespace != instance.Namespace || pod.Name == "" || pod.UID == "" || pod.ResourceVersion == "" {
				return nil, fmt.Errorf("Pod %s has conflicting instance identity", pod.Name)
			}
			if hasStoppedAccountProcess(pod) {
				continue
			}
			// A running but unready or terminating JVM can still receive
			// direct traffic or serve an already established connection.
			role, fleet := pod.Labels[workload.RoleLabel], pod.Labels[workload.FleetLabel]
			container := runningOpenSearch(pod)
			address, err := netip.ParseAddr(pod.Status.PodIP)
			address = address.Unmap()
			if (role != "search" && role != "ingest") || fleet == "" || pod.Spec.HostNetwork || pod.Spec.NodeName == "" || container == nil ||
				err != nil || address.Zone() != "" || !address.IsGlobalUnicast() || addresses[address] || cohort[pod.UID].Name != "" {
				return nil, fmt.Errorf("Pod %s lacks a unique, observable Java process", pod.Name)
			}
			if len(cohort) >= 512 {
				return nil, fmt.Errorf("runtime cohort exceeds the observation limit")
			}
			addresses[address] = true
			cohort[pod.UID] = runtimeCohortPod{Name: pod.Name, Role: role, Fleet: fleet, Version: pod.ResourceVersion, IP: address, ContainerID: container.ContainerID, RestartCount: container.RestartCount}
			if fleet == instance.Status.ServingFleetIntentByRole[role] {
				serving[role] = true
			}
		}
		if page.Continue == "" {
			break
		}
		if _, seen := seenContinue[page.Continue]; seen {
			return nil, fmt.Errorf("namespace Pod pagination did not advance")
		}
		seenContinue[page.Continue] = struct{}{}
		options.Continue = page.Continue
	}
	if len(serving) == 0 {
		return nil, fmt.Errorf("runtime cohort has no observed serving Pods")
	}
	for role, pool := range instance.Spec.Pools {
		for _, zone := range pool.Zones {
			if (zone.Replicas > 0 || zone.WarmSpare > 0) && !serving[role] {
				return nil, fmt.Errorf("runtime cohort lacks the serving fleet for role %s", role)
			}
		}
	}
	return cohort, nil
}

func hasStoppedAccountProcess(pod *corev1.Pod) bool {
	if !terminalCapacityPod(pod) || pod.Status.Reason == "NodeLost" {
		return false
	}
	if pod.Spec.NodeName == "" {
		return true
	}
	for _, status := range pod.Status.ContainerStatuses {
		if stopped := status.State.Terminated; status.Name == "opensearch" && stopped != nil &&
			stopped.ContainerID != "" && !stopped.FinishedAt.IsZero() && stopped.Reason != "ContainerStatusUnknown" {
			return true
		}
	}
	return false
}
