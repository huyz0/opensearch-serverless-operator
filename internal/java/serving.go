package java

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// CheckServing requires local readiness and a valid lease with a minimum
// remaining lifetime. It is not evidence of strict cache warmth, peer routing,
// or sustained availability. The two reads are not an atomic snapshot.
func CheckServing(ctx context.Context, client *http.Client, baseURL, expectedNode, role string, minimumLease time.Duration) error {
	_, _, err := checkServingUntil(ctx, client, baseURL, expectedNode, role, minimumLease)
	return err
}

func checkServingUntil(ctx context.Context, client *http.Client, baseURL, expectedNode, role string, minimumLease time.Duration) (time.Time, Lease, error) {
	if expectedNode == "" || (role != "search" && role != "ingest") || minimumLease < 0 {
		return time.Time{}, Lease{}, fmt.Errorf("node name, role and nonnegative lease margin are required")
	}
	started := time.Now()
	ready, err := GetReadiness(ctx, client, baseURL)
	if err != nil {
		return time.Time{}, Lease{}, err
	}
	if !ready.Ready || (role == "search" && !ready.ServesSearches) {
		return time.Time{}, Lease{}, fmt.Errorf("Java process is not serving the requested role")
	}
	lease, err := GetLease(ctx, client, baseURL)
	if err != nil {
		return time.Time{}, Lease{}, err
	}
	if lease.NodeName != expectedNode || !lease.Valid || lease.ExpiresInMillis <= minimumLease.Milliseconds() || !lease.HasRole(role) {
		return time.Time{}, Lease{}, fmt.Errorf("local lease is invalid, expiring, belongs to another node, or the local role is missing")
	}
	ready, err = GetReadiness(ctx, client, baseURL)
	if err != nil {
		return time.Time{}, Lease{}, err
	}
	if !ready.Ready || (role == "search" && !ready.ServesSearches) {
		return time.Time{}, Lease{}, fmt.Errorf("Java process stopped serving during observation")
	}
	deadline := lease.EvidenceValidUntil(started, minimumLease)
	if !time.Now().Before(deadline) {
		return time.Time{}, Lease{}, fmt.Errorf("local lease may expire before the required margin")
	}
	return deadline, lease, nil
}
