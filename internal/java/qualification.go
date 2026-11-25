package java

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// RoleQualification binds local readiness and lease checks to one JVM.
// StatsProcessVerified records whether stats also reported that incarnation.
type RoleQualification struct {
	ProcessIncarnation   string
	StatsProcessVerified bool
	LeaseValidUntil      time.Time
	LocalSearchActivity  *LocalSearchActivity
}

// SearchQualification also requires completed warm-up without a timeout or error.
type SearchQualification = RoleQualification

// IngestQualification binds a local serving and lease check to one JVM
// incarnation. It does not prove shard ownership, write success or takeover.
type IngestQualification = RoleQualification

// QualifyIngest requires the patched process-incarnation signal, local ingest
// role, readiness and lease margin before and after the observation.
func QualifyIngest(ctx context.Context, client *http.Client, baseURL, expectedNode string, minimumLease time.Duration) (IngestQualification, error) {
	before, err := GetWarmup(ctx, client, baseURL)
	if err != nil || !before.Ready {
		return IngestQualification{}, fmt.Errorf("ingest process incarnation or readiness is unavailable")
	}
	deadline, stats, err := checkServingUntil(ctx, client, baseURL, expectedNode, "ingest", minimumLease)
	if err != nil {
		return IngestQualification{}, err
	}
	after, err := GetWarmup(ctx, client, baseURL)
	if err != nil || !after.Ready || before.ProcessIncarnation != after.ProcessIncarnation ||
		(stats.ProcessIncarnation != "" && stats.ProcessIncarnation != after.ProcessIncarnation) {
		return IngestQualification{}, fmt.Errorf("ingest process changed during observation")
	}
	if !time.Now().Before(deadline) {
		return IngestQualification{}, fmt.Errorf("ingest lease margin expired during recheck")
	}
	return IngestQualification{ProcessIncarnation: after.ProcessIncarnation, StatsProcessVerified: stats.ProcessIncarnation != "", LeaseValidUntil: deadline}, nil
}

// QualifySearch reads a specific process directly; a load-balanced Service
// is not a valid endpoint. The caller must bind the endpoint to the observed
// Pod UID, container identity and IP, verify those identities again after the
// reads, and limit the evidence's lifetime. Results are not an atomic snapshot
// and cannot guarantee future availability or actual cache residency.
func QualifySearch(ctx context.Context, client *http.Client, baseURL, expectedNode string, minimumLease time.Duration) (SearchQualification, error) {
	before, err := GetWarmup(ctx, client, baseURL)
	if err != nil {
		return SearchQualification{}, err
	}
	if !before.Ready || before.Result != "completed" || before.TimedOut {
		return SearchQualification{}, fmt.Errorf("search warm-up is not qualified")
	}
	deadline, stats, err := checkServingUntil(ctx, client, baseURL, expectedNode, "search", minimumLease)
	if err != nil {
		return SearchQualification{}, err
	}
	after, err := GetWarmup(ctx, client, baseURL)
	if err != nil {
		return SearchQualification{}, err
	}
	if !after.Ready || after.Result != "completed" || after.TimedOut || before.ProcessIncarnation != after.ProcessIncarnation ||
		(stats.ProcessIncarnation != "" && stats.ProcessIncarnation != after.ProcessIncarnation) {
		return SearchQualification{}, fmt.Errorf("search warm-up or process changed during qualification")
	}
	if !time.Now().Before(deadline) {
		return SearchQualification{}, fmt.Errorf("local lease margin expired during warm-up recheck")
	}
	qualification := SearchQualification{ProcessIncarnation: after.ProcessIncarnation, StatsProcessVerified: stats.ProcessIncarnation != "", LeaseValidUntil: deadline}
	if qualification.StatsProcessVerified {
		qualification.LocalSearchActivity = stats.LocalSearchActivity
	}
	return qualification, nil
}
