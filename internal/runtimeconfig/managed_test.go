package runtimeconfig_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/storage"
)

func TestManagedArguments(t *testing.T) {
	input := runtimeconfig.ManagedInputs{NodeName: "node", ClusterName: "cluster", Role: "search", Zone: "zone-a", Home: "/work", ConfigPath: "/config", NetworkHost: "10.0.0.1", Store: storage.S3{Bucket: "bucket", Region: "region"}, JVMHeap: "2Gi"}
	args, err := runtimeconfig.ManagedArguments(input, []string{"-Dcustom=literal $(x)"})
	if err != nil || !slices.Contains(args, "-Dserverless.roles=search") || !slices.Contains(args, "-Dnode.attr.zone=zone-a") || !slices.Contains(args, "-Dcustom=literal $(x)") {
		t.Fatalf("unexpected managed arguments %v: %v", args, err)
	}
	if _, err := runtimeconfig.ManagedArguments(input, []string{"-Dserverless.store.endpoint=other"}); err == nil {
		t.Fatal("user must not override managed store settings")
	}
	if _, err := runtimeconfig.ManagedArguments(input, []string{"-Ds3.client.default.identity_token_file=/untrusted"}); err == nil {
		t.Fatal("user must not override the S3 identity token path")
	}
	input.S3WebIdentity = true
	identityArgs, err := runtimeconfig.ManagedArguments(input, nil)
	if err != nil || !slices.Contains(identityArgs, "-Ds3.client.default.identity_token_file="+runtimeconfig.S3WebIdentityTokenPath) {
		t.Fatalf("managed S3 identity token setting missing: %v", err)
	}
	if !slices.Contains(args, "-Xmx2147483648") || !slices.Contains(args, "-Xms2147483648") {
		t.Fatal("expected equal initial and maximum heap")
	}
	fractionalUnit := input
	fractionalUnit.JVMHeap = "1.5Gi"
	converted, err := runtimeconfig.ManagedArguments(fractionalUnit, nil)
	if err != nil || !slices.Contains(converted, "-Xms1610612736") || !slices.Contains(converted, "-Xmx1610612736") {
		t.Fatalf("fractional units that resolve to whole bytes must launch: %v", err)
	}

	if _, err := runtimeconfig.ManagedArguments(input, []string{"-Xmx1g"}); err == nil {
		t.Fatal("user heap override must fail")
	}
	if _, err := runtimeconfig.ManagedArguments(input, []string{"-Dserverless.transport.advertise_host=attacker"}); err == nil {
		t.Fatal("user peer identity override accepted")
	}
	if _, err := runtimeconfig.ManagedArguments(input, []string{"-Dserverless.search.require_warmup_completion=false"}); err == nil {
		t.Fatal("user warm-up gate override accepted")
	}
	input.RequireWarmupCompletion = true
	if _, err := runtimeconfig.ManagedArguments(input, nil); err == nil {
		t.Fatal("strict mode without warm-on-join accepted")
	}
	input.WarmOnJoin = true
	args, err = runtimeconfig.ManagedArguments(input, nil)
	if err != nil || !slices.Contains(args, "-Dserverless.search.require_warmup_completion=true") {
		t.Fatalf("strict Java setting missing: %v", err)
	}
	input.PeerTLS = true
	input.PeerAdvertiseHost = "node.example"
	input.PeerTLSLeafSHA256 = strings.Repeat("a", 64)
	input.PeerTLSTrustSHA256 = strings.Repeat("b", 64)
	input.HTTPTLSCertificateHash = strings.Repeat("c", 64)
	args, err = runtimeconfig.ManagedArguments(input, nil)
	if err != nil || !slices.Contains(args, "-Dserverless.plugins=org.opensearch.serverless.operator.PeerTlsPlugin,org.opensearch.serverless.operator.HttpTlsPlugin") ||
		!slices.Contains(args, "-Dserverless.http_tls.expected_certificate_sha256="+input.HTTPTLSCertificateHash) {
		t.Fatalf("managed HTTP and peer TLS settings missing: %v", err)
	}
	if _, err := runtimeconfig.ManagedArguments(input, []string{"-Dhttp.type=netty4"}); err == nil {
		t.Fatal("plaintext HTTP transport override accepted")
	}
	const receiptReader = "-Dserverless.writer_handoff.reader=receipt-reader"
	if _, err := runtimeconfig.ManagedArguments(input, []string{receiptReader}); err == nil {
		t.Fatal("receipt reader configured without managed Basic Auth")
	}
	input.BasicAuth = true
	args, err = runtimeconfig.ManagedArguments(input, []string{receiptReader})
	if err != nil || !slices.Contains(args, receiptReader) {
		t.Fatalf("managed HTTPS and Basic Auth did not retain the receipt reader: %v", err)
	}
	const prepareWriter = "-Dserverless.writer_handoff.prepare_user=retirement-operator"
	const persistReceipt = "-Dserverless.writer_handoff.persist_on_shutdown=true"
	if _, err := runtimeconfig.ManagedArguments(input, []string{prepareWriter, persistReceipt}); err == nil {
		t.Fatal("writer retirement preparation enabled on search")
	}
	input.WarmOnJoin = false
	input.RequireWarmupCompletion = false
	input.Role = "ingest"
	if _, err := runtimeconfig.ManagedArguments(input, []string{prepareWriter}); err == nil {
		t.Fatal("writer retirement preparation configured without durable receipts")
	}
	args, err = runtimeconfig.ManagedArguments(input, []string{prepareWriter, persistReceipt})
	if err != nil || !slices.Contains(args, prepareWriter) || !slices.Contains(args, persistReceipt) {
		t.Fatalf("managed ingest did not retain valid retirement settings: %v", err)
	}
	custom := []string{prepareWriter, persistReceipt}
	if !runtimeconfig.MatchesPodRuntime(args, custom) {
		t.Fatal("runtime witness did not recognize the complete managed TLS, auth and S3 argument set")
	}
	withoutStore := slices.DeleteFunc(slices.Clone(args), func(arg string) bool {
		return strings.HasPrefix(arg, "-Dserverless.store.bucket=")
	})
	if runtimeconfig.MatchesPodRuntime(withoutStore, custom) {
		t.Fatal("runtime witness accepted a missing managed property")
	}

}

func TestManagedPodArgumentsMatchLiteralPrimaryStore(t *testing.T) {
	input := runtimeconfig.ManagedInputs{
		NodeName: "node", ClusterName: "cluster", Role: "ingest", Zone: "zone-a", Home: "/work", ConfigPath: "/config",
		NetworkHost: runtimeconfig.PodIPReference, JVMHeap: "2Gi",
		Store: storage.S3{Bucket: "bucket", Region: "region", Endpoint: "https://storage.example/$$", PathStyleAccess: true},
	}
	args, err := runtimeconfig.ManagedPodArguments(input, nil)
	if err != nil || !runtimeconfig.MatchesPodRuntime(args, nil) || !runtimeconfig.MatchesPodPrimaryStore(args, input.Store) {
		t.Fatalf("rendered arguments failed runtime or store qualification: %v", err)
	}
	legacy, err := runtimeconfig.ManagedArguments(input, nil)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeconfig.MatchesPodPrimaryStore(legacy, input.Store) {
		t.Fatal("accepted an endpoint whose literal dollars would be collapsed by kubelet")
	}
	other := input.Store
	other.Endpoint = ""
	if runtimeconfig.MatchesPodPrimaryStore(args, other) {
		t.Fatal("accepted an endpoint when the pinned store had none")
	}
}
