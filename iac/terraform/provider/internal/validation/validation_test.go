package validation

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

func fixture(t *testing.T) string {
	t.Helper()
	value, err := os.ReadFile("testdata/resolved.json")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(value))
}

func repositoryFixture(t *testing.T, name string) string {
	t.Helper()
	value, err := os.ReadFile("../../../../../docs/contracts/examples/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(value))
}

func TestValidateReproducesStaticContract(t *testing.T) {
	resolved := fixture(t)
	result, err := Validate(resolved, "production")
	if err != nil {
		t.Fatal(err)
	}

	canonical, err := canonicalJSON([]byte(resolved))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	if result.Contract != "graphforge-infra-validation/1" {
		t.Fatalf("contract = %q", result.Contract)
	}
	if result.ResolvedConfigSHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("resolved digest = %q", result.ResolvedConfigSHA256)
	}
	if result.StaticValidity.Status != "valid" {
		t.Fatalf("static validity = %q", result.StaticValidity.Status)
	}
	if result.PlannedInfrastructure.Status != "validated" ||
		result.PlannedInfrastructure.Mutation != "none" ||
		result.PlannedInfrastructure.Ownership != "external" ||
		result.PlannedInfrastructure.Kind != "service" ||
		result.PlannedInfrastructure.Execution != "container" ||
		result.PlannedInfrastructure.Scheduling != "long_running" ||
		result.PlannedInfrastructure.Replicas != 2 {
		t.Fatalf("unexpected plan: %#v", result.PlannedInfrastructure)
	}
	if result.Connectivity.Status != "not_checked" || result.Readiness.Status != "not_checked" {
		t.Fatalf("live states were claimed: %#v %#v", result.Connectivity, result.Readiness)
	}
	if result.CapabilityCompatibility.Status != "requirements_declared" ||
		len(result.CapabilityCompatibility.Requirements) != 2 {
		t.Fatalf("unexpected compatibility: %#v", result.CapabilityCompatibility)
	}
	for _, expected := range []string{
		`"contract":"graphforge-infra-validation/1"`,
		`"mutation":"none"`,
		`"connectivity":{"status":"not_checked"}`,
		`"readiness":{"status":"not_checked"}`,
		`"capability_compatibility":{"requirements":`,
	} {
		if !strings.Contains(result.JSON, expected) {
			t.Fatalf("receipt does not contain %s: %s", expected, result.JSON)
		}
	}
}

func TestValidateIsCanonicalAndIndependentOfInputFormatting(t *testing.T) {
	resolved := fixture(t)
	formatted := "\n  " + strings.ReplaceAll(resolved, `,"`, ",\n  \"") + "\n"
	left, err := Validate(resolved, "local")
	if err != nil {
		t.Fatal(err)
	}
	right, err := Validate(formatted, "local")
	if err != nil {
		t.Fatal(err)
	}
	if left.ResolvedConfigSHA256 != right.ResolvedConfigSHA256 || left.JSON != right.JSON {
		t.Fatal("formatting changed canonical validation evidence")
	}
}

func TestValidateRejectsUnknownAndSemanticallyInvalidTargets(t *testing.T) {
	resolved := fixture(t)
	if _, err := Validate(resolved, "missing"); err == nil {
		t.Fatal("unknown target accepted")
	}
	invalid := strings.Replace(resolved, `"tls_required":true`, `"tls_required":false`, 1)
	invalid = strings.Replace(invalid, `"exposure":"private"`, `"exposure":"public"`, 1)
	if _, err := Validate(invalid, "production"); err == nil {
		t.Fatal("public target without TLS accepted")
	}
	withUnknown := strings.Replace(resolved, `"ownership":"external"`, `"ownership":"external","credential":"forbidden"`, 1)
	if _, err := Validate(withUnknown, "production"); err == nil {
		t.Fatal("unknown target field accepted")
	}
}

func TestValidateRejectsInlineSourceCredentials(t *testing.T) {
	resolved := fixture(t)
	withCredentials := strings.Replace(
		resolved,
		"https://example.invalid/",
		"https://user:password@example.invalid/",
		1,
	)
	if _, err := Validate(withCredentials, "production"); err == nil {
		t.Fatal("source URI with inline credentials accepted")
	}
}

func TestValidateRejectsNumbersAboveJSONSafeInteger(t *testing.T) {
	resolved := fixture(t)
	for name, mutation := range map[string]string{
		"capacity_bytes": strings.Replace(
			resolved,
			`"capacity_bytes":10737418240`,
			`"capacity_bytes":9007199254740992`,
			1,
		),
		"cpu_millis": strings.Replace(
			resolved,
			`"cpu_millis":1000`,
			`"cpu_millis":9007199254740992`,
			1,
		),
		"memory_bytes": strings.Replace(
			resolved,
			`"memory_bytes":2147483648`,
			`"memory_bytes":9007199254740992`,
			1,
		),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Validate(mutation, "production"); err == nil {
				t.Fatal("number above the JSON safe-integer maximum accepted")
			}
		})
	}
}

func TestSecretSentinelNeverEntersReceiptOrDiagnostics(t *testing.T) {
	sentinel := strings.Join([]string{"GRAPHFORGE_SECRET", "SENTINEL_DO_NOT_LEAK"}, "_")
	resolved := fixture(t)
	if strings.Contains(resolved, sentinel) {
		t.Fatal("valid resolved fixture contains a secret value")
	}
	result, err := Validate(resolved, "production")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(result.JSON, sentinel) || strings.Contains(result.SelectedTargetJSON, sentinel) {
		t.Fatal("secret sentinel entered validation evidence")
	}

	withSecretValue := strings.Replace(
		resolved,
		`"source":"secret_manager"`,
		`"source":"secret_manager","value":"`+sentinel+`"`,
		1,
	)
	_, err = Validate(withSecretValue, "production")
	if err == nil {
		t.Fatal("secret value was accepted in resolved JSON")
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatal("secret sentinel entered diagnostics")
	}
}

func TestMatchesSharedGoldenAcrossAllTargetKinds(t *testing.T) {
	resolved := repositoryFixture(t, "graphforge-resolved-v1.json")
	for _, targetID := range []string{
		"external-host",
		"external-job",
		"external-worker",
		"local",
		"local-service",
		"production",
	} {
		if _, err := Validate(resolved, targetID); err != nil {
			t.Fatalf("%s: %v", targetID, err)
		}
	}

	result, err := Validate(resolved, "production")
	if err != nil {
		t.Fatal(err)
	}
	const expectedDigest = "eb9bd7c49ae277c62892b028d2c93e8328c24dd3e3fd5ecbabb4f2c94637e9e7"
	if result.ResolvedConfigSHA256 != expectedDigest {
		t.Fatalf("shared resolved digest = %q", result.ResolvedConfigSHA256)
	}
	golden := repositoryFixture(t, "graphforge-infra-validation-production-v1.json")
	if result.JSON != golden {
		t.Fatalf("Terraform receipt differs from shared Rust golden\nwant: %s\n got: %s", golden, result.JSON)
	}
}
