// Package validation implements graphforge-infra-validation/1 without IO.
package validation

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
)

const (
	resolvedContract   = "graphforge-resolved-config/1"
	validationContract = "graphforge-infra-validation/1"
	maxJSONSafeInteger = int64(9_007_199_254_740_991)
)

var stableIDPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[-_][a-z0-9]+)*$`)

type resolvedConfig struct {
	Contract string            `json:"contract"`
	Project  project           `json:"project"`
	Sources  []source          `json:"sources"`
	Secrets  []secretReference `json:"secrets"`
	Targets  []target          `json:"targets"`
}

type project struct {
	IntegrationRoot string `json:"integration_root"`
	State           string `json:"state"`
	Imports         string `json:"imports"`
	Exports         string `json:"exports"`
	Ontology        string `json:"ontology"`
	Schemas         string `json:"schemas"`
	Seeds           string `json:"seeds"`
	Migrations      string `json:"migrations"`
}

type source struct {
	ID        string `json:"id"`
	URI       string `json:"uri"`
	SHA256    string `json:"sha256"`
	MediaType string `json:"media_type,omitempty"`
}

type secretReference struct {
	ID     string `json:"id"`
	Source string `json:"source"`
}

type target struct {
	ID            string                  `json:"id"`
	Kind          string                  `json:"kind"`
	Ownership     string                  `json:"ownership"`
	Artifact      artifact                `json:"artifact"`
	Topology      topology                `json:"topology"`
	Capabilities  []capabilityRequirement `json:"capabilities"`
	Write         writeConfig             `json:"write"`
	Storage       storage                 `json:"storage"`
	Resources     resources               `json:"resources"`
	Network       network                 `json:"network"`
	Health        health                  `json:"health"`
	Observability observability           `json:"observability"`
	Backup        backup                  `json:"backup"`
	SourceIDs     []string                `json:"source_ids"`
	SecretIDs     []string                `json:"secret_ids"`
}

type artifact struct {
	Kind    string `json:"kind"`
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
}

type topology struct {
	Execution  string `json:"execution"`
	Scheduling string `json:"scheduling"`
	Replicas   int64  `json:"replicas"`
}

type capabilityRequirement struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
}

type writeConfig struct {
	Mode              string `json:"mode"`
	QueueCapacity     *int64 `json:"queue_capacity,omitempty"`
	MaxRebaseAttempts *int64 `json:"max_rebase_attempts,omitempty"`
}

type storage struct {
	Kind          string `json:"kind"`
	Persistent    *bool  `json:"persistent,omitempty"`
	Class         string `json:"class,omitempty"`
	CapacityBytes *int64 `json:"capacity_bytes,omitempty"`
}

type resources struct {
	CPUMillis   *int64 `json:"cpu_millis,omitempty"`
	MemoryBytes *int64 `json:"memory_bytes,omitempty"`
}

type network struct {
	Exposure    string `json:"exposure,omitempty"`
	Port        *int64 `json:"port,omitempty"`
	TLSRequired *bool  `json:"tls_required,omitempty"`
}

type health struct {
	TimeoutSeconds int64 `json:"timeout_seconds"`
}

type observability struct {
	Logs    bool `json:"logs"`
	Metrics bool `json:"metrics"`
	Traces  bool `json:"traces"`
}

type backup struct {
	Checkpoints    bool   `json:"checkpoints"`
	RetentionCount *int64 `json:"retention_count,omitempty"`
}

// Result is the provider-neutral validation receipt plus its canonical JSON.
type Result struct {
	Contract                string
	ResolvedConfigSHA256    string
	SelectedTargetJSON      string
	StaticValidity          status
	PlannedInfrastructure   plan
	Connectivity            status
	Readiness               status
	CapabilityCompatibility capabilityCompatibility
	JSON                    string
}

type status struct {
	Status string `json:"status"`
}

type plan struct {
	Status     string   `json:"status"`
	Mutation   string   `json:"mutation"`
	Ownership  string   `json:"ownership"`
	Kind       string   `json:"kind"`
	Execution  string   `json:"execution"`
	Scheduling string   `json:"scheduling"`
	Replicas   int64    `json:"replicas"`
	Artifact   artifact `json:"artifact"`
}

type capabilityCompatibility struct {
	Status       string                  `json:"status"`
	Requirements []capabilityRequirement `json:"requirements"`
}

type receipt struct {
	Contract                string                  `json:"contract"`
	ResolvedConfigSHA256    string                  `json:"resolved_config_sha256"`
	Target                  json.RawMessage         `json:"target"`
	StaticValidity          status                  `json:"static_validity"`
	PlannedInfrastructure   plan                    `json:"planned_infrastructure"`
	Connectivity            status                  `json:"connectivity"`
	Readiness               status                  `json:"readiness"`
	CapabilityCompatibility capabilityCompatibility `json:"capability_compatibility"`
}

// Validate selects and statically validates a resolved target. It performs no
// filesystem, process, network, provider, credential, or GraphForge-state IO.
func Validate(resolvedJSON, targetID string) (Result, error) {
	if !validStableID(targetID) {
		return Result{}, errors.New("target must be a stable identifier")
	}

	var config resolvedConfig
	if err := decodeStrict([]byte(resolvedJSON), &config); err != nil {
		return Result{}, errors.New("resolved_json is not a closed graphforge-resolved-config/1 document")
	}
	if err := validateRequiredFields([]byte(resolvedJSON)); err != nil {
		return Result{}, errors.New("resolved_json is missing required graphforge-resolved-config/1 fields")
	}
	if config.Contract != resolvedContract {
		return Result{}, errors.New("resolved_json uses an unsupported contract")
	}
	if err := validateResolvedConfig(config); err != nil {
		return Result{}, err
	}

	var selected *target
	for i := range config.Targets {
		if config.Targets[i].ID == targetID {
			selected = &config.Targets[i]
			break
		}
	}
	if selected == nil {
		return Result{}, errors.New("target is not declared by resolved_json")
	}
	if err := validateTarget(*selected); err != nil {
		return Result{}, err
	}

	canonicalResolved, err := canonicalJSON([]byte(resolvedJSON))
	if err != nil {
		return Result{}, errors.New("resolved_json cannot be canonicalized")
	}
	digest := sha256.Sum256(canonicalResolved)
	selectedJSON, err := canonicalMarshal(selected)
	if err != nil {
		return Result{}, errors.New("selected target cannot be encoded")
	}

	staticValidity := status{Status: "valid"}
	planned := plan{
		Status:     "validated",
		Mutation:   "none",
		Ownership:  selected.Ownership,
		Kind:       selected.Kind,
		Execution:  selected.Topology.Execution,
		Scheduling: selected.Topology.Scheduling,
		Replicas:   selected.Topology.Replicas,
		Artifact:   selected.Artifact,
	}
	notChecked := status{Status: "not_checked"}
	compatibility := capabilityCompatibility{
		Status:       "requirements_declared",
		Requirements: selected.Capabilities,
	}
	validationReceipt := receipt{
		Contract:                validationContract,
		ResolvedConfigSHA256:    hex.EncodeToString(digest[:]),
		Target:                  selectedJSON,
		StaticValidity:          staticValidity,
		PlannedInfrastructure:   planned,
		Connectivity:            notChecked,
		Readiness:               notChecked,
		CapabilityCompatibility: compatibility,
	}
	receiptJSON, err := canonicalMarshal(validationReceipt)
	if err != nil {
		return Result{}, errors.New("validation receipt cannot be encoded")
	}

	return Result{
		Contract:                validationContract,
		ResolvedConfigSHA256:    validationReceipt.ResolvedConfigSHA256,
		SelectedTargetJSON:      string(selectedJSON),
		StaticValidity:          staticValidity,
		PlannedInfrastructure:   planned,
		Connectivity:            notChecked,
		Readiness:               notChecked,
		CapabilityCompatibility: compatibility,
		JSON:                    string(receiptJSON),
	}, nil
}

func validateRequiredFields(document []byte) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(document, &root); err != nil {
		return err
	}
	if err := requireKeys(root, "contract", "project", "sources", "secrets", "targets"); err != nil {
		return err
	}

	var projectValue map[string]json.RawMessage
	if err := json.Unmarshal(root["project"], &projectValue); err != nil {
		return err
	}
	if err := requireKeys(
		projectValue,
		"integration_root",
		"state",
		"imports",
		"exports",
		"ontology",
		"schemas",
		"seeds",
		"migrations",
	); err != nil {
		return err
	}

	if err := requireArrayObjectKeys(root["sources"], "id", "uri", "sha256"); err != nil {
		return err
	}
	if err := requireArrayObjectKeys(root["secrets"], "id", "source"); err != nil {
		return err
	}

	var targets []map[string]json.RawMessage
	if err := json.Unmarshal(root["targets"], &targets); err != nil {
		return err
	}
	for _, targetValue := range targets {
		if err := requireKeys(
			targetValue,
			"id",
			"kind",
			"ownership",
			"artifact",
			"topology",
			"capabilities",
			"write",
			"storage",
			"resources",
			"network",
			"health",
			"observability",
			"backup",
			"source_ids",
			"secret_ids",
		); err != nil {
			return err
		}
		for field, keys := range map[string][]string{
			"artifact": {"kind", "version", "sha256"},
			"topology": {"execution", "scheduling", "replicas"},
			"write":    {"mode"},
			"storage":  {"kind"},
			"health":   {"timeout_seconds"},
		} {
			var object map[string]json.RawMessage
			if err := json.Unmarshal(targetValue[field], &object); err != nil {
				return err
			}
			if err := requireKeys(object, keys...); err != nil {
				return err
			}
		}
		if err := requireArrayObjectKeys(targetValue["capabilities"], "id", "version"); err != nil {
			return err
		}
	}
	return nil
}

func requireKeys(object map[string]json.RawMessage, keys ...string) error {
	for _, key := range keys {
		value, present := object[key]
		if !present || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return errors.New("required field is absent")
		}
	}
	return nil
}

func requireArrayObjectKeys(value json.RawMessage, keys ...string) error {
	var objects []map[string]json.RawMessage
	if err := json.Unmarshal(value, &objects); err != nil {
		return err
	}
	for _, object := range objects {
		if err := requireKeys(object, keys...); err != nil {
			return err
		}
	}
	return nil
}

func decodeStrict(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON content")
	}
	return nil
}

func canonicalJSON(data []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing JSON content")
	}
	return encodeCanonical(value)
}

func canonicalMarshal(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return canonicalJSON(encoded)
}

func encodeCanonical(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'}), nil
}

func validateResolvedConfig(config resolvedConfig) error {
	if config.Project.IntegrationRoot != ".graphforge" ||
		config.Project.State != ".graphforge/state" ||
		config.Project.Imports != ".graphforge/imports" ||
		config.Project.Exports != ".graphforge/exports" {
		return errors.New("resolved_json has invalid repository paths")
	}
	if !validRelativePath(config.Project.Ontology) ||
		!validRelativePath(config.Project.Schemas) ||
		!validRelativePath(config.Project.Seeds) ||
		!validRelativePath(config.Project.Migrations) {
		return errors.New("resolved_json has invalid definition paths")
	}
	if len(config.Sources) > 256 || len(config.Secrets) > 128 ||
		len(config.Targets) == 0 || len(config.Targets) > 64 {
		return errors.New("resolved_json exceeds contract bounds")
	}
	sourceIDs := make(map[string]struct{}, len(config.Sources))
	for _, item := range config.Sources {
		if !validStableID(item.ID) || !validSHA256(item.SHA256) || item.URI == "" ||
			len(item.URI) > 2048 || len(item.MediaType) > 128 || uriHasInlineCredentials(item.URI) {
			return errors.New("resolved_json has an invalid source reference")
		}
		if _, duplicate := sourceIDs[item.ID]; duplicate {
			return errors.New("resolved_json has duplicate source identifiers")
		}
		sourceIDs[item.ID] = struct{}{}
	}
	secretIDs := make(map[string]struct{}, len(config.Secrets))
	for _, item := range config.Secrets {
		if !validStableID(item.ID) || !oneOf(item.Source, "environment", "pulumi", "terraform", "secret_manager") {
			return errors.New("resolved_json has an invalid secret reference")
		}
		if _, duplicate := secretIDs[item.ID]; duplicate {
			return errors.New("resolved_json has duplicate secret identifiers")
		}
		secretIDs[item.ID] = struct{}{}
	}
	targetIDs := make(map[string]struct{}, len(config.Targets))
	for _, item := range config.Targets {
		if !validStableID(item.ID) {
			return errors.New("resolved_json has an invalid target identifier")
		}
		if _, duplicate := targetIDs[item.ID]; duplicate {
			return errors.New("resolved_json has duplicate target identifiers")
		}
		targetIDs[item.ID] = struct{}{}
		if err := validateTarget(item); err != nil {
			return err
		}
		for _, id := range item.SourceIDs {
			if _, exists := sourceIDs[id]; !exists {
				return errors.New("resolved target references an unknown source")
			}
		}
		for _, id := range item.SecretIDs {
			if _, exists := secretIDs[id]; !exists {
				return errors.New("resolved target references an unknown secret")
			}
		}
	}
	return nil
}

func validateTarget(value target) error {
	if !oneOf(value.Kind, "embedded", "service", "worker", "job", "host") ||
		!oneOf(value.Ownership, "embedded", "local", "external") ||
		!oneOf(value.Artifact.Kind, "python_wheel", "node_package", "native_binary", "oci_image") ||
		value.Artifact.Version == "" || len(value.Artifact.Version) > 128 ||
		!validSHA256(value.Artifact.SHA256) {
		return errors.New("resolved target has invalid identity or artifact fields")
	}
	if !oneOf(value.Topology.Execution, "process", "container", "host") ||
		!oneOf(value.Topology.Scheduling, "long_running", "on_demand") ||
		value.Topology.Replicas < 1 || value.Topology.Replicas > 1024 {
		return errors.New("resolved target has invalid topology")
	}
	if (value.Kind == "embedded") != (value.Ownership == "embedded") {
		return errors.New("resolved target has incompatible kind and ownership")
	}
	if value.Kind == "embedded" &&
		(value.Topology.Execution != "process" || value.Topology.Scheduling != "long_running" ||
			value.Topology.Replicas != 1 || value.Storage.Kind != "local" ||
			(value.Network.Exposure != "" && value.Network.Exposure != "none")) {
		return errors.New("resolved embedded target has incompatible topology")
	}
	if (value.Kind == "host") != (value.Topology.Execution == "host") {
		return errors.New("resolved host target has incompatible execution")
	}
	if (value.Kind == "job") != (value.Topology.Scheduling == "on_demand") {
		return errors.New("resolved job target has incompatible scheduling")
	}
	if value.Kind == "service" && value.Network.Port == nil {
		return errors.New("resolved service target requires a network port")
	}
	if value.Network.Exposure == "public" && (value.Network.TLSRequired == nil || !*value.Network.TLSRequired) {
		return errors.New("resolved public target requires TLS")
	}
	if !oneOf(value.Write.Mode, "single_writer", "queued_writer", "optimistic_multi_writer") ||
		!oneOf(value.Storage.Kind, "local", "volume", "object") {
		return errors.New("resolved target has invalid write or storage configuration")
	}
	if value.Write.Mode == "single_writer" &&
		(value.Write.QueueCapacity != nil || value.Write.MaxRebaseAttempts != nil) {
		return errors.New("resolved single-writer target has incompatible settings")
	}
	if value.Write.Mode == "queued_writer" && value.Write.QueueCapacity == nil {
		return errors.New("resolved queued-writer target requires queue capacity")
	}
	if value.Write.Mode == "optimistic_multi_writer" && value.Write.MaxRebaseAttempts == nil {
		return errors.New("resolved optimistic-writer target requires rebase attempts")
	}
	if value.Backup.RetentionCount != nil && !value.Backup.Checkpoints {
		return errors.New("resolved target backup retention requires checkpoints")
	}
	if value.Network.Exposure != "" && !oneOf(value.Network.Exposure, "none", "private", "public") {
		return errors.New("resolved target has invalid network exposure")
	}
	if value.Network.Port != nil && (*value.Network.Port < 1 || *value.Network.Port > 65535) {
		return errors.New("resolved target has invalid network port")
	}
	if value.Health.TimeoutSeconds < 1 || value.Health.TimeoutSeconds > 300 {
		return errors.New("resolved target has invalid health timeout")
	}
	if value.Write.QueueCapacity != nil &&
		(*value.Write.QueueCapacity < 1 || *value.Write.QueueCapacity > 65536) {
		return errors.New("resolved target has invalid queue capacity")
	}
	if value.Write.MaxRebaseAttempts != nil &&
		(*value.Write.MaxRebaseAttempts < 0 || *value.Write.MaxRebaseAttempts > 64) {
		return errors.New("resolved target has invalid rebase attempts")
	}
	if value.Storage.CapacityBytes != nil &&
		(*value.Storage.CapacityBytes < 1 || *value.Storage.CapacityBytes > maxJSONSafeInteger) {
		return errors.New("resolved target has invalid storage capacity")
	}
	if len(value.Storage.Class) > 128 {
		return errors.New("resolved target has invalid storage class")
	}
	if value.Resources.CPUMillis != nil &&
		(*value.Resources.CPUMillis < 1 || *value.Resources.CPUMillis > maxJSONSafeInteger) ||
		value.Resources.MemoryBytes != nil &&
			(*value.Resources.MemoryBytes < 1 || *value.Resources.MemoryBytes > maxJSONSafeInteger) {
		return errors.New("resolved target has invalid resource requirements")
	}
	if value.Backup.RetentionCount != nil &&
		(*value.Backup.RetentionCount < 1 || *value.Backup.RetentionCount > 1024) {
		return errors.New("resolved target has invalid backup retention")
	}
	if len(value.Capabilities) > 64 || len(value.SourceIDs) > 256 || len(value.SecretIDs) > 128 {
		return errors.New("resolved target exceeds contract bounds")
	}
	capabilities := make(map[string]struct{}, len(value.Capabilities))
	for _, requirement := range value.Capabilities {
		if !validStableID(requirement.ID) || requirement.Version < 1 || requirement.Version > 65535 {
			return errors.New("resolved target has an invalid capability requirement")
		}
		if _, duplicate := capabilities[requirement.ID]; duplicate {
			return errors.New("resolved target has duplicate capability requirements")
		}
		capabilities[requirement.ID] = struct{}{}
	}
	if hasDuplicate(value.SourceIDs) || hasDuplicate(value.SecretIDs) {
		return errors.New("resolved target has duplicate references")
	}
	return nil
}

func validStableID(value string) bool {
	return len(value) <= 64 && stableIDPattern.MatchString(value)
}

func validRelativePath(value string) bool {
	if value == "" || len(value) > 1024 || strings.HasPrefix(value, "/") ||
		strings.Contains(value, `\`) {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == ".." {
			return false
		}
	}
	for _, character := range []byte(value) {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func uriHasInlineCredentials(value string) bool {
	separator := strings.Index(value, "://")
	if separator < 0 {
		return false
	}
	authority := value[separator+3:]
	if slash := strings.IndexByte(authority, '/'); slash >= 0 {
		authority = authority[:slash]
	}
	return strings.ContainsRune(authority, '@')
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}

func oneOf(value string, options ...string) bool {
	for _, option := range options {
		if value == option {
			return true
		}
	}
	return false
}

func hasDuplicate(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}
