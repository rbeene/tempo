// Package hookstate owns retained capture eligibility, separately from activity
// identity and host delivery evidence. It never grants host trust.
package hookstate

import (
	"context"
	"time"
)

const DeclarationVersion = "tempo-native-hooks-v1"

// Artifact records a reviewed file, or its required absence (SHA256 == "absent").
// Roles are runtime, executable, definitions, skill, or configuration. Content is
// hashed locally but never returned, persisted, or interpreted as instructions.
type Artifact struct {
	Role   string `json:"role"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

const installedInventoryVersion = "tempo-installed-static-v1"
const maxProfileArtifacts = 128

type Context struct {
	InventoryVersion string     `json:"inventory_version,omitempty"`
	Host             string     `json:"host"`
	Scope            string     `json:"scope"`
	Path             string     `json:"path"`
	RuntimeVersion   string     `json:"runtime_version"`
	Surface          string     `json:"surface"`
	Artifacts        []Artifact `json:"artifacts"`
	Conflicts        []string   `json:"conflicts"`
}

type Profile struct {
	Basis              string  `json:"basis"`
	State              string  `json:"state"`
	Revision           string  `json:"revision"`
	Fingerprint        string  `json:"fingerprint"`
	DeclarationVersion string  `json:"declaration_version"`
	CaptureEligible    bool    `json:"capture_eligible"`
	Context            Context `json:"context"`
	DiagnosticCode     string  `json:"diagnostic_code"`
}

type ConfirmInput struct {
	Context
	Fingerprint, DeclarationVersion, RequestID string
	Confirmed                                  bool
}

type RevokeInput struct {
	Host, Scope, Path, IfRevision, RequestID string
	Confirmed                                bool
}

type Options struct {
	CodexSystemDir   string
	ClaudeManagedDir string
	HomeDir          string
	Executable       string
	BuildVersion     string
	DiscoverRuntime  func(context.Context, string) (Runtime, error)
	Path             string
	LockTimeout      time.Duration
}

type Service struct {
	options Options
	fail    func(string) error
}

type Error struct {
	RequestID string `json:"request_id,omitempty"`
	Code      string
	Retryable bool
	Uncertain bool
}

func (e *Error) Error() string { return "hook capture policy operation failed" }
func New(o Options) *Service   { return &Service{options: o} }

// Preview is read-only and computes the current confirmation fingerprint.
func (s *Service) Preview(ctx context.Context, input Context) (Profile, error) {
	current, err := sampleContext(ctx, input)
	if err != nil {
		return Profile{}, err
	}
	disk, _, err := s.read(ctx)
	if err != nil {
		return Profile{}, err
	}
	revision := "0"
	if old, ok := disk.Profiles[profileKey(current.Host, current.Scope, current.Path)]; ok {
		revision = old.Revision
	}
	return Profile{Basis: "none", State: "absent", Revision: revision, Context: current, Fingerprint: profileFingerprint(current, revision), DeclarationVersion: DeclarationVersion}, nil
}

// Confirm retains operator_declared eligibility, never host_observed evidence.
func (s *Service) Confirm(ctx context.Context, in ConfirmInput) (Profile, error) {
	p, err := s.confirm(ctx, in)
	return p, requestError(err, in.RequestID)
}

func (s *Service) Revoke(ctx context.Context, in RevokeInput) (Profile, error) {
	p, err := s.revoke(ctx, in)
	return p, requestError(err, in.RequestID)
}

// Eligibility selects the scoped policy and invalidates known artifact drift.
// It does not create an absent store or establish source-session continuity.
func (s *Service) Eligibility(ctx context.Context, host, cwd string) (Profile, error) {
	diagnostic := beginEligibilityDiagnostic(ctx)
	defer diagnostic.finish()
	if ctx.Err() != nil {
		return Profile{}, &Error{Code: "state_busy", Retryable: true}
	}
	readStart := diagnostic.now()
	disk, exists, err := s.read(ctx)
	diagnostic.phase(0, readStart)
	if err != nil {
		return Profile{}, err
	}
	if !exists {
		return Profile{Basis: "none", State: "absent", Revision: "0", DiagnosticCode: "profile_required"}, nil
	}
	return s.eligibility(ctx, disk, host, cwd)
}
