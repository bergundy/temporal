package nexusauthz

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Wildcard is the literal that matches any subject or operation in a policy
// rule.
const Wildcard = "*"

// Policy decides whether a propagated identity may invoke a particular
// (endpoint, service, operation) triple.
type Policy interface {
	Allow(identity PropagatedIdentity, endpoint, service, operation string) bool
}

// allowEntry describes a single (endpoint, service, operation) triple that a
// rule grants access to. An operation of "*" matches any operation.
type allowEntry struct {
	Endpoint  string `yaml:"endpoint"`
	Service   string `yaml:"service"`
	Operation string `yaml:"operation"`
}

// policyRule is the YAML schema for one rule. A subject of "*" matches any
// caller subject. An empty Permissions list means "no permission is required";
// otherwise every listed permission must be present in the caller's
// permissions.
type policyRule struct {
	Subject     string       `yaml:"subject"`
	Permissions []string     `yaml:"permissions"`
	Allow       []allowEntry `yaml:"allow"`
}

// policyFile is the YAML schema for the policy file itself.
type policyFile struct {
	Rules []policyRule `yaml:"rules"`
}

// MapPolicy is the default Policy implementation backed by an in-memory list
// of rules loaded from a YAML file.
type MapPolicy struct {
	rules []policyRule
}

var _ Policy = (*MapPolicy)(nil)

// LoadPolicy reads and parses a policy YAML file from disk.
func LoadPolicy(path string) (Policy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read policy file %q: %w", path, err)
	}
	var file policyFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parse policy file %q: %w", path, err)
	}
	return &MapPolicy{rules: file.Rules}, nil
}

// MustLoadPolicy is the panic-on-error variant of LoadPolicy. It is intended
// to be called from `main` during startup.
func MustLoadPolicy(path string) Policy {
	p, err := LoadPolicy(path)
	if err != nil {
		panic(err)
	}
	return p
}

// NewMapPolicy constructs an in-memory Policy from a list of rules. Primarily
// useful in tests; production callers should use LoadPolicy.
func NewMapPolicy(rules []policyRule) *MapPolicy {
	return &MapPolicy{rules: rules}
}

// Allow reports whether the identity is permitted to invoke the given
// (endpoint, service, operation) triple. A rule matches if:
//   - rule.Subject == identity.Subject, or rule.Subject == "*";
//   - every permission listed in rule.Permissions is present in
//     identity.Permissions (an empty list means no permission is required);
//   - at least one of rule.Allow matches the requested (endpoint, service,
//     operation) — operation "*" matches anything.
func (p *MapPolicy) Allow(identity PropagatedIdentity, endpoint, service, operation string) bool {
	for _, rule := range p.rules {
		if !subjectMatches(rule.Subject, identity.Subject) {
			continue
		}
		if !hasAllPermissions(identity.Permissions, rule.Permissions) {
			continue
		}
		for _, entry := range rule.Allow {
			if entry.Endpoint != endpoint {
				continue
			}
			if entry.Service != service {
				continue
			}
			if entry.Operation != Wildcard && entry.Operation != operation {
				continue
			}
			return true
		}
	}
	return false
}

func subjectMatches(ruleSubject, callerSubject string) bool {
	return ruleSubject == Wildcard || ruleSubject == callerSubject
}

func hasAllPermissions(have, required []string) bool {
	if len(required) == 0 {
		return true
	}
	set := make(map[string]struct{}, len(have))
	for _, p := range have {
		set[p] = struct{}{}
	}
	for _, r := range required {
		if _, ok := set[r]; !ok {
			return false
		}
	}
	return true
}
