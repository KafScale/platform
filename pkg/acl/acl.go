// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package acl

import "strings"

type Action string

type Resource string

const (
	ActionAny        Action = "*"
	ActionProduce    Action = "produce"
	ActionFetch      Action = "fetch"
	ActionGroupRead  Action = "group_read"
	ActionGroupWrite Action = "group_write"
	ActionGroupAdmin Action = "group_admin"
	ActionAdmin      Action = "admin"
)

const (
	ResourceAny     Resource = "*"
	ResourceTopic   Resource = "topic"
	ResourceGroup   Resource = "group"
	ResourceCluster Resource = "cluster"
)

type Rule struct {
	Action   Action   `json:"action"`
	Resource Resource `json:"resource"`
	Name     string   `json:"name"`
}

type PrincipalRules struct {
	Name  string `json:"name"`
	Allow []Rule `json:"allow"`
	Deny  []Rule `json:"deny"`
}

type Config struct {
	Enabled       bool             `json:"enabled"`
	DefaultPolicy string           `json:"default_policy"`
	Principals    []PrincipalRules `json:"principals"`
}

type Authorizer struct {
	enabled      bool
	defaultAllow bool
	principals   map[string]PrincipalRules
}

func NewAuthorizer(cfg Config) *Authorizer {
	defaultAllow := strings.EqualFold(strings.TrimSpace(cfg.DefaultPolicy), "allow")
	principals := make(map[string]PrincipalRules, len(cfg.Principals))
	for _, p := range cfg.Principals {
		name := strings.TrimSpace(p.Name)
		if name == "" {
			continue
		}
		principals[name] = p
	}
	return &Authorizer{
		enabled:      cfg.Enabled,
		defaultAllow: defaultAllow,
		principals:   principals,
	}
}

func (a *Authorizer) Enabled() bool {
	if a == nil {
		return false
	}
	return a.enabled
}

func (a *Authorizer) Allows(principal string, action Action, resource Resource, name string) bool {
	if a == nil || !a.enabled {
		return true
	}
	principal = strings.TrimSpace(principal)
	if principal == "" {
		principal = "anonymous"
	}
	rules, ok := a.principals[principal]
	if !ok {
		return a.defaultAllow
	}
	for _, rule := range rules.Deny {
		if matches(rule, action, resource, name) {
			return false
		}
	}
	for _, rule := range rules.Allow {
		if matches(rule, action, resource, name) {
			return true
		}
	}
	return a.defaultAllow
}

func matches(rule Rule, action Action, resource Resource, name string) bool {
	if !actionMatches(rule.Action, action) {
		return false
	}
	if !resourceMatches(rule.Resource, resource) {
		return false
	}
	return nameMatches(rule.Name, name)
}

func actionMatches(rule Action, action Action) bool {
	if rule == "" || rule == ActionAny {
		return true
	}
	return strings.EqualFold(string(rule), string(action))
}

func resourceMatches(rule Resource, resource Resource) bool {
	if rule == "" || rule == ResourceAny {
		return true
	}
	return strings.EqualFold(string(rule), string(resource))
}

func nameMatches(ruleName, name string) bool {
	ruleName = strings.TrimSpace(ruleName)
	if ruleName == "" || ruleName == "*" {
		return true
	}
	if strings.HasSuffix(ruleName, "*") {
		prefix := strings.TrimSuffix(ruleName, "*")
		return strings.HasPrefix(name, prefix)
	}
	return ruleName == name
}
