// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package schemaregistry

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"time"

	js "github.com/santhosh-tekuri/jsonschema/v6"
)

// Compatibility compares schema structure, rather than validating a finite set
// of example records. Annotation changes do not restrict a reader's schema.
type jsonRule struct {
	kind     string
	original int
	schema   *js.Schema
	integer  bool
	children []*jsonRule
}
type jsonRuleBuilder struct {
	cache map[*js.Schema]*jsonRule
	raw   map[*js.Schema]any
}

var jsonTrue = &jsonRule{kind: "true"}
var jsonFalse = &jsonRule{kind: "false"}

func (b jsonRuleBuilder) rule(s *js.Schema) *jsonRule {
	if n := b.cache[s]; n != nil {
		return n
	}
	out := &jsonRule{}
	b.cache[s] = out
	if s.Bool != nil {
		if *s.Bool {
			*out = *jsonTrue
		} else {
			*out = *jsonFalse
		}
		return out
	}
	var parts []*jsonRule
	atom := func(kind string, integer bool) *jsonRule { return &jsonRule{kind: kind, schema: s, integer: integer} }
	if s.Ref != nil {
		target := b.rule(s.Ref)
		if s.DraftVersion < 2019 {
			out.kind = "ref"
			out.children = []*jsonRule{target}
			return out
		}
		parts = append(parts, target)
	}
	if s.RecursiveRef != nil {
		parts = append(parts, b.rule(s.RecursiveRef))
	}
	if s.DynamicRef != nil {
		parts = append(parts, b.rule(s.DynamicRef.Ref))
	}
	if s.Types != nil {
		var types []*jsonRule
		for _, kind := range s.Types.ToStrings() {
			integer := kind == "integer"
			if integer {
				kind = "number"
			}
			types = append(types, atom(kind, integer))
		}
		if len(types) == 1 {
			parts = append(parts, types[0])
		} else {
			parts = append(parts, &jsonRule{kind: "any", children: types})
		}
	} else {
		if s.Minimum != nil || s.Maximum != nil || s.ExclusiveMinimum != nil || s.ExclusiveMaximum != nil || s.MultipleOf != nil {
			parts = append(parts, atom("number", false))
		}
		if s.MinLength != nil || s.MaxLength != nil || s.Pattern != nil || s.Format != nil {
			parts = append(parts, atom("string", false))
		}
		if len(s.Properties) > 0 || len(s.PatternProperties) > 0 || len(s.Required) > 0 || s.AdditionalProperties != nil || s.MinProperties != nil || s.MaxProperties != nil || len(s.Dependencies) > 0 || len(s.DependentRequired) > 0 || len(s.DependentSchemas) > 0 || s.PropertyNames != nil || s.UnevaluatedProperties != nil {
			parts = append(parts, atom("object", false))
		}
		if s.Items != nil || s.Items2020 != nil || len(s.PrefixItems) > 0 || s.AdditionalItems != nil || s.MinItems != nil || s.MaxItems != nil || s.UniqueItems || s.Contains != nil || s.MinContains != nil || s.MaxContains != nil || s.UnevaluatedItems != nil {
			parts = append(parts, atom("array", false))
		}
	}
	if s.If != nil {
		parts = append(parts, atom("conditional", false))
	}
	if s.Enum != nil {
		parts = append(parts, atom("enum", false))
	}
	if s.Const != nil {
		parts = append(parts, atom("const", false))
	}
	if s.Not != nil {
		parts = append(parts, &jsonRule{kind: "not", schema: s, children: []*jsonRule{b.rule(s.Not)}})
	}
	for _, group := range []struct {
		kind    string
		schemas []*js.Schema
	}{{"all", s.AllOf}, {"any", s.AnyOf}, {"one", s.OneOf}} {
		if group.schemas != nil {
			n := &jsonRule{kind: group.kind, schema: s, original: len(group.schemas)}
			seen := map[string]bool{}
			for _, child := range group.schemas {
				key, _ := json.Marshal(b.raw[child])
				if !seen[string(key)] {
					n.children = append(n.children, b.rule(child))
					seen[string(key)] = true
				}
			}
			parts = append(parts, n)
		}
	}
	switch len(parts) {
	case 0:
		*out = *jsonTrue
	case 1:
		out.kind = "ref"
		out.schema = s
		out.children = parts
	default:
		out.kind = "all"
		out.schema = s
		out.children = parts
	}
	return out
}

func checkJSONReads(readerText string, readerRefs []resolvedReference, writerText string, writerRefs []resolvedReference) error {
	reader, err := parseJSONSchema(readerText, readerRefs)
	if err != nil {
		return err
	}
	writer, err := parseJSONSchema(writerText, writerRefs)
	if err != nil {
		return err
	}
	b := jsonRuleBuilder{cache: map[*js.Schema]*jsonRule{}, raw: reader.raw}
	for node, raw := range writer.raw {
		b.raw[node] = raw
	}
	c := jsonComparison{builder: b, active: map[jsonRulePair]bool{}, deadline: time.Now().Add(2 * time.Second)}
	result := c.compare(b.rule(writer.root), b.rule(reader.root))
	if err := writer.matchError(); err != nil {
		return err
	}
	if err := reader.matchError(); err != nil {
		return err
	}
	return result
}

type jsonRulePair struct{ writer, reader *jsonRule }
type jsonComparison struct {
	builder  jsonRuleBuilder
	active   map[jsonRulePair]bool
	steps    int
	deadline time.Time
}

func (c *jsonComparison) bad(keyword string) error {
	return fmt.Errorf("JSON Schema reader narrows %s", keyword)
}
func (c *jsonComparison) compare(w, r *jsonRule) error {
	c.steps++
	if c.steps > 100000 || time.Now().After(c.deadline) {
		return c.bad("comparison budget")
	}
	pair := jsonRulePair{w, r}
	if c.active[pair] {
		return nil
	}
	c.active[pair] = true
	defer delete(c.active, pair)
	if w.kind == "ref" {
		return c.compare(w.children[0], r)
	}
	if r.kind == "ref" {
		return c.compare(w, r.children[0])
	}
	if w.kind == "false" || r.kind == "true" {
		return nil
	}
	if w.kind == "true" || r.kind == "false" {
		return c.bad("type")
	}
	wc, rc := jsonCombined(w), jsonCombined(r)
	if wc && rc {
		return c.combined(w, r)
	}
	if !wc && rc {
		if jsonBranchCount(r) == 1 {
			return c.compare(w, r.children[0])
		}
		if r.kind == "any" || r.kind == "one" {
			for _, child := range r.children {
				if c.compare(w, child) == nil {
					return nil
				}
			}
		}
		return c.bad("combined type")
	}
	if wc && !rc {
		if jsonBranchCount(w) == 1 {
			return c.compare(w.children[0], r)
		}
		if w.kind == "all" {
			for _, child := range w.children {
				if c.compare(child, r) == nil {
					return nil
				}
			}
		}
		return c.bad("combined type")
	}
	if w.kind != r.kind {
		return c.bad("type")
	}
	a, b := w.schema, r.schema
	switch w.kind {
	case "number":
		if r.integer && !w.integer {
			return c.bad("integer type")
		}
		for _, bounds := range []struct {
			name     string
			old, new *big.Rat
			lower    bool
		}{{"minimum", a.Minimum, b.Minimum, true}, {"maximum", a.Maximum, b.Maximum, false}, {"exclusiveMinimum", a.ExclusiveMinimum, b.ExclusiveMinimum, true}, {"exclusiveMaximum", a.ExclusiveMaximum, b.ExclusiveMaximum, false}} {
			if bounds.new != nil && (bounds.old == nil || (bounds.lower && bounds.new.Cmp(bounds.old) > 0) || (!bounds.lower && bounds.new.Cmp(bounds.old) < 0)) {
				return c.bad(bounds.name)
			}
		}
		if b.MultipleOf != nil && (a.MultipleOf == nil || !new(big.Rat).Quo(a.MultipleOf, b.MultipleOf).IsInt()) {
			return c.bad("multipleOf")
		}
	case "string":
		if jsonMinNarrowed(a.MinLength, b.MinLength) || jsonMaxNarrowed(a.MaxLength, b.MaxLength) {
			return c.bad("string length")
		}
		if b.Pattern != nil && (a.Pattern == nil || a.Pattern.String() != b.Pattern.String()) {
			return c.bad("pattern")
		}
	case "enum":
		for _, value := range a.Enum.Values {
			found := false
			for _, other := range b.Enum.Values {
				if jsonValueEqual(value, other) {
					found = true
					break
				}
			}
			if !found {
				return c.bad("enum")
			}
		}
	case "const":
		if !jsonValueEqual(*a.Const, *b.Const) {
			return c.bad("const")
		}
	case "conditional":
		// Confluent treats conditional schemas as a distinct kind but does not
		// report changes to their branches as compatibility differences.
		return nil
	case "not":
		return c.compare(r.children[0], w.children[0])
	case "object":
		return c.object(a, b)
	case "array":
		return c.array(a, b)
	}
	return nil
}
func jsonCombined(n *jsonRule) bool      { return n.kind == "all" || n.kind == "any" || n.kind == "one" }
func jsonMinNarrowed(old, new *int) bool { return new != nil && (old == nil || *new > *old) }
func jsonMaxNarrowed(old, new *int) bool { return new != nil && (old == nil || *new < *old) }
func jsonValueEqual(a, b any) bool       { return reflect.DeepEqual(a, b) }

// Distinct branches must have distinct compatible counterparts. A polynomial
// augmenting-path matcher avoids enumerating permutations of large unions.
func (c *jsonComparison) combined(w, r *jsonRule) error {
	if w.kind != r.kind && r.kind != "any" && !(jsonBranchCount(w) == 1 && jsonBranchCount(r) == 1) && !(jsonBranchCount(w) == 1 && r.kind == "one") && !(jsonBranchCount(r) == 1 && w.kind == "all") {
		return c.bad("combined criterion")
	}
	if (r.kind == "all" && len(r.children) > len(w.children)) || (w.kind != "all" && len(w.children) > len(r.children)) {
		return c.bad("combined branches")
	}
	left, right := w.children, r.children
	if len(left) > len(right) {
		left, right = right, left
	}
	edges := make([][]int, len(left))
	for i, a := range left {
		for j, b := range right {
			old, new := a, b
			if len(w.children) > len(r.children) {
				old, new = b, a
			}
			if c.compare(old, new) == nil {
				edges[i] = append(edges[i], j)
			}
		}
	}
	owners := make([]int, len(right))
	for i := range owners {
		owners[i] = -1
	}
	var match func(int, []bool) bool
	match = func(i int, seen []bool) bool {
		for _, j := range edges[i] {
			if seen[j] {
				continue
			}
			seen[j] = true
			if owners[j] < 0 || match(owners[j], seen) {
				owners[j] = i
				return true
			}
		}
		return false
	}
	for i := range left {
		if !match(i, make([]bool, len(right))) {
			return c.bad("combined branch")
		}
	}
	return nil
}

func (c *jsonComparison) extra(value any) *jsonRule {
	switch v := value.(type) {
	case bool:
		if !v {
			return jsonFalse
		}
	case *js.Schema:
		return c.builder.rule(v)
	}
	return jsonTrue
}
func (c *jsonComparison) nullable(w, r *js.Schema) error {
	if r == nil {
		return nil
	}
	if w == nil {
		return c.bad("schema added")
	}
	return c.compare(c.builder.rule(w), c.builder.rule(r))
}
func (c *jsonComparison) openObject(s *js.Schema) bool {
	if c.extra(s.AdditionalProperties) != jsonTrue || len(s.PatternProperties) > 0 {
		return false
	}
	return s.UnevaluatedProperties == nil || c.isTrue(c.builder.rule(s.UnevaluatedProperties))
}
func (c *jsonComparison) isTrue(n *jsonRule) bool {
	seen := map[*jsonRule]bool{}
	for n.kind == "ref" && !seen[n] {
		seen[n] = true
		n = n.children[0]
	}
	return n.kind == "true"
}
func (c *jsonComparison) isFalse(n *jsonRule) bool {
	seen := map[*jsonRule]bool{}
	for n.kind == "ref" && !seen[n] {
		seen[n] = true
		n = n.children[0]
	}
	return n.kind == "false"
}
func (c *jsonComparison) propertyModel(s *js.Schema, name string) (*jsonRule, error) {
	var matching []*jsonRule
	seen := map[string]bool{}
	for pattern, child := range s.PatternProperties {
		if time.Now().After(c.deadline) {
			return nil, c.bad("comparison budget")
		}
		if pattern.MatchString(name) {
			key, _ := json.Marshal(c.builder.raw[child])
			if !seen[string(key)] {
				matching = append(matching, c.builder.rule(child))
				seen[string(key)] = true
			}
		}
		if re, ok := pattern.(*jsonRegexp); ok && re.err != nil {
			return nil, re.err
		}
	}
	if len(matching) == 1 {
		return matching[0], nil
	}
	if len(matching) > 1 {
		return &jsonRule{kind: "all", children: matching}, nil
	}
	if child, ok := s.AdditionalProperties.(*js.Schema); ok {
		return c.builder.rule(child), nil
	}
	if s.AdditionalProperties == nil && s.UnevaluatedProperties != nil && !c.isTrue(c.builder.rule(s.UnevaluatedProperties)) && !c.isFalse(c.builder.rule(s.UnevaluatedProperties)) {
		return c.builder.rule(s.UnevaluatedProperties), nil
	}
	return nil, nil
}
func (c *jsonComparison) object(w, r *js.Schema) error {
	if jsonMinNarrowed(w.MinProperties, r.MinProperties) || jsonMaxNarrowed(w.MaxProperties, r.MaxProperties) {
		return c.bad("property count")
	}
	if err := c.additional(w.AdditionalProperties, r.AdditionalProperties); err != nil {
		return err
	}
	if err := c.nullable(w.UnevaluatedProperties, r.UnevaluatedProperties); err != nil {
		return err
	}
	for name, old := range w.Properties {
		if new, ok := r.Properties[name]; ok {
			if slices.Contains(r.Required, name) && !slices.Contains(w.Required, name) && new.Default == nil {
				return c.bad("required property " + name)
			}
			if err := c.compare(c.builder.rule(old), c.builder.rule(new)); err != nil {
				return err
			}
		} else if !c.openObject(r) {
			model, err := c.propertyModel(r, name)
			if err != nil {
				return err
			}
			if model != nil {
				if err := c.compare(c.builder.rule(old), model); err != nil {
					return err
				}
			} else if !c.isFalse(c.builder.rule(old)) {
				return c.bad("removed property " + name)
			}
		}
	}
	for name, new := range r.Properties {
		if _, ok := w.Properties[name]; ok {
			continue
		}
		if c.openObject(w) {
			if !c.isTrue(c.builder.rule(new)) {
				return c.bad("property added to open model: " + name)
			}
		} else {
			model, err := c.propertyModel(w, name)
			if err != nil {
				return err
			}
			if model != nil {
				if err := c.compare(model, c.builder.rule(new)); err != nil {
					return err
				}
			}
			if slices.Contains(r.Required, name) && new.Default == nil {
				return c.bad("new required property " + name)
			}
		}
	}
	if err := c.dependencies(w, r); err != nil {
		return err
	}
	return nil
}
func (c *jsonComparison) dependencies(w, r *js.Schema) error {
	maps := func(s *js.Schema) map[string]any {
		m := map[string]any{}
		for k, v := range s.Dependencies {
			m[k] = v
		}
		for k, v := range s.DependentRequired {
			m[k] = v
		}
		for k, v := range s.DependentSchemas {
			m[k] = v
		}
		return m
	}
	old, new := maps(w), maps(r)
	for key, value := range new {
		switch v := value.(type) {
		case []string:
			before, ok := old[key].([]string)
			if !ok {
				return c.bad("dependency " + key)
			}
			for _, name := range v {
				if !slices.Contains(before, name) {
					return c.bad("dependency " + key)
				}
			}
		case *js.Schema:
			before, ok := old[key].(*js.Schema)
			if !ok {
				return c.bad("schema dependency " + key)
			}
			if err := c.compare(c.builder.rule(before), c.builder.rule(v)); err != nil {
				return err
			}
		}
	}
	return nil
}
func jsonItems(s *js.Schema) (*js.Schema, []*js.Schema) {
	single, _ := s.Items.(*js.Schema)
	tuple, _ := s.Items.([]*js.Schema)
	if s.Items2020 != nil && len(s.PrefixItems) == 0 && s.Items2020.Bool == nil {
		single = s.Items2020
	}
	if s.PrefixItems != nil {
		tuple = s.PrefixItems
	}
	return single, tuple
}
func (c *jsonComparison) openArray(s *js.Schema) bool {
	return c.extra(jsonAdditionalItems(s)) == jsonTrue && (s.UnevaluatedItems == nil || c.isTrue(c.builder.rule(s.UnevaluatedItems)))
}
func (c *jsonComparison) itemModel(s *js.Schema) *jsonRule {
	if child, ok := jsonAdditionalItems(s).(*js.Schema); ok {
		return c.builder.rule(child)
	}
	if jsonAdditionalItems(s) == nil && s.UnevaluatedItems != nil && !c.isTrue(c.builder.rule(s.UnevaluatedItems)) && !c.isFalse(c.builder.rule(s.UnevaluatedItems)) {
		return c.builder.rule(s.UnevaluatedItems)
	}
	return nil
}
func (c *jsonComparison) array(w, r *js.Schema) error {
	if jsonMinNarrowed(w.MinItems, r.MinItems) || jsonMaxNarrowed(w.MaxItems, r.MaxItems) || jsonMinNarrowed(jsonContainsMinimum(w), jsonContainsMinimum(r)) || jsonMaxNarrowed(w.MaxContains, r.MaxContains) {
		return c.bad("array length")
	}
	if r.UniqueItems && !w.UniqueItems {
		return c.bad("uniqueItems")
	}
	if err := c.additional(jsonAdditionalItems(w), jsonAdditionalItems(r)); err != nil {
		return err
	}
	if err := c.nullable(w.UnevaluatedItems, r.UnevaluatedItems); err != nil {
		return err
	}
	oldSingle, oldTuple := jsonItems(w)
	newSingle, newTuple := jsonItems(r)
	if err := c.nullable(oldSingle, newSingle); err != nil {
		return err
	}
	for i := 0; i < max(len(oldTuple), len(newTuple)); i++ {
		switch {
		case i < len(oldTuple) && i < len(newTuple):
			if err := c.compare(c.builder.rule(oldTuple[i]), c.builder.rule(newTuple[i])); err != nil {
				return err
			}
		case i < len(oldTuple):
			if !c.openArray(r) {
				if model := c.itemModel(r); model != nil {
					if err := c.compare(c.builder.rule(oldTuple[i]), model); err != nil {
						return err
					}
				} else if !c.isFalse(c.builder.rule(oldTuple[i])) {
					return c.bad("removed tuple item")
				}
			}
		default:
			if c.openArray(w) {
				if !c.isTrue(c.builder.rule(newTuple[i])) {
					return c.bad("tuple item added to open model")
				}
			} else if model := c.itemModel(w); model != nil {
				if err := c.compare(model, c.builder.rule(newTuple[i])); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *jsonComparison) additional(w, r any) error {
	if _, ok := r.(*js.Schema); ok {
		if _, ok := w.(*js.Schema); !ok && !c.isFalse(c.extra(w)) {
			return c.bad("additional schema added")
		}
	}
	return c.compare(c.extra(w), c.extra(r))
}
func jsonContainsMinimum(s *js.Schema) *int {
	if s.Contains == nil || s.DraftVersion < 2019 {
		return nil
	}
	if s.MinContains != nil {
		return s.MinContains
	}
	one := 1
	return &one
}

func jsonBranchCount(n *jsonRule) int {
	if n.original > 0 {
		return n.original
	}
	return len(n.children)
}

func jsonAdditionalItems(s *js.Schema) any {
	if s.DraftVersion >= 2020 && s.PrefixItems != nil && s.Items2020 != nil {
		if s.Items2020.Bool != nil {
			return *s.Items2020.Bool
		}
		return s.Items2020
	}
	return s.AdditionalItems
}
