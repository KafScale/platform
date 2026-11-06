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
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dlclark/regexp2"
	js "github.com/santhosh-tekuri/jsonschema/v6"
)

const schemaTypeJSON = "JSON"
const jsonSchemaURL = "https://kafscale.invalid/schema.json"

// JSON schema resources are supplied explicitly by registry references. In
// particular, a schema must never cause the registry to read a URL or file.
type jsonSchemaLoader struct{}

func (jsonSchemaLoader) Load(uri string) (any, error) {
	return nil, fmt.Errorf("unresolved JSON Schema reference %q", uri)
}

type jsonRegexp struct {
	*regexp2.Regexp
	err error
}

func (r *jsonRegexp) MatchString(s string) bool {
	ok, err := r.Regexp.MatchString(s)
	if err != nil {
		r.err = err
	}
	return err == nil && ok
}
func compileJSONRegexp(pattern string) (js.Regexp, error) {
	re, err := regexp2.Compile(pattern, regexp2.ECMAScript)
	if err != nil {
		return nil, err
	}
	re.MatchTimeout = 25 * time.Millisecond
	return &jsonRegexp{Regexp: re}, nil
}

type jsonSchema struct {
	root *js.Schema
	raw  map[*js.Schema]any
}

func parseJSONSchema(text string, refs []resolvedReference) (*jsonSchema, error) {
	root, err := js.UnmarshalJSON(strings.NewReader(text))
	if err != nil {
		return nil, err
	}
	docs := map[string]any{jsonSchemaURL: root}
	c := js.NewCompiler()
	c.DefaultDraft(js.Draft7)
	c.UseLoader(jsonSchemaLoader{})
	c.UseRegexpEngine(compileJSONRegexp)
	base, _ := url.Parse(jsonSchemaURL)
	if obj, ok := root.(map[string]any); ok {
		if id, ok := obj["$id"].(string); ok {
			u, err := url.Parse(id)
			if err != nil {
				return nil, err
			}
			base = base.ResolveReference(u)

		}
	}
	for _, ref := range refs {
		if ref.Type != "" && ref.Type != schemaTypeJSON {
			return nil, fmt.Errorf("reference %q has schema type %s, want JSON", ref.Name, ref.Type)
		}
		doc, err := js.UnmarshalJSON(strings.NewReader(ref.Text))
		if err != nil {
			return nil, err
		}
		uri, err := url.Parse(ref.Name)
		if err != nil {
			return nil, err
		}
		location := base.ResolveReference(uri).String()
		docs[location] = doc
		// A draft-7 reference object ignores its sibling $id. Register the
		// relative name at the retrieval URL as well as the declared base.
		if obj, ok := root.(map[string]any); ok {
			if _, ok := obj["$ref"]; ok {
				retrieval, _ := url.Parse(jsonSchemaURL)
				alias := retrieval.ResolveReference(uri).String()
				if alias != location {
					docs[alias] = doc
					if err := c.AddResource(alias, jsonCompilerDocument(doc)); err != nil {
						return nil, err
					}
				}
			}
		}
		if err := c.AddResource(location, jsonCompilerDocument(doc)); err != nil {
			return nil, err
		}
	}
	if err := c.AddResource(jsonSchemaURL, jsonCompilerDocument(root)); err != nil {
		return nil, err
	}
	compiled, err := c.Compile(jsonSchemaURL)
	if err != nil {
		return nil, err
	}
	// Validation compilers discard additionalItems without tuple items. The
	// registry nevertheless compares explicitly supplied keywords. Retain those
	// and original nodes for identity of combined branches.
	parsed := &jsonSchema{root: compiled, raw: map[*js.Schema]any{}}
	var visit func(*js.Schema) error
	visit = func(node *js.Schema) error {
		if node == nil {
			return nil
		}
		if _, ok := parsed.raw[node]; ok {
			return nil
		}
		// Locations and array indices below are emitted by the compiler.
		location, _ := url.Parse(node.Location)
		pointer := location.Fragment
		location.Fragment = ""
		raw := docs[location.String()]
		if pointer != "" {
			for _, part := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
				part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
				switch value := raw.(type) {
				case map[string]any:
					raw = value[part]
				case []any:
					i, _ := strconv.Atoi(part)
					raw = value[i]
				}
			}
		}
		parsed.raw[node] = raw
		if obj, ok := raw.(map[string]any); ok {
			if prefix, ok := obj["prefixItems"].([]any); ok && len(prefix) == 0 && node.DraftVersion >= 2020 {
				node.PrefixItems = []*js.Schema{}
			}
			if value, ok := obj["additionalItems"]; ok && node.AdditionalItems == nil && (node.Ref == nil || node.DraftVersion >= 2019) {
				if b, ok := value.(bool); ok {
					node.AdditionalItems = b
				} else {
					if _, ok := value.(map[string]any); !ok {
						return fmt.Errorf("additionalItems must be an object or boolean")
					}
					child, err := c.Compile(node.Location + "/additionalItems")
					if err != nil {
						return err
					}
					node.AdditionalItems = child
				}
			}
		}
		for _, child := range jsonSchemaChildren(node) {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(compiled); err != nil {
		return nil, err
	}
	return parsed, nil
}

func jsonSchemaChildren(s *js.Schema) []*js.Schema {
	children := []*js.Schema{s.Ref, s.RecursiveRef, s.Not, s.If, s.Then, s.Else, s.PropertyNames, s.UnevaluatedProperties, s.Contains, s.UnevaluatedItems, s.Items2020}
	if s.DynamicRef != nil {
		children = append(children, s.DynamicRef.Ref)
	}
	for _, group := range [][]*js.Schema{s.AllOf, s.AnyOf, s.OneOf, s.PrefixItems} {
		children = append(children, group...)
	}
	for _, group := range []map[string]*js.Schema{s.Properties, s.DependentSchemas} {
		for _, child := range group {
			children = append(children, child)
		}
	}
	for _, child := range s.PatternProperties {
		children = append(children, child)
	}
	for _, value := range s.Dependencies {
		if child, ok := value.(*js.Schema); ok {
			children = append(children, child)
		}
	}
	for _, value := range []any{s.Items, s.AdditionalItems, s.AdditionalProperties} {
		switch v := value.(type) {
		case *js.Schema:
			children = append(children, v)
		case []*js.Schema:
			children = append(children, v...)
		}
	}
	return children
}

func canonicalJSONSchema(text string, normalize bool) (string, error) {
	value, err := decodeOrdered(text)
	if err != nil {
		return "", err
	}
	switch value.(type) {
	case *jsonObject, bool:
	default:
		return "", fmt.Errorf("JSON Schema must be an object or boolean")
	}
	var buf bytes.Buffer
	if err := writeJSONSchemaValue(&buf, value, normalize); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func writeJSONSchemaValue(buf *bytes.Buffer, value any, sorted bool) error {
	switch v := value.(type) {
	case *jsonObject:
		keys := v.keys
		if sorted {
			keys = slices.Clone(keys)
			slices.Sort(keys)
		}
		buf.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeString(buf, key)
			buf.WriteByte(':')
			if err := writeJSONSchemaValue(buf, v.values[key], sorted); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeJSONSchemaValue(buf, item, sorted); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case string:
		writeString(buf, v)
	case json.Number:
		number, err := jsonSchemaNumber(string(v))
		if err != nil {
			return err
		}
		buf.WriteString(number)
	case bool:
		buf.WriteString(strconv.FormatBool(v))
	case nil:
		buf.WriteString("null")
	}
	return nil
}

// Jackson's JSON-schema representation uses arbitrary precision decimals,
// including their scale. Avro's floating point canonicalizer is different.
func jsonSchemaNumber(raw string) (string, error) {
	if !strings.ContainsAny(raw, ".eE") {
		if raw == "-0" {
			return "0", nil
		}
		return raw, nil
	}
	mantissa, exponent, hasExp := strings.Cut(strings.ToLower(raw), "e")
	exp := 0
	if hasExp {
		var err error
		exp, err = strconv.Atoi(exponent)
		if err != nil || exp > 10000 || exp < -10000 {
			return "", fmt.Errorf("JSON Schema numeric exponent exceeds the supported bound")
		}
	}
	negative := strings.HasPrefix(mantissa, "-")
	mantissa = strings.TrimPrefix(mantissa, "-")
	integer, fraction, _ := strings.Cut(mantissa, ".")
	digits := strings.TrimLeft(integer+fraction, "0")
	if digits == "" {
		digits = "0"
		negative = false
	}
	scale := len(fraction) - exp
	adjusted := len(digits) - scale - 1
	var out string
	if scale >= 0 && adjusted >= -6 {
		if scale == 0 {
			out = digits
		} else if scale >= len(digits) {
			out = "0." + strings.Repeat("0", scale-len(digits)) + digits
		} else {
			at := len(digits) - scale
			out = digits[:at] + "." + digits[at:]
		}
	} else {
		out = digits[:1]
		if len(digits) > 1 {
			out += "." + digits[1:]
		}
		out += "E"
		if adjusted >= 0 {
			out += "+"
		}
		out += strconv.Itoa(adjusted)
	}
	if negative {
		out = "-" + out
	}
	return out, nil
}

// Confluent permits an empty prefixItems list; the JSON Schema metaschema
// requires a nonempty list. It is equivalent to no prefix items for validation.
func jsonCompilerDocument(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, child := range v {
			switch key {
			case "properties", "patternProperties", "definitions", "$defs", "dependentSchemas", "dependencies":
				if group, ok := child.(map[string]any); ok {
					copy := make(map[string]any, len(group))
					for name, schema := range group {
						copy[name] = jsonCompilerDocument(schema)
					}
					out[key] = copy
				} else {
					out[key] = child
				}
			case "prefixItems":
				if items, ok := child.([]any); ok && len(items) == 0 {
					continue
				}
				out[key] = jsonCompilerDocument(child)
			case "items", "additionalItems", "additionalProperties", "unevaluatedItems", "unevaluatedProperties", "contains", "propertyNames", "not", "if", "then", "else", "allOf", "anyOf", "oneOf":
				out[key] = jsonCompilerDocument(child)
			default:
				out[key] = child
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, child := range v {
			out[i] = jsonCompilerDocument(child)
		}
		return out
	default:
		return value
	}
}

func (s *jsonSchema) matchError() error {
	for node := range s.raw {
		for pattern := range node.PatternProperties {
			if re, ok := pattern.(*jsonRegexp); ok && re.err != nil {
				return fmt.Errorf("JSON Schema pattern comparison: %w", re.err)
			}
		}
	}
	return nil
}
