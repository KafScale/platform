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
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

type jsonObject struct {
	keys   []string
	values map[string]any
}

func (o *jsonObject) get(key string) (any, bool) {
	v, ok := o.values[key]
	return v, ok
}

func (o *jsonObject) str(key string) (string, bool) {
	v, ok := o.values[key].(string)
	return v, ok
}

func decodeOrdered(text string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	v, err := decodeValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after the schema")
	}
	return v, nil
}

func decodeValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			obj := &jsonObject{values: map[string]any{}}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, _ := kt.(string)
				val, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				if _, dup := obj.values[key]; !dup {
					obj.keys = append(obj.keys, key)
				}
				obj.values[key] = val
			}
			_, err := dec.Token()
			return obj, err
		case '[':
			arr := []any{}
			for dec.More() {
				val, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				arr = append(arr, val)
			}
			_, err := dec.Token()
			return arr, err
		}
		return nil, fmt.Errorf("unexpected %v", t)
	default:
		return tok, nil
	}
}

var avroPrimitives = map[string]bool{
	"null": true, "boolean": true, "int": true, "long": true,
	"float": true, "double": true, "bytes": true, "string": true,
}

var schemaReserved = map[string]bool{
	"doc": true, "fields": true, "items": true, "name": true, "namespace": true,
	"size": true, "symbols": true, "values": true, "type": true, "aliases": true, "default": true,
}

var fieldReserved = map[string]bool{
	"default": true, "doc": true, "name": true, "order": true, "type": true, "aliases": true,
}

type avroWriter struct {
	buf       bytes.Buffer
	defined   map[string]bool
	space     string
	normalize bool
}

func splitName(name, explicit string, hasExplicit bool, enclosing string) (string, string) {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[:i], name[i+1:]
	}
	if hasExplicit {
		return explicit, name
	}
	return enclosing, name
}

func fullName(space, name string) string {
	if space == "" {
		return name
	}
	return space + "." + name
}

func qualified(full, space string) string {
	i := strings.LastIndexByte(full, '.')
	if i < 0 {
		return full
	}
	if full[:i] == space {
		return full[i+1:]
	}
	return full
}

func (w *avroWriter) resolve(name string) string {
	if strings.Contains(name, ".") {
		return name
	}
	if full := fullName(w.space, name); w.defined[full] {
		return full
	}
	if w.defined[name] {
		return name
	}
	return fullName(w.space, name)
}

func referenceNames(refs []resolvedReference) (map[string]bool, error) {
	out := map[string]bool{}
	for _, ref := range refs {
		v, err := decodeOrdered(ref.Text)
		if err != nil {
			return nil, err
		}
		collectNames(v, "", out)
	}
	return out, nil
}

func collectNames(v any, space string, out map[string]bool) {
	switch t := v.(type) {
	case []any:
		for _, item := range t {
			collectNames(item, space, out)
		}
	case *jsonObject:
		typ, _ := t.get("type")
		kind, ok := typ.(string)
		if !ok {
			collectNames(typ, space, out)
			return
		}
		switch kind {
		case "record", "error", "enum", "fixed":
			name, _ := t.str("name")
			ns, hasNS := t.str("namespace")
			typeSpace, short := splitName(name, ns, hasNS, space)
			out[fullName(typeSpace, short)] = true
			if fields, ok := t.values["fields"].([]any); ok {
				for _, f := range fields {
					if fo, ok := f.(*jsonObject); ok {
						ft, _ := fo.get("type")
						collectNames(ft, typeSpace, out)
					}
				}
			}
		case "array":
			items, _ := t.get("items")
			collectNames(items, space, out)
		case "map":
			values, _ := t.get("values")
			collectNames(values, space, out)
		}
	}
}

func canonicalAvro(text string, refs []resolvedReference, normalize bool) (string, error) {
	v, err := decodeOrdered(text)
	if err != nil {
		return "", err
	}
	defined, err := referenceNames(refs)
	if err != nil {
		return "", err
	}
	w := &avroWriter{defined: defined, normalize: normalize}
	if err := w.schema(v); err != nil {
		return "", err
	}
	return w.buf.String(), nil
}

func (w *avroWriter) schema(v any) error {
	switch t := v.(type) {
	case string:
		if avroPrimitives[t] {
			writeString(&w.buf, t)
			return nil
		}
		writeString(&w.buf, qualified(w.resolve(t), w.space))
		return nil
	case []any:
		w.buf.WriteByte('[')
		for i, item := range t {
			if i > 0 {
				w.buf.WriteByte(',')
			}
			if err := w.schema(item); err != nil {
				return err
			}
		}
		w.buf.WriteByte(']')
		return nil
	case *jsonObject:
		return w.object(t)
	}
	return fmt.Errorf("unsupported schema value %v", v)
}

func (w *avroWriter) object(o *jsonObject) error {
	typ, _ := o.get("type")
	kind, ok := typ.(string)
	if !ok {
		return w.schema(typ)
	}
	switch kind {
	case "record", "error", "enum", "fixed":
		return w.named(o, kind)
	case "array", "map":
		child := "items"
		if kind == "map" {
			child = "values"
		}
		w.buf.WriteString(`{"type":`)
		writeString(&w.buf, kind)
		w.buf.WriteString(`,"` + child + `":`)
		c, _ := o.get(child)
		if err := w.schema(c); err != nil {
			return err
		}
		w.props(o, schemaReserved)
		w.buf.WriteByte('}')
		return nil
	}
	if !avroPrimitives[kind] {
		return w.schema(kind)
	}
	if !w.hasProps(o, schemaReserved) {
		writeString(&w.buf, kind)
		return nil
	}
	w.buf.WriteString(`{"type":`)
	writeString(&w.buf, kind)
	w.props(o, schemaReserved)
	w.buf.WriteByte('}')
	return nil
}

func (w *avroWriter) named(o *jsonObject, kind string) error {
	name, _ := o.str("name")
	ns, hasNS := o.str("namespace")
	if w.normalize && ns == "" {
		hasNS = false
	}
	typeSpace, short := splitName(name, ns, hasNS, w.space)
	full := fullName(typeSpace, short)
	if w.defined[full] {
		writeString(&w.buf, qualified(full, w.space))
		return nil
	}
	w.defined[full] = true
	if w.normalize && kind == "error" {
		kind = "record"
	}
	w.buf.WriteString(`{"type":`)
	writeString(&w.buf, kind)
	w.buf.WriteString(`,"name":`)
	writeString(&w.buf, short)
	if typeSpace != "" {
		if typeSpace != w.space {
			w.buf.WriteString(`,"namespace":`)
			writeString(&w.buf, typeSpace)
		}
	} else if w.space != "" && !w.normalize {
		w.buf.WriteString(`,"namespace":""`)
	}
	if doc, ok := o.str("doc"); ok {
		w.buf.WriteString(`,"doc":`)
		writeString(&w.buf, doc)
	}
	switch kind {
	case "record", "error":
		saved := w.space
		w.space = typeSpace
		w.buf.WriteString(`,"fields":[`)
		fields, _ := o.values["fields"].([]any)
		for i, f := range fields {
			if i > 0 {
				w.buf.WriteByte(',')
			}
			fo, ok := f.(*jsonObject)
			if !ok {
				return errors.New("record field must be an object")
			}
			if err := w.field(fo); err != nil {
				return err
			}
		}
		w.buf.WriteByte(']')
		w.space = saved
	case "enum":
		w.buf.WriteString(`,"symbols":`)
		syms, _ := o.get("symbols")
		w.value(syms, false)
		if def, ok := o.get("default"); ok && def != nil {
			w.buf.WriteString(`,"default":`)
			w.value(def, false)
		}
	case "fixed":
		w.buf.WriteString(`,"size":`)
		size, _ := o.get("size")
		w.value(size, false)
	}
	w.props(o, schemaReserved)
	if aliases, ok := o.values["aliases"].([]any); ok && len(aliases) > 0 {
		out := make([]string, 0, len(aliases))
		for _, a := range aliases {
			s, _ := a.(string)
			as, an := splitName(s, "", false, typeSpace)
			out = append(out, qualified(fullName(as, an), typeSpace))
		}
		w.stringList("aliases", out)
	}
	w.buf.WriteByte('}')
	return nil
}

func (w *avroWriter) field(f *jsonObject) error {
	name, _ := f.str("name")
	w.buf.WriteString(`{"name":`)
	writeString(&w.buf, name)
	w.buf.WriteString(`,"type":`)
	typ, _ := f.get("type")
	if err := w.schema(typ); err != nil {
		return err
	}
	if doc, ok := f.str("doc"); ok {
		w.buf.WriteString(`,"doc":`)
		writeString(&w.buf, doc)
	}
	if def, ok := f.get("default"); ok {
		w.buf.WriteString(`,"default":`)
		w.value(def, w.normalize)
	}
	if order, ok := f.str("order"); ok && strings.ToLower(order) != "ascending" {
		w.buf.WriteString(`,"order":`)
		writeString(&w.buf, strings.ToLower(order))
	}
	if aliases, ok := f.values["aliases"].([]any); ok && len(aliases) > 0 {
		out := make([]string, 0, len(aliases))
		for _, a := range aliases {
			s, _ := a.(string)
			out = append(out, s)
		}
		w.stringList("aliases", out)
	}
	w.props(f, fieldReserved)
	w.buf.WriteByte('}')
	return nil
}

func (w *avroWriter) stringList(key string, list []string) {
	if w.normalize {
		list = slices.Clone(list)
		slices.Sort(list)
	}
	w.buf.WriteString(`,"` + key + `":[`)
	for i, s := range list {
		if i > 0 {
			w.buf.WriteByte(',')
		}
		writeString(&w.buf, s)
	}
	w.buf.WriteByte(']')
}

func (w *avroWriter) hasProps(o *jsonObject, reserved map[string]bool) bool {
	for _, k := range o.keys {
		if !reserved[k] {
			return true
		}
	}
	return false
}

func (w *avroWriter) props(o *jsonObject, reserved map[string]bool) {
	keys := o.keys
	if w.normalize {
		keys = slices.Clone(keys)
		slices.Sort(keys)
	}
	for _, k := range keys {
		if reserved[k] {
			continue
		}
		w.buf.WriteByte(',')
		writeString(&w.buf, k)
		w.buf.WriteByte(':')
		w.value(o.values[k], w.normalize)
	}
}

func (w *avroWriter) value(v any, sorted bool) {
	switch t := v.(type) {
	case nil:
		w.buf.WriteString("null")
	case bool:
		w.buf.WriteString(strconv.FormatBool(t))
	case string:
		writeString(&w.buf, t)
	case json.Number:
		w.buf.WriteString(javaNumber(string(t)))
	case []any:
		w.buf.WriteByte('[')
		for i, item := range t {
			if i > 0 {
				w.buf.WriteByte(',')
			}
			w.value(item, sorted)
		}
		w.buf.WriteByte(']')
	case *jsonObject:
		keys := t.keys
		if sorted {
			keys = slices.Clone(keys)
			slices.Sort(keys)
		}
		w.buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				w.buf.WriteByte(',')
			}
			writeString(&w.buf, k)
			w.buf.WriteByte(':')
			w.value(t.values[k], sorted)
		}
		w.buf.WriteByte('}')
	}
}

func javaNumber(raw string) string {
	if !strings.ContainsAny(raw, ".eE") {
		return raw
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return raw
	}
	return javaDouble(f)
}

func javaDouble(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	case math.IsNaN(f):
		return "NaN"
	case f == 0:
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}
	abs := math.Abs(f)
	if abs >= 1e-3 && abs < 1e7 {
		s := strconv.FormatFloat(f, 'f', -1, 64)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
		return s
	}
	s := strconv.FormatFloat(f, 'e', -1, 64)
	if !strings.Contains(s[:strings.IndexByte(s, 'e')], ".") {
		s = strconv.FormatFloat(f, 'e', 1, 64)
	}
	mant, exp, _ := strings.Cut(s, "e")
	if !strings.Contains(mant, ".") {
		mant += ".0"
	}
	e, _ := strconv.Atoi(exp)
	return mant + "E" + strconv.Itoa(e)
}

func writeString(buf *bytes.Buffer, s string) {
	const hex = "0123456789ABCDEF"
	buf.WriteByte('"')
	for i := 0; i < len(s); {
		c := s[i]
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size == 1 {
				buf.WriteString(`\uFFFD`)
			} else {
				buf.WriteString(s[i : i+size])
			}
			i += size
			continue
		}
		switch c {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		case '\b':
			buf.WriteString(`\b`)
		case '\f':
			buf.WriteString(`\f`)
		default:
			if c < 0x20 {
				buf.WriteString(`\u00`)
				buf.WriteByte(hex[c>>4])
				buf.WriteByte(hex[c&0xF])
			} else {
				buf.WriteByte(c)
			}
		}
		i++
	}
	buf.WriteByte('"')
}
