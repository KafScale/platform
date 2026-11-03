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
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/ast"
	"github.com/bufbuild/protocompile/linker"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoprint"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	schemaTypeProtobuf = "PROTOBUF"
	protoMainFile      = "kafscale-schema-registry-main.proto"
)

type protoSchema struct {
	file linker.Result
	refs map[string]resolvedReference
}

func parseProto(text string, refs []resolvedReference) (*protoSchema, error) {
	sources := map[string]string{protoMainFile: text}
	byName := make(map[string]resolvedReference, len(refs))
	for _, ref := range refs {
		sources[ref.Name] = ref.Text
		byName[ref.Name] = ref
	}
	compiler := protocompile.Compiler{
		RetainASTs: true,
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: func(path string) (io.ReadCloser, error) {
				src, ok := sources[path]
				if !ok {
					return nil, fmt.Errorf("import %q is not a reference of this schema", path)
				}
				return io.NopCloser(strings.NewReader(src)), nil
			},
		}),
	}
	files, err := compiler.Compile(context.Background(), protoMainFile)
	if err != nil {
		return nil, err
	}
	res, ok := files[0].(linker.Result)
	if !ok || res.AST() == nil {
		return nil, fmt.Errorf("protobuf schema has no syntax tree")
	}
	return &protoSchema{file: res, refs: byName}, nil
}

var protoTight = map[string]bool{".": true, "<": true, "(": true, "[": true}

var protoTightBefore = map[string]bool{".": true, ">": true, ")": true, "]": true, ",": true, ";": true}

func (s *protoSchema) text(n ast.Node) string {
	file := s.file.AST()
	tokens := file.Tokens()
	var b strings.Builder
	prev := ""
	last := n.End()
	for tok, ok := n.Start(), true; ok; tok, ok = tokens.Next(tok) {
		raw := file.TokenInfo(tok).RawText()
		if strings.HasPrefix(raw, "'") && strings.HasSuffix(raw, "'") && len(raw) > 1 {
			raw = strconv.Quote(file.TokenInfo(tok).RawText()[1 : len(raw)-1])
		}
		if prev != "" && !protoTight[prev] && !protoTightBefore[raw] {
			b.WriteByte(' ')
		}
		b.WriteString(raw)
		prev = raw
		if tok == last {
			break
		}
	}
	return b.String()
}

type protoPrinter struct {
	s         *protoSchema
	normalize bool
	b         strings.Builder
}

func (s *protoSchema) canonical(normalize bool) string {
	p := &protoPrinter{s: s, normalize: normalize}
	p.file(s.file.FileDescriptorProto())
	return p.b.String()
}

func (p *protoPrinter) line(parts ...string) {
	p.b.WriteString(strings.Join(parts, " "))
	p.b.WriteByte('\n')
}

func (p *protoPrinter) sorted(list []string) []string {
	if p.normalize {
		list = slices.Clone(list)
		slices.Sort(list)
		list = slices.Compact(list)
	}
	return list
}

func (p *protoPrinter) decls(n ast.Node) (options, ranges, extends []string) {
	var add func(ast.Node)
	add = func(d ast.Node) {
		switch d := d.(type) {
		case *ast.OptionNode:
			options = append(options, p.s.text(d))
		case *ast.ReservedNode, *ast.ExtensionRangeNode:
			ranges = append(ranges, p.s.text(d))
		case *ast.ExtendNode, *ast.ServiceNode:
			extends = append(extends, p.s.text(d))
		}
	}
	switch n := n.(type) {
	case *ast.FileNode:
		for _, d := range n.Decls {
			add(d)
		}
	case *ast.MessageNode:
		for _, d := range n.Decls {
			add(d)
		}
	case *ast.EnumNode:
		for _, d := range n.Decls {
			add(d)
		}
	}
	return options, ranges, extends
}

func (p *protoPrinter) file(fd *descriptorpb.FileDescriptorProto) {
	if syntax := p.s.file.AST().Syntax; syntax != nil && !(p.normalize && syntax.Syntax.AsString() == "proto2") {
		p.line("syntax", syntax.Syntax.AsString())
	}
	if fd.Package != nil {
		p.line("package", fd.GetPackage())
	}
	public := map[int32]bool{}
	for _, i := range fd.PublicDependency {
		public[i] = true
	}
	var imports, publicImports []string
	for i, dep := range fd.Dependency {
		if public[int32(i)] {
			publicImports = append(publicImports, dep)
		} else {
			imports = append(imports, dep)
		}
	}
	for _, dep := range p.sorted(imports) {
		p.line("import", dep)
	}
	for _, dep := range p.sorted(publicImports) {
		p.line("import public", dep)
	}
	options, _, extends := p.decls(p.s.file.AST())
	for _, o := range p.sorted(options) {
		p.line(o)
	}
	for _, m := range fd.MessageType {
		p.message(m)
	}
	for _, e := range fd.EnumType {
		p.enum(e)
	}
	for _, e := range extends {
		p.line(e)
	}
}

func (p *protoPrinter) message(m *descriptorpb.DescriptorProto) {
	if m.GetOptions().GetMapEntry() {
		return
	}
	p.line("message", m.GetName(), "{")
	options, ranges, extends := p.decls(p.s.file.MessageNode(m))
	for _, r := range p.sorted(ranges) {
		p.line(r)
	}
	for _, o := range p.sorted(options) {
		p.line(o)
	}
	var plain []*descriptorpb.FieldDescriptorProto
	oneofs := make([][]*descriptorpb.FieldDescriptorProto, len(m.OneofDecl))
	for _, f := range m.Field {
		if f.OneofIndex != nil && !f.GetProto3Optional() {
			oneofs[f.GetOneofIndex()] = append(oneofs[f.GetOneofIndex()], f)
		} else {
			plain = append(plain, f)
		}
	}
	byTag := func(a, b *descriptorpb.FieldDescriptorProto) int { return int(a.GetNumber()) - int(b.GetNumber()) }
	if p.normalize {
		plain = slices.Clone(plain)
		slices.SortStableFunc(plain, byTag)
	}
	for _, f := range plain {
		p.field(m, f)
	}
	order := make([]int, 0, len(oneofs))
	for i, fields := range oneofs {
		if len(fields) == 0 && (p.normalize || m.OneofDecl[i] == nil) {
			continue
		}
		if p.normalize {
			slices.SortStableFunc(fields, byTag)
		}
		order = append(order, i)
	}
	if p.normalize {
		slices.SortStableFunc(order, func(a, b int) int { return byTag(oneofs[a][0], oneofs[b][0]) })
	}
	for _, i := range order {
		p.line("oneof", m.OneofDecl[i].GetName(), "{")
		for _, f := range oneofs[i] {
			p.field(m, f)
		}
		p.line("}")
	}
	for _, e := range extends {
		p.line(e)
	}
	for _, n := range m.NestedType {
		p.message(n)
	}
	for _, e := range m.EnumType {
		p.enum(e)
	}
	p.line("}")
}

func (p *protoPrinter) mapEntry(m *descriptorpb.DescriptorProto, typeName string) *descriptorpb.DescriptorProto {
	for _, n := range m.NestedType {
		if n.GetOptions().GetMapEntry() && strings.HasSuffix(typeName, "."+n.GetName()) {
			return n
		}
	}
	return nil
}

func scalarName(f *descriptorpb.FieldDescriptorProto) string {
	return strings.ToLower(strings.TrimPrefix(f.GetType().String(), "TYPE_"))
}

func (p *protoPrinter) fieldType(f *descriptorpb.FieldDescriptorProto) string {
	if f.GetTypeName() != "" {
		return f.GetTypeName()
	}
	return scalarName(f)
}

func (p *protoPrinter) field(m *descriptorpb.DescriptorProto, f *descriptorpb.FieldDescriptorProto) {
	node := p.s.file.FieldNode(f)
	var parts []string
	label, typ := "", ""
	if node != nil {
		if l := node.FieldLabel(); l != nil && !reflectNil(l) {
			label = p.s.text(l)
		}
		if mf, ok := node.(*ast.MapFieldNode); ok {
			typ = "map<" + p.s.text(mf.MapType.KeyType) + ", " + p.s.text(mf.MapType.ValueType) + ">"
		} else if t := node.FieldType(); t != nil && !reflectNil(t) {
			typ = p.s.text(t)
		}
	}
	if p.normalize {
		if entry := p.mapEntry(m, f.GetTypeName()); entry != nil && len(entry.Field) == 2 {
			label = ""
			typ = "map<" + p.fieldType(entry.Field[0]) + ", " + p.fieldType(entry.Field[1]) + ">"
		} else if f.GetTypeName() != "" {
			typ = f.GetTypeName()
		}
	}
	if label != "" {
		parts = append(parts, label)
	}
	parts = append(parts, typ, f.GetName(), "=", strconv.Itoa(int(f.GetNumber())))
	if node != nil {
		if opts := node.GetOptions(); opts != nil {
			var list []string
			for _, o := range opts.Options {
				list = append(list, p.s.text(o))
			}
			list = p.sorted(list)
			parts = append(parts, "["+strings.Join(list, ", ")+"]")
		}
	}
	p.line(parts...)
}

func reflectNil(n ast.Node) bool {
	switch n := n.(type) {
	case *ast.KeywordNode:
		return n == nil
	case *ast.IdentNode:
		return n == nil
	case *ast.CompoundIdentNode:
		return n == nil
	}
	return false
}

func (p *protoPrinter) enum(e *descriptorpb.EnumDescriptorProto) {
	p.line("enum", e.GetName(), "{")
	options, ranges, _ := p.decls(p.s.file.EnumNode(e))
	for _, r := range p.sorted(ranges) {
		p.line(r)
	}
	for _, o := range p.sorted(options) {
		p.line(o)
	}
	values := e.Value
	if p.normalize {
		values = slices.Clone(values)
		slices.SortStableFunc(values, func(a, b *descriptorpb.EnumValueDescriptorProto) int {
			if a.GetNumber() != b.GetNumber() {
				return int(a.GetNumber()) - int(b.GetNumber())
			}
			return strings.Compare(a.GetName(), b.GetName())
		})
	}
	for _, v := range values {
		parts := []string{v.GetName(), "=", strconv.Itoa(int(v.GetNumber()))}
		if node, ok := p.s.file.EnumValueNode(v).(*ast.EnumValueNode); ok {
			if opts := node.Options; opts != nil {
				parts = append(parts, p.s.text(opts))
			}
		}
		p.line(parts...)
	}
	p.line("}")
}

func protoFromDescriptor(text string, refs []resolvedReference) (string, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" || strings.ContainsAny(trimmed, " \t\r\n;{") {
		return text, nil
	}
	data, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		return text, nil
	}
	fd := &descriptorpb.FileDescriptorProto{}
	if err := proto.Unmarshal(data, fd); err != nil {
		return "", fmt.Errorf("schema is neither protobuf text nor a base64 FileDescriptorProto: %w", err)
	}
	fd.Name = proto.String(protoMainFile)
	sources := make(map[string]string, len(refs))
	for _, ref := range refs {
		sources[ref.Name] = ref.Text
	}
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: func(path string) (io.ReadCloser, error) {
				src, ok := sources[path]
				if !ok {
					return nil, fmt.Errorf("import %q is not a reference of this schema", path)
				}
				return io.NopCloser(strings.NewReader(src)), nil
			},
		}),
	}
	files := &protoregistry.Files{}
	if len(fd.GetDependency()) > 0 {
		deps, err := compiler.Compile(context.Background(), fd.GetDependency()...)
		if err != nil {
			return "", err
		}
		for _, dep := range deps {
			if err := registerProtoFile(files, dep); err != nil {
				return "", err
			}
		}
	}
	file, err := protodesc.NewFile(fd, files)
	if err != nil {
		return "", err
	}
	wrapped, err := desc.WrapFile(file)
	if err != nil {
		return "", err
	}
	out, err := (&protoprint.Printer{OmitComments: protoprint.CommentsAll}).PrintProtoToString(wrapped)
	if err != nil {
		return "", err
	}
	return out, nil
}

func registerProtoFile(files *protoregistry.Files, fd protoreflect.FileDescriptor) error {
	if _, err := files.FindFileByPath(fd.Path()); err == nil {
		return nil
	}
	imports := fd.Imports()
	for i := 0; i < imports.Len(); i++ {
		if err := registerProtoFile(files, imports.Get(i).FileDescriptor); err != nil {
			return err
		}
	}
	return files.RegisterFile(fd)
}

func prepareProto(text string, refs []resolvedReference, normalize bool) (string, string, error) {
	s, err := parseProto(text, refs)
	if err != nil {
		return "", "", err
	}
	normalized := s.canonical(true)
	if normalize {
		return normalized, normalized, nil
	}
	return s.canonical(false), normalized, nil
}

type protoDiff struct {
	kind string
	path string
}

var protoCompatible = map[string]bool{
	"MESSAGE_ADDED": true, "MESSAGE_MOVED": true, "ENUM_ADDED": true, "ENUM_REMOVED": true,
	"ENUM_CONST_ADDED": true, "ENUM_CONST_CHANGED": true, "ENUM_CONST_REMOVED": true,
	"FIELD_ADDED": true, "FIELD_REMOVED": true, "FIELD_NAME_CHANGED": true,
	"FIELD_STRING_OR_BYTES_LABEL_CHANGED": true, "ONEOF_ADDED": true, "ONEOF_REMOVED": true,
	"ONEOF_FIELD_ADDED": true, "ONEOF_FIELD_MOVED_TO_TOP_LEVEL": true,
}

type protoCtx struct {
	orig, upd *protoSchema
	path      []string
	active    map[protoreflect.MessageDescriptor]bool
	diffs     []protoDiff
}

func (c *protoCtx) add(kind string, extra ...string) {
	p := append(slices.Clone(c.path), extra...)
	c.diffs = append(c.diffs, protoDiff{kind: kind, path: "#/" + strings.Join(p, "/")})
}

func (c *protoCtx) enter(name string) func() {
	c.path = append(c.path, name)
	return func() { c.path = c.path[:len(c.path)-1] }
}

func (c *protoCtx) incompatible() []protoDiff {
	var out []protoDiff
	for _, d := range c.diffs {
		if !protoCompatible[d.kind] {
			out = append(out, d)
		}
	}
	return out
}

func checkProtoReads(readerText string, readerRefs []resolvedReference, writerText string, writerRefs []resolvedReference) error {
	writer, err := parseProto(writerText, writerRefs)
	if err != nil {
		return err
	}
	reader, err := parseProto(readerText, readerRefs)
	if err != nil {
		return err
	}
	c := &protoCtx{orig: writer, upd: reader, active: map[protoreflect.MessageDescriptor]bool{}}
	if writer.file.Package() != reader.file.Package() {
		c.add("PACKAGE_CHANGED")
	}
	c.types(writer.file.Messages(), writer.file.Enums(), reader.file.Messages(), reader.file.Enums())
	bad := c.incompatible()
	if len(bad) == 0 {
		return nil
	}
	parts := make([]string, 0, len(bad))
	for _, d := range bad {
		parts = append(parts, fmt.Sprintf("{errorType:%q, description:\"%s at path '%s'\"}", d.kind, strings.ToLower(strings.ReplaceAll(d.kind, "_", " ")), d.path))
	}
	return fmt.Errorf("%s", strings.Join(parts, ", "))
}

func messageIndex(ms protoreflect.MessageDescriptors) (map[string]protoreflect.MessageDescriptor, map[string]int) {
	byName := map[string]protoreflect.MessageDescriptor{}
	index := map[string]int{}
	n := 0
	for i := 0; i < ms.Len(); i++ {
		m := ms.Get(i)
		if m.IsMapEntry() {
			continue
		}
		byName[string(m.Name())] = m
		index[string(m.Name())] = n
		n++
	}
	return byName, index
}

func enumIndex(es protoreflect.EnumDescriptors) map[string]protoreflect.EnumDescriptor {
	out := map[string]protoreflect.EnumDescriptor{}
	for i := 0; i < es.Len(); i++ {
		out[string(es.Get(i).Name())] = es.Get(i)
	}
	return out
}

func unionKeys[V any](a, b map[string]V) []string {
	var out []string
	for k := range a {
		out = append(out, k)
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

func (c *protoCtx) types(om protoreflect.MessageDescriptors, oe protoreflect.EnumDescriptors, um protoreflect.MessageDescriptors, ue protoreflect.EnumDescriptors) {
	origMsgs, origIdx := messageIndex(om)
	updMsgs, updIdx := messageIndex(um)
	for _, name := range unionKeys(origMsgs, updMsgs) {
		leave := c.enter(name)
		o, u := origMsgs[name], updMsgs[name]
		switch {
		case u == nil:
			c.add("MESSAGE_REMOVED")
		case o == nil:
			c.add("MESSAGE_ADDED")
		default:
			c.message(o, u)
			if origIdx[name] != updIdx[name] {
				c.add("MESSAGE_MOVED")
			}
		}
		leave()
	}
	origEnums, updEnums := enumIndex(oe), enumIndex(ue)
	for _, name := range unionKeys(origEnums, updEnums) {
		leave := c.enter(name)
		o, u := origEnums[name], updEnums[name]
		switch {
		case u == nil:
			c.add("ENUM_REMOVED")
		case o == nil:
			c.add("ENUM_ADDED")
		default:
			c.enum(o, u)
		}
		leave()
	}
}

func (c *protoCtx) enum(o, u protoreflect.EnumDescriptor) {
	byNum := func(e protoreflect.EnumDescriptor) map[string]string {
		out := map[string]string{}
		for i := 0; i < e.Values().Len(); i++ {
			v := e.Values().Get(i)
			out[strconv.Itoa(int(v.Number()))] = string(v.Name())
		}
		return out
	}
	ov, uv := byNum(o), byNum(u)
	for _, tag := range unionKeys(ov, uv) {
		on, inO := ov[tag]
		un, inU := uv[tag]
		switch {
		case !inU:
			c.add("ENUM_CONST_REMOVED", tag)
		case !inO:
			c.add("ENUM_CONST_ADDED", tag)
		case on != un:
			c.add("ENUM_CONST_CHANGED", tag)
		}
	}
}

func realOneof(f protoreflect.FieldDescriptor) string {
	if o := f.ContainingOneof(); o != nil && !o.IsSynthetic() {
		return string(o.Name())
	}
	return ""
}

func oneofs(m protoreflect.MessageDescriptor) map[string]protoreflect.OneofDescriptor {
	out := map[string]protoreflect.OneofDescriptor{}
	for i := 0; i < m.Oneofs().Len(); i++ {
		if o := m.Oneofs().Get(i); !o.IsSynthetic() {
			out[string(o.Name())] = o
		}
	}
	return out
}

func (c *protoCtx) message(o, u protoreflect.MessageDescriptor) {
	if !c.active[o] {
		c.active[o] = true
		c.messageFields(o, u)
		delete(c.active, o)
	}
	c.types(o.Messages(), o.Enums(), u.Messages(), u.Enums())
}

func (c *protoCtx) messageFields(o, u protoreflect.MessageDescriptor) {
	of, uf := o.Fields(), u.Fields()
	oo, uo := oneofs(o), oneofs(u)
	deleted := map[string]bool{}
	for name, one := range oo {
		if _, ok := uo[name]; ok {
			continue
		}
		survives := false
		for i := 0; i < one.Fields().Len(); i++ {
			if uf.ByNumber(one.Fields().Get(i).Number()) != nil {
				survives = true
			}
		}
		if !survives {
			deleted[name] = true
		}
	}
	for i := 0; i < u.Oneofs().Len(); i++ {
		one := u.Oneofs().Get(i)
		if one.IsSynthetic() {
			continue
		}
		groups := map[string]bool{}
		moved := 0
		for j := 0; j < one.Fields().Len(); j++ {
			f := of.ByNumber(one.Fields().Get(j).Number())
			switch {
			case f == nil:
			case realOneof(f) != "":
				groups["oneof:"+realOneof(f)] = true
			default:
				groups["field:"+strconv.Itoa(int(f.Number()))] = true
				moved++
			}
		}
		if len(groups) > 1 {
			if moved > 1 {
				c.add("MULTIPLE_FIELDS_MOVED_TO_ONEOF", string(one.Name()))
			} else {
				c.add("FIELD_MOVED_TO_EXISTING_ONEOF", string(one.Name()))
			}
		}
	}
	for _, name := range unionKeys(oo, uo) {
		if _, ok := uo[name]; !ok {
			c.add("ONEOF_REMOVED", name)
		} else if _, ok := oo[name]; !ok {
			c.add("ONEOF_ADDED", name)
		}
	}
	var tags []int
	for i := 0; i < of.Len(); i++ {
		tags = append(tags, int(of.Get(i).Number()))
	}
	for i := 0; i < uf.Len(); i++ {
		if of.ByNumber(uf.Get(i).Number()) == nil {
			tags = append(tags, int(uf.Get(i).Number()))
		}
	}
	slices.Sort(tags)
	for _, tag := range tags {
		t := strconv.Itoa(tag)
		ofd, ufd := of.ByNumber(protoreflect.FieldNumber(tag)), uf.ByNumber(protoreflect.FieldNumber(tag))
		var oOne, uOne string
		if ofd != nil {
			oOne = realOneof(ofd)
		}
		if ufd != nil {
			uOne = realOneof(ufd)
		}
		switch {
		case ufd == nil:
			if oOne != "" {
				if !deleted[oOne] {
					c.add("ONEOF_FIELD_REMOVED", oOne, t)
				}
			} else if ofd.Cardinality() == protoreflect.Required {
				c.add("REQUIRED_FIELD_REMOVED", t)
			} else {
				c.add("FIELD_REMOVED", t)
			}
		case ofd == nil:
			if uOne != "" {
				c.add("ONEOF_FIELD_ADDED", uOne, t)
			} else if ufd.Cardinality() == protoreflect.Required {
				c.add("REQUIRED_FIELD_ADDED", t)
			} else {
				c.add("FIELD_ADDED", t)
			}
		default:
			if oOne != "" && uOne == "" {
				c.add("ONEOF_FIELD_MOVED_TO_TOP_LEVEL", t)
			}
			var leave []func()
			if uOne != "" {
				leave = append(leave, c.enter(uOne))
			}
			leave = append(leave, c.enter(t))
			c.field(ofd, ufd)
			for i := len(leave) - 1; i >= 0; i-- {
				leave[i]()
			}
		}
	}
}

type protoLabel int

const (
	labelNone protoLabel = iota
	labelOptional
	labelRequired
	labelRepeated
)

func labelOf(f protoreflect.FieldDescriptor) protoLabel {
	if f.IsMap() || realOneof(f) != "" {
		return labelNone
	}
	switch f.Cardinality() {
	case protoreflect.Repeated:
		return labelRepeated
	case protoreflect.Required:
		if f.ParentFile().Syntax() == protoreflect.Proto2 {
			return labelRequired
		}
		return labelNone
	}
	if f.ParentFile().Syntax() == protoreflect.Proto2 || f.HasOptionalKeyword() {
		return labelOptional
	}
	return labelNone
}

func (c *protoCtx) field(o, u protoreflect.FieldDescriptor) {
	if o.Name() != u.Name() {
		c.add("FIELD_NAME_CHANGED")
	}
	c.labelsAndTypes(labelOf(o), labelOf(u), o, u)
}

type protoKind int

const (
	kindScalar protoKind = iota
	kindMap
	kindMessage
)

func kindOf(f protoreflect.FieldDescriptor) protoKind {
	switch {
	case f.IsMap():
		return kindMap
	case f.Kind() == protoreflect.MessageKind || f.Kind() == protoreflect.GroupKind:
		return kindMessage
	}
	return kindScalar
}

func scalarGroup(f protoreflect.FieldDescriptor) string {
	switch f.Kind() {
	case protoreflect.EnumKind, protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.BoolKind:
		return "GENERAL_NUMBER"
	case protoreflect.Sint32Kind, protoreflect.Sint64Kind:
		return "SIGNED_NUMBER"
	case protoreflect.StringKind, protoreflect.BytesKind:
		return "STRING_OR_BYTES"
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		return "FIXED32"
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		return "FIXED64"
	case protoreflect.FloatKind:
		return "FLOAT"
	}
	return "DOUBLE"
}

func (c *protoCtx) labelsAndTypes(ol, ul protoLabel, o, u protoreflect.FieldDescriptor) {
	ok, uk := kindOf(o), kindOf(u)
	if ok != uk {
		c.add("FIELD_KIND_CHANGED")
		return
	}
	switch ok {
	case kindScalar:
		og, ug := scalarGroup(o), scalarGroup(u)
		if og != ug {
			c.add("FIELD_SCALAR_KIND_CHANGED")
		} else if ol != labelNone && ul != labelNone && ol != ul {
			if og == "STRING_OR_BYTES" {
				c.add("FIELD_STRING_OR_BYTES_LABEL_CHANGED")
			} else {
				c.add("FIELD_NUMERIC_LABEL_CHANGED")
			}
		}
	case kindMessage:
		c.messageType(o.Message(), u.Message())
	case kindMap:
		c.labelsAndTypes(labelNone, labelNone, o.MapKey(), u.MapKey())
		c.labelsAndTypes(labelNone, labelNone, o.MapValue(), u.MapValue())
	}
}

func localName(m protoreflect.MessageDescriptor) string {
	full := string(m.FullName())
	if pkg := string(m.ParentFile().Package()); pkg != "" {
		return strings.TrimPrefix(full, pkg+".")
	}
	return full
}

func (s *protoSchema) refOf(m protoreflect.MessageDescriptor) (resolvedReference, bool) {
	path := m.ParentFile().Path()
	if path == protoMainFile {
		return resolvedReference{Version: -1}, true
	}
	ref, ok := s.refs[path]
	return ref, ok
}

func (c *protoCtx) messageType(o, u protoreflect.MessageDescriptor) {
	if localName(o) != localName(u) {
		c.add("FIELD_NAMED_TYPE_CHANGED")
		return
	}
	or, oOK := c.orig.refOf(o)
	ur, uOK := c.upd.refOf(u)
	if oOK && uOK && or.Subject == ur.Subject && or.Version == ur.Version {
		return
	}
	sub := &protoCtx{orig: c.orig, upd: c.upd, path: slices.Clone(c.path), active: c.active}
	sub.message(o, u)
	c.diffs = append(c.diffs, sub.diffs...)
	if len(sub.incompatible()) > 0 {
		c.add("FIELD_NAMED_TYPE_CHANGED")
	}
}

func (r *Registry) serializedProto(ctx context.Context, s *Schema) (string, error) {
	refs, err := r.storedReferences(ctx, s)
	if err != nil {
		return "", err
	}
	parsed, err := parseProto(s.Text, refs)
	if err != nil {
		return "", err
	}
	fd := protodesc.ToFileDescriptorProto(parsed.file)
	fd.Name = proto.String("default")
	data, err := proto.Marshal(fd)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}
