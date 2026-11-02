// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const srContentType = "application/vnd.schemaregistry.v1+json"

type schemaImportConfig struct {
	From     string
	To       string
	FromAuth string
	ToAuth   string
	Subjects *regexp.Regexp
	DryRun   bool
	Force    bool
}

type srReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

type srVersion struct {
	Subject    string        `json:"subject"`
	Version    int           `json:"version"`
	ID         int64         `json:"id"`
	GUID       string        `json:"guid,omitempty"`
	SchemaType string        `json:"schemaType,omitempty"`
	Schema     string        `json:"schema"`
	References []srReference `json:"references,omitempty"`
	Deleted    bool          `json:"-"`
	Dependency bool          `json:"-"`
}

type srError struct {
	Status  int
	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

func (e *srError) Error() string {
	return fmt.Sprintf("HTTP %d, error code %d: %s", e.Status, e.Code, e.Message)
}

func isSRCode(err error, codes ...int) bool {
	var e *srError
	return errors.As(err, &e) && slices.Contains(codes, e.Code)
}

type srClient struct {
	base string
	auth string
	http *http.Client
}

func newSRClient(base, auth string) (*srClient, error) {
	u, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid schema registry URL %q", base)
	}
	header := ""
	switch {
	case auth == "":
	case strings.HasPrefix(auth, "bearer:"):
		header = "Bearer " + strings.TrimPrefix(auth, "bearer:")
	case strings.Contains(auth, ":"):
		header = "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
	default:
		return nil, errors.New("auth must be user:password or bearer:<token>")
	}
	return &srClient{base: u.String(), auth: header, http: &http.Client{Timeout: 30 * time.Second}}, nil
}

func (c *srClient) call(ctx context.Context, method, path string, body any, out any) error {
	var data []byte
	if body != nil {
		var err error
		if data, err = json.Marshal(body); err != nil {
			return err
		}
	}
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Accept", srContentType)
		if body != nil {
			req.Header.Set("Content-Type", srContentType)
		}
		if c.auth != "" {
			req.Header.Set("Authorization", c.auth)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%s %s: %w", method, path, err)
			continue
		}
		payload, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("%s %s: read body: %w", method, path, err)
			continue
		}
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(payload)))
			continue
		}
		if resp.StatusCode >= 300 {
			e := &srError{Status: resp.StatusCode}
			if json.Unmarshal(payload, e) != nil || e.Message == "" {
				e.Message = strings.TrimSpace(string(payload))
			}
			return fmt.Errorf("%s %s: %w", method, path, e)
		}
		if out == nil {
			return nil
		}
		if err := json.Unmarshal(payload, out); err != nil {
			return fmt.Errorf("%s %s: decode: %w", method, path, err)
		}
		return nil
	}
	return lastErr
}

func subjectPath(subject string) string {
	return "/subjects/" + url.PathEscape(subject)
}

func runSchemaCommand(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) == 0 || args[0] != "import" {
		return errors.New("usage: kafscale-cli schema import --from <url> [--to <url>] [--auth user:password] [--subjects <regex>] [--dry-run]")
	}
	fs := flag.NewFlagSet("schema import", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var (
		from     = fs.String("from", "", "Source schema registry URL, for example a Confluent Schema Registry")
		to       = fs.String("to", envOrDefault("KAFSCALE_SCHEMA_REGISTRY_URL", "http://localhost:8081"), "Target KafScale schema registry URL")
		fromAuth = fs.String("auth", envOrDefault("KAFSCALE_SCHEMA_IMPORT_AUTH", ""), "Source credentials as user:password or bearer:<token>")
		toAuth   = fs.String("to-auth", envOrDefault("KAFSCALE_SCHEMA_REGISTRY_AUTH", ""), "Target credentials as user:password or bearer:<token>")
		subjects = fs.String("subjects", "", "Only import subjects matching this regular expression")
		dryRun   = fs.Bool("dry-run", false, "Report what would change without writing to the target")
		force    = fs.Bool("force", false, "Switch a target that already holds schemas to IMPORT mode")
	)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *from == "" {
		return errors.New("--from is required")
	}
	cfg := schemaImportConfig{From: *from, To: *to, FromAuth: *fromAuth, ToAuth: *toAuth, DryRun: *dryRun, Force: *force}
	if *subjects != "" {
		re, err := regexp.Compile(*subjects)
		if err != nil {
			return fmt.Errorf("parse --subjects: %w", err)
		}
		cfg.Subjects = re
	}
	return executeSchemaImport(ctx, stdout, cfg)
}

type schemaImport struct {
	cfg       schemaImportConfig
	src       *srClient
	dst       *srClient
	out       io.Writer
	items     map[string]*srVersion
	importing bool
	tempModes map[string]string
	contexts  []string
	planned   map[int64]*srVersion

	imported, skipped, deleted, settings, conflicts int
}

func itemKey(subject string, version int) string {
	return subject + "\x00" + strconv.Itoa(version)
}

func executeSchemaImport(ctx context.Context, stdout io.Writer, cfg schemaImportConfig) error {
	src, err := newSRClient(cfg.From, cfg.FromAuth)
	if err != nil {
		return fmt.Errorf("--from: %w", err)
	}
	dst, err := newSRClient(cfg.To, cfg.ToAuth)
	if err != nil {
		return fmt.Errorf("--to: %w", err)
	}
	im := &schemaImport{cfg: cfg, src: src, dst: dst, out: stdout, items: map[string]*srVersion{}, tempModes: map[string]string{}, planned: map[int64]*srVersion{}}
	subjects, err := im.plan(ctx)
	if err != nil {
		return err
	}
	order, err := im.order()
	if err != nil {
		return err
	}
	prefix := ""
	if cfg.DryRun {
		prefix = "dry run: "
	}
	_, _ = fmt.Fprintf(stdout, "%splanned %d subjects and %d schema versions from %s\n", prefix, len(subjects), len(order), src.base)
	runErr := im.run(ctx, order, subjects)
	_, _ = fmt.Fprintf(stdout, "%simported %d, skipped %d, soft-deleted %d, settings changed %d, conflicts %d, skipped contexts %d\n", prefix, im.imported, im.skipped, im.deleted, im.settings, im.conflicts, len(im.contexts))
	if !cfg.DryRun {
		var mode modeBody
		if err := dst.call(ctx, http.MethodGet, "/mode", nil, &mode); err == nil && mode.Mode == "IMPORT" {
			if runErr != nil {
				_, _ = fmt.Fprintln(stdout, "the run stopped early; rerun the same command to resume it")
			}
			_, _ = fmt.Fprintf(stdout, "target is still in IMPORT mode; after the last import run switch it with: PUT %s/mode {\"mode\":\"READWRITE\"}\n", dst.base)
		}
	}
	if runErr != nil {
		return runErr
	}
	if im.conflicts > 0 {
		return fmt.Errorf("schema import found %d conflicts", im.conflicts)
	}
	if len(im.contexts) > 0 {
		return fmt.Errorf("schema import skipped %d subjects in non-default contexts", len(im.contexts))
	}
	return nil
}

func (im *schemaImport) run(ctx context.Context, order []*srVersion, subjects []string) error {
	for _, v := range order {
		if err := im.importVersion(ctx, v); err != nil {
			if rerr := im.restoreModes(ctx); rerr != nil {
				return errors.Join(err, rerr)
			}
			return err
		}
	}
	if err := im.restoreModes(ctx); err != nil {
		return err
	}
	return im.syncSettings(ctx, subjects)
}

func subjectContext(subject string) string {
	if !strings.HasPrefix(subject, ":.") {
		return ""
	}
	rest := subject[1:]
	end := strings.Index(rest, ":")
	if end < 0 {
		return ""
	}
	if ctx := rest[:end]; ctx != "." {
		return ctx
	}
	return ""
}

func (im *schemaImport) plan(ctx context.Context) ([]string, error) {
	var all []string
	if err := im.src.call(ctx, http.MethodGet, "/subjects?deleted=true", nil, &all); err != nil {
		return nil, fmt.Errorf("list source subjects: %w", err)
	}
	var subjects []string
	for _, s := range all {
		if im.cfg.Subjects != nil && !im.cfg.Subjects.MatchString(s) {
			continue
		}
		if c := subjectContext(s); c != "" {
			im.contexts = append(im.contexts, s)
			_, _ = fmt.Fprintf(im.out, "skipped: subject %s is in context %s; KafScale has only the default context, so it is not imported\n", s, c)
			continue
		}
		subjects = append(subjects, s)
	}
	slices.Sort(subjects)
	for _, s := range subjects {
		if err := im.fetchSubject(ctx, s, 0, false); err != nil {
			return nil, err
		}
	}
	for {
		var missing []srReference
		for _, v := range im.items {
			for _, ref := range v.References {
				if im.items[itemKey(ref.Subject, ref.Version)] == nil {
					missing = append(missing, ref)
				}
			}
		}
		if len(missing) == 0 {
			return subjects, nil
		}
		for _, ref := range missing {
			if err := im.fetchSubject(ctx, ref.Subject, ref.Version, true); err != nil {
				return nil, err
			}
		}
	}
}

func (im *schemaImport) fetchSubject(ctx context.Context, subject string, only int, dependency bool) error {
	if c := subjectContext(subject); c != "" {
		return fmt.Errorf("source subject %s is in context %s, which KafScale does not support", subject, c)
	}
	var versions, live []int
	if err := im.src.call(ctx, http.MethodGet, subjectPath(subject)+"/versions?deleted=true", nil, &versions); err != nil {
		return fmt.Errorf("list versions of source subject %s: %w", subject, err)
	}
	if err := im.src.call(ctx, http.MethodGet, subjectPath(subject)+"/versions", nil, &live); err != nil && !isSRCode(err, 40401) {
		return fmt.Errorf("list live versions of source subject %s: %w", subject, err)
	}
	if only > 0 {
		if !slices.Contains(versions, only) {
			return fmt.Errorf("source subject %s has no version %d, which another schema references", subject, only)
		}
		versions = []int{only}
	}
	for _, n := range versions {
		key := itemKey(subject, n)
		if im.items[key] != nil {
			continue
		}
		v := &srVersion{}
		if err := im.src.call(ctx, http.MethodGet, fmt.Sprintf("%s/versions/%d?deleted=true", subjectPath(subject), n), nil, v); err != nil {
			return fmt.Errorf("read source subject %s version %d: %w", subject, n, err)
		}
		if v.SchemaType == "" {
			v.SchemaType = "AVRO"
		}
		v.Subject, v.Version = subject, n
		v.Deleted = !slices.Contains(live, n)
		v.Dependency = dependency
		im.items[key] = v
	}
	return nil
}

func (im *schemaImport) order() ([]*srVersion, error) {
	sorted := make([]*srVersion, 0, len(im.items))
	for _, v := range im.items {
		sorted = append(sorted, v)
	}
	slices.SortFunc(sorted, func(a, b *srVersion) int {
		if a.ID != b.ID {
			return cmp.Compare(a.ID, b.ID)
		}
		if a.Subject != b.Subject {
			return strings.Compare(a.Subject, b.Subject)
		}
		return a.Version - b.Version
	})
	state := map[*srVersion]int{}
	var out []*srVersion
	var visit func(v *srVersion) error
	visit = func(v *srVersion) error {
		switch state[v] {
		case 1:
			return fmt.Errorf("source subject %s version %d is part of a reference cycle", v.Subject, v.Version)
		case 2:
			return nil
		}
		state[v] = 1
		for _, ref := range v.References {
			if err := visit(im.items[itemKey(ref.Subject, ref.Version)]); err != nil {
				return err
			}
		}
		state[v] = 2
		out = append(out, v)
		return nil
	}
	for _, v := range sorted {
		if err := visit(v); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func sameSchema(a, b *srVersion) bool {
	if a.ID != b.ID || !strings.EqualFold(a.SchemaType, b.SchemaType) || !slices.Equal(a.References, b.References) {
		return false
	}
	return a.Schema == b.Schema || (a.GUID != "" && a.GUID == b.GUID)
}

func (im *schemaImport) conflict(format string, args ...any) {
	im.conflicts++
	_, _ = fmt.Fprintf(im.out, "conflict: "+format+"\n", args...)
}

func (im *schemaImport) importVersion(ctx context.Context, v *srVersion) error {
	path := fmt.Sprintf("%s/versions/%d", subjectPath(v.Subject), v.Version)
	pending := false
	existing := &srVersion{}
	err := im.dst.call(ctx, http.MethodGet, path+"?deleted=true", nil, existing)
	switch {
	case err == nil:
		if existing.SchemaType == "" {
			existing.SchemaType = "AVRO"
		}
		if !sameSchema(v, existing) {
			im.conflict("subject %s version %d is id %d in the source and id %d with different content in the target\n  source: %s\n  target: %s", v.Subject, v.Version, v.ID, existing.ID, v.Schema, existing.Schema)
			return nil
		}
		im.skipped++
	case isSRCode(err, 40401, 40402):
		byID := &srVersion{}
		err := im.dst.call(ctx, http.MethodGet, fmt.Sprintf("/schemas/ids/%d", v.ID), nil, byID)
		switch {
		case err == nil:
			if byID.SchemaType == "" {
				byID.SchemaType = "AVRO"
			}
			byID.ID = v.ID
			if !sameSchema(v, byID) {
				im.conflict("schema id %d already holds different content in the target\n  source: %s\n  target: %s", v.ID, v.Schema, byID.Schema)
				return nil
			}
		case !isSRCode(err, 40403):
			return fmt.Errorf("read target schema id %d: %w", v.ID, err)
		}
		if prev := im.planned[v.ID]; prev != nil && !sameSchema(v, prev) {
			im.conflict("schema id %d is used by subject %s version %d and subject %s version %d with different content in the source", v.ID, prev.Subject, prev.Version, v.Subject, v.Version)
			return nil
		}
		im.planned[v.ID] = v
		if im.cfg.DryRun {
			_, _ = fmt.Fprintf(im.out, "would import subject %s version %d id %d\n", v.Subject, v.Version, v.ID)
			im.imported++
			pending = true
			break
		}
		if err := im.enterImport(ctx, v.Subject); err != nil {
			return err
		}
		body := map[string]any{"schema": v.Schema, "schemaType": v.SchemaType, "id": v.ID, "version": v.Version}
		if len(v.References) > 0 {
			body["references"] = v.References
		}
		var out struct {
			ID int64 `json:"id"`
		}
		if err := im.dst.call(ctx, http.MethodPost, subjectPath(v.Subject)+"/versions", body, &out); err != nil {
			if isSRCode(err, 42205) {
				im.conflict("subject %s version %d id %d: %v", v.Subject, v.Version, v.ID, err)
				return nil
			}
			return fmt.Errorf("import subject %s version %d id %d: %w", v.Subject, v.Version, v.ID, err)
		}
		if out.ID != v.ID {
			im.conflict("subject %s version %d was stored as id %d instead of %d", v.Subject, v.Version, out.ID, v.ID)
			return nil
		}
		im.imported++
	default:
		return fmt.Errorf("read target subject %s version %d: %w", v.Subject, v.Version, err)
	}
	if !v.Deleted {
		return nil
	}
	if !pending {
		var live []int
		if err := im.dst.call(ctx, http.MethodGet, subjectPath(v.Subject)+"/versions", nil, &live); err != nil && !isSRCode(err, 40401) {
			return fmt.Errorf("list target versions of %s: %w", v.Subject, err)
		}
		if !slices.Contains(live, v.Version) {
			return nil
		}
	}
	if im.cfg.DryRun {
		_, _ = fmt.Fprintf(im.out, "would soft-delete subject %s version %d\n", v.Subject, v.Version)
		im.deleted++
		return nil
	}
	if err := im.dst.call(ctx, http.MethodDelete, path, nil, nil); err != nil {
		return fmt.Errorf("soft-delete target subject %s version %d: %w", v.Subject, v.Version, err)
	}
	im.deleted++
	return nil
}

type modeBody struct {
	Mode string `json:"mode"`
}

func (im *schemaImport) enterImport(ctx context.Context, subject string) error {
	if !im.importing {
		var mode modeBody
		if err := im.dst.call(ctx, http.MethodGet, "/mode", nil, &mode); err != nil {
			return fmt.Errorf("read target mode: %w", err)
		}
		if mode.Mode != "IMPORT" {
			path := "/mode"
			if im.cfg.Force {
				path += "?force=true"
			}
			if err := im.dst.call(ctx, http.MethodPut, path, modeBody{Mode: "IMPORT"}, nil); err != nil {
				if isSRCode(err, 42205) {
					return fmt.Errorf("switch target to IMPORT mode: %w; rerun with --force to import into a registry that already holds schemas", err)
				}
				return fmt.Errorf("switch target to IMPORT mode: %w", err)
			}
			_, _ = fmt.Fprintln(im.out, "switched target to IMPORT mode")
		}
		im.importing = true
	}
	if _, done := im.tempModes[subject]; done {
		return nil
	}
	var mode modeBody
	err := im.dst.call(ctx, http.MethodGet, "/mode/"+url.PathEscape(subject), nil, &mode)
	if isSRCode(err, 40409) {
		im.tempModes[subject] = ""
		return nil
	}
	if err != nil {
		return fmt.Errorf("read target mode of %s: %w", subject, err)
	}
	if mode.Mode == "IMPORT" {
		im.tempModes[subject] = ""
		return nil
	}
	if err := im.dst.call(ctx, http.MethodPut, "/mode/"+url.PathEscape(subject)+"?force=true", modeBody{Mode: "IMPORT"}, nil); err != nil {
		return fmt.Errorf("switch target subject %s to IMPORT mode: %w", subject, err)
	}
	im.tempModes[subject] = mode.Mode
	return nil
}

func (im *schemaImport) restoreModes(ctx context.Context) error {
	for subject, mode := range im.tempModes {
		if mode == "" {
			continue
		}
		if err := im.dst.call(ctx, http.MethodPut, "/mode/"+url.PathEscape(subject), modeBody{Mode: mode}, nil); err != nil {
			return fmt.Errorf("restore target mode of %s to %s: %w", subject, mode, err)
		}
	}
	return nil
}

type configBody struct {
	Compatibility      string `json:"compatibility,omitempty"`
	CompatibilityLevel string `json:"compatibilityLevel,omitempty"`
}

func (im *schemaImport) setting(ctx context.Context, kind, scope string, notSet int) error {
	read := func(c *srClient) (string, error) {
		path := "/" + kind
		if scope != "" {
			path += "/" + url.PathEscape(scope) + "?defaultToGlobal=false"
		}
		if kind == "config" {
			var body configBody
			err := c.call(ctx, http.MethodGet, path, nil, &body)
			if isSRCode(err, notSet) {
				return "", nil
			}
			return body.CompatibilityLevel, err
		}
		var body modeBody
		err := c.call(ctx, http.MethodGet, path, nil, &body)
		if isSRCode(err, notSet) {
			return "", nil
		}
		return body.Mode, err
	}
	name := "global " + kind
	if scope != "" {
		name = kind + " of " + scope
	}
	want, err := read(im.src)
	if err != nil {
		return fmt.Errorf("read source %s: %w", name, err)
	}
	if want == "" {
		return nil
	}
	have, err := read(im.dst)
	if err != nil {
		return fmt.Errorf("read target %s: %w", name, err)
	}
	if strings.EqualFold(want, have) {
		return nil
	}
	im.settings++
	if im.cfg.DryRun {
		_, _ = fmt.Fprintf(im.out, "would set %s to %s\n", name, want)
		return nil
	}
	path := "/" + kind
	if scope != "" {
		path += "/" + url.PathEscape(scope)
	}
	var body any = configBody{Compatibility: want}
	if kind == "mode" {
		path += "?force=true"
		body = modeBody{Mode: want}
	}
	if err := im.dst.call(ctx, http.MethodPut, path, body, nil); err != nil {
		return fmt.Errorf("set target %s to %s: %w", name, want, err)
	}
	_, _ = fmt.Fprintf(im.out, "set %s to %s\n", name, want)
	return nil
}

func (im *schemaImport) syncSettings(ctx context.Context, subjects []string) error {
	if err := im.setting(ctx, "config", "", 40408); err != nil {
		return err
	}
	for _, s := range subjects {
		if err := im.setting(ctx, "config", s, 40408); err != nil {
			return err
		}
		if err := im.setting(ctx, "mode", s, 40409); err != nil {
			return err
		}
	}
	var mode modeBody
	if err := im.src.call(ctx, http.MethodGet, "/mode", nil, &mode); err == nil && mode.Mode != "" && mode.Mode != "READWRITE" {
		_, _ = fmt.Fprintf(im.out, "source global mode is %s; the target global mode is left for you to set\n", mode.Mode)
	}
	return nil
}
