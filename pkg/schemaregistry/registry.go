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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KafScale/platform/pkg/storage"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	keyPrefix          = "/kafscale/schemas/"
	DefaultInlineLimit = 64 << 10
	ModeReadWrite      = "READWRITE"
	ModeReadOnly       = "READONLY"
	ModeImport         = "IMPORT"
	CompatNone         = "NONE"
	CompatBackward     = "BACKWARD"
	maxReferenceDepth  = 32
)

type Reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

type Schema struct {
	ID         int64
	Type       string
	Text       string
	References []Reference
}

type SubjectVersion struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

type VersionedSchema struct {
	Subject string
	Version int
	Schema  *Schema
}

type RegisterRequest struct {
	Schema     string
	SchemaType string
	References []Reference
	ID         int64
	Version    int
}

type schemaRecord struct {
	SchemaType string      `json:"schemaType"`
	Hash       string      `json:"hash"`
	References []Reference `json:"references,omitempty"`
	Schema     string      `json:"schema,omitempty"`
	S3Key      string      `json:"s3Key,omitempty"`
}

type versionRecord struct {
	ID      int64 `json:"id"`
	Deleted bool  `json:"deleted,omitempty"`
}

type versionEntry struct {
	versionRecord
	rev int64
}

type Config struct {
	Client      *clientv3.Client
	S3          storage.S3Client
	Namespace   string
	InlineLimit int
}

type Registry struct {
	cli         *clientv3.Client
	s3          storage.S3Client
	namespace   string
	inlineLimit int
	conflicts   atomic.Uint64
	mu          sync.RWMutex
	cache       map[int64]*Schema
}

func New(cfg Config) (*Registry, error) {
	if cfg.Client == nil {
		return nil, errors.New("etcd client required")
	}
	if cfg.InlineLimit <= 0 {
		return nil, errors.New("inline limit must be positive")
	}
	if cfg.S3 != nil && cfg.Namespace == "" {
		return nil, errors.New("s3 namespace required")
	}
	return &Registry{
		cli:         cfg.Client,
		s3:          cfg.S3,
		namespace:   cfg.Namespace,
		inlineLimit: cfg.InlineLimit,
		cache:       make(map[int64]*Schema),
	}, nil
}

func (r *Registry) Conflicts() uint64 {
	return r.conflicts.Load()
}

func esc(s string) string {
	return url.PathEscape(s)
}

func counterKey() string { return keyPrefix + "counter" }

func idKey(id int64) string { return keyPrefix + "ids/" + strconv.FormatInt(id, 10) }

func idSubjectsPrefix(id int64) string { return idKey(id) + "/subjects/" }

func idSubjectKey(id int64, subject string, version int) string {
	return idSubjectsPrefix(id) + esc(subject) + "/" + strconv.Itoa(version)
}

func hashKey(schemaType, hash string) string { return keyPrefix + "hash/" + schemaType + "/" + hash }

func subjectsPrefix() string { return keyPrefix + "subjects/" }

func subjectPrefix(subject string) string { return subjectsPrefix() + esc(subject) + "/" }

func versionsPrefix(subject string) string { return subjectPrefix(subject) + "versions/" }

func versionKey(subject string, version int) string {
	return versionsPrefix(subject) + strconv.Itoa(version)
}

func latestKey(subject string) string { return subjectPrefix(subject) + "latest" }

func subjectHashKey(subject string, id int64) string {
	return subjectPrefix(subject) + "hash/" + strconv.FormatInt(id, 10)
}

func refByPrefix(subject string, version int) string {
	return keyPrefix + "refby/" + esc(subject) + "/" + strconv.Itoa(version) + "/"
}

func refByKey(ref Reference, subject string, version int) string {
	return refByPrefix(ref.Subject, ref.Version) + esc(subject) + "/" + strconv.Itoa(version)
}

func configKey(subject string) string {
	if subject == "" {
		return keyPrefix + "config/_global"
	}
	return keyPrefix + "config/subjects/" + esc(subject)
}

func modeKey(subject string) string {
	if subject == "" {
		return keyPrefix + "mode/_global"
	}
	return keyPrefix + "mode/subjects/" + esc(subject)
}

type snapshot map[string]*mvccpb.KeyValue

func (s snapshot) rev(key string) int64 {
	if kv := s[key]; kv != nil {
		return kv.ModRevision
	}
	return 0
}

func (s snapshot) int(key string) (int64, error) {
	kv := s[key]
	if kv == nil {
		return 0, nil
	}
	return strconv.ParseInt(string(kv.Value), 10, 64)
}

func (s snapshot) version(key string) (int, error) {
	kv := s[key]
	if kv == nil {
		return 0, nil
	}
	v, err := strconv.ParseInt(string(kv.Value), 10, 32)
	return int(v), err
}

func (s snapshot) str(key string) string {
	if kv := s[key]; kv != nil {
		return string(kv.Value)
	}
	return ""
}

func (s snapshot) fence(keys ...string) []clientv3.Cmp {
	cmps := make([]clientv3.Cmp, 0, len(keys))
	for _, key := range keys {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "=", s.rev(key)))
	}
	return cmps
}

func (r *Registry) read(ctx context.Context, keys []string, prefixes []string) (snapshot, error) {
	ops := make([]clientv3.Op, 0, len(keys)+len(prefixes))
	for _, key := range keys {
		ops = append(ops, clientv3.OpGet(key))
	}
	for _, prefix := range prefixes {
		ops = append(ops, clientv3.OpGet(prefix, clientv3.WithPrefix()))
	}
	resp, err := r.cli.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return nil, err
	}
	snap := make(snapshot)
	for _, op := range resp.Responses {
		for _, kv := range op.GetResponseRange().Kvs {
			snap[string(kv.Key)] = kv
		}
	}
	return snap, nil
}

func (s snapshot) versions(subject string) (map[int]versionEntry, error) {
	prefix := versionsPrefix(subject)
	out := make(map[int]versionEntry)
	for key, kv := range s {
		rest, ok := strings.CutPrefix(key, prefix)
		if !ok {
			continue
		}
		v, err := strconv.Atoi(rest)
		if err != nil {
			return nil, fmt.Errorf("bad version key %q", key)
		}
		var rec versionRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			return nil, fmt.Errorf("decode %q: %w", key, err)
		}
		out[v] = versionEntry{versionRecord: rec, rev: kv.ModRevision}
	}
	return out, nil
}

func sortedVersions(versions map[int]versionEntry, deleted bool) []int {
	out := make([]int, 0, len(versions))
	for v, e := range versions {
		if deleted || !e.Deleted {
			out = append(out, v)
		}
	}
	slices.Sort(out)
	return out
}

func effective(snap snapshot, subjectKey, globalKey, fallback string) string {
	if v := snap.str(subjectKey); v != "" {
		return v
	}
	if v := snap.str(globalKey); v != "" {
		return v
	}
	return fallback
}

func validSubject(subject string) error {
	if subject == "" || strings.ContainsAny(subject, "\x00\n\r") {
		return errBadRequest("invalid subject name %q", subject)
	}
	return nil
}

func storeErr(ctx context.Context, err error) error {
	var regErr *Error
	if errors.As(err, &regErr) {
		return err
	}
	if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
		return errTimeout(err)
	}
	return errStore(err)
}

func backoff(ctx context.Context, attempt int) error {
	limit := time.Millisecond << min(attempt, 6)
	limit = min(limit, 50*time.Millisecond)
	d := limit/2 + rand.N(limit/2+1)
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Registry) schemaByID(ctx context.Context, id int64) (*Schema, error) {
	r.mu.RLock()
	s := r.cache[id]
	r.mu.RUnlock()
	if s != nil {
		return s, nil
	}
	resp, err := r.cli.Get(ctx, idKey(id))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var rec schemaRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		return nil, fmt.Errorf("decode schema %d: %w", id, err)
	}
	text := rec.Schema
	if rec.S3Key != "" {
		if r.s3 == nil {
			return nil, fmt.Errorf("schema %d is stored in S3 but S3 is not configured", id)
		}
		body, err := r.s3.DownloadSegment(ctx, rec.S3Key, nil)
		if err != nil {
			return nil, fmt.Errorf("read schema %d from S3: %w", id, err)
		}
		text = string(body)
	}
	s = &Schema{ID: id, Type: rec.SchemaType, Text: text, References: rec.References}
	r.mu.Lock()
	r.cache[id] = s
	r.mu.Unlock()
	return s, nil
}

func (r *Registry) SchemaByID(ctx context.Context, id int64) (*Schema, error) {
	s, err := r.schemaByID(ctx, id)
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	if s == nil {
		return nil, errSchemaNotFound()
	}
	return s, nil
}

func (r *Registry) resolveReferences(ctx context.Context, refs []Reference, depth int, seen map[string]bool, out *[]resolvedReference) (map[string]int64, error) {
	if depth > maxReferenceDepth {
		return nil, errInvalidSchema("references nested deeper than %d levels", maxReferenceDepth)
	}
	revs := make(map[string]int64, len(refs))
	for _, ref := range refs {
		if ref.Name == "" || ref.Subject == "" || ref.Version <= 0 {
			return nil, errInvalidSchema("reference needs a name, a subject and a version above 0: %+v", ref)
		}
		key := versionKey(ref.Subject, ref.Version)
		resp, err := r.cli.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if len(resp.Kvs) == 0 {
			return nil, errInvalidSchema("reference %s points to subject %s version %d, which does not exist", ref.Name, ref.Subject, ref.Version)
		}
		var rec versionRecord
		if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
			return nil, fmt.Errorf("decode %q: %w", key, err)
		}
		if rec.Deleted {
			return nil, errInvalidSchema("reference %s points to subject %s version %d, which is deleted", ref.Name, ref.Subject, ref.Version)
		}
		revs[key] = resp.Kvs[0].ModRevision
		if seen[ref.Name] {
			continue
		}
		s, err := r.schemaByID(ctx, rec.ID)
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, fmt.Errorf("schema %d for subject %s version %d is missing", rec.ID, ref.Subject, ref.Version)
		}
		if _, err := r.resolveReferences(ctx, s.References, depth+1, seen, out); err != nil {
			return nil, err
		}
		seen[ref.Name] = true
		*out = append(*out, resolvedReference{Name: ref.Name, Text: s.Text})
	}
	return revs, nil
}

func (r *Registry) storedReferences(ctx context.Context, s *Schema) ([]resolvedReference, error) {
	var out []resolvedReference
	if err := r.resolveStoredReferences(ctx, s.References, 0, map[string]bool{}, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Registry) resolveStoredReferences(ctx context.Context, refs []Reference, depth int, seen map[string]bool, out *[]resolvedReference) error {
	if depth > maxReferenceDepth {
		return fmt.Errorf("references nested deeper than %d levels", maxReferenceDepth)
	}
	for _, ref := range refs {
		if seen[ref.Name] {
			continue
		}
		resp, err := r.cli.Get(ctx, versionKey(ref.Subject, ref.Version))
		if err != nil {
			return err
		}
		if len(resp.Kvs) == 0 {
			return fmt.Errorf("reference %s points to subject %s version %d, which does not exist", ref.Name, ref.Subject, ref.Version)
		}
		var rec versionRecord
		if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
			return err
		}
		s, err := r.schemaByID(ctx, rec.ID)
		if err != nil {
			return err
		}
		if s == nil {
			return fmt.Errorf("schema %d is missing", rec.ID)
		}
		if err := r.resolveStoredReferences(ctx, s.References, depth+1, seen, out); err != nil {
			return err
		}
		seen[ref.Name] = true
		*out = append(*out, resolvedReference{Name: ref.Name, Text: s.Text})
	}
	return nil
}

type prepared struct {
	schemaType string
	hash       string
	refs       []resolvedReference
	refRevs    map[string]int64
}

func (r *Registry) prepare(ctx context.Context, req RegisterRequest) (*prepared, error) {
	schemaType := req.SchemaType
	if schemaType == "" {
		schemaType = schemaTypeAvro
	}
	if schemaType != schemaTypeAvro {
		return nil, errInvalidSchema("schema type %s is not supported", schemaType)
	}
	if strings.TrimSpace(req.Schema) == "" {
		return nil, errInvalidSchema("empty schema")
	}
	var refs []resolvedReference
	refRevs, err := r.resolveReferences(ctx, req.References, 0, map[string]bool{}, &refs)
	if err != nil {
		return nil, err
	}
	if _, err := parseAvro(req.Schema, refs); err != nil {
		return nil, errInvalidSchema("%v", err)
	}
	canonical, err := canonicalText(req.Schema)
	if err != nil {
		return nil, errInvalidSchema("%v", err)
	}
	return &prepared{
		schemaType: schemaType,
		hash:       schemaHash(schemaType, canonical, req.References),
		refs:       refs,
		refRevs:    refRevs,
	}, nil
}

func (r *Registry) newRecord(ctx context.Context, req RegisterRequest, p *prepared) (string, error) {
	rec := schemaRecord{SchemaType: p.schemaType, Hash: p.hash, References: req.References}
	if len(req.Schema) <= r.inlineLimit {
		rec.Schema = req.Schema
	} else {
		if r.s3 == nil {
			return "", errInvalidSchema("schema is %d bytes, schemas above %d bytes need S3 storage and S3 is not configured", len(req.Schema), r.inlineLimit)
		}
		sum := sha256.Sum256([]byte(req.Schema))
		rec.S3Key = r.namespace + "/@schemas/sha256/" + hex.EncodeToString(sum[:])
		if err := r.s3.UploadSegment(ctx, rec.S3Key, []byte(req.Schema)); err != nil {
			return "", fmt.Errorf("write schema text to S3: %w", err)
		}
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (r *Registry) Register(ctx context.Context, subject string, req RegisterRequest) (int64, int, error) {
	if err := validSubject(subject); err != nil {
		return 0, 0, err
	}
	if req.ID < 0 || req.Version < 0 {
		return 0, 0, errBadRequest("id and version must not be negative")
	}
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			r.conflicts.Add(1)
			if err := backoff(ctx, attempt); err != nil {
				return 0, 0, errTimeout(fmt.Errorf("register on subject %s gave up after %d attempts: %w", subject, attempt, err))
			}
		}
		p, err := r.prepare(ctx, req)
		if err != nil {
			return 0, 0, storeErr(ctx, err)
		}
		record, err := r.newRecord(ctx, req, p)
		if err != nil {
			return 0, 0, storeErr(ctx, err)
		}
		id, version, err := r.tryRegister(ctx, subject, req, p, record)
		if err != nil {
			return 0, 0, storeErr(ctx, err)
		}
		if id > 0 {
			return id, version, nil
		}
	}
}

func refOps(refs []Reference, subject string, version int) []clientv3.Op {
	ops := make([]clientv3.Op, 0, len(refs))
	for _, ref := range refs {
		ops = append(ops, clientv3.OpPut(refByKey(ref, subject, version), ""))
	}
	return ops
}

func refFence(revs map[string]int64) []clientv3.Cmp {
	cmps := make([]clientv3.Cmp, 0, len(revs))
	for key, rev := range revs {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "=", rev))
	}
	return cmps
}

func versionValue(id int64) string {
	data, _ := json.Marshal(versionRecord{ID: id})
	return string(data)
}

func (r *Registry) tryRegister(ctx context.Context, subject string, req RegisterRequest, p *prepared, record string) (int64, int, error) {
	hk := hashKey(p.schemaType, p.hash)
	lk := latestKey(subject)
	keys := []string{counterKey(), hk, modeKey(""), modeKey(subject), configKey(""), configKey(subject)}
	if req.ID > 0 {
		keys = append(keys, idKey(req.ID))
	}
	snap, err := r.read(ctx, keys, []string{subjectPrefix(subject)})
	if err != nil {
		return 0, 0, err
	}
	mode := effective(snap, modeKey(subject), modeKey(""), ModeReadWrite)
	switch mode {
	case ModeReadOnly:
		return 0, 0, errNotPermitted("Subject %s is in read-only mode", subject)
	case ModeImport:
		if req.ID == 0 {
			return 0, 0, errNotPermitted("Subject %s is in import mode, register with an explicit id", subject)
		}
		return r.tryImport(ctx, subject, req, p, record, snap)
	}
	if req.ID > 0 || req.Version > 0 {
		return 0, 0, errNotPermitted("Schema id and version can only be set in IMPORT mode")
	}
	versions, err := snap.versions(subject)
	if err != nil {
		return 0, 0, err
	}
	latest, err := snap.version(lk)
	if err != nil {
		return 0, 0, err
	}
	cmps := snap.fence(lk, modeKey(""), modeKey(subject), configKey(""), configKey(subject))
	cmps = append(cmps, refFence(p.refRevs)...)
	var ops []clientv3.Op
	var id int64
	if kv := snap[hk]; kv != nil {
		if id, err = strconv.ParseInt(string(kv.Value), 10, 64); err != nil {
			return 0, 0, err
		}
		if v, err := snap.version(subjectHashKey(subject, id)); err != nil {
			return 0, 0, err
		} else if e, ok := versions[v]; ok && !e.Deleted {
			return id, v, nil
		}
		cmps = append(cmps, snap.fence(hk)...)
	} else {
		n, err := snap.int(counterKey())
		if err != nil {
			return 0, 0, err
		}
		id = n + 1
		cmps = append(cmps, snap.fence(counterKey())...)
		cmps = append(cmps,
			clientv3.Compare(clientv3.CreateRevision(hk), "=", 0),
			clientv3.Compare(clientv3.CreateRevision(idKey(id)), "=", 0))
		ops = append(ops,
			clientv3.OpPut(idKey(id), record),
			clientv3.OpPut(hk, strconv.FormatInt(id, 10)),
			clientv3.OpPut(counterKey(), strconv.FormatInt(id, 10)))
	}
	live := sortedVersions(versions, false)
	if len(live) > 0 && effective(snap, configKey(subject), configKey(""), CompatBackward) == CompatBackward {
		prev := live[len(live)-1]
		if err := r.checkAgainst(ctx, req.Schema, p.refs, versions[prev].ID); err != nil {
			return 0, 0, err
		}
		cmps = append(cmps, snap.fence(versionKey(subject, prev))...)
	}
	version := latest + 1
	shk := subjectHashKey(subject, id)
	cmps = append(cmps, snap.fence(shk)...)
	cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(versionKey(subject, version)), "=", 0))
	ops = append(ops,
		clientv3.OpPut(versionKey(subject, version), versionValue(id)),
		clientv3.OpPut(lk, strconv.Itoa(version)),
		clientv3.OpPut(shk, strconv.Itoa(version)),
		clientv3.OpPut(idSubjectKey(id, subject, version), ""))
	ops = append(ops, refOps(req.References, subject, version)...)
	resp, err := r.cli.Txn(ctx).If(cmps...).Then(ops...).Commit()
	if err != nil {
		return 0, 0, err
	}
	if !resp.Succeeded {
		return 0, 0, nil
	}
	return id, version, nil
}

func (r *Registry) checkAgainst(ctx context.Context, text string, refs []resolvedReference, prevID int64) error {
	prev, err := r.schemaByID(ctx, prevID)
	if err != nil {
		return err
	}
	if prev == nil {
		return fmt.Errorf("schema %d is missing", prevID)
	}
	prevRefs, err := r.storedReferences(ctx, prev)
	if err != nil {
		return err
	}
	if err := checkBackward(text, refs, prev.Text, prevRefs); err != nil {
		return errIncompatible(err.Error())
	}
	return nil
}

func (r *Registry) tryImport(ctx context.Context, subject string, req RegisterRequest, p *prepared, record string, snap snapshot) (int64, int, error) {
	id := req.ID
	ik := idKey(id)
	lk := latestKey(subject)
	cmps := snap.fence(lk, modeKey(""), modeKey(subject))
	cmps = append(cmps, refFence(p.refRevs)...)
	var ops []clientv3.Op
	if kv := snap[ik]; kv != nil {
		existing, err := r.schemaByID(ctx, id)
		if err != nil {
			return 0, 0, err
		}
		if existing == nil || existing.Type != p.schemaType || existing.Text != req.Schema || !slices.Equal(existing.References, req.References) {
			return 0, 0, errNotPermitted("Schema id %d already holds a different schema", id)
		}
		cmps = append(cmps, snap.fence(ik)...)
	} else {
		n, err := snap.int(counterKey())
		if err != nil {
			return 0, 0, err
		}
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(ik), "=", 0))
		ops = append(ops, clientv3.OpPut(ik, record))
		hk := hashKey(p.schemaType, p.hash)
		if snap[hk] == nil {
			cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(hk), "=", 0))
			ops = append(ops, clientv3.OpPut(hk, strconv.FormatInt(id, 10)))
		}
		if id > n {
			cmps = append(cmps, snap.fence(counterKey())...)
			ops = append(ops, clientv3.OpPut(counterKey(), strconv.FormatInt(id, 10)))
		}
	}
	versions, err := snap.versions(subject)
	if err != nil {
		return 0, 0, err
	}
	latest, err := snap.version(lk)
	if err != nil {
		return 0, 0, err
	}
	shk := subjectHashKey(subject, id)
	indexed, err := snap.version(shk)
	if err != nil {
		return 0, 0, err
	}
	version := req.Version
	if version == 0 {
		if e, ok := versions[indexed]; ok && !e.Deleted && len(ops) == 0 {
			return id, indexed, nil
		}
		version = latest + 1
	}
	if e, ok := versions[version]; ok {
		if e.ID != id {
			return 0, 0, errNotPermitted("Version %d of subject %s already holds schema id %d", version, subject, e.ID)
		}
		return id, version, nil
	}
	cmps = append(cmps, snap.fence(shk)...)
	cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(versionKey(subject, version)), "=", 0))
	ops = append(ops,
		clientv3.OpPut(versionKey(subject, version), versionValue(id)),
		clientv3.OpPut(idSubjectKey(id, subject, version), ""))
	if version > latest {
		ops = append(ops, clientv3.OpPut(lk, strconv.Itoa(version)))
	}
	if indexed == 0 {
		ops = append(ops, clientv3.OpPut(shk, strconv.Itoa(version)))
	}
	ops = append(ops, refOps(req.References, subject, version)...)
	resp, err := r.cli.Txn(ctx).If(cmps...).Then(ops...).Commit()
	if err != nil {
		return 0, 0, err
	}
	if !resp.Succeeded {
		return 0, 0, nil
	}
	return id, version, nil
}
