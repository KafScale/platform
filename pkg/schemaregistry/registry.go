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
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KafScale/platform/pkg/storage"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	keyPrefix          = "/kafscale/schemas/"
	DefaultInlineLimit = 64 << 10
	DefaultCacheBytes  = 32 << 20
	MaxSchemaBytes     = 1 << 20
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
	Norm       string
}

type SubjectVersion struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

type VersionedSchema struct {
	Subject string
	Version int
	Schema  *Schema
	Deleted bool
}

type RegisterRequest struct {
	Schema     string
	SchemaType string
	References []Reference
	ID         int64
	Version    int
	Normalize  bool
}

type schemaRecord struct {
	SchemaType string      `json:"schemaType"`
	Hash       string      `json:"hash"`
	Norm       string      `json:"norm,omitempty"`
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
	cli          *clientv3.Client
	s3           storage.S3Client
	markers      storage.ConditionalIndexWriter
	namespace    string
	inlineLimit  int
	conflicts    atomic.Uint64
	mu           sync.RWMutex
	cache        map[int64]*Schema
	cacheBytes   int
	cacheLimit   int
	counter      int64
	revision     int64
	clusterID    uint64
	raftTerm     uint64
	resets       atomic.Uint64
	beforeCommit func()
}

func New(cfg Config) (*Registry, error) {
	if cfg.Client == nil {
		return nil, errors.New("etcd client required")
	}
	if cfg.InlineLimit <= 0 {
		return nil, errors.New("inline limit must be positive")
	}
	var markers storage.ConditionalIndexWriter
	if cfg.S3 != nil {
		if cfg.Namespace == "" {
			return nil, errors.New("s3 namespace required")
		}
		var ok bool
		if markers, ok = cfg.S3.(storage.ConditionalIndexWriter); !ok {
			return nil, errors.New("s3 client does not support conditional writes")
		}
	}
	return &Registry{
		cli:         cfg.Client,
		s3:          cfg.S3,
		markers:     markers,
		namespace:   cfg.Namespace,
		inlineLimit: cfg.InlineLimit,
		cache:       make(map[int64]*Schema),
		cacheLimit:  DefaultCacheBytes,
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

func guidKey(guid string) string { return keyPrefix + "guids/" + guid }

func (r *Registry) idMarkerKey(id int64) string {
	return fmt.Sprintf("%s/@schemas/ids/%020d", r.namespace, id)
}

func schemaGUID(text string, refs []Reference) string {
	h := md5.New()
	h.Write([]byte(text))
	for _, ref := range refs {
		h.Write([]byte(ref.Name))
		h.Write([]byte(ref.Subject))
		_ = binary.Write(h, binary.BigEndian, int32(ref.Version))
	}
	b := h.Sum(nil)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func subjectsPrefix() string { return keyPrefix + "subjects/" }

func subjectPrefix(subject string) string { return subjectsPrefix() + esc(subject) + "/" }

func versionsPrefix(subject string) string { return subjectPrefix(subject) + "versions/" }

func versionKey(subject string, version int) string {
	return versionsPrefix(subject) + strconv.Itoa(version)
}

func latestKey(subject string) string { return subjectPrefix(subject) + "latest" }

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
	r.observe(resp.Header)
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
	s, counter := r.cache[id], r.counter
	r.mu.RUnlock()
	if s != nil && id <= counter {
		return s, nil
	}
	if id > counter {
		if err := r.syncCounter(ctx); err != nil {
			return nil, err
		}
	}
	resp, err := r.cli.Get(ctx, idKey(id))
	if err != nil {
		return nil, err
	}
	r.observe(resp.Header)
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
	s = &Schema{ID: id, Type: rec.SchemaType, Text: text, References: rec.References, Norm: rec.Norm}
	r.mu.Lock()
	r.cacheLocked(s)
	r.counter = max(r.counter, id)
	r.mu.Unlock()
	return s, nil
}

func (r *Registry) Init(ctx context.Context) error {
	return r.syncCounter(ctx)
}

func (r *Registry) CacheResets() uint64 {
	return r.resets.Load()
}

func (r *Registry) syncCounter(ctx context.Context) error {
	resp, err := r.cli.Get(ctx, counterKey())
	if err != nil {
		return err
	}
	r.observe(resp.Header)
	var n int64
	if len(resp.Kvs) > 0 {
		if n, err = strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if n < r.counter {
		r.resetLocked()
	}
	r.counter = n
	return nil
}

func (r *Registry) observe(h *etcdserverpb.ResponseHeader) {
	if h == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if h.Revision < r.revision || (r.clusterID != 0 && h.ClusterId != r.clusterID) || h.RaftTerm < r.raftTerm {
		r.resetLocked()
		r.counter = 0
	}
	r.revision, r.clusterID, r.raftTerm = h.Revision, h.ClusterId, h.RaftTerm
}

func (r *Registry) resetLocked() {
	clear(r.cache)
	r.cacheBytes = 0
	r.resets.Add(1)
}

func (r *Registry) cacheLocked(s *Schema) {
	size := len(s.Text)
	if size > r.cacheLimit {
		return
	}
	if old := r.cache[s.ID]; old != nil {
		r.cacheBytes -= len(old.Text)
		delete(r.cache, s.ID)
	}
	for id, old := range r.cache {
		if r.cacheBytes+size <= r.cacheLimit {
			break
		}
		r.cacheBytes -= len(old.Text)
		delete(r.cache, id)
	}
	r.cache[s.ID] = s
	r.cacheBytes += size
}

func (r *Registry) CacheBytes() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cacheBytes
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

func (s *Schema) GUID() string { return schemaGUID(s.Text, s.References) }

func (r *Registry) SchemaByGUID(ctx context.Context, guid string) (*Schema, error) {
	resp, err := r.cli.Get(ctx, guidKey(guid))
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	r.observe(resp.Header)
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	id, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return nil, err
	}
	return r.SchemaByID(ctx, id)
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
	canonical  string
	hash       string
	norm       string
	guid       string
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
	if len(req.Schema) > MaxSchemaBytes {
		return nil, errTooLarge()
	}
	var refs []resolvedReference
	refRevs, err := r.resolveReferences(ctx, req.References, 0, map[string]bool{}, &refs)
	if err != nil {
		return nil, err
	}
	if _, err := parseAvro(req.Schema, refs); err != nil {
		return nil, errInvalidSchema("%v", err)
	}
	canonical, err := canonicalAvro(req.Schema, refs, req.Normalize)
	if err != nil {
		return nil, errInvalidSchema("%v", err)
	}
	normalized, err := canonicalAvro(req.Schema, refs, true)
	if err != nil {
		return nil, errInvalidSchema("%v", err)
	}
	return &prepared{
		schemaType: schemaType,
		canonical:  canonical,
		hash:       schemaHash(schemaType, canonical, req.References),
		norm:       schemaHash(schemaType, normalized, req.References),
		guid:       schemaGUID(canonical, req.References),
		refs:       refs,
		refRevs:    refRevs,
	}, nil
}

type recordWriter struct {
	r        *Registry
	req      RegisterRequest
	p        *prepared
	uploaded string
	writes   int
	claimed  int64
}

const (
	maxClaimProbes    = 64
	linearClaimProbes = 32
	maxConflictRetry  = 3
	conflictRetryWait = 20 * time.Millisecond
)

func (r *Registry) putMarker(ctx context.Context, id int64, hash string) error {
	var err error
	for attempt := 0; attempt <= maxConflictRetry; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(conflictRetryWait):
			}
		}
		err = r.markers.UploadIndexIfAbsent(ctx, r.idMarkerKey(id), []byte(hash))
		if !errors.Is(err, storage.ErrConditionalConflict) {
			return err
		}
	}
	return err
}

func (w *recordWriter) claim(ctx context.Context, counter int64) (int64, error) {
	if w.r.s3 == nil {
		return counter + 1, nil
	}
	if w.claimed > counter {
		return w.claimed, nil
	}
	id, step := counter+1, int64(1)
	for probe := range maxClaimProbes {
		if id > math.MaxInt32 {
			break
		}
		err := w.r.putMarker(ctx, id, w.p.hash)
		switch {
		case err == nil:
			w.claimed = id
			return id, nil
		case errors.Is(err, storage.ErrPreconditionFailed), errors.Is(err, storage.ErrConditionalConflict):
			if probe < linearClaimProbes {
				id++
				continue
			}
			id += step + rand.Int64N(step)
			step *= 2
		default:
			return 0, fmt.Errorf("write schema id %d marker to S3: %w", id, err)
		}
	}
	return 0, fmt.Errorf("no free schema id found above %d in S3 id markers", counter)
}

func (w *recordWriter) claimImport(ctx context.Context, id int64) error {
	if w.r.s3 == nil || w.claimed == id {
		return nil
	}
	err := w.r.putMarker(ctx, id, w.p.hash)
	if errors.Is(err, storage.ErrPreconditionFailed) {
		data, readErr := w.r.s3.DownloadIndex(ctx, w.r.idMarkerKey(id))
		if readErr != nil {
			return fmt.Errorf("read schema id %d marker from S3: %w", id, readErr)
		}
		if string(data) != w.p.hash {
			return errNotPermitted("Schema id %d was issued before for a different schema, recorded in S3 at %s", id, w.r.idMarkerKey(id))
		}
		err = nil
	}
	if err != nil {
		return fmt.Errorf("write schema id %d marker to S3: %w", id, err)
	}
	w.claimed = id
	return nil
}

func (r *Registry) CheckIDMarkers(ctx context.Context) error {
	if r.s3 == nil {
		return nil
	}
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(rand.Uint32())
	}
	key := fmt.Sprintf("%s/@schemas/probe/%x", r.namespace, b)
	defer func() { _ = r.s3.DeleteIndex(context.WithoutCancel(ctx), key) }()
	if err := r.markers.UploadIndexIfAbsent(ctx, key, []byte("probe")); err != nil {
		return fmt.Errorf("S3 conditional write probe at %s failed: %w", key, err)
	}
	err := r.markers.UploadIndexIfAbsent(ctx, key, []byte("probe"))
	if err == nil {
		return fmt.Errorf("S3 backend accepted a second PutObject with If-None-Match at %s, so it ignores the condition and schema ids cannot be fenced", key)
	}
	if !errors.Is(err, storage.ErrPreconditionFailed) {
		return fmt.Errorf("S3 conditional write probe at %s: second write gave %w, want 412 PreconditionFailed", key, err)
	}
	return nil
}

func (w *recordWriter) record(ctx context.Context) (string, error) {
	p := w.p
	rec := schemaRecord{SchemaType: p.schemaType, Hash: p.hash, Norm: p.norm, References: w.req.References}
	if len(p.canonical) <= w.r.inlineLimit {
		rec.Schema = p.canonical
	} else {
		if w.r.s3 == nil {
			return "", errInvalidSchema("schema is %d bytes, schemas above %d bytes need S3 storage and S3 is not configured", len(p.canonical), w.r.inlineLimit)
		}
		sum := sha256.Sum256([]byte(p.canonical))
		rec.S3Key = w.r.namespace + "/@schemas/sha256/" + hex.EncodeToString(sum[:])
		if w.uploaded != rec.S3Key {
			if err := w.r.s3.UploadSegment(ctx, rec.S3Key, []byte(p.canonical)); err != nil {
				return "", fmt.Errorf("write schema text to S3: %w", err)
			}
			w.uploaded = rec.S3Key
			w.writes++
		}
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (r *Registry) Register(ctx context.Context, subject string, req RegisterRequest) (*VersionedSchema, error) {
	if err := validSubject(subject); err != nil {
		return nil, err
	}
	if req.ID < 0 || req.Version < 0 {
		return nil, errBadRequest("id and version must not be negative")
	}
	w := &recordWriter{r: r, req: req}
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			r.conflicts.Add(1)
			if err := backoff(ctx, attempt); err != nil {
				return nil, errTimeout(fmt.Errorf("register on subject %s gave up after %d attempts: %w", subject, attempt, err))
			}
		}
		p, err := r.prepare(ctx, req)
		if err != nil {
			return nil, storeErr(ctx, err)
		}
		w.p = p
		id, version, err := r.tryRegister(ctx, subject, req, p, w)
		if err != nil {
			return nil, storeErr(ctx, err)
		}
		if id > 0 {
			out, err := r.versioned(ctx, subject, version, id)
			if err != nil {
				return nil, storeErr(ctx, err)
			}
			return out, nil
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

func topVersion(versions map[int]versionEntry) int {
	top := 0
	for v := range versions {
		top = max(top, v)
	}
	return top
}

func (r *Registry) subjectMatch(ctx context.Context, versions map[int]versionEntry, p *prepared, deleted bool) (int, int64, error) {
	vs := sortedVersions(versions, deleted)
	for i := len(vs) - 1; i >= 0; i-- {
		id := versions[vs[i]].ID
		s, err := r.schemaByID(ctx, id)
		if err != nil {
			return 0, 0, err
		}
		if s != nil && s.Type == p.schemaType && s.Norm == p.norm {
			return vs[i], id, nil
		}
	}
	return 0, 0, nil
}

func (r *Registry) commit(ctx context.Context, cmps []clientv3.Cmp, ops []clientv3.Op) (bool, error) {
	if r.beforeCommit != nil {
		r.beforeCommit()
	}
	resp, err := r.cli.Txn(ctx).If(cmps...).Then(ops...).Commit()
	if err != nil {
		return false, err
	}
	r.observe(resp.Header)
	return resp.Succeeded, nil
}

func (r *Registry) tryRegister(ctx context.Context, subject string, req RegisterRequest, p *prepared, w *recordWriter) (int64, int, error) {
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
	versions, err := snap.versions(subject)
	if err != nil {
		return 0, 0, err
	}
	if req.ID == 0 && req.Version == 0 {
		if v, id, err := r.subjectMatch(ctx, versions, p, false); err != nil || v > 0 {
			return id, v, err
		}
	}
	mode := effective(snap, modeKey(subject), modeKey(""), ModeReadWrite)
	switch mode {
	case ModeReadOnly:
		return 0, 0, errNotPermitted("Subject %s is in read-only mode", subject)
	case ModeImport:
		if req.ID == 0 {
			return 0, 0, errNotPermitted("Subject %s is in import mode, register with an explicit id", subject)
		}
		return r.tryImport(ctx, subject, req, p, w, snap)
	}
	if req.ID > 0 || req.Version > 0 {
		return 0, 0, errNotPermitted("Schema id and version can only be set in IMPORT mode")
	}
	cmps := snap.fence(lk, modeKey(""), modeKey(subject), configKey(""), configKey(subject))
	cmps = append(cmps, refFence(p.refRevs)...)
	var ops []clientv3.Op
	var id int64
	if kv := snap[hk]; kv != nil {
		if id, err = strconv.ParseInt(string(kv.Value), 10, 64); err != nil {
			return 0, 0, err
		}
		cmps = append(cmps, snap.fence(hk)...)
	} else {
		n, err := snap.int(counterKey())
		if err != nil {
			return 0, 0, err
		}
		if id, err = w.claim(ctx, n); err != nil {
			return 0, 0, err
		}
		record, err := w.record(ctx)
		if err != nil {
			return 0, 0, err
		}
		cmps = append(cmps, snap.fence(counterKey())...)
		cmps = append(cmps,
			clientv3.Compare(clientv3.CreateRevision(hk), "=", 0),
			clientv3.Compare(clientv3.CreateRevision(idKey(id)), "=", 0))
		ops = append(ops,
			clientv3.OpPut(idKey(id), record),
			clientv3.OpPut(hk, strconv.FormatInt(id, 10)),
			clientv3.OpPut(guidKey(p.guid), strconv.FormatInt(id, 10)),
			clientv3.OpPut(counterKey(), strconv.FormatInt(id, 10)))
	}
	live := sortedVersions(versions, false)
	if len(live) > 0 && effective(snap, configKey(subject), configKey(""), CompatBackward) == CompatBackward {
		prev := live[len(live)-1]
		if err := r.checkAgainst(ctx, subject, p.canonical, p.refs, prev, versions[prev].ID); err != nil {
			return 0, 0, err
		}
		cmps = append(cmps, snap.fence(versionKey(subject, prev))...)
	}
	top := topVersion(versions)
	version := top + 1
	if top > 0 {
		cmps = append(cmps, snap.fence(versionKey(subject, top))...)
	}
	for v, e := range versions {
		if e.Deleted && e.ID == id {
			cmps = append(cmps, snap.fence(versionKey(subject, v))...)
			ops = append(ops, clientv3.OpDelete(versionKey(subject, v)), clientv3.OpDelete(idSubjectKey(id, subject, v)))
			for _, ref := range req.References {
				ops = append(ops, clientv3.OpDelete(refByKey(ref, subject, v)))
			}
		}
	}
	cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(versionKey(subject, version)), "=", 0))
	ops = append(ops,
		clientv3.OpPut(versionKey(subject, version), versionValue(id)),
		clientv3.OpPut(lk, strconv.Itoa(version)),
		clientv3.OpPut(idSubjectKey(id, subject, version), ""))
	ops = append(ops, refOps(req.References, subject, version)...)
	ok, err := r.commit(ctx, cmps, ops)
	if err != nil || !ok {
		return 0, 0, err
	}
	return id, version, nil
}

func (r *Registry) checkAgainst(ctx context.Context, subject, text string, refs []resolvedReference, prevVersion int, prevID int64) error {
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
		return errIncompatible(subject, prevVersion, prev.Text, err.Error())
	}
	return nil
}

func (r *Registry) tryImport(ctx context.Context, subject string, req RegisterRequest, p *prepared, w *recordWriter, snap snapshot) (int64, int, error) {
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
		if existing == nil || existing.Type != p.schemaType || existing.Text != p.canonical || !slices.Equal(existing.References, req.References) {
			return 0, 0, errNotPermitted("Schema id %d already holds a different schema", id)
		}
		cmps = append(cmps, snap.fence(ik)...)
	} else {
		n, err := snap.int(counterKey())
		if err != nil {
			return 0, 0, err
		}
		if err := w.claimImport(ctx, id); err != nil {
			return 0, 0, err
		}
		record, err := w.record(ctx)
		if err != nil {
			return 0, 0, err
		}
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(ik), "=", 0))
		ops = append(ops, clientv3.OpPut(ik, record))
		hk := hashKey(p.schemaType, p.hash)
		if snap[hk] == nil {
			cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(hk), "=", 0))
			ops = append(ops, clientv3.OpPut(hk, strconv.FormatInt(id, 10)), clientv3.OpPut(guidKey(p.guid), strconv.FormatInt(id, 10)))
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
	top := topVersion(versions)
	version := req.Version
	if version == 0 {
		if len(ops) == 0 {
			live := sortedVersions(versions, false)
			for i := len(live) - 1; i >= 0; i-- {
				if versions[live[i]].ID == id {
					return id, live[i], nil
				}
			}
		}
		version = top + 1
		if top > 0 {
			cmps = append(cmps, snap.fence(versionKey(subject, top))...)
		}
	}
	if e, ok := versions[version]; ok {
		if e.ID != id {
			return 0, 0, errNotPermitted("Version %d of subject %s already holds schema id %d", version, subject, e.ID)
		}
		return id, version, nil
	}
	cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(versionKey(subject, version)), "=", 0))
	ops = append(ops,
		clientv3.OpPut(versionKey(subject, version), versionValue(id)),
		clientv3.OpPut(idSubjectKey(id, subject, version), ""))
	if version > top {
		ops = append(ops, clientv3.OpPut(lk, strconv.Itoa(version)))
	}
	ops = append(ops, refOps(req.References, subject, version)...)
	ok, err := r.commit(ctx, cmps, ops)
	if err != nil || !ok {
		return 0, 0, err
	}
	return id, version, nil
}
