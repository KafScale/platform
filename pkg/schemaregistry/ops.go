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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/url"
	"slices"
	"strconv"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const latestVersion = -1

func ParseVersion(raw string) (int, error) {
	if raw == "latest" {
		return latestVersion, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || (v != latestVersion && (v <= 0 || v > math.MaxInt32)) {
		return 0, errInvalidVersion(raw)
	}
	return v, nil
}

func (r *Registry) subjectSnapshot(ctx context.Context, subject string) (snapshot, map[int]versionEntry, error) {
	if err := validSubject(subject); err != nil {
		return nil, nil, err
	}
	snap, err := r.read(ctx, []string{modeKey(""), modeKey(subject)}, []string{subjectPrefix(subject)})
	if err != nil {
		return nil, nil, err
	}
	versions, err := snap.versions(subject)
	if err != nil {
		return nil, nil, err
	}
	return snap, versions, nil
}

func (r *Registry) versioned(ctx context.Context, subject string, version int, id int64) (*VersionedSchema, error) {
	s, err := r.schemaByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, fmt.Errorf("schema %d for subject %s version %d is missing", id, subject, version)
	}
	return &VersionedSchema{Subject: subject, Version: version, Schema: s}, nil
}

func pickVersion(subject string, versions map[int]versionEntry, version int, deleted bool) (int, error) {
	visible := sortedVersions(versions, deleted)
	if len(visible) == 0 {
		return 0, errSubjectNotFound(subject)
	}
	if version == latestVersion {
		return visible[len(visible)-1], nil
	}
	e, ok := versions[version]
	if !ok || (e.Deleted && !deleted) {
		return 0, errVersionNotFound(version)
	}
	return version, nil
}

func (r *Registry) Version(ctx context.Context, subject string, version int, deleted bool) (*VersionedSchema, error) {
	_, versions, err := r.subjectSnapshot(ctx, subject)
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	v, err := pickVersion(subject, versions, version, deleted)
	if err != nil {
		return nil, err
	}
	out, err := r.versioned(ctx, subject, v, versions[v].ID)
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	out.Deleted = versions[v].Deleted
	return out, nil
}

func (r *Registry) Versions(ctx context.Context, subject string, deleted bool) ([]int, error) {
	_, versions, err := r.subjectSnapshot(ctx, subject)
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	out := sortedVersions(versions, deleted)
	if len(out) == 0 {
		return nil, errSubjectNotFound(subject)
	}
	return out, nil
}

func (r *Registry) Subjects(ctx context.Context, deleted bool) ([]string, error) {
	resp, err := r.cli.Get(ctx, subjectsPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	visible := make(map[string]bool)
	for _, kv := range resp.Kvs {
		escaped, rest, ok := strings.Cut(strings.TrimPrefix(string(kv.Key), subjectsPrefix()), "/")
		if !ok || !strings.HasPrefix(rest, "versions/") {
			continue
		}
		subject, err := url.PathUnescape(escaped)
		if err != nil {
			return nil, errStore(fmt.Errorf("bad subject key %q", kv.Key))
		}
		var rec versionRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			return nil, errStore(fmt.Errorf("decode %q: %w", kv.Key, err))
		}
		if deleted || !rec.Deleted {
			visible[subject] = true
		}
	}
	out := make([]string, 0, len(visible))
	for subject := range visible {
		out = append(out, subject)
	}
	slices.Sort(out)
	return out, nil
}

func (r *Registry) IDVersions(ctx context.Context, id int64, deleted bool) ([]SubjectVersion, error) {
	if _, err := r.SchemaByID(ctx, id); err != nil {
		return nil, err
	}
	resp, err := r.cli.Get(ctx, idSubjectsPrefix(id), clientv3.WithPrefix())
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	out := make([]SubjectVersion, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		escaped, rawVersion, ok := strings.Cut(strings.TrimPrefix(string(kv.Key), idSubjectsPrefix(id)), "/")
		subject, err := url.PathUnescape(escaped)
		if !ok || err != nil {
			return nil, errStore(fmt.Errorf("bad id subject key %q", kv.Key))
		}
		version, err := strconv.Atoi(rawVersion)
		if err != nil {
			return nil, errStore(fmt.Errorf("bad id subject key %q", kv.Key))
		}
		if !deleted {
			vr, err := r.cli.Get(ctx, versionKey(subject, version))
			if err != nil {
				return nil, storeErr(ctx, err)
			}
			var rec versionRecord
			if len(vr.Kvs) == 0 {
				continue
			}
			if err := json.Unmarshal(vr.Kvs[0].Value, &rec); err != nil {
				return nil, errStore(err)
			}
			if rec.Deleted {
				continue
			}
		}
		out = append(out, SubjectVersion{Subject: subject, Version: version})
	}
	slices.SortFunc(out, func(a, b SubjectVersion) int {
		if c := strings.Compare(a.Subject, b.Subject); c != 0 {
			return c
		}
		return a.Version - b.Version
	})
	return out, nil
}

func (r *Registry) Lookup(ctx context.Context, subject string, req RegisterRequest, deleted bool) (*VersionedSchema, error) {
	_, versions, err := r.subjectSnapshot(ctx, subject)
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	if len(sortedVersions(versions, deleted)) == 0 {
		return nil, errSubjectNotFound(subject)
	}
	p, err := r.prepare(ctx, req)
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	v, id, err := r.subjectMatch(ctx, versions, p, deleted)
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	if v == 0 {
		return nil, errSchemaNotFound()
	}
	out, err := r.versioned(ctx, subject, v, id)
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	out.Deleted = versions[v].Deleted
	return out, nil
}

func (r *Registry) Compatible(ctx context.Context, subject string, version int, req RegisterRequest) (bool, []string, error) {
	if err := validSubject(subject); err != nil {
		return false, nil, err
	}
	snap, err := r.read(ctx, []string{configKey(""), configKey(subject)}, []string{subjectPrefix(subject)})
	if err != nil {
		return false, nil, storeErr(ctx, err)
	}
	versions, err := snap.versions(subject)
	if err != nil {
		return false, nil, errStore(err)
	}
	level := effective(snap, configKey(subject), configKey(""), CompatBackward)
	var targets []int
	switch {
	case version == 0:
		targets = compatTargets(level, versions)
	case version == latestVersion && len(sortedVersions(versions, false)) == 0:
	case version > 0 && len(sortedVersions(versions, false)) == 0:
		return false, nil, errVersionNotFound(version)
	default:
		v, err := pickVersion(subject, versions, version, false)
		if err != nil {
			return false, nil, err
		}
		if level != CompatNone {
			targets = []int{v}
		}
	}
	p, err := r.prepare(ctx, req)
	if err != nil {
		return false, nil, storeErr(ctx, err)
	}
	for _, v := range targets {
		err = r.checkAgainst(ctx, subject, level, p.canonical, p.refs, v, versions[v].ID)
		var regErr *Error
		if errors.As(err, &regErr) && regErr.Code == 40901 {
			return false, regErr.details, nil
		}
		if err != nil {
			return false, nil, storeErr(ctx, err)
		}
	}
	return true, nil, nil
}

func (r *Registry) ReferencedBy(ctx context.Context, subject string, version int) ([]int64, error) {
	_, versions, err := r.subjectSnapshot(ctx, subject)
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	v, err := pickVersion(subject, versions, version, false)
	if err != nil {
		return nil, err
	}
	resp, err := r.cli.Get(ctx, refByPrefix(subject, v), clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	seen := map[int64]bool{}
	out := []int64{}
	for _, kv := range resp.Kvs {
		escaped, rawVersion, ok := strings.Cut(strings.TrimPrefix(string(kv.Key), refByPrefix(subject, v)), "/")
		refSubject, err := url.PathUnescape(escaped)
		if !ok || err != nil {
			return nil, errStore(fmt.Errorf("bad reference key %q", kv.Key))
		}
		vr, err := r.cli.Get(ctx, versionKey(refSubject, mustAtoi(rawVersion)))
		if err != nil {
			return nil, storeErr(ctx, err)
		}
		if len(vr.Kvs) == 0 {
			continue
		}
		var rec versionRecord
		if err := json.Unmarshal(vr.Kvs[0].Value, &rec); err != nil {
			return nil, errStore(err)
		}
		if !seen[rec.ID] {
			seen[rec.ID] = true
			out = append(out, rec.ID)
		}
	}
	slices.Sort(out)
	return out, nil
}

func mustAtoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

type SchemaFilter struct {
	SubjectPrefix string
	Deleted       bool
	LatestOnly    bool
	Offset        int
	Limit         int
}

func (r *Registry) Schemas(ctx context.Context, f SchemaFilter) ([]*VersionedSchema, error) {
	resp, err := r.cli.Get(ctx, subjectsPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, storeErr(ctx, err)
	}
	type entry struct {
		subject string
		version int
		rec     versionRecord
	}
	var all []entry
	for _, kv := range resp.Kvs {
		escaped, rest, ok := strings.Cut(strings.TrimPrefix(string(kv.Key), subjectsPrefix()), "/")
		raw, isVersion := strings.CutPrefix(rest, "versions/")
		if !ok || !isVersion {
			continue
		}
		subject, err := url.PathUnescape(escaped)
		if err != nil {
			return nil, errStore(fmt.Errorf("bad subject key %q", kv.Key))
		}
		if !strings.HasPrefix(subject, f.SubjectPrefix) {
			continue
		}
		var rec versionRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			return nil, errStore(fmt.Errorf("decode %q: %w", kv.Key, err))
		}
		if rec.Deleted && !f.Deleted {
			continue
		}
		all = append(all, entry{subject: subject, version: mustAtoi(raw), rec: rec})
	}
	slices.SortFunc(all, func(a, b entry) int {
		if c := strings.Compare(a.subject, b.subject); c != 0 {
			return c
		}
		return a.version - b.version
	})
	if f.LatestOnly {
		var latest []entry
		for i, e := range all {
			if i+1 == len(all) || all[i+1].subject != e.subject {
				latest = append(latest, e)
			}
		}
		all = latest
	}
	if f.Offset > 0 {
		all = all[min(f.Offset, len(all)):]
	}
	if f.Limit > 0 {
		all = all[:min(f.Limit, len(all))]
	}
	out := make([]*VersionedSchema, 0, len(all))
	for _, e := range all {
		v, err := r.versioned(ctx, e.subject, e.version, e.rec.ID)
		if err != nil {
			return nil, storeErr(ctx, err)
		}
		v.Deleted = e.rec.Deleted
		out = append(out, v)
	}
	return out, nil
}

func (r *Registry) retry(ctx context.Context, try func() (bool, error)) error {
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			r.conflicts.Add(1)
			if err := backoff(ctx, attempt); err != nil {
				return errTimeout(fmt.Errorf("gave up after %d attempts: %w", attempt, err))
			}
		}
		done, err := try()
		if err != nil {
			return storeErr(ctx, err)
		}
		if done {
			return nil
		}
	}
}

func (r *Registry) checkWritable(snap snapshot, subject string) error {
	if effective(snap, modeKey(subject), modeKey(""), ModeReadWrite) == ModeReadOnly {
		return errNotPermitted("Subject %s is in read-only mode", subject)
	}
	return nil
}

func (r *Registry) checkUnreferenced(ctx context.Context, subject string, version int) (clientv3.Cmp, error) {
	prefix := refByPrefix(subject, version)
	resp, err := r.cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return clientv3.Cmp{}, err
	}
	if resp.Count > 0 {
		return clientv3.Cmp{}, errReferenceExists(subject, version)
	}
	return clientv3.Compare(clientv3.CreateRevision(prefix), "=", 0).WithPrefix(), nil
}

func (r *Registry) indexOps(ctx context.Context, subject string, version int, id int64) ([]clientv3.Op, error) {
	s, err := r.schemaByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, fmt.Errorf("schema %d is missing", id)
	}
	ops := []clientv3.Op{clientv3.OpDelete(idSubjectKey(id, subject, version))}
	for _, ref := range s.References {
		ops = append(ops, clientv3.OpDelete(refByKey(ref, subject, version)))
	}
	return ops, nil
}

func (r *Registry) DeleteSubject(ctx context.Context, subject string, permanent bool) ([]int, error) {
	var out []int
	err := r.retry(ctx, func() (bool, error) {
		snap, versions, err := r.subjectSnapshot(ctx, subject)
		if err != nil {
			return false, err
		}
		if len(versions) == 0 {
			return false, errSubjectNotFound(subject)
		}
		if err := r.checkWritable(snap, subject); err != nil {
			return false, err
		}
		live := sortedVersions(versions, false)
		all := sortedVersions(versions, true)
		cmps := snap.fence(modeKey(""), modeKey(subject), latestKey(subject))
		var ops []clientv3.Op
		if !permanent {
			if len(live) == 0 {
				return false, errSubjectSoftDeleted(subject)
			}
			for _, v := range live {
				ops = append(ops, clientv3.OpPut(versionKey(subject, v), deletedValue(versions[v].ID)))
			}
			out = live
		} else {
			if len(live) > 0 {
				return false, errSubjectNotSoftDeleted(subject)
			}
			for _, v := range all {
				removed, err := r.indexOps(ctx, subject, v, versions[v].ID)
				if err != nil {
					return false, err
				}
				ops = append(ops, removed...)
			}
			ops = append(ops, clientv3.OpDelete(subjectPrefix(subject), clientv3.WithPrefix()))
			out = all
		}
		for _, v := range all {
			cmp, err := r.checkUnreferenced(ctx, subject, v)
			if err != nil {
				return false, err
			}
			cmps = append(cmps, cmp)
			cmps = append(cmps, snap.fence(versionKey(subject, v))...)
		}
		resp, err := r.cli.Txn(ctx).If(cmps...).Then(ops...).Commit()
		if err != nil {
			return false, err
		}
		return resp.Succeeded, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func deletedValue(id int64) string {
	data, _ := json.Marshal(versionRecord{ID: id, Deleted: true})
	return string(data)
}

func (r *Registry) DeleteVersion(ctx context.Context, subject string, version int, permanent bool) (int, error) {
	var out int
	err := r.retry(ctx, func() (bool, error) {
		snap, versions, err := r.subjectSnapshot(ctx, subject)
		if err != nil {
			return false, err
		}
		v, err := pickVersion(subject, versions, version, permanent || version != latestVersion)
		if err != nil {
			return false, err
		}
		if err := r.checkWritable(snap, subject); err != nil {
			return false, err
		}
		e := versions[v]
		if !permanent && e.Deleted {
			return false, errVersionSoftDeleted(subject, v)
		}
		if permanent && !e.Deleted {
			return false, errVersionNotSoftDeleted(subject, v)
		}
		cmp, err := r.checkUnreferenced(ctx, subject, v)
		if err != nil {
			return false, err
		}
		cmps := append(snap.fence(modeKey(""), modeKey(subject), versionKey(subject, v), latestKey(subject)), cmp)
		ops := []clientv3.Op{clientv3.OpPut(versionKey(subject, v), deletedValue(e.ID))}
		if permanent {
			if ops, err = r.indexOps(ctx, subject, v, e.ID); err != nil {
				return false, err
			}
			ops = append(ops, clientv3.OpDelete(versionKey(subject, v)))
			rest := maps.Clone(versions)
			delete(rest, v)
			if top := topVersion(rest); top > 0 {
				ops = append(ops, clientv3.OpPut(latestKey(subject), strconv.Itoa(top)))
			} else {
				ops = append(ops, clientv3.OpDelete(latestKey(subject)))
			}
		}
		resp, err := r.cli.Txn(ctx).If(cmps...).Then(ops...).Commit()
		if err != nil {
			return false, err
		}
		out = v
		return resp.Succeeded, nil
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (r *Registry) getSetting(ctx context.Context, key string) (string, error) {
	resp, err := r.cli.Get(ctx, key)
	if err != nil {
		return "", storeErr(ctx, err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

func (r *Registry) setting(ctx context.Context, subject string, defaultToGlobal bool, key func(string) string, fallback string, missing func(string) *Error) (string, error) {
	if subject != "" {
		if err := validSubject(subject); err != nil {
			return "", err
		}
		v, err := r.getSetting(ctx, key(subject))
		if err != nil || v != "" {
			return v, err
		}
		if !defaultToGlobal {
			return "", missing(subject)
		}
	}
	v, err := r.getSetting(ctx, key(""))
	if err != nil || v != "" {
		return v, err
	}
	return fallback, nil
}

func (r *Registry) deleteSetting(ctx context.Context, subject string, key string, fallback string) (string, error) {
	if subject != "" {
		if err := validSubject(subject); err != nil {
			return "", err
		}
	}
	resp, err := r.cli.Delete(ctx, key, clientv3.WithPrevKV())
	if err != nil {
		return "", storeErr(ctx, err)
	}
	if len(resp.PrevKvs) == 0 {
		if subject != "" {
			return "", errSubjectNotFound(subject)
		}
		return fallback, nil
	}
	return string(resp.PrevKvs[0].Value), nil
}

func (r *Registry) Compatibility(ctx context.Context, subject string, defaultToGlobal bool) (string, error) {
	return r.setting(ctx, subject, defaultToGlobal, configKey, CompatBackward, errSubjectConfigNotFound)
}

func (r *Registry) SetCompatibility(ctx context.Context, subject, level string) error {
	level = strings.ToUpper(level)
	if _, ok := compatRules[level]; !ok {
		return errInvalidCompatibility(level)
	}
	if subject != "" {
		if err := validSubject(subject); err != nil {
			return err
		}
	}
	if _, err := r.cli.Put(ctx, configKey(subject), level); err != nil {
		return storeErr(ctx, err)
	}
	return nil
}

func (r *Registry) DeleteCompatibility(ctx context.Context, subject string) (string, error) {
	return r.deleteSetting(ctx, subject, configKey(subject), CompatBackward)
}

func (r *Registry) Mode(ctx context.Context, subject string, defaultToGlobal bool) (string, error) {
	return r.setting(ctx, subject, defaultToGlobal, modeKey, ModeReadWrite, errSubjectModeNotFound)
}

func (r *Registry) SetMode(ctx context.Context, subject, mode string, force bool) error {
	if mode != ModeReadWrite && mode != ModeReadOnly && mode != ModeImport {
		return errInvalidMode(mode)
	}
	if subject != "" {
		if err := validSubject(subject); err != nil {
			return err
		}
	}
	var cmps []clientv3.Cmp
	if mode == ModeImport && !force {
		empty := counterKey()
		if subject != "" {
			empty = latestKey(subject)
		}
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(empty), "=", 0))
	}
	resp, err := r.cli.Txn(ctx).If(cmps...).Then(clientv3.OpPut(modeKey(subject), mode)).Commit()
	if err != nil {
		return storeErr(ctx, err)
	}
	if !resp.Succeeded {
		if subject == "" {
			return errNotPermitted("Cannot import since the registry already holds schemas. Set force=true to override")
		}
		return errNotPermitted("Cannot import since subject %s already has versions. Set force=true to override", subject)
	}
	return nil
}

func (r *Registry) DeleteMode(ctx context.Context, subject string) (string, error) {
	return r.deleteSetting(ctx, subject, modeKey(subject), ModeReadWrite)
}

func (r *Registry) Ready(ctx context.Context) error {
	_, err := r.cli.Get(ctx, counterKey())
	return err
}
