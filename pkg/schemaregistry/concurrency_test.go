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
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
)

const (
	instances  = 4
	goroutines = 32
)

type result struct {
	subject string
	schema  string
	id      int64
	version int
}

func newInstances(t *testing.T) []*Registry {
	t.Helper()
	endpoints := testutil.StartEmbeddedEtcd(t)
	regs := make([]*Registry, instances)
	for i := range regs {
		reg, err := New(Config{Client: newClient(t, endpoints), InlineLimit: DefaultInlineLimit})
		if err != nil {
			t.Fatal(err)
		}
		regs[i] = reg
	}
	if err := regs[0].SetCompatibility(context.Background(), "", CompatNone); err != nil {
		t.Fatal(err)
	}
	return regs
}

func distinctSchema(n int) string {
	return fmt.Sprintf(`{"type":"record","name":"R%d","fields":[{"name":"f","type":"string"}]}`, n)
}

func registerAll(t *testing.T, regs []*Registry, work func(g int) []result) []result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var mu sync.Mutex
	var out []result
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, goroutines)
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			reg := regs[g%len(regs)]
			for _, r := range work(g) {
				id, version, err := reg.Register(ctx, r.subject, RegisterRequest{Schema: r.schema})
				if err != nil {
					errs <- fmt.Errorf("goroutine %d subject %s: %w", g, r.subject, err)
					return
				}
				r.id, r.version = id, version
				mu.Lock()
				out = append(out, r)
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	return out
}

func checkSubjects(t *testing.T, reg *Registry, results []result) {
	t.Helper()
	ctx := context.Background()
	bySubject := make(map[string]map[int]int64)
	for _, r := range results {
		if bySubject[r.subject] == nil {
			bySubject[r.subject] = make(map[int]int64)
		}
		if prev, ok := bySubject[r.subject][r.version]; ok && prev != r.id {
			t.Fatalf("subject %s version %d returned ids %d and %d", r.subject, r.version, prev, r.id)
		}
		bySubject[r.subject][r.version] = r.id
	}
	for subject, want := range bySubject {
		versions, err := reg.Versions(ctx, subject, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(versions) != len(want) {
			t.Fatalf("subject %s has versions %v, callers saw %d versions", subject, versions, len(want))
		}
		var prevID int64
		for i, v := range versions {
			if v != i+1 {
				t.Fatalf("subject %s versions %v are not 1..n", subject, versions)
			}
			got, err := reg.Version(ctx, subject, v, false)
			if err != nil {
				t.Fatal(err)
			}
			if got.Schema.ID != want[v] {
				t.Fatalf("subject %s version %d stored id %d, caller saw %d", subject, v, got.Schema.ID, want[v])
			}
			if got.Schema.ID <= prevID {
				t.Fatalf("subject %s ids not strictly increasing by version", subject)
			}
			prevID = got.Schema.ID
		}
	}
}

func TestConcurrentDistinctSchemasGetUniqueIncreasingIDs(t *testing.T) {
	regs := newInstances(t)
	const perGoroutine = 4
	results := registerAll(t, regs, func(g int) []result {
		out := make([]result, perGoroutine)
		for i := range out {
			out[i] = result{subject: fmt.Sprintf("s%d", g%4), schema: distinctSchema(g*perGoroutine + i)}
		}
		return out
	})
	if len(results) != goroutines*perGoroutine {
		t.Fatalf("got %d results", len(results))
	}
	ids := make([]int64, 0, len(results))
	for _, r := range results {
		ids = append(ids, r.id)
	}
	slices.Sort(ids)
	for i := 1; i < len(ids); i++ {
		if ids[i] == ids[i-1] {
			t.Fatalf("id %d given to two different schemas", ids[i])
		}
	}
	checkSubjects(t, regs[1], results)
	for _, r := range results {
		s, err := regs[2].SchemaByID(context.Background(), r.id)
		if err != nil {
			t.Fatal(err)
		}
		if s.Text != r.schema {
			t.Fatalf("id %d holds %s, want %s", r.id, s.Text, r.schema)
		}
	}
}

func TestConcurrentIdenticalSchemaSameSubject(t *testing.T) {
	regs := newInstances(t)
	results := registerAll(t, regs, func(int) []result {
		return []result{{subject: "same", schema: distinctSchema(1)}}
	})
	for _, r := range results {
		if r.id != results[0].id || r.version != 1 {
			t.Fatalf("got id %d version %d, want id %d version 1", r.id, r.version, results[0].id)
		}
	}
	checkSubjects(t, regs[0], results)
}

func TestConcurrentIdenticalSchemaManySubjects(t *testing.T) {
	regs := newInstances(t)
	results := registerAll(t, regs, func(g int) []result {
		return []result{
			{subject: fmt.Sprintf("subject-%d", g), schema: distinctSchema(1)},
			{subject: fmt.Sprintf("shared-%d", g%2), schema: distinctSchema(2)},
		}
	})
	ids := map[string]int64{}
	for _, r := range results {
		if prev, ok := ids[r.schema]; ok && prev != r.id {
			t.Fatalf("schema got ids %d and %d", prev, r.id)
		}
		ids[r.schema] = r.id
		if r.version != 1 {
			t.Fatalf("subject %s got version %d", r.subject, r.version)
		}
	}
	if len(ids) != 2 {
		t.Fatalf("ids %v", ids)
	}
	checkSubjects(t, regs[3], results)
}

func TestConcurrentMixedSchemas(t *testing.T) {
	regs := newInstances(t)
	results := registerAll(t, regs, func(g int) []result {
		return []result{
			{subject: "mixed", schema: distinctSchema(g / 2)},
			{subject: fmt.Sprintf("other-%d", g%3), schema: distinctSchema(g / 2)},
		}
	})
	ids := map[string]int64{}
	seen := map[int64]string{}
	for _, r := range results {
		if prev, ok := ids[r.schema]; ok && prev != r.id {
			t.Fatalf("schema got ids %d and %d", prev, r.id)
		}
		if prev, ok := seen[r.id]; ok && prev != r.schema {
			t.Fatalf("id %d given to two schemas", r.id)
		}
		ids[r.schema], seen[r.id] = r.id, r.schema
	}
	if len(ids) != goroutines/2 {
		t.Fatalf("got %d ids, want %d", len(ids), goroutines/2)
	}
	versions, err := regs[0].Versions(context.Background(), "mixed", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != goroutines/2 {
		t.Fatalf("mixed has versions %v, want %d", versions, goroutines/2)
	}
	for _, v := range versions {
		got, err := regs[0].Version(context.Background(), "mixed", v, false)
		if err != nil {
			t.Fatal(err)
		}
		if seen[got.Schema.ID] == "" {
			t.Fatalf("version %d holds id %d that no caller saw", v, got.Schema.ID)
		}
	}
}

func TestConcurrentImportAndRegister(t *testing.T) {
	regs := newInstances(t)
	ctx := context.Background()
	if err := regs[0].SetMode(ctx, "imported", ModeImport, false); err != nil {
		t.Fatal(err)
	}
	type claim struct {
		id     int64
		schema string
	}
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	claims := make(chan claim, goroutines)
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reg := regs[g%len(regs)]
			schema := distinctSchema(1000 + g)
			if g%2 == 0 {
				id := int64(1000 + g)
				_, _, err := reg.Register(ctx, "imported", RegisterRequest{Schema: schema, ID: id, Version: g + 1})
				var se *Error
				switch {
				case err == nil:
					claims <- claim{id, schema}
				case errors.As(err, &se) && se.Code == 42205:
				default:
					errs <- err
				}
				return
			}
			id, _, err := reg.Register(ctx, fmt.Sprintf("live-%d", g), RegisterRequest{Schema: schema})
			if err != nil {
				errs <- err
				return
			}
			claims <- claim{id, schema}
		}()
	}
	wg.Wait()
	close(errs)
	close(claims)
	for err := range errs {
		t.Fatal(err)
	}
	owner := map[int64]string{}
	var maxID int64
	for c := range claims {
		if prev, ok := owner[c.id]; ok && prev != c.schema {
			t.Fatalf("id %d given to two schemas", c.id)
		}
		owner[c.id] = c.schema
		maxID = max(maxID, c.id)
	}
	if len(owner) < goroutines/2 {
		t.Fatalf("only %d registrations succeeded", len(owner))
	}
	for id, schema := range owner {
		s, err := regs[1].SchemaByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if s.Text != schema {
			t.Fatalf("id %d holds %s, its caller registered %s", id, s.Text, schema)
		}
	}
	if err := regs[0].SetMode(ctx, "imported", ModeReadWrite, false); err != nil {
		t.Fatal(err)
	}
	id, _, err := regs[0].Register(ctx, "after", RegisterRequest{Schema: distinctSchema(5000)})
	if err != nil {
		t.Fatal(err)
	}
	if id <= maxID {
		t.Fatalf("id after import %d must be above the highest id %d", id, maxID)
	}
}

func TestConcurrentDeleteAndReferenceRegister(t *testing.T) {
	for range 10 {
		regs := newInstances(t)
		ctx := context.Background()
		if _, _, err := regs[0].Register(ctx, "addr", RegisterRequest{Schema: address}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var regErr, delErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, regErr = regs[1].Register(ctx, "person", RegisterRequest{Schema: person, References: []Reference{{Name: "example.Address", Subject: "addr", Version: 1}}})
		}()
		go func() {
			defer wg.Done()
			_, delErr = regs[2].DeleteVersion(ctx, "addr", 1, false)
		}()
		wg.Wait()
		if (regErr == nil) == (delErr == nil) {
			t.Fatalf("exactly one of register (%v) and delete (%v) must win", regErr, delErr)
		}
	}
}
