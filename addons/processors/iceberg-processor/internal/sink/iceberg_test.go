package sink

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/config"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/decoder"
)

func TestRecordsToArrow(t *testing.T) {
	iceSchema := defaultSchema()
	arrowSchema, err := tableSchemaToArrow(iceSchema)
	if err != nil {
		t.Fatalf("schema conversion: %v", err)
	}

	records := []Record{
		{
			Topic:     "orders",
			Partition: 0,
			Offset:    10,
			Timestamp: 1234,
			Key:       []byte("k1"),
			Value:     []byte("v1"),
			Headers:   []decoder.Header{{Key: "h1", Value: []byte("v1")}},
		},
	}

	rdr, err := recordsToArrow(arrowSchema, nil, records)
	if err != nil {
		t.Fatalf("recordsToArrow: %v", err)
	}
	defer rdr.Release()

	if !rdr.Next() {
		t.Fatalf("expected record batch")
	}

	batch := rdr.RecordBatch()
	if batch.NumCols() != int64(arrowSchema.NumFields()) {
		t.Fatalf("unexpected column count: %d", batch.NumCols())
	}

	col := batch.Column(1).(*array.String)
	if col.Value(0) != "orders" {
		t.Fatalf("unexpected topic: %s", col.Value(0))
	}
}

func TestRecordsToArrowWithColumns(t *testing.T) {
	iceSchema := iceberg.NewSchema(1,
		append(baseFields(),
			iceberg.NestedField{ID: 9, Name: "order_id", Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 10, Name: "status", Type: iceberg.PrimitiveTypes.String},
		)...)
	arrowSchema, err := tableSchemaToArrow(iceSchema)
	if err != nil {
		t.Fatalf("schema conversion: %v", err)
	}

	records := []Record{
		{
			Topic:     "orders",
			Partition: 0,
			Offset:    10,
			Timestamp: 1234,
			Value:     []byte(`{"order_id":42,"status":"new"}`),
		},
	}
	columns := []config.Column{
		{Name: "order_id", Type: "long"},
		{Name: "status", Type: "string"},
	}

	rdr, err := recordsToArrow(arrowSchema, columns, records)
	if err != nil {
		t.Fatalf("recordsToArrow: %v", err)
	}
	defer rdr.Release()
	if !rdr.Next() {
		t.Fatalf("expected record batch")
	}

	batch := rdr.RecordBatch()
	orderID := batch.Column(8).(*array.Int64)
	if orderID.Value(0) != 42 {
		t.Fatalf("unexpected order_id: %d", orderID.Value(0))
	}
	status := batch.Column(9).(*array.String)
	if status.Value(0) != "new" {
		t.Fatalf("unexpected status: %s", status.Value(0))
	}
}

func TestEnsureTablePathsBackfill(t *testing.T) {
	ident := table.Identifier{"demo", "demo_topic_1"}
	meta, err := table.NewMetadata(defaultSchema(), iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "", iceberg.Properties{})
	if err != nil {
		t.Fatalf("metadata init: %v", err)
	}
	cat := &fakeCatalog{meta: meta}
	tbl := table.New(ident, meta, "", func(context.Context) (io.IO, error) { return nil, nil }, cat)

	writer := &icebergWriter{
		catalog:   cat,
		warehouse: "s3://kafscale-snapshots/iceberg",
	}
	updated, err := writer.ensureTablePaths(context.Background(), tbl, ident)
	if err != nil {
		t.Fatalf("ensureTablePaths: %v", err)
	}
	if updated == nil {
		t.Fatalf("expected updated table")
	}

	props := updated.Properties()
	if props[table.WriteDataPathKey] != "s3://kafscale-snapshots/iceberg/demo/demo_topic_1/data" {
		t.Fatalf("unexpected data path: %s", props[table.WriteDataPathKey])
	}
	if props[table.WriteMetadataPathKey] != "s3://kafscale-snapshots/iceberg/demo/demo_topic_1/metadata" {
		t.Fatalf("unexpected metadata path: %s", props[table.WriteMetadataPathKey])
	}
}

func TestEnsureTablePathsNoopWhenSet(t *testing.T) {
	ident := table.Identifier{"demo", "demo_topic_1"}
	props := iceberg.Properties{
		table.WriteDataPathKey:     "s3://kafscale-snapshots/iceberg/demo/demo_topic_1/data",
		table.WriteMetadataPathKey: "s3://kafscale-snapshots/iceberg/demo/demo_topic_1/metadata",
	}
	meta, err := table.NewMetadata(defaultSchema(), iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "s3://kafscale-snapshots/iceberg/demo/demo_topic_1", props)
	if err != nil {
		t.Fatalf("metadata init: %v", err)
	}
	cat := &fakeCatalog{meta: meta}
	tbl := table.New(ident, meta, "", func(context.Context) (io.IO, error) { return nil, nil }, cat)

	writer := &icebergWriter{
		catalog:   cat,
		warehouse: "s3://kafscale-snapshots/iceberg",
	}
	updated, err := writer.ensureTablePaths(context.Background(), tbl, ident)
	if err != nil {
		t.Fatalf("ensureTablePaths: %v", err)
	}
	if updated != nil {
		t.Fatalf("expected no table update when paths already set")
	}
	if cat.commitCalls != 0 {
		t.Fatalf("expected no commits, got %d", cat.commitCalls)
	}
}

func TestHasSnapshotDataFilesAdded(t *testing.T) {
	tbl := tableWithSnapshotSummary(t, iceberg.Properties{
		"added-data-files": "1",
	})
	if !hasSnapshotDataFiles(tbl) {
		t.Fatalf("expected added data files to be detected")
	}
}

func TestHasSnapshotDataFilesZeroAdded(t *testing.T) {
	tbl := tableWithSnapshotSummary(t, iceberg.Properties{
		"added-data-files": "0",
	})
	if hasSnapshotDataFiles(tbl) {
		t.Fatalf("expected zero added data files to be treated as empty")
	}
}

func TestHasSnapshotDataFilesTotalFallback(t *testing.T) {
	tbl := tableWithSnapshotSummary(t, iceberg.Properties{
		"total-data-files": "3",
	})
	if !hasSnapshotDataFiles(tbl) {
		t.Fatalf("expected total-data-files fallback to be used")
	}
}

func TestHasSnapshotDataFilesMissingSummary(t *testing.T) {
	tbl := tableWithSnapshot(t, nil)
	if !hasSnapshotDataFiles(tbl) {
		t.Fatalf("expected missing summary to be treated as unknown data files")
	}
}

func TestLoadTableRefreshesSchema(t *testing.T) {
	ident := table.Identifier{"demo", "demo_topic_1"}
	meta, err := table.NewMetadata(defaultSchema(), iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "s3://bucket/demo/demo_topic_1", iceberg.Properties{})
	if err != nil {
		t.Fatalf("metadata init: %v", err)
	}
	cat := &fakeCatalog{meta: meta}

	writer := &icebergWriter{
		catalog:   cat,
		warehouse: "s3://bucket",
		mappings: map[string]tableMapping{
			"demo-topic": {
				identifier: ident,
				autoCreate: false,
				mode:       "append",
				schema: config.MappingSchemaConfig{
					Source: "mapping",
					Columns: []config.Column{
						{Name: "first", Type: "string"},
					},
				},
			},
		},
		schemas: make(map[string]*topicSchema),
		tables:  make(map[string]*table.Table),
	}

	_, schema, err := writer.loadTable(context.Background(), "demo-topic")
	if err != nil {
		t.Fatalf("loadTable: %v", err)
	}
	if len(schema.columns) != 1 || schema.columns[0].Name != "first" {
		t.Fatalf("unexpected initial columns: %+v", schema.columns)
	}

	writer.mappings["demo-topic"] = tableMapping{
		identifier: ident,
		autoCreate: false,
		mode:       "append",
		schema: config.MappingSchemaConfig{
			Source: "mapping",
			Columns: []config.Column{
				{Name: "first", Type: "string"},
				{Name: "second", Type: "string"},
			},
		},
	}

	_, schema, err = writer.loadTable(context.Background(), "demo-topic")
	if err != nil {
		t.Fatalf("loadTable refresh: %v", err)
	}
	if len(schema.columns) != 2 {
		t.Fatalf("expected schema refresh to include new column, got %+v", schema.columns)
	}
}

func tableSchemaToArrow(schema *iceberg.Schema) (*arrow.Schema, error) {
	return table.SchemaToArrowSchema(schema, nil, true, false)
}

func tableWithSnapshotSummary(t *testing.T, props iceberg.Properties) *table.Table {
	t.Helper()
	return tableWithSnapshot(t, &table.Summary{
		Operation:  table.OpAppend,
		Properties: props,
	})
}

func tableWithSnapshot(t *testing.T, summary *table.Summary) *table.Table {
	t.Helper()

	ident := table.Identifier{"demo", "demo_topic_1"}
	meta, err := table.NewMetadata(defaultSchema(), iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "s3://bucket/demo/demo_topic_1", iceberg.Properties{})
	if err != nil {
		t.Fatalf("metadata init: %v", err)
	}
	builder, err := table.MetadataBuilderFromBase(meta, "")
	if err != nil {
		t.Fatalf("metadata builder: %v", err)
	}
	snap := table.Snapshot{
		SnapshotID:     1,
		SequenceNumber: 1,
		TimestampMs:    time.Now().UnixMilli(),
		Summary:        summary,
	}
	if err := builder.AddSnapshot(&snap); err != nil {
		t.Fatalf("add snapshot: %v", err)
	}
	if err := builder.SetSnapshotRef(table.MainBranch, snap.SnapshotID, table.BranchRef); err != nil {
		t.Fatalf("set snapshot ref: %v", err)
	}
	meta, err = builder.Build()
	if err != nil {
		t.Fatalf("build metadata: %v", err)
	}

	return table.New(ident, meta, "", func(context.Context) (io.IO, error) { return nil, nil }, &fakeCatalog{meta: meta})
}

type fakeCatalog struct {
	meta        table.Metadata
	commitCalls int
}

func (f *fakeCatalog) CatalogType() catalog.Type { return catalog.REST }

func (f *fakeCatalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeCatalog) CommitTable(ctx context.Context, identifier table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	f.commitCalls++
	builder, err := table.MetadataBuilderFromBase(f.meta, "")
	if err != nil {
		return nil, "", err
	}
	for _, update := range updates {
		if err := update.Apply(builder); err != nil {
			return nil, "", err
		}
	}
	meta, err := builder.Build()
	if err != nil {
		return nil, "", err
	}
	f.meta = meta
	return meta, "", nil
}

func (f *fakeCatalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(func(table.Identifier, error) bool) {}
}

func (f *fakeCatalog) LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	return table.New(identifier, f.meta, "", func(context.Context) (io.IO, error) { return nil, nil }, f), nil
}

func (f *fakeCatalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	return errors.New("not implemented")
}

func (f *fakeCatalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeCatalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	return false, errors.New("not implemented")
}

func (f *fakeCatalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeCatalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	return errors.New("not implemented")
}

func (f *fakeCatalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	return errors.New("not implemented")
}

func (f *fakeCatalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	return false, errors.New("not implemented")
}

func (f *fakeCatalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeCatalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	return catalog.PropertiesUpdateSummary{}, errors.New("not implemented")
}
