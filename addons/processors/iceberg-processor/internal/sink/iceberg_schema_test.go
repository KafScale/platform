package sink

import (
	"context"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/config"
)

func TestSchemaNeedsUpdateForNewColumn(t *testing.T) {
	current := iceberg.NewSchema(1, baseFields()...)
	desired := iceberg.NewSchema(2,
		append(baseFields(),
			iceberg.NestedField{ID: 9, Name: "order_id", Type: iceberg.PrimitiveTypes.Int64},
		)...)

	needsUpdate, err := schemaNeedsUpdate(current, desired, false)
	if err != nil {
		t.Fatalf("schemaNeedsUpdate: %v", err)
	}
	if !needsUpdate {
		t.Fatalf("expected schema update for new column")
	}
}

func TestSchemaNeedsUpdateRejectsIncompatibleType(t *testing.T) {
	current := iceberg.NewSchema(1,
		append(baseFields(),
			iceberg.NestedField{ID: 9, Name: "status", Type: iceberg.PrimitiveTypes.String},
		)...)
	desired := iceberg.NewSchema(2,
		append(baseFields(),
			iceberg.NestedField{ID: 9, Name: "status", Type: iceberg.PrimitiveTypes.Int64},
		)...)

	_, err := schemaNeedsUpdate(current, desired, false)
	if err == nil {
		t.Fatalf("expected error for incompatible type change")
	}
}

func TestSchemaNeedsUpdateAllowsWidening(t *testing.T) {
	current := iceberg.NewSchema(1,
		append(baseFields(),
			iceberg.NestedField{ID: 9, Name: "count", Type: iceberg.PrimitiveTypes.Int32},
		)...)
	desired := iceberg.NewSchema(2,
		append(baseFields(),
			iceberg.NestedField{ID: 9, Name: "count", Type: iceberg.PrimitiveTypes.Int64},
		)...)

	needsUpdate, err := schemaNeedsUpdate(current, desired, true)
	if err != nil {
		t.Fatalf("schemaNeedsUpdate: %v", err)
	}
	if !needsUpdate {
		t.Fatalf("expected schema update for widening")
	}
}

func TestResolveColumnsFromMapping(t *testing.T) {
	columns := []config.Column{
		{Name: "order_id", Type: "long"},
	}
	out, err := resolveColumns(context.Background(), config.SchemaConfig{}, config.MappingSchemaConfig{
		Source:  "mapping",
		Columns: columns,
	}, "orders")
	if err != nil {
		t.Fatalf("resolveColumns: %v", err)
	}
	if len(out) != 1 || out[0].Name != "order_id" {
		t.Fatalf("unexpected columns: %+v", out)
	}
}

func TestColumnsFromSchemaBytes(t *testing.T) {
	columns, err := columnsFromSchemaBytes([]byte(`{
  "type": "object",
  "properties": {
    "order_id": {"type": "integer"},
    "status": {"type": "string"},
    "price": {"type": "number"},
    "active": {"type": "boolean"},
    "ignored": {"type": "object"}
  },
  "required": ["order_id", "status"]
}`))
	if err != nil {
		t.Fatalf("columnsFromSchemaBytes: %v", err)
	}
	got := map[string]config.Column{}
	for _, col := range columns {
		got[col.Name] = col
	}

	expect := map[string]config.Column{
		"order_id": {Name: "order_id", Type: "long", Required: true},
		"status":   {Name: "status", Type: "string", Required: true},
		"price":    {Name: "price", Type: "double", Required: false},
		"active":   {Name: "active", Type: "boolean", Required: false},
	}
	if len(got) != len(expect) {
		t.Fatalf("unexpected column count: %d", len(got))
	}
	for name, exp := range expect {
		col, ok := got[name]
		if !ok {
			t.Fatalf("missing column %s", name)
		}
		if col.Type != exp.Type || col.Required != exp.Required {
			t.Fatalf("unexpected column %s: %+v", name, col)
		}
	}
}
