package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	restcatalog "github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/config"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/decoder"
)

const defaultTableSchemaID = 1

// New returns an Iceberg writer wired to the config mappings.
func New(cfg config.Config) (Writer, error) {
	props := iceberg.Properties{
		"type": cfg.Iceberg.Catalog.Type,
		"uri":  cfg.Iceberg.Catalog.URI,
	}
	if cfg.Iceberg.Warehouse != "" {
		props["warehouse"] = cfg.Iceberg.Warehouse
	}
	if cfg.Iceberg.Catalog.Token != "" {
		props["token"] = cfg.Iceberg.Catalog.Token
	}
	if cfg.Iceberg.Catalog.Username != "" || cfg.Iceberg.Catalog.Password != "" {
		credential := cfg.Iceberg.Catalog.Username
		if cfg.Iceberg.Catalog.Password != "" {
			credential = credential + ":" + cfg.Iceberg.Catalog.Password
		}
		props["credential"] = credential
	}
	if cfg.S3.Region != "" {
		props[iceio.S3Region] = cfg.S3.Region
	}
	if cfg.S3.Endpoint != "" {
		props[iceio.S3EndpointURL] = cfg.S3.Endpoint
	}
	if cfg.S3.PathStyle {
		props[iceio.S3ForceVirtualAddressing] = "false"
	} else if cfg.S3.Endpoint != "" {
		props[iceio.S3ForceVirtualAddressing] = "true"
	}

	var cat catalog.Catalog
	var err error
	if cfg.Iceberg.Catalog.Type == "rest" && os.Getenv("ICEBERG_PROCESSOR_REST_DEBUG") != "" {
		transport := &loggingTransport{base: http.DefaultTransport}
		opts := []restcatalog.Option{
			restcatalog.WithWarehouseLocation(cfg.Iceberg.Warehouse),
			restcatalog.WithCustomTransport(transport),
			restcatalog.WithAdditionalProps(props),
		}
		cat, err = restcatalog.NewCatalog(context.Background(), "rest", cfg.Iceberg.Catalog.URI, opts...)
	} else {
		if cfg.Iceberg.Catalog.Type == "rest" && cfg.Iceberg.Warehouse != "" {
			props["warehouse"] = strings.TrimRight(cfg.Iceberg.Warehouse, "/")
		}
		cat, err = catalog.Load(context.Background(), cfg.Iceberg.Catalog.Type, props)
	}
	if err != nil {
		return nil, err
	}

	mappings := make(map[string]tableMapping, len(cfg.Mappings))
	for _, mapping := range cfg.Mappings {
		mappings[mapping.Topic] = tableMapping{
			identifier: catalog.ToIdentifier(mapping.Table),
			autoCreate: mapping.CreateTableIfAbsent,
			mode:       mapping.Mode,
			schema:     mapping.Schema,
		}
	}

	return &icebergWriter{
		catalog:   cat,
		warehouse: strings.TrimRight(cfg.Iceberg.Warehouse, "/"),
		mappings:  mappings,
		registry:  cfg.Schema,
		schemas:   make(map[string]*topicSchema),
		tables:    make(map[string]*table.Table),
	}, nil
}

type tableMapping struct {
	identifier table.Identifier
	autoCreate bool
	mode       string
	schema     config.MappingSchemaConfig
}

type icebergWriter struct {
	catalog   catalog.Catalog
	warehouse string
	mappings  map[string]tableMapping
	registry  config.SchemaConfig
	schemas   map[string]*topicSchema

	mu        sync.Mutex
	tables    map[string]*table.Table
	initLocks map[string]*sync.Mutex
}

type topicSchema struct {
	iceberg *iceberg.Schema
	arrow   *arrow.Schema
	columns []config.Column
}

func (w *icebergWriter) Write(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}

	buckets := groupByTopic(records)
	for topic, topicRecords := range buckets {
		tbl, schema, err := w.loadTable(ctx, topic)
		if err != nil {
			return err
		}

		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			recordReader, err := recordsToArrow(schema.arrow, schema.columns, topicRecords)
			if err != nil {
				return err
			}
			updated, err := tbl.Append(ctx, recordReader, iceberg.Properties{
				"kafscale.commit.attempt": fmt.Sprintf("%d", attempt+1),
			})
			recordReader.Release()
			if err == nil {
				tbl = updated
				if hasSnapshotDataFiles(tbl) == false {
					return fmt.Errorf("iceberg commit produced no data files for topic %q", topic)
				}
				if reloaded, loadErr := loadTableWithRetry(ctx, w.catalog, w.mappings[topic].identifier, 3, 150*time.Millisecond); loadErr == nil {
					tbl = reloaded
					w.mu.Lock()
					w.tables[topic] = tbl
					w.mu.Unlock()
				}
				lastErr = nil
				break
			}
			lastErr = err
			if isCommitConflict(err) {
				reloaded, loadErr := loadTableWithRetry(ctx, w.catalog, w.mappings[topic].identifier, 5, 200*time.Millisecond)
				if loadErr == nil {
					tbl = reloaded
					w.mu.Lock()
					w.tables[topic] = tbl
					w.mu.Unlock()
				}
			}
			if err := sleepWithContext(ctx, time.Duration(attempt+1)*200*time.Millisecond); err != nil {
				return err
			}
		}
		if lastErr != nil {
			return lastErr
		}
	}

	return nil
}

func (w *icebergWriter) Close(ctx context.Context) error {
	return nil
}

func (w *icebergWriter) loadTable(ctx context.Context, topic string) (*table.Table, *topicSchema, error) {
	mapping, ok := w.mappings[topic]
	if !ok {
		return nil, nil, fmt.Errorf("no table mapping for topic %q", topic)
	}
	if mapping.mode != "append" {
		return nil, nil, fmt.Errorf("unsupported mapping mode %q for topic %q", mapping.mode, topic)
	}

	lock := w.topicLock(topic)
	lock.Lock()
	defer lock.Unlock()

	w.mu.Lock()
	tbl := w.tables[topic]
	schema := w.schemas[topic]
	w.mu.Unlock()
	if tbl != nil && schema != nil {
		updatedTbl, updatedSchema, err := w.ensureSchema(ctx, tbl, mapping, topic)
		if err != nil {
			return nil, nil, err
		}
		if updatedTbl != nil {
			tbl = updatedTbl
		}
		if updatedSchema != nil {
			schema = updatedSchema
		}
		w.mu.Lock()
		w.tables[topic] = tbl
		w.schemas[topic] = schema
		w.mu.Unlock()
		return tbl, schema, nil
	}

	if tbl == nil {
		var err error
		tbl, err = w.catalog.LoadTable(ctx, mapping.identifier)
		if err != nil {
			if errors.Is(err, catalog.ErrNoSuchTable) {
				if !mapping.autoCreate {
					return nil, nil, err
				}
				log.Printf("iceberg: creating table %v for topic %s", mapping.identifier, topic)
				tbl, err = w.createTableWithRetry(ctx, mapping, topic)
				if err != nil {
					log.Printf("iceberg: create table %v failed: %T %v", mapping.identifier, err, err)
					log.Printf("iceberg: create table %v error details: %+v", mapping.identifier, err)
					return nil, nil, err
				}
			} else {
				log.Printf("iceberg: load table %v failed: %T %v", mapping.identifier, err, err)
				log.Printf("iceberg: load table %v error details: %+v", mapping.identifier, err)
				return nil, nil, err
			}
		}
	}
	tbl, schema, err := w.ensureSchema(ctx, tbl, mapping, topic)
	if err != nil {
		return nil, nil, err
	}

	w.mu.Lock()
	w.tables[topic] = tbl
	if schema != nil {
		w.schemas[topic] = schema
	}
	w.mu.Unlock()
	return tbl, schema, nil
}

func (w *icebergWriter) topicLock(topic string) *sync.Mutex {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.initLocks == nil {
		w.initLocks = make(map[string]*sync.Mutex)
	}
	lock := w.initLocks[topic]
	if lock == nil {
		lock = &sync.Mutex{}
		w.initLocks[topic] = lock
	}
	return lock
}

func (w *icebergWriter) createTable(ctx context.Context, ident table.Identifier, schemaCfg config.MappingSchemaConfig, topic string) (*table.Table, error) {
	if len(ident) > 1 {
		namespace := ident[:len(ident)-1]
		if _, isRest := w.catalog.(*restcatalog.Catalog); isRest {
			if err := w.catalog.CreateNamespace(ctx, namespace, iceberg.Properties{}); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
				return nil, err
			}
		} else {
			exists, err := w.catalog.CheckNamespaceExists(ctx, namespace)
			if err != nil {
				return nil, err
			}
			if !exists {
				if err := w.catalog.CreateNamespace(ctx, namespace, iceberg.Properties{}); err != nil {
					return nil, err
				}
			}
		}
	}

	location := ""
	if w.warehouse != "" {
		location = w.warehouse + "/" + strings.Join(ident, "/")
	}
	props := iceberg.Properties{
		"write.format.default": "parquet",
	}
	if location != "" {
		props[table.WriteDataPathKey] = location + "/data"
		props[table.WriteMetadataPathKey] = location + "/metadata"
	}
	opts := []catalog.CreateTableOpt{
		catalog.WithProperties(props),
	}
	if location != "" {
		opts = append(opts, catalog.WithLocation(location))
	}

	desired, _, err := w.buildDesiredSchema(ctx, schemaCfg, topic, nil)
	if err != nil {
		return nil, err
	}
	return w.catalog.CreateTable(ctx, ident, desired, opts...)
}

func (w *icebergWriter) createTableWithRetry(ctx context.Context, mapping tableMapping, topic string) (*table.Table, error) {
	const attempts = 3
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		log.Printf("iceberg: create table attempt %d/%d for %v", i+1, attempts, mapping.identifier)
		tbl, err := w.createTable(ctx, mapping.identifier, mapping.schema, topic)
		if err == nil {
			log.Printf("iceberg: create table %v succeeded", mapping.identifier)
			return tbl, nil
		}
		if _, isRest := w.catalog.(*restcatalog.Catalog); isRest {
			if isCreateConflict(err) || errors.Is(err, catalog.ErrTableAlreadyExists) {
				log.Printf("iceberg: create table %v conflicted, retrying load", mapping.identifier)
			} else {
				log.Printf("iceberg: create table %v failed, attempting load", mapping.identifier)
			}
			loaded, loadErr := loadTableWithRetry(ctx, w.catalog, mapping.identifier, 20, 250*time.Millisecond)
			if loadErr == nil {
				log.Printf("iceberg: create table %v load succeeded", mapping.identifier)
				return loaded, nil
			}
			log.Printf("iceberg: create table %v load failed: %T %v", mapping.identifier, loadErr, loadErr)
		}
		if err := sleepWithContext(ctx, time.Duration(i+1)*300*time.Millisecond); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("create table %v failed after retries", mapping.identifier)
}

func isCreateConflict(err error) bool {
	if errors.Is(err, catalog.ErrTableAlreadyExists) || errors.Is(err, restcatalog.ErrCommitFailed) {
		return true
	}
	return strings.Contains(err.Error(), "branch main was created concurrently")
}

func isCommitConflict(err error) bool {
	if errors.Is(err, restcatalog.ErrCommitFailed) {
		return true
	}
	return strings.Contains(err.Error(), "branch main was created concurrently")
}

func hasSnapshotDataFiles(tbl *table.Table) bool {
	if tbl == nil {
		return false
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return false
	}
	if snap.Summary == nil || len(snap.Summary.Properties) == 0 {
		return true
	}
	if count, ok := summaryCount(snap.Summary.Properties, "added-data-files"); ok {
		return count > 0
	}
	if count, ok := summaryCount(snap.Summary.Properties, "total-data-files"); ok {
		return count > 0
	}
	return true
}

func summaryCount(props iceberg.Properties, key string) (int, bool) {
	raw := props[key]
	if raw == "" {
		return 0, false
	}
	count, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return count, true
}

func loadTableWithRetry(ctx context.Context, cat catalog.Catalog, ident table.Identifier, attempts int, baseDelay time.Duration) (*table.Table, error) {
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		tbl, err := cat.LoadTable(ctx, ident)
		if err == nil {
			return tbl, nil
		}
		lastErr = err
		delay := baseDelay * time.Duration(i+1)
		if delay > 2*time.Second {
			delay = 2 * time.Second
		}
		if err := sleepWithContext(ctx, delay); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type loggingTransport struct {
	base http.RoundTripper
}

func (t *loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		log.Printf("iceberg-rest http %s %s failed: %v", req.Method, req.URL, err)
		return resp, err
	}
	if resp.StatusCode >= 400 {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8192))
		_ = resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(body))
		if readErr != nil {
			log.Printf("iceberg-rest http %s %s -> %d (read error: %v)", req.Method, req.URL, resp.StatusCode, readErr)
		} else {
			log.Printf("iceberg-rest http %s %s -> %d body=%s", req.Method, req.URL, resp.StatusCode, strings.TrimSpace(string(body)))
		}
	} else {
		log.Printf("iceberg-rest http %s %s -> %d", req.Method, req.URL, resp.StatusCode)
	}
	return resp, nil
}

func (w *icebergWriter) ensureSchema(ctx context.Context, tbl *table.Table, mapping tableMapping, topic string) (*table.Table, *topicSchema, error) {
	current := tbl.Schema()
	if updated, err := w.ensureTablePaths(ctx, tbl, mapping.identifier); err != nil {
		return nil, nil, err
	} else if updated != nil {
		tbl = updated
		current = tbl.Schema()
	}
	desired, columns, err := w.buildDesiredSchema(ctx, mapping.schema, topic, current)
	if err != nil {
		return nil, nil, err
	}

	needsUpdate, err := schemaNeedsUpdate(current, desired, mapping.schema.AllowTypeWidening)
	if err != nil {
		return nil, nil, err
	}
	if needsUpdate {
		for attempt := 0; attempt < 3; attempt++ {
			updates := []table.Update{
				table.NewAddSchemaUpdate(desired),
				table.NewSetCurrentSchemaUpdate(-1),
			}
			reqs := []table.Requirement{
				table.AssertCurrentSchemaID(current.ID),
			}
			if _, _, err := w.catalog.CommitTable(ctx, mapping.identifier, reqs, updates); err != nil {
				if !isCommitConflict(err) {
					return nil, nil, err
				}
				reloaded, loadErr := loadTableWithRetry(ctx, w.catalog, mapping.identifier, 5, 200*time.Millisecond)
				if loadErr != nil {
					return nil, nil, err
				}
				tbl = reloaded
				current = tbl.Schema()
				desired, columns, err = w.buildDesiredSchema(ctx, mapping.schema, topic, current)
				if err != nil {
					return nil, nil, err
				}
				needsUpdate, err = schemaNeedsUpdate(current, desired, mapping.schema.AllowTypeWidening)
				if err != nil {
					return nil, nil, err
				}
				if !needsUpdate {
					break
				}
				continue
			}
			reloaded, loadErr := loadTableWithRetry(ctx, w.catalog, mapping.identifier, 5, 200*time.Millisecond)
			if loadErr != nil {
				return nil, nil, loadErr
			}
			tbl = reloaded
			current = tbl.Schema()
			break
		}
	}

	arrowSchema, err := table.SchemaToArrowSchema(current, nil, true, false)
	if err != nil {
		return nil, nil, err
	}
	return tbl, &topicSchema{iceberg: current, arrow: arrowSchema, columns: columns}, nil
}

func (w *icebergWriter) ensureTablePaths(ctx context.Context, tbl *table.Table, ident table.Identifier) (*table.Table, error) {
	props := iceberg.Properties{}
	current := tbl.Properties()
	location := tbl.Location()
	if location == "" && w.warehouse != "" {
		location = w.warehouse + "/" + strings.Join(ident, "/")
	}
	if location == "" {
		return nil, nil
	}
	if current[table.WriteDataPathKey] == "" {
		props[table.WriteDataPathKey] = location + "/data"
	}
	if current[table.WriteMetadataPathKey] == "" {
		props[table.WriteMetadataPathKey] = location + "/metadata"
	}
	if len(props) == 0 {
		return nil, nil
	}

	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, _, err := w.catalog.CommitTable(ctx, ident, nil, []table.Update{table.NewSetPropertiesUpdate(props)}); err != nil {
			if !isCommitConflict(err) {
				return nil, err
			}
			reloaded, loadErr := loadTableWithRetry(ctx, w.catalog, ident, 5, 200*time.Millisecond)
			if loadErr != nil {
				return nil, err
			}
			tbl = reloaded
			current = tbl.Properties()
			props = iceberg.Properties{}
			location = tbl.Location()
			if location == "" && w.warehouse != "" {
				location = w.warehouse + "/" + strings.Join(ident, "/")
			}
			if location == "" {
				return nil, nil
			}
			if current[table.WriteDataPathKey] == "" {
				props[table.WriteDataPathKey] = location + "/data"
			}
			if current[table.WriteMetadataPathKey] == "" {
				props[table.WriteMetadataPathKey] = location + "/metadata"
			}
			if len(props) == 0 {
				return tbl, nil
			}
			if err := sleepWithContext(ctx, time.Duration(attempt+1)*200*time.Millisecond); err != nil {
				return nil, err
			}
			continue
		}
		reloaded, loadErr := loadTableWithRetry(ctx, w.catalog, ident, 3, 150*time.Millisecond)
		if loadErr != nil {
			return nil, loadErr
		}
		return reloaded, nil
	}
	return nil, fmt.Errorf("failed to update table paths for %v", ident)
}

func (w *icebergWriter) buildDesiredSchema(ctx context.Context, schemaCfg config.MappingSchemaConfig, topic string, existing *iceberg.Schema) (*iceberg.Schema, []config.Column, error) {
	columns, err := resolveColumns(ctx, w.registry, schemaCfg, topic)
	if err != nil {
		return nil, nil, err
	}

	fieldIDs := map[string]int{}
	maxID := 0
	if existing != nil {
		for _, id := range existing.FieldIDs() {
			if id > maxID {
				maxID = id
			}
		}
		for _, field := range existing.Fields() {
			fieldIDs[field.Name] = field.ID
		}
	}

	fields := make([]iceberg.NestedField, 0, len(baseFields())+len(columns))
	for _, base := range baseFields() {
		if id, ok := fieldIDs[base.Name]; ok {
			base.ID = id
		} else if base.ID <= maxID {
			maxID++
			base.ID = maxID
		}
		fields = append(fields, base)
		if base.ID > maxID {
			maxID = base.ID
		}
	}

	for _, col := range columns {
		id, ok := fieldIDs[col.Name]
		if !ok {
			maxID++
			id = maxID
		}
		fieldType, err := icebergTypeForColumn(col.Type)
		if err != nil {
			return nil, nil, err
		}
		fields = append(fields, iceberg.NestedField{
			ID:       id,
			Name:     col.Name,
			Type:     fieldType,
			Required: col.Required,
		})
	}

	schemaID := defaultTableSchemaID
	if existing != nil {
		schemaID = existing.ID + 1
	}
	return iceberg.NewSchema(schemaID, fields...), columns, nil
}

func resolveColumns(ctx context.Context, registry config.SchemaConfig, schemaCfg config.MappingSchemaConfig, topic string) ([]config.Column, error) {
	switch schemaCfg.Source {
	case "mapping":
		return schemaCfg.Columns, nil
	case "registry":
		return columnsFromRegistry(ctx, registry, topic)
	case "none":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported schema source %q", schemaCfg.Source)
	}
}

func columnsFromRegistry(ctx context.Context, registry config.SchemaConfig, topic string) ([]config.Column, error) {
	if registry.Registry.BaseURL == "" {
		return nil, fmt.Errorf("schema.registry.base_url is required for registry columns")
	}
	url := strings.TrimRight(registry.Registry.BaseURL, "/") + "/" + topic + ".json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	timeout := time.Duration(registry.Registry.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("schema registry status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	columns, err := columnsFromSchemaBytes(body)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns resolved from schema registry for topic %q", topic)
	}
	return columns, nil
}

func jsonSchemaType(raw interface{}) string {
	if rawMap, ok := raw.(map[string]interface{}); ok {
		if t, ok := rawMap["type"].(string); ok {
			return t
		}
		if tList, ok := rawMap["type"].([]interface{}); ok {
			for _, entry := range tList {
				if ts, ok := entry.(string); ok && ts != "null" {
					return ts
				}
			}
		}
	}
	return ""
}

func mapJSONType(value string) (string, bool) {
	switch strings.ToLower(value) {
	case "integer":
		return "long", true
	case "number":
		return "double", true
	case "boolean":
		return "boolean", true
	case "string":
		return "string", true
	default:
		return "", false
	}
}

func columnsFromSchemaBytes(body []byte) ([]config.Column, error) {
	var schemaDoc map[string]interface{}
	if err := json.Unmarshal(body, &schemaDoc); err != nil {
		return nil, err
	}
	props, _ := schemaDoc["properties"].(map[string]interface{})
	requiredSet := map[string]bool{}
	if requiredRaw, ok := schemaDoc["required"].([]interface{}); ok {
		for _, item := range requiredRaw {
			if name, ok := item.(string); ok {
				requiredSet[name] = true
			}
		}
	}
	columns := make([]config.Column, 0, len(props))
	for name, raw := range props {
		colType := jsonSchemaType(raw)
		if colType == "" {
			continue
		}
		mapped, ok := mapJSONType(colType)
		if !ok {
			continue
		}
		columns = append(columns, config.Column{
			Name:     name,
			Type:     mapped,
			Required: requiredSet[name],
		})
	}
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Name < columns[j].Name
	})
	return columns, nil
}

func icebergTypeForColumn(value string) (iceberg.Type, error) {
	switch strings.ToLower(value) {
	case "boolean":
		return iceberg.PrimitiveTypes.Bool, nil
	case "int":
		return iceberg.PrimitiveTypes.Int32, nil
	case "long":
		return iceberg.PrimitiveTypes.Int64, nil
	case "float":
		return iceberg.PrimitiveTypes.Float32, nil
	case "double":
		return iceberg.PrimitiveTypes.Float64, nil
	case "string":
		return iceberg.PrimitiveTypes.String, nil
	case "binary":
		return iceberg.PrimitiveTypes.Binary, nil
	case "timestamp":
		return iceberg.PrimitiveTypes.Timestamp, nil
	case "date":
		return iceberg.PrimitiveTypes.Date, nil
	default:
		return nil, fmt.Errorf("unsupported column type %q", value)
	}
}

func schemaNeedsUpdate(current *iceberg.Schema, desired *iceberg.Schema, allowWiden bool) (bool, error) {
	for _, field := range desired.Fields() {
		existing, ok := current.FindFieldByName(field.Name)
		if !ok {
			return true, nil
		}
		if existing.Type.Equals(field.Type) {
			continue
		}
		if allowWiden && isWidening(existing.Type, field.Type) {
			return true, nil
		}
		return false, fmt.Errorf("incompatible type change for %q: %s -> %s", field.Name, existing.Type, field.Type)
	}
	return false, nil
}

func isWidening(from iceberg.Type, to iceberg.Type) bool {
	switch from.(type) {
	case iceberg.Int32Type:
		_, ok := to.(iceberg.Int64Type)
		return ok
	case iceberg.Float32Type:
		_, ok := to.(iceberg.Float64Type)
		return ok
	default:
		return false
	}
}

func defaultSchema() *iceberg.Schema {
	return iceberg.NewSchema(defaultTableSchemaID, baseFields()...)
}

func baseFields() []iceberg.NestedField {
	return []iceberg.NestedField{
		{ID: 1, Name: "record_id", Type: iceberg.PrimitiveTypes.String, Required: true},
		{ID: 2, Name: "topic", Type: iceberg.PrimitiveTypes.String, Required: true},
		{ID: 3, Name: "partition", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		{ID: 4, Name: "offset", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		{ID: 5, Name: "timestamp_ms", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		{ID: 6, Name: "key", Type: iceberg.PrimitiveTypes.Binary, Required: false},
		{ID: 7, Name: "value", Type: iceberg.PrimitiveTypes.Binary, Required: false},
		{ID: 8, Name: "headers", Type: iceberg.PrimitiveTypes.String, Required: false},
	}
}

func recordsToArrow(schema *arrow.Schema, columns []config.Column, records []Record) (array.RecordReader, error) {
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	recordIDBuilder := builder.Field(0).(*array.StringBuilder)
	topicBuilder := builder.Field(1).(*array.StringBuilder)
	partitionBuilder := builder.Field(2).(*array.Int32Builder)
	offsetBuilder := builder.Field(3).(*array.Int64Builder)
	timestampBuilder := builder.Field(4).(*array.Int64Builder)
	keyBuilder := builder.Field(5).(*array.BinaryBuilder)
	valueBuilder := builder.Field(6).(*array.BinaryBuilder)
	headersBuilder := builder.Field(7).(*array.StringBuilder)
	columnBuilders := buildColumnBuilders(builder, columns)

	for _, record := range records {
		recordIDBuilder.Append(fmt.Sprintf("%s:%d:%d", record.Topic, record.Partition, record.Offset))
		topicBuilder.Append(record.Topic)
		partitionBuilder.Append(record.Partition)
		offsetBuilder.Append(record.Offset)
		timestampBuilder.Append(record.Timestamp)
		if record.Key == nil {
			keyBuilder.AppendNull()
		} else {
			keyBuilder.Append(record.Key)
		}
		if record.Value == nil {
			valueBuilder.AppendNull()
		} else {
			valueBuilder.Append(record.Value)
		}
		headersBuilder.Append(serializeHeaders(record.Headers))
		if len(columnBuilders) > 0 {
			values := extractJSONValues(record.Value)
			appendColumnValues(columnBuilders, values)
		}
	}

	record := builder.NewRecord()
	recordReader, err := array.NewRecordReader(schema, []arrow.RecordBatch{record})
	if err != nil {
		record.Release()
		return nil, err
	}
	return recordReader, nil
}

func serializeHeaders(headers []decoder.Header) string {
	if len(headers) == 0 {
		return ""
	}

	type header struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	out := make([]header, 0, len(headers))
	for _, h := range headers {
		out = append(out, header{Key: h.Key, Value: string(h.Value)})
	}

	encoded, err := json.Marshal(out)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func groupByTopic(records []Record) map[string][]Record {
	buckets := make(map[string][]Record)
	for _, record := range records {
		buckets[record.Topic] = append(buckets[record.Topic], record)
	}
	return buckets
}

type columnBuilder struct {
	name       string
	typ        string
	append     func(interface{})
	appendNull func()
}

func buildColumnBuilders(builder *array.RecordBuilder, columns []config.Column) []columnBuilder {
	if len(columns) == 0 {
		return nil
	}

	builders := make([]columnBuilder, 0, len(columns))
	for i, col := range columns {
		field := builder.Field(8 + i)
		switch strings.ToLower(col.Type) {
		case "boolean":
			b := field.(*array.BooleanBuilder)
			builders = append(builders, columnBuilder{
				name: col.Name,
				typ:  col.Type,
				append: func(value interface{}) {
					v, ok := asBool(value)
					if !ok {
						b.AppendNull()
						return
					}
					b.Append(v)
				},
				appendNull: b.AppendNull,
			})
		case "int":
			b := field.(*array.Int32Builder)
			builders = append(builders, columnBuilder{
				name: col.Name,
				typ:  col.Type,
				append: func(value interface{}) {
					v, ok := asInt64(value)
					if !ok {
						b.AppendNull()
						return
					}
					b.Append(int32(v))
				},
				appendNull: b.AppendNull,
			})
		case "long":
			b := field.(*array.Int64Builder)
			builders = append(builders, columnBuilder{
				name: col.Name,
				typ:  col.Type,
				append: func(value interface{}) {
					v, ok := asInt64(value)
					if !ok {
						b.AppendNull()
						return
					}
					b.Append(v)
				},
				appendNull: b.AppendNull,
			})
		case "float":
			b := field.(*array.Float32Builder)
			builders = append(builders, columnBuilder{
				name: col.Name,
				typ:  col.Type,
				append: func(value interface{}) {
					v, ok := asFloat64(value)
					if !ok {
						b.AppendNull()
						return
					}
					b.Append(float32(v))
				},
				appendNull: b.AppendNull,
			})
		case "double":
			b := field.(*array.Float64Builder)
			builders = append(builders, columnBuilder{
				name: col.Name,
				typ:  col.Type,
				append: func(value interface{}) {
					v, ok := asFloat64(value)
					if !ok {
						b.AppendNull()
						return
					}
					b.Append(v)
				},
				appendNull: b.AppendNull,
			})
		case "string":
			b := field.(*array.StringBuilder)
			builders = append(builders, columnBuilder{
				name: col.Name,
				typ:  col.Type,
				append: func(value interface{}) {
					v, ok := asString(value)
					if !ok {
						b.AppendNull()
						return
					}
					b.Append(v)
				},
				appendNull: b.AppendNull,
			})
		case "binary":
			b := field.(*array.BinaryBuilder)
			builders = append(builders, columnBuilder{
				name: col.Name,
				typ:  col.Type,
				append: func(value interface{}) {
					v, ok := asBytes(value)
					if !ok {
						b.AppendNull()
						return
					}
					b.Append(v)
				},
				appendNull: b.AppendNull,
			})
		case "timestamp":
			b := field.(*array.TimestampBuilder)
			builders = append(builders, columnBuilder{
				name: col.Name,
				typ:  col.Type,
				append: func(value interface{}) {
					v, ok := asTimestamp(value)
					if !ok {
						b.AppendNull()
						return
					}
					b.Append(v)
				},
				appendNull: b.AppendNull,
			})
		case "date":
			b := field.(*array.Date32Builder)
			builders = append(builders, columnBuilder{
				name: col.Name,
				typ:  col.Type,
				append: func(value interface{}) {
					v, ok := asDate(value)
					if !ok {
						b.AppendNull()
						return
					}
					b.Append(v)
				},
				appendNull: b.AppendNull,
			})
		}
	}
	return builders
}

func extractJSONValues(payload []byte) map[string]interface{} {
	if len(payload) == 0 {
		return nil
	}
	var out map[string]interface{}
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil
	}
	return out
}

func appendColumnValues(builders []columnBuilder, values map[string]interface{}) {
	for _, col := range builders {
		if values == nil {
			col.appendNull()
			continue
		}
		val, ok := values[col.name]
		if !ok {
			col.appendNull()
			continue
		}
		col.append(val)
	}
}

func asBool(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	default:
		return false, false
	}
}

func asInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	case json.Number:
		out, err := v.Int64()
		return out, err == nil
	default:
		return 0, false
	}
}

func asFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		out, err := v.Float64()
		return out, err == nil
	default:
		return 0, false
	}
}

func asString(value interface{}) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case json.Number:
		return v.String(), true
	default:
		return "", false
	}
}

func asBytes(value interface{}) ([]byte, bool) {
	switch v := value.(type) {
	case string:
		return []byte(v), true
	case []byte:
		return v, true
	default:
		return nil, false
	}
}

func asTimestamp(value interface{}) (arrow.Timestamp, bool) {
	if v, ok := asInt64(value); ok {
		return arrow.Timestamp(v), true
	}
	if s, ok := asString(value); ok {
		if ts, err := time.Parse(time.RFC3339, s); err == nil {
			return arrow.Timestamp(ts.UnixMilli()), true
		}
	}
	return 0, false
}

func asDate(value interface{}) (arrow.Date32, bool) {
	if v, ok := asInt64(value); ok {
		return arrow.Date32(v), true
	}
	if s, ok := asString(value); ok {
		if ts, err := time.Parse(time.RFC3339, s); err == nil {
			days := ts.Unix() / 86400
			return arrow.Date32(days), true
		}
	}
	return 0, false
}
