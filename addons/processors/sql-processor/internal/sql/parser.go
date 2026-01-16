// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package sql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func Parse(query string) (Query, error) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return Query{}, fmt.Errorf("empty query")
	}

	trimmed = strings.TrimSuffix(trimmed, ";")
	lower := strings.ToLower(trimmed)
	fields := strings.Fields(lower)
	if len(fields) == 0 {
		return Query{}, fmt.Errorf("empty query")
	}

	switch fields[0] {
	case "show":
		return parseShow(fields)
	case "describe":
		return parseDescribe(fields)
	case "select":
		return parseSelect(trimmed, lower, fields)
	case "explain":
		return parseExplain(trimmed)
	default:
		return Query{Type: QueryUnknown}, fmt.Errorf("unsupported statement")
	}
}

func parseShow(fields []string) (Query, error) {
	if len(fields) >= 2 && fields[1] == "topics" {
		return Query{Type: QueryShowTopics}, nil
	}
	if len(fields) >= 4 && fields[1] == "partitions" && fields[2] == "from" {
		return Query{Type: QueryShowPartitions, Topic: fields[3]}, nil
	}
	return Query{Type: QueryUnknown}, fmt.Errorf("unsupported show statement")
}

func parseDescribe(fields []string) (Query, error) {
	if len(fields) < 2 {
		return Query{Type: QueryUnknown}, fmt.Errorf("describe requires topic")
	}
	return Query{Type: QueryDescribe, Topic: fields[1]}, nil
}

func parseExplain(raw string) (Query, error) {
	trimmed := strings.TrimSpace(raw)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "explain") {
		return Query{Type: QueryUnknown}, fmt.Errorf("invalid explain")
	}
	inner := strings.TrimSpace(trimmed[len("explain"):])
	if inner == "" {
		return Query{Type: QueryUnknown}, fmt.Errorf("explain requires query")
	}
	parsed, err := Parse(inner)
	if err != nil {
		return Query{Type: QueryUnknown}, err
	}
	if parsed.Type != QuerySelect {
		return Query{Type: QueryUnknown}, fmt.Errorf("explain supports select only")
	}
	return Query{Type: QueryExplain, Explain: &parsed}, nil
}

func parseSelect(raw string, lower string, fields []string) (Query, error) {
	selectCols, err := parseSelectColumns(raw, lower)
	if err != nil {
		return Query{Type: QueryUnknown}, err
	}

	topic, topicAlias, err := parseFromClause(fields)
	if err != nil {
		return Query{Type: QueryUnknown}, err
	}

	joinType, joinTopic, joinAlias := parseJoin(fields)
	var joinOn *JoinCondition
	if joinType != "" && joinTopic == "" {
		return Query{Type: QueryUnknown}, fmt.Errorf("join requires topic")
	}
	if joinTopic != "" {
		cond, condErr := parseJoinCondition(raw, lower, topic, topicAlias, joinTopic, joinAlias)
		if condErr != nil {
			return Query{Type: QueryUnknown}, condErr
		}
		joinOn = cond
	}

	partition, offsetMin, offsetMax, err := parseFilters(fields)
	if err != nil {
		return Query{Type: QueryUnknown}, err
	}

	tsMin, tsMax, err := parseTSFilters(raw)
	if err != nil {
		return Query{Type: QueryUnknown}, err
	}

	return Query{
		Type:       QuerySelect,
		Topic:      topic,
		TopicAlias: topicAlias,
		JoinTopic:  joinTopic,
		JoinAlias:  joinAlias,
		JoinType:   joinType,
		JoinOn:     joinOn,
		Select:     selectCols,
		GroupBy:    parseGroupBy(raw, lower),
		OrderBy:    parseOrderBy(raw, lower),
		OrderDesc:  parseOrderDesc(raw, lower),
		Limit:      parseLimitToken(lower),
		Partition:  partition,
		OffsetMin:  offsetMin,
		OffsetMax:  offsetMax,
		TsMin:      tsMin,
		TsMax:      tsMax,
		TimeWindow: parseKeywordValue(lower, "within"),
		Last:       parseKeywordValue(lower, "last"),
		Tail:       parseKeywordValue(lower, "tail"),
		ScanFull:   hasToken(fields, "scan") && hasToken(fields, "full"),
	}, nil
}

func parseJoin(fields []string) (string, string, string) {
	for i, field := range fields {
		if field == "join" && i+1 < len(fields) {
			joinTopic := fields[i+1]
			joinAlias := ""
			if i+2 < len(fields) && !isKeyword(fields[i+2]) {
				joinAlias = fields[i+2]
			}
			return "inner", joinTopic, joinAlias
		}
		if field == "left" && i+2 < len(fields) && fields[i+1] == "join" {
			joinTopic := fields[i+2]
			joinAlias := ""
			if i+3 < len(fields) && !isKeyword(fields[i+3]) {
				joinAlias = fields[i+3]
			}
			return "left", joinTopic, joinAlias
		}
	}
	return "", "", ""
}

func parseFromClause(fields []string) (string, string, error) {
	for i, field := range fields {
		if field != "from" || i+1 >= len(fields) {
			continue
		}
		topic := fields[i+1]
		alias := ""
		if i+2 < len(fields) && !isKeyword(fields[i+2]) {
			alias = fields[i+2]
		}
		return topic, alias, nil
	}
	return "", "", fmt.Errorf("select requires from <topic>")
}

func parseSelectColumns(raw string, lower string) ([]SelectColumn, error) {
	selectIdx := keywordIndex(lower, "select")
	fromIdx := keywordIndex(lower, "from")
	if selectIdx == -1 || fromIdx == -1 || fromIdx <= selectIdx {
		return nil, fmt.Errorf("select requires from <topic>")
	}

	rawCols := strings.TrimSpace(raw[selectIdx+len("select") : fromIdx])
	if rawCols == "" {
		return []SelectColumn{{Kind: SelectColumnStar, Raw: "*"}}, nil
	}
	parts := splitColumns(rawCols)
	cols := make([]SelectColumn, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		col, err := parseSelectColumn(part)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	if len(cols) == 0 {
		return []SelectColumn{{Kind: SelectColumnStar, Raw: "*"}}, nil
	}
	return cols, nil
}

func parseFilters(fields []string) (*int32, *int64, *int64, error) {
	whereIdx := indexOf(fields, "where")
	if whereIdx == -1 {
		return nil, nil, nil, nil
	}

	var partition *int32
	var offsetMin *int64
	var offsetMax *int64
	for i := whereIdx + 1; i < len(fields); i++ {
		switch fields[i] {
		case "limit", "last", "tail", "within", "scan":
			return partition, offsetMin, offsetMax, nil
		case "and":
			continue
		case "_partition":
			if i+2 >= len(fields) || fields[i+1] != "=" {
				return nil, nil, nil, fmt.Errorf("unsupported partition filter")
			}
			value, err := parseInt32(fields[i+2])
			if err != nil {
				return nil, nil, nil, fmt.Errorf("invalid partition value")
			}
			partition = &value
			i += 2
		case "_offset":
			if i+2 >= len(fields) {
				return nil, nil, nil, fmt.Errorf("unsupported offset filter")
			}
			op := fields[i+1]
			value, err := parseInt64(fields[i+2])
			if err != nil {
				return nil, nil, nil, fmt.Errorf("invalid offset value")
			}
			switch op {
			case ">=":
				offsetMin = &value
			case "<=":
				offsetMax = &value
			default:
				return nil, nil, nil, fmt.Errorf("unsupported offset operator")
			}
			i += 2
		default:
			return nil, nil, nil, fmt.Errorf("unsupported where clause")
		}
	}
	return partition, offsetMin, offsetMax, nil
}

func indexOf(fields []string, token string) int {
	for i, field := range fields {
		if field == token {
			return i
		}
	}
	return -1
}

func parseInt32(raw string) (int32, error) {
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(value), nil
}

func parseInt64(raw string) (int64, error) {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func hasToken(fields []string, token string) bool {
	for _, field := range fields {
		if field == token {
			return true
		}
	}
	return false
}

func isKeyword(value string) bool {
	switch value {
	case "join", "left", "where", "group", "order", "limit", "last", "tail", "within", "scan":
		return true
	default:
		return false
	}
}

func parseKeywordValue(lower string, keyword string) string {
	re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(keyword) + `\s+(\S+)`)
	match := re.FindStringSubmatch(lower)
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(match[1])
}

func parseLimitToken(lower string) string {
	re := regexp.MustCompile(`(?i)\blimit\s+(\d+)`)
	match := re.FindStringSubmatch(lower)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

func parseGroupBy(raw string, lower string) []string {
	groupIdx := keywordIndex(lower, "group by")
	if groupIdx == -1 {
		return nil
	}
	rest := raw[groupIdx+len("group by"):]
	restLower := lower[groupIdx+len("group by"):]
	end := clauseEnd(restLower, []string{"order by", "limit", "last", "tail", "within", "scan"})
	list := strings.TrimSpace(rest[:end])
	return splitIdentifiers(list)
}

func parseOrderBy(raw string, lower string) string {
	orderIdx := keywordIndex(lower, "order by")
	if orderIdx == -1 {
		return ""
	}
	rest := raw[orderIdx+len("order by"):]
	restLower := lower[orderIdx+len("order by"):]
	end := clauseEnd(restLower, []string{"limit", "last", "tail", "within", "scan", "group by"})
	list := strings.TrimSpace(rest[:end])
	fields := strings.Fields(strings.ToLower(list))
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func parseOrderDesc(raw string, lower string) bool {
	orderIdx := keywordIndex(lower, "order by")
	if orderIdx == -1 {
		return false
	}
	rest := raw[orderIdx+len("order by"):]
	restLower := strings.ToLower(rest)
	end := clauseEnd(restLower, []string{"limit", "last", "tail", "within", "scan", "group by"})
	fields := strings.Fields(restLower[:end])
	if len(fields) < 2 {
		return false
	}
	return fields[1] == "desc"
}

func parseTSFilters(raw string) (*int64, *int64, error) {
	reBetween := regexp.MustCompile(`(?i)_ts\s+between\s+'([^']+)'\s+and\s+'([^']+)'`)
	if match := reBetween.FindStringSubmatch(raw); len(match) == 3 {
		min, err := parseTimestampLiteral(match[1])
		if err != nil {
			return nil, nil, err
		}
		max, err := parseTimestampLiteral(match[2])
		if err != nil {
			return nil, nil, err
		}
		return &min, &max, nil
	}

	var min *int64
	var max *int64
	reGte := regexp.MustCompile(`(?i)_ts\s*>=\s*('?[^'\s]+'?|\d+)`)
	if match := reGte.FindStringSubmatch(raw); len(match) == 2 {
		value, err := parseTimestampLiteral(strings.Trim(match[1], "'"))
		if err != nil {
			return nil, nil, err
		}
		min = &value
	}
	reLte := regexp.MustCompile(`(?i)_ts\s*<=\s*('?[^'\s]+'?|\d+)`)
	if match := reLte.FindStringSubmatch(raw); len(match) == 2 {
		value, err := parseTimestampLiteral(strings.Trim(match[1], "'"))
		if err != nil {
			return nil, nil, err
		}
		max = &value
	}
	return min, max, nil
}

func parseTimestampLiteral(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty timestamp")
	}
	if numeric, err := strconv.ParseInt(value, 10, 64); err == nil {
		return numeric, nil
	}
	layouts := []string{
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		time.RFC3339,
	}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts.UnixMilli(), nil
		}
	}
	return 0, fmt.Errorf("invalid timestamp %q", value)
}

func splitColumns(raw string) []string {
	parts := []string{}
	start := 0
	depth := 0
	for i, r := range raw {
		switch r {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, raw[start:i])
				start = i + 1
			}
		}
	}
	if start < len(raw) {
		parts = append(parts, raw[start:])
	}
	return parts
}

func parseSelectColumn(raw string) (SelectColumn, error) {
	expr, alias := splitAlias(raw)
	exprLower := strings.ToLower(strings.TrimSpace(expr))
	out := SelectColumn{Raw: raw, Alias: alias}

	if exprLower == "*" {
		out.Kind = SelectColumnStar
		return out, nil
	}

	if agg := parseAggregate(expr); agg != nil {
		out.Kind = SelectColumnAggregate
		out.AggFunc = agg.Func
		out.AggStar = agg.Star
		out.AggColumn = agg.Column
		out.AggJSONPath = agg.JSONPath
		out.AggSource = agg.Source
		if out.Alias == "" {
			out.Alias = agg.Name
		}
		return out, nil
	}

	if jsonValue := parseJSONValue(expr); jsonValue != nil {
		out.Kind = SelectColumnJSONValue
		out.JSONPath = jsonValue.Path
		out.Source = jsonValue.Source
		if out.Alias == "" {
			out.Alias = "json_value"
		}
		return out, nil
	}
	if jsonQuery := parseJSONQuery(expr); jsonQuery != nil {
		out.Kind = SelectColumnJSONQuery
		out.JSONPath = jsonQuery.Path
		out.Source = jsonQuery.Source
		if out.Alias == "" {
			out.Alias = "json_query"
		}
		return out, nil
	}
	if jsonExists := parseJSONExists(expr); jsonExists != nil {
		out.Kind = SelectColumnJSONExists
		out.JSONPath = jsonExists.Path
		out.Source = jsonExists.Source
		if out.Alias == "" {
			out.Alias = "json_exists"
		}
		return out, nil
	}

	source, column := parseColumnRef(exprLower)
	out.Kind = SelectColumnField
	out.Source = source
	out.Column = column
	if out.Alias == "" {
		out.Alias = column
	}
	return out, nil
}

type aggregateSpec struct {
	Func     string
	Star     bool
	Column   string
	JSONPath string
	Source   string
	Name     string
}

func parseAggregate(expr string) *aggregateSpec {
	re := regexp.MustCompile(`(?i)^(count|min|max|sum|avg)\s*\((.*)\)$`)
	match := re.FindStringSubmatch(strings.TrimSpace(expr))
	if len(match) != 3 {
		return nil
	}
	fn := strings.ToLower(match[1])
	arg := strings.TrimSpace(match[2])
	spec := &aggregateSpec{Func: fn, Name: fn}
	if arg == "*" {
		spec.Star = true
		spec.Name = fn
		return spec
	}
	if jsonValue := parseJSONValue(arg); jsonValue != nil {
		spec.JSONPath = jsonValue.Path
		spec.Source = jsonValue.Source
		spec.Name = fn + "_json"
		return spec
	}
	source, column := parseColumnRef(strings.ToLower(arg))
	spec.Column = column
	spec.Source = source
	spec.Name = fn + "_" + column
	return spec
}

type jsonValueSpec struct {
	Source string
	Path   string
}

func parseJSONValue(expr string) *jsonValueSpec {
	return parseJSONFunc(expr, "json_value")
}

func parseJSONQuery(expr string) *jsonValueSpec {
	return parseJSONFunc(expr, "json_query")
}

func parseJSONExists(expr string) *jsonValueSpec {
	return parseJSONFunc(expr, "json_exists")
}

func parseJSONFunc(expr string, name string) *jsonValueSpec {
	re := regexp.MustCompile(`(?i)^` + regexp.QuoteMeta(name) + `\s*\(\s*([a-zA-Z0-9_\.]+)\s*,\s*'([^']+)'\s*\)$`)
	match := re.FindStringSubmatch(strings.TrimSpace(expr))
	if len(match) != 3 {
		return nil
	}
	source, column := parseColumnRef(strings.ToLower(match[1]))
	if column != "_value" {
		return nil
	}
	return &jsonValueSpec{Source: source, Path: match[2]}
}

func parseColumnRef(expr string) (string, string) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", ""
	}
	parts := strings.Split(expr, ".")
	if len(parts) == 2 {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	return "", expr
}

func splitAlias(raw string) (string, string) {
	re := regexp.MustCompile(`(?i)\s+as\s+`)
	parts := re.Split(raw, -1)
	if len(parts) == 2 {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	fields := strings.Fields(raw)
	if len(fields) > 1 {
		expr := strings.Join(fields[:len(fields)-1], " ")
		alias := fields[len(fields)-1]
		if strings.Contains(expr, "(") && strings.Contains(expr, ")") {
			return expr, alias
		}
	}
	return raw, ""
}

func keywordIndex(lower, keyword string) int {
	re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(keyword) + `\b`)
	loc := re.FindStringIndex(lower)
	if loc == nil {
		return -1
	}
	return loc[0]
}

func clauseEnd(lower string, stopKeywords []string) int {
	end := len(lower)
	for _, keyword := range stopKeywords {
		if idx := keywordIndex(lower, keyword); idx != -1 && idx < end {
			end = idx
		}
	}
	return end
}

func splitIdentifiers(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(strings.ToLower(part))
		if name == "" {
			continue
		}
		out = append(out, name)
	}
	return out
}

func parseJoinCondition(raw string, lower string, topic string, alias string, joinTopic string, joinAlias string) (*JoinCondition, error) {
	joinIdx := keywordIndex(lower, "join")
	if joinIdx == -1 {
		return nil, nil
	}
	onIdx := keywordIndex(lower[joinIdx:], "on")
	if onIdx == -1 {
		return &JoinCondition{
			Left:  JoinExpr{Kind: JoinExprKey, Side: "left"},
			Right: JoinExpr{Kind: JoinExprKey, Side: "right"},
		}, nil
	}
	onIdx += joinIdx
	rest := raw[onIdx+len("on"):]
	restLower := lower[onIdx+len("on"):]
	end := clauseEnd(restLower, []string{"within", "last", "tail", "limit", "where", "group by", "order by", "scan"})
	expr := strings.TrimSpace(rest[:end])
	parts := strings.Split(expr, "=")
	if len(parts) != 2 {
		return nil, fmt.Errorf("join requires equality predicate")
	}
	left, err := parseJoinExpr(strings.TrimSpace(parts[0]), topic, alias, joinTopic, joinAlias)
	if err != nil {
		return nil, err
	}
	right, err := parseJoinExpr(strings.TrimSpace(parts[1]), topic, alias, joinTopic, joinAlias)
	if err != nil {
		return nil, err
	}
	return &JoinCondition{Left: left, Right: right}, nil
}

func parseJoinExpr(raw string, topic string, alias string, joinTopic string, joinAlias string) (JoinExpr, error) {
	if jsonValue := parseJSONValue(raw); jsonValue != nil {
		side := resolveJoinSide(jsonValue.Source, topic, alias, joinTopic, joinAlias)
		return JoinExpr{Kind: JoinExprJSON, Source: jsonValue.Source, Side: side, JSONPath: jsonValue.Path}, nil
	}
	source, column := parseColumnRef(strings.ToLower(raw))
	if column != "_key" {
		return JoinExpr{}, fmt.Errorf("join supports _key or json_value only")
	}
	side := resolveJoinSide(source, topic, alias, joinTopic, joinAlias)
	return JoinExpr{Kind: JoinExprKey, Source: source, Side: side}, nil
}

func resolveJoinSide(source string, topic string, alias string, joinTopic string, joinAlias string) string {
	switch source {
	case "", alias, topic:
		return "left"
	case joinAlias, joinTopic:
		return "right"
	default:
		return "left"
	}
}
