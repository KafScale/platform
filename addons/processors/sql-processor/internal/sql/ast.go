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

type QueryType string

const (
	QueryUnknown        QueryType = "unknown"
	QueryShowTopics     QueryType = "show_topics"
	QueryShowPartitions QueryType = "show_partitions"
	QueryDescribe       QueryType = "describe"
	QuerySelect         QueryType = "select"
	QueryExplain        QueryType = "explain"
)

type Query struct {
	Type QueryType

	Topic      string
	TopicAlias string
	JoinTopic  string
	JoinAlias  string
	JoinType   string
	JoinOn     *JoinCondition

	Select    []SelectColumn
	GroupBy   []string
	OrderBy   string
	OrderDesc bool
	Limit     string

	Partition *int32
	OffsetMin *int64
	OffsetMax *int64
	TsMin     *int64
	TsMax     *int64

	TimeWindow string
	Last       string
	Tail       string
	ScanFull   bool

	Explain *Query
}

type SelectColumnKind string

const (
	SelectColumnUnknown    SelectColumnKind = "unknown"
	SelectColumnStar       SelectColumnKind = "star"
	SelectColumnField      SelectColumnKind = "field"
	SelectColumnJSONValue  SelectColumnKind = "json_value"
	SelectColumnJSONQuery  SelectColumnKind = "json_query"
	SelectColumnJSONExists SelectColumnKind = "json_exists"
	SelectColumnAggregate  SelectColumnKind = "aggregate"
)

type SelectColumn struct {
	Raw    string
	Alias  string
	Kind   SelectColumnKind
	Source string

	Column   string
	JSONPath string

	AggFunc     string
	AggColumn   string
	AggJSONPath string
	AggStar     bool
	AggSource   string
}

type JoinExprKind string

const (
	JoinExprKey  JoinExprKind = "key"
	JoinExprJSON JoinExprKind = "json"
)

type JoinCondition struct {
	Left  JoinExpr
	Right JoinExpr
}

type JoinExpr struct {
	Kind     JoinExprKind
	Source   string
	Side     string
	JSONPath string
}
