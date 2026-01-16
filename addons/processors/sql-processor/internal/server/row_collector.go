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

package server

import (
	"time"

	"github.com/jackc/pgproto3/v2"
)

type rowCollector struct {
	maxRows    int
	fields     []pgproto3.FieldDescription
	rows       [][][]byte
	commandTag []byte
	truncated  bool
}

func newRowCollector(maxRows int) *rowCollector {
	return &rowCollector{maxRows: maxRows}
}

func (c *rowCollector) capture(msg pgproto3.BackendMessage) {
	if c == nil {
		return
	}
	switch m := msg.(type) {
	case *pgproto3.RowDescription:
		c.fields = copyFieldDescriptions(m.Fields)
	case *pgproto3.DataRow:
		if c.maxRows > 0 && len(c.rows) >= c.maxRows {
			c.truncated = true
			return
		}
		c.rows = append(c.rows, copyRowValues(m.Values))
	case *pgproto3.CommandComplete:
		if len(m.CommandTag) > 0 {
			c.commandTag = append([]byte(nil), m.CommandTag...)
		}
	}
}

func (c *rowCollector) entry() *cacheEntry {
	if c == nil || c.truncated || len(c.fields) == 0 {
		return nil
	}
	return &cacheEntry{
		created:    nowTime(),
		fields:     c.fields,
		rows:       c.rows,
		commandTag: c.commandTag,
		rowsCount:  len(c.rows),
	}
}

func copyFieldDescriptions(fields []pgproto3.FieldDescription) []pgproto3.FieldDescription {
	out := make([]pgproto3.FieldDescription, len(fields))
	copy(out, fields)
	return out
}

func copyRowValues(values [][]byte) [][]byte {
	out := make([][]byte, len(values))
	for i, value := range values {
		if value == nil {
			continue
		}
		buf := make([]byte, len(value))
		copy(buf, value)
		out[i] = buf
	}
	return out
}

func nowTime() time.Time {
	return time.Now()
}
