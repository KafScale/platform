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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/hamba/avro/v2"
)

const schemaTypeAvro = "AVRO"

type resolvedReference struct {
	Name string
	Text string
}

func parseAvro(text string, refs []resolvedReference) (avro.Schema, error) {
	cache := &avro.SchemaCache{}
	for _, ref := range refs {
		if _, err := avro.ParseWithCache(ref.Text, "", cache); err != nil {
			return nil, err
		}
	}
	return avro.ParseWithCache(text, "", cache)
}

func canonicalText(text string) (string, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(text)); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func schemaHash(schemaType, canonical string, refs []Reference) string {
	h := sha256.New()
	h.Write([]byte(schemaType))
	h.Write([]byte{0})
	h.Write([]byte(canonical))
	for _, ref := range refs {
		h.Write([]byte{0})
		h.Write([]byte(ref.Name))
		h.Write([]byte{0})
		h.Write([]byte(ref.Subject))
		h.Write([]byte{0})
		h.Write([]byte(strconv.Itoa(ref.Version)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func checkBackward(newText string, newRefs []resolvedReference, oldText string, oldRefs []resolvedReference) error {
	reader, err := parseAvro(newText, newRefs)
	if err != nil {
		return err
	}
	writer, err := parseAvro(oldText, oldRefs)
	if err != nil {
		return err
	}
	return avro.NewSchemaCompatibility().Compatible(reader, writer)
}
