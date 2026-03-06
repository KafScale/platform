// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package idoc

import (
	"encoding/json"
	"strings"
	"testing"
)

const sampleIDoc = `<?xml version="1.0"?>
<IDOC>
  <EDI_DC40>
    <DOCNUM>123</DOCNUM>
  </EDI_DC40>
  <E1EDP01>
    <POSEX>10</POSEX>
  </E1EDP01>
  <E1EDKA1>
    <PARVW>AG</PARVW>
  </E1EDKA1>
</IDOC>`

func TestExplodeXML(t *testing.T) {
	cfg := ExplodeConfig{
		ItemSegments:    []string{"E1EDP01"},
		PartnerSegments: []string{"E1EDKA1"},
	}
	res, err := ExplodeXML([]byte(sampleIDoc), cfg)
	if err != nil {
		t.Fatalf("explode: %v", err)
	}
	if res.Header.Root != "IDOC" {
		t.Fatalf("expected root IDOC, got %q", res.Header.Root)
	}
	if len(res.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(res.Items))
	}
	if len(res.Partners) != 1 {
		t.Fatalf("expected 1 partner, got %d", len(res.Partners))
	}
	if len(res.Segments) == 0 {
		t.Fatalf("expected segments")
	}
}

func TestExplodeXMLWithAllSegmentTypes(t *testing.T) {
	xmlData := `<?xml version="1.0"?>
<IDOC BEGIN="1">
  <E1EDP01><POSEX>10</POSEX></E1EDP01>
  <E1EDKA1><PARVW>AG</PARVW></E1EDKA1>
  <E1EDS01><STATU>active</STATU></E1EDS01>
  <E1EDT01><DATUM>20260101</DATUM></E1EDT01>
</IDOC>`

	cfg := ExplodeConfig{
		ItemSegments:    []string{"E1EDP01"},
		PartnerSegments: []string{"E1EDKA1"},
		StatusSegments:  []string{"E1EDS01"},
		DateSegments:    []string{"E1EDT01"},
	}
	res, err := ExplodeXML([]byte(xmlData), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(res.Items))
	}
	if len(res.Partners) != 1 {
		t.Fatalf("expected 1 partner, got %d", len(res.Partners))
	}
	if len(res.Statuses) != 1 {
		t.Fatalf("expected 1 status, got %d", len(res.Statuses))
	}
	if len(res.Dates) != 1 {
		t.Fatalf("expected 1 date, got %d", len(res.Dates))
	}
	// Verify field capture
	if res.Items[0].Fields["POSEX"] != "10" {
		t.Fatalf("expected POSEX=10, got %q", res.Items[0].Fields["POSEX"])
	}
	// Verify attributes on root
	if res.Header.Attributes["BEGIN"] != "1" {
		t.Fatalf("expected BEGIN=1 attribute, got %v", res.Header.Attributes)
	}
}

func TestExplodeXMLInvalid(t *testing.T) {
	_, err := ExplodeXML([]byte("<broken><<<"), ExplodeConfig{})
	if err == nil {
		t.Fatal("expected error for malformed XML")
	}
}

func TestExplodeXMLEmpty(t *testing.T) {
	res, err := ExplodeXML([]byte(""), ExplodeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Header.Root != "" {
		t.Fatalf("expected empty root for empty XML, got %q", res.Header.Root)
	}
}

func TestToTopicRecords(t *testing.T) {
	cfg := ExplodeConfig{
		ItemSegments:    []string{"E1EDP01"},
		PartnerSegments: []string{"E1EDKA1"},
		StatusSegments:  []string{"E1EDS01"},
		DateSegments:    []string{"E1EDT01"},
	}
	xmlData := `<?xml version="1.0"?>
<IDOC>
  <E1EDP01><POSEX>10</POSEX></E1EDP01>
  <E1EDKA1><PARVW>AG</PARVW></E1EDKA1>
  <E1EDS01><STATU>active</STATU></E1EDS01>
  <E1EDT01><DATUM>20260101</DATUM></E1EDT01>
</IDOC>`

	res, err := ExplodeXML([]byte(xmlData), cfg)
	if err != nil {
		t.Fatal(err)
	}

	topics := TopicConfig{
		Header:   "idoc-headers",
		Segments: "idoc-segments",
		Items:    "idoc-items",
		Partners: "idoc-partners",
		Statuses: "idoc-statuses",
		Dates:    "idoc-dates",
	}
	records, err := res.ToTopicRecords(topics)
	if err != nil {
		t.Fatal(err)
	}

	if len(records["idoc-headers"]) != 1 {
		t.Fatalf("expected 1 header record, got %d", len(records["idoc-headers"]))
	}
	// Verify the header is valid JSON
	var hdr Header
	if err := json.Unmarshal(records["idoc-headers"][0], &hdr); err != nil {
		t.Fatalf("header is not valid JSON: %v", err)
	}
	if hdr.Root != "IDOC" {
		t.Fatalf("expected root IDOC, got %q", hdr.Root)
	}

	if len(records["idoc-items"]) != 1 {
		t.Fatalf("expected 1 item record, got %d", len(records["idoc-items"]))
	}
	if len(records["idoc-partners"]) != 1 {
		t.Fatalf("expected 1 partner record, got %d", len(records["idoc-partners"]))
	}
	if len(records["idoc-statuses"]) != 1 {
		t.Fatalf("expected 1 status record, got %d", len(records["idoc-statuses"]))
	}
	if len(records["idoc-dates"]) != 1 {
		t.Fatalf("expected 1 date record, got %d", len(records["idoc-dates"]))
	}
	if len(records["idoc-segments"]) == 0 {
		t.Fatal("expected segment records")
	}
}

func TestToTopicRecordsEmptyTopics(t *testing.T) {
	res := Result{
		Header:   Header{Root: "IDOC"},
		Segments: []Segment{{Name: "S1"}},
	}
	// Empty TopicConfig means no topic names → skip all
	records, err := res.ToTopicRecords(TopicConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("expected empty records, got %d topics", len(records))
	}
}

func TestToTopicRecordsPartialTopics(t *testing.T) {
	res := Result{
		Header: Header{Root: "IDOC"},
		Items:  []Segment{{Name: "E1EDP01"}},
	}
	records, err := res.ToTopicRecords(TopicConfig{Header: "hdr", Items: "items"})
	if err != nil {
		t.Fatal(err)
	}
	if len(records["hdr"]) != 1 {
		t.Fatal("expected header record")
	}
	if len(records["items"]) != 1 {
		t.Fatal("expected item record")
	}
}

func TestSliceToSetEmpty(t *testing.T) {
	s := sliceToSet([]string{"", "  ", " a "})
	if !s["a"] {
		t.Fatal("expected trimmed 'a' in set")
	}
	if len(s) != 1 {
		t.Fatalf("expected 1 element, got %d", len(s))
	}
}

func TestAttrsToMapEmpty(t *testing.T) {
	m := attrsToMap(nil)
	if m != nil {
		t.Fatal("expected nil for empty attrs")
	}
}

func TestBuildPath(t *testing.T) {
	stack := []segmentFrame{{Name: "IDOC"}, {Name: "E1EDP01"}}
	path := buildPath(stack, "POSEX")
	if path != "IDOC/E1EDP01/POSEX" {
		t.Fatalf("expected IDOC/E1EDP01/POSEX, got %s", path)
	}
}

func TestExplodeXMLPaths(t *testing.T) {
	xmlData := `<ROOT><CHILD><LEAF>val</LEAF></CHILD></ROOT>`
	res, err := ExplodeXML([]byte(xmlData), ExplodeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	// Find the LEAF segment
	for _, seg := range res.Segments {
		if seg.Name == "LEAF" {
			if !strings.Contains(seg.Path, "ROOT/CHILD/LEAF") {
				t.Fatalf("expected path containing ROOT/CHILD/LEAF, got %s", seg.Path)
			}
			return
		}
	}
	t.Fatal("LEAF segment not found")
}
