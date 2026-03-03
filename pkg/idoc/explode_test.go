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

import "testing"

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
