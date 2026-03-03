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
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
)

// ExplodeConfig defines IDoc segment routing rules.
type ExplodeConfig struct {
	ItemSegments    []string
	PartnerSegments []string
	StatusSegments  []string
	DateSegments    []string
}

// Header captures the root IDoc element info.
type Header struct {
	Root       string            `json:"root"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Segment captures a single XML segment.
type Segment struct {
	Name       string            `json:"name"`
	Path       string            `json:"path"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Value      string            `json:"value,omitempty"`
}

// Result holds exploded IDoc data.
type Result struct {
	Header   Header
	Segments []Segment
	Items    []Segment
	Partners []Segment
	Statuses []Segment
	Dates    []Segment
}

// TopicRecords renders exploded data as JSON records per topic.
type TopicRecords map[string][][]byte

// ExplodeXML parses IDoc XML and routes segments according to config.
func ExplodeXML(raw []byte, cfg ExplodeConfig) (Result, error) {
	decoder := xml.NewDecoder(bytes.NewReader(raw))
	decoder.Strict = false
	decoder.AutoClose = xml.HTMLAutoClose

	segmentStack := make([]segmentFrame, 0, 16)
	result := Result{}
	segmentSets := buildSegmentSets(cfg)

	for {
		tok, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			if err == io.EOF {
				break
			}
			return Result{}, fmt.Errorf("xml token: %w", err)
		}

		switch t := tok.(type) {
		case xml.StartElement:
			frame := segmentFrame{
				Name:       t.Name.Local,
				Path:       buildPath(segmentStack, t.Name.Local),
				Attributes: attrsToMap(t.Attr),
			}
			segmentStack = append(segmentStack, frame)
			if result.Header.Root == "" {
				result.Header = Header{Root: t.Name.Local, Attributes: frame.Attributes}
			}
		case xml.CharData:
			if len(segmentStack) == 0 {
				continue
			}
			segmentStack[len(segmentStack)-1].Value += string([]byte(t))
		case xml.EndElement:
			if len(segmentStack) == 0 {
				continue
			}
			frame := segmentStack[len(segmentStack)-1]
			segmentStack = segmentStack[:len(segmentStack)-1]
			seg := Segment{
				Name:       frame.Name,
				Path:       frame.Path,
				Attributes: frame.Attributes,
				Value:      strings.TrimSpace(frame.Value),
			}
			result.Segments = append(result.Segments, seg)
			switch {
			case segmentSets.items[seg.Name]:
				result.Items = append(result.Items, seg)
			case segmentSets.partners[seg.Name]:
				result.Partners = append(result.Partners, seg)
			case segmentSets.statuses[seg.Name]:
				result.Statuses = append(result.Statuses, seg)
			case segmentSets.dates[seg.Name]:
				result.Dates = append(result.Dates, seg)
			}
		}
	}

	return result, nil
}

// ToTopicRecords converts Result into JSON record slices per topic.
func (r Result) ToTopicRecords(topics TopicConfig) (TopicRecords, error) {
	out := TopicRecords{}
	if topics.Header != "" {
		data, err := json.Marshal(r.Header)
		if err != nil {
			return nil, err
		}
		out[topics.Header] = append(out[topics.Header], data)
	}
	appendSegments := func(name string, segments []Segment) error {
		if name == "" {
			return nil
		}
		for _, seg := range segments {
			data, err := json.Marshal(seg)
			if err != nil {
				return err
			}
			out[name] = append(out[name], data)
		}
		return nil
	}
	if err := appendSegments(topics.Segments, r.Segments); err != nil {
		return nil, err
	}
	if err := appendSegments(topics.Items, r.Items); err != nil {
		return nil, err
	}
	if err := appendSegments(topics.Partners, r.Partners); err != nil {
		return nil, err
	}
	if err := appendSegments(topics.Statuses, r.Statuses); err != nil {
		return nil, err
	}
	if err := appendSegments(topics.Dates, r.Dates); err != nil {
		return nil, err
	}
	return out, nil
}

// TopicConfig maps logical outputs to topic names.
type TopicConfig struct {
	Header   string
	Segments string
	Items    string
	Partners string
	Statuses string
	Dates    string
}

type segmentFrame struct {
	Name       string
	Path       string
	Attributes map[string]string
	Value      string
}

type segmentSets struct {
	items    map[string]bool
	partners map[string]bool
	statuses map[string]bool
	dates    map[string]bool
}

func buildSegmentSets(cfg ExplodeConfig) segmentSets {
	return segmentSets{
		items:    sliceToSet(cfg.ItemSegments),
		partners: sliceToSet(cfg.PartnerSegments),
		statuses: sliceToSet(cfg.StatusSegments),
		dates:    sliceToSet(cfg.DateSegments),
	}
}

func sliceToSet(values []string) map[string]bool {
	set := map[string]bool{}
	for _, val := range values {
		val = strings.TrimSpace(val)
		if val == "" {
			continue
		}
		set[val] = true
	}
	return set
}

func attrsToMap(attrs []xml.Attr) map[string]string {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		out[attr.Name.Local] = attr.Value
	}
	return out
}

func buildPath(stack []segmentFrame, name string) string {
	parts := make([]string, 0, len(stack)+1)
	for _, frame := range stack {
		parts = append(parts, frame.Name)
	}
	parts = append(parts, name)
	return strings.Join(parts, "/")
}
