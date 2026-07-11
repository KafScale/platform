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

package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/KafScale/platform/pkg/idoc"
	"github.com/KafScale/platform/pkg/lfs"
)

func main() {
	inputPath := flag.String("input", "", "Path to input file (XML or JSONL envelopes). Empty reads stdin.")
	outputDir := flag.String("out", envOrDefault("KAFSCALE_IDOC_OUTPUT_DIR", "idoc-output"), "Output directory for topic files")
	flag.Parse()

	ctx := context.Background()
	resolver, err := buildResolver(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolver: %v\n", err)
		os.Exit(1)
	}

	input, err := openInput(*inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "input: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = input.Close() }()

	cfg := idoc.ExplodeConfig{
		ItemSegments:    parseCSV(envOrDefault("KAFSCALE_IDOC_ITEM_SEGMENTS", "E1EDP01,E1EDP19")),
		PartnerSegments: parseCSV(envOrDefault("KAFSCALE_IDOC_PARTNER_SEGMENTS", "E1EDKA1")),
		StatusSegments:  parseCSV(envOrDefault("KAFSCALE_IDOC_STATUS_SEGMENTS", "E1STATS")),
		DateSegments:    parseCSV(envOrDefault("KAFSCALE_IDOC_DATE_SEGMENTS", "E1EDK03")),
	}
	topics := idoc.TopicConfig{
		Header:   envOrDefault("KAFSCALE_IDOC_TOPIC_HEADER", "idoc-headers"),
		Segments: envOrDefault("KAFSCALE_IDOC_TOPIC_SEGMENTS", "idoc-segments"),
		Items:    envOrDefault("KAFSCALE_IDOC_TOPIC_ITEMS", "idoc-items"),
		Partners: envOrDefault("KAFSCALE_IDOC_TOPIC_PARTNERS", "idoc-partners"),
		Statuses: envOrDefault("KAFSCALE_IDOC_TOPIC_STATUS", "idoc-status"),
		Dates:    envOrDefault("KAFSCALE_IDOC_TOPIC_DATES", "idoc-dates"),
	}

	writer := newTopicWriter(*outputDir)
	if err := writer.ensureDir(); err != nil {
		fmt.Fprintf(os.Stderr, "output: %v\n", err)
		os.Exit(1)
	}

	if isXMLInput(*inputPath) {
		payload, err := io.ReadAll(input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read xml: %v\n", err)
			os.Exit(1)
		}
		processPayload(ctx, resolver, payload, cfg, topics, writer)
		return
	}

	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		processPayload(ctx, resolver, []byte(line), cfg, topics, writer)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan: %v\n", err)
		os.Exit(1)
	}
}

func buildResolver(ctx context.Context) (*lfs.Resolver, error) {
	bucket := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_BUCKET"))
	region := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_REGION"))
	endpoint := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_ENDPOINT"))
	accessKey := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_ACCESS_KEY"))
	secretKey := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_SECRET_KEY"))
	sessionToken := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_SESSION_TOKEN"))
	forcePathStyle := envBoolDefault("KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE", endpoint != "")
	maxSize := envInt64("KAFSCALE_IDOC_MAX_BLOB_SIZE", 0)
	validate := envBoolDefault("KAFSCALE_IDOC_VALIDATE_CHECKSUM", true)

	if bucket == "" || region == "" {
		return lfs.NewResolver(lfs.ResolverConfig{MaxSize: maxSize, ValidateChecksum: validate}, nil), nil
	}

	s3Client, err := lfs.NewS3Client(ctx, lfs.S3Config{
		Bucket:          bucket,
		Region:          region,
		Endpoint:        endpoint,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
		ForcePathStyle:  forcePathStyle,
	})
	if err != nil {
		return nil, err
	}
	return lfs.NewResolver(lfs.ResolverConfig{MaxSize: maxSize, ValidateChecksum: validate}, s3Client), nil
}

func resolvePayload(ctx context.Context, resolver *lfs.Resolver, raw []byte) ([]byte, error) {
	trimmed := strings.TrimSpace(string(raw))
	if strings.HasPrefix(trimmed, "<") {
		return raw, nil
	}
	if resolver == nil {
		return nil, errors.New("resolver not configured")
	}
	res, ok, err := resolver.Resolve(ctx, raw)
	if err != nil {
		return nil, err
	}
	if !ok {
		return raw, nil
	}
	return res.Payload, nil
}

func openInput(path string) (*os.File, error) {
	if strings.TrimSpace(path) == "" {
		return os.Stdin, nil
	}
	return os.Open(path)
}

type topicWriter struct {
	base string
}

func newTopicWriter(base string) *topicWriter {
	return &topicWriter{base: base}
}

func (w *topicWriter) ensureDir() error {
	return os.MkdirAll(w.base, 0o755)
}

func (w *topicWriter) write(records idoc.TopicRecords) error {
	for topic, entries := range records {
		if len(entries) == 0 {
			continue
		}
		path := filepath.Join(w.base, fmt.Sprintf("%s.jsonl", topic))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		writer := bufio.NewWriter(f)
		for _, entry := range entries {
			if _, err := writer.Write(entry); err != nil {
				_ = f.Close()
				return err
			}
			if err := writer.WriteByte('\n'); err != nil {
				_ = f.Close()
				return err
			}
		}
		if err := writer.Flush(); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func parseCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		val := strings.TrimSpace(part)
		if val != "" {
			out = append(out, val)
		}
	}
	return out
}

func envOrDefault(key, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return fallback
}

func envBoolDefault(key string, fallback bool) bool {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}
	switch strings.ToLower(val) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func envInt64(key string, fallback int64) int64 {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}
	parsed, err := parseInt64(val)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseInt64(raw string) (int64, error) {
	var out int64
	for _, ch := range strings.TrimSpace(raw) {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid integer")
		}
		out = out*10 + int64(ch-'0')
	}
	return out, nil
}

func processPayload(ctx context.Context, resolver *lfs.Resolver, raw []byte, cfg idoc.ExplodeConfig, topics idoc.TopicConfig, writer *topicWriter) {
	payload, err := resolvePayload(ctx, resolver, raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve payload: %v\n", err)
		return
	}
	result, err := idoc.ExplodeXML(payload, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "explode: %v\n", err)
		return
	}
	records, err := result.ToTopicRecords(topics)
	if err != nil {
		fmt.Fprintf(os.Stderr, "records: %v\n", err)
		return
	}
	if err := writer.write(records); err != nil {
		fmt.Fprintf(os.Stderr, "write: %v\n", err)
		return
	}
}

func isXMLInput(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	return strings.HasSuffix(strings.ToLower(path), ".xml")
}
