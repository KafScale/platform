package discovery

import "testing"

func TestParseSegmentKey(t *testing.T) {
	cases := []struct {
		name      string
		prefix    string
		key       string
		expected  segmentKey
		kind      string
		shouldOK  bool
	}{
		{
			name:   "kfs with namespace",
			prefix: "prod/",
			key:    "prod/orders/0/segment-00000000000000000042.kfs",
			expected: segmentKey{
				topic:      "orders",
				partition:  0,
				baseOffset: 42,
			},
			kind:     "kfs",
			shouldOK: true,
		},
		{
			name:   "index without namespace",
			prefix: "",
			key:    "orders/12/segment-7.index",
			expected: segmentKey{
				topic:      "orders",
				partition:  12,
				baseOffset: 7,
			},
			kind:     "index",
			shouldOK: true,
		},
		{
			name:     "invalid prefix",
			prefix:   "prod/",
			key:      "dev/orders/0/segment-1.kfs",
			shouldOK: false,
		},
		{
			name:     "invalid filename",
			prefix:   "",
			key:      "orders/0/segment-abc.kfs",
			shouldOK: false,
		},
	}

	for _, tc := range cases {
		got, kind, ok := parseSegmentKey(tc.prefix, tc.key)
		if ok != tc.shouldOK {
			t.Fatalf("%s: expected ok=%v, got %v", tc.name, tc.shouldOK, ok)
		}
		if !tc.shouldOK {
			continue
		}
		if got != tc.expected {
			t.Fatalf("%s: expected %+v, got %+v", tc.name, tc.expected, got)
		}
		if kind != tc.kind {
			t.Fatalf("%s: expected kind %q, got %q", tc.name, tc.kind, kind)
		}
	}
}
