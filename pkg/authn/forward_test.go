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

package authn

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testKey = bytes.Repeat([]byte{17}, 32)

func TestForwardIdentity(t *testing.T) {
	for _, principal := range []string{"service-a", "service-b", ProbePrincipal, strings.Repeat("x", 256)} {
		server, client := net.Pipe()
		done := make(chan error, 1)
		go func() {
			defer server.Close()
			got, err := ReceiveIdentity(server, testKey)
			if err == nil && got != principal {
				err = errors.New("wrong principal")
			}
			done <- err
			if err == nil {
				_, _ = io.Copy(io.Discard, server)
			}
		}()
		if err := SendIdentity(client, testKey, principal); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		client.Close()
	}
	ctx := WithPrincipal(context.Background(), "a")
	if Principal(ctx) != "a" || Principal(context.Background()) != "" {
		t.Fatal("context principal mismatch")
	}
}
func TestReadKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key")
	if _, err := ReadKey(path); err == nil {
		t.Fatal("missing key accepted")
	}
	writeTestFile(t, path, []byte("short"))
	if _, err := ReadKey(path); err == nil {
		t.Fatal("short key accepted")
	}
	key := append([]byte(" \n"), testKey...)
	writeTestFile(t, path, key)
	got, err := ReadKey(path)
	if err != nil || !bytes.Equal(got, key) {
		t.Fatal("key altered", err)
	}
}

type scriptConn struct {
	reader        io.Reader
	writer        io.Writer
	deadlineError bool
}

func (c *scriptConn) Read(b []byte) (int, error)  { return c.reader.Read(b) }
func (c *scriptConn) Write(b []byte) (int, error) { return c.writer.Write(b) }
func (c *scriptConn) Close() error                { return nil }
func (c *scriptConn) LocalAddr() net.Addr         { return &net.TCPAddr{} }
func (c *scriptConn) RemoteAddr() net.Addr        { return &net.TCPAddr{} }
func (c *scriptConn) SetDeadline(time.Time) error {
	if c.deadlineError {
		return errors.New("deadline")
	}
	return nil
}
func (c *scriptConn) SetReadDeadline(t time.Time) error  { return c.SetDeadline(t) }
func (c *scriptConn) SetWriteDeadline(t time.Time) error { return c.SetDeadline(t) }

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func scripted(data []byte) *scriptConn {
	return &scriptConn{reader: bytes.NewReader(data), writer: io.Discard}
}
func TestSendIdentityFailures(t *testing.T) {
	challenge := append([]byte(challengeMagic), make([]byte, 32)...)
	for _, principal := range []string{"", strings.Repeat("a", 257), "a b", "a\n", string([]byte{255})} {
		if SendIdentity(scripted(nil), testKey, principal) == nil {
			t.Fatal("invalid principal accepted")
		}
	}
	if SendIdentity(scripted(nil), nil, "a") == nil {
		t.Fatal("short key accepted")
	}
	c := scripted(nil)
	c.deadlineError = true
	if SendIdentity(c, testKey, "a") == nil {
		t.Fatal("deadline error ignored")
	}
	for _, data := range [][]byte{nil, make([]byte, len(challenge)), challenge, append(append([]byte{}, challenge...), 0)} {
		if SendIdentity(scripted(data), testKey, "a") == nil {
			t.Fatal("bad challenge/ack accepted")
		}
	}
	c = scripted(challenge)
	c.writer = failWriter{}
	if SendIdentity(c, testKey, "a") == nil {
		t.Fatal("write failure ignored")
	}
}
func TestReceiveIdentityFailures(t *testing.T) {
	if _, err := ReceiveIdentity(scripted(nil), nil); err == nil {
		t.Fatal("short key accepted")
	}
	c := scripted(nil)
	c.deadlineError = true
	if _, err := ReceiveIdentity(c, testKey); err == nil {
		t.Fatal("deadline ignored")
	}
	c = scripted(nil)
	c.writer = failWriter{}
	if _, err := ReceiveIdentity(c, testKey); err == nil {
		t.Fatal("write failure ignored")
	}
	for _, data := range [][]byte{nil, {0, 0}, {1, 1}, {0, 1}, {0, 1, 'a'}, append([]byte{0, 1, 'a'}, make([]byte, sha256.Size)...), append([]byte{0, 1, ' '}, make([]byte, sha256.Size)...)} {
		if _, err := ReceiveIdentity(scripted(data), testKey); err == nil {
			t.Fatal("invalid response accepted")
		}
	}
}

type failNthWrite struct {
	net.Conn
	n, failAt int
}

func (c *failNthWrite) Write(b []byte) (int, error) {
	c.n++
	if c.n == c.failAt {
		return 0, errors.New("write failed")
	}
	return c.Conn.Write(b)
}
func TestIdentityAckWriteFailure(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		_, err := ReceiveIdentity(&failNthWrite{Conn: server, failAt: 2}, testKey)
		done <- err
	}()
	if err := SendIdentity(client, testKey, "a"); err == nil {
		t.Fatal("missing ack accepted")
	}
	if err := <-done; err == nil {
		t.Fatal("ack failure ignored")
	}
}
func TestIdentityProofCannotBeReplayed(t *testing.T) {
	var captured []byte
	for i := 0; i < 2; i++ {
		server, client := net.Pipe()
		done := make(chan error, 1)
		go func() {
			defer server.Close()
			_, err := ReceiveIdentity(server, testKey)
			done <- err
			if err == nil {
				_, _ = io.Copy(io.Discard, server)
			}
		}()
		challenge := make([]byte, len(challengeMagic)+32)
		if _, err := io.ReadFull(client, challenge); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			captured = make([]byte, 2)
			binary.BigEndian.PutUint16(captured, 1)
			captured = append(captured, 'a')
			captured = append(captured, identityMAC(testKey, challenge, []byte("a"))...)
		}
		if err := writeBytes(client, captured); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			ack := make([]byte, 1)
			if _, err := io.ReadFull(client, ack); err != nil || ack[0] != 1 {
				t.Fatal("valid proof rejected", err)
			}
		}
		err := <-done
		client.Close()
		if (i == 0) != (err == nil) {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
}

func TestIdentityDeadlineResetFailures(t *testing.T) {
	for _, receiverFails := range []bool{false, true} {
		server, client := net.Pipe()
		done := make(chan error, 1)
		go func() {
			defer server.Close()
			c := net.Conn(server)
			if receiverFails {
				c = &deadlineConn{Conn: c, failAt: 2}
			}
			_, err := ReceiveIdentity(c, testKey)
			done <- err
			if err == nil {
				_, _ = io.Copy(io.Discard, server)
			}
		}()
		c := net.Conn(client)
		if !receiverFails {
			c = &deadlineConn{Conn: c, failAt: 2}
		}
		err := SendIdentity(c, testKey, "a")
		client.Close()
		serverErr := <-done
		if receiverFails && serverErr == nil {
			t.Fatal("receiver deadline reset failure ignored")
		}
		if !receiverFails && err == nil {
			t.Fatal("sender deadline reset failure ignored")
		}
	}
}

func FuzzReceiveIdentity(f *testing.F) {
	for _, seed := range [][]byte{{}, {0, 0}, {0, 1, 'a'}, {1, 1}, {255, 255}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) { _, _ = ReceiveIdentity(scripted(data), testKey) })
}

func BenchmarkIdentityHandshake(b *testing.B) {
	for i := 0; i < b.N; i++ {
		server, client := net.Pipe()
		done := make(chan error, 1)
		go func() {
			defer server.Close()
			_, err := ReceiveIdentity(server, testKey)
			done <- err
			if err == nil {
				_, _ = io.Copy(io.Discard, server)
			}
		}()
		if err := SendIdentity(client, testKey, "service-a"); err != nil {
			b.Fatal(err)
		}
		if err := <-done; err != nil {
			b.Fatal(err)
		}
		client.Close()
	}
}
