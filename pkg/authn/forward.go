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

// Package authn authenticates Kafka connections and their proxy identities.
package authn

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const handshakeTimeout = 5 * time.Second
const challengeMagic = "KSAUTH01"
const ProbePrincipal = "__kafscale_proxy_probe"
const LFSPrincipal = "__kafscale_lfs_service"

type principalKey struct{}

// WithPrincipal binds an authenticated identity to internal routing calls.
func WithPrincipal(ctx context.Context, principal string) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

func Principal(ctx context.Context) string {
	principal, _ := ctx.Value(principalKey{}).(string)
	return principal
}

func validPrincipal(principal string) bool {
	return len(principal) > 0 && len(principal) <= 256 && utf8.ValidString(principal) && strings.IndexFunc(principal, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) == -1
}

// ReadKey reads a raw shared secret, without trimming or logging its contents.
// Reading on each connection permits Secret volume rotation without pod restarts.
func ReadKey(path string) ([]byte, error) {
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(key) < 32 {
		return nil, errors.New("proxy identity key must contain at least 32 bytes")
	}
	return key, nil
}

func identityMAC(key, challenge, principal []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(challenge)
	mac.Write(principal)
	return mac.Sum(nil)
}

func writeBytes(w io.Writer, data []byte) error {
	_, err := io.Copy(w, bytes.NewReader(data))
	return err
}

// SendIdentity answers a broker-generated challenge before any Kafka frames.
// The principal is fixed for the lifetime of this backend connection.
func SendIdentity(conn net.Conn, key []byte, principal string) error {
	if !validPrincipal(principal) || len(key) < 32 {
		return errors.New("invalid proxy identity configuration")
	}
	if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return err
	}
	challenge := make([]byte, len(challengeMagic)+32)
	if _, err := io.ReadFull(conn, challenge); err != nil {
		return err
	}
	if string(challenge[:len(challengeMagic)]) != challengeMagic {
		return errors.New("broker identity challenge required")
	}
	payload := make([]byte, 2, 2+len(principal)+sha256.Size)
	binary.BigEndian.PutUint16(payload, uint16(len(principal)))
	payload = append(payload, principal...)
	payload = append(payload, identityMAC(key, challenge, []byte(principal))...)
	if err := writeBytes(conn, payload); err != nil {
		return err
	}
	ack := make([]byte, 1)
	if _, err := io.ReadFull(conn, ack); err != nil {
		return err
	}
	if ack[0] != 1 {
		return errors.New("broker rejected proxy identity")
	}
	return conn.SetDeadline(time.Time{})
}

// ReceiveIdentity requires proof of the shared key, bound to a fresh challenge.
// It never accepts legacy Kafka frames or client.id as an identity fallback.
// NetworkPolicies must still protect the plaintext internal Kafka stream.
func ReceiveIdentity(conn net.Conn, key []byte) (string, error) {
	if len(key) < 32 {
		return "", errors.New("invalid proxy identity key")
	}
	if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return "", err
	}
	challenge := make([]byte, len(challengeMagic)+32)
	copy(challenge, challengeMagic)
	rand.Read(challenge[len(challengeMagic):])
	if err := writeBytes(conn, challenge); err != nil {
		return "", err
	}
	size := make([]byte, 2)
	if _, err := io.ReadFull(conn, size); err != nil {
		return "", err
	}
	n := int(binary.BigEndian.Uint16(size))
	if n == 0 || n > 256 {
		return "", errors.New("invalid proxy principal length")
	}
	payload := make([]byte, n+sha256.Size)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return "", err
	}
	principal := string(payload[:n])
	if !validPrincipal(principal) || !hmac.Equal(payload[n:], identityMAC(key, challenge, payload[:n])) {
		return "", errors.New("invalid proxy identity proof")
	}
	if err := writeBytes(conn, []byte{1}); err != nil {
		return "", err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return "", err
	}
	return principal, nil
}
