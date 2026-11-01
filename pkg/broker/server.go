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

package broker

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/KafScale/platform/pkg/protocol"
)

// Handler processes parsed Kafka protocol requests and returns the response payload.
type Handler interface {
	Handle(ctx context.Context, header *protocol.RequestHeader, req kmsg.Request) ([]byte, error)
}

// Server implements minimal Kafka TCP handling for milestone 1.
type Server struct {
	Addr string
	// WriteTimeout defaults to 30 seconds when nonpositive.
	WriteTimeout    time.Duration
	Handler         Handler
	ConnContextFunc ConnContextFunc
	listener        net.Listener
	wg              sync.WaitGroup
}

// ConnContextFunc can wrap a connection and attach connection-scoped context data.
type ConnContextFunc func(conn net.Conn) (net.Conn, *ConnContext, error)

// ListenAndServe starts accepting Kafka protocol connections.
func (s *Server) ListenAndServe(ctx context.Context) error {
	if s.Handler == nil {
		return errors.New("broker.Server requires a Handler")
	}
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	s.listener = ln
	log.Printf("broker listening on %s", ln.Addr())

	go func() {
		<-ctx.Done()
		_ = s.listener.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			if ne, ok := err.(net.Error); ok && !ne.Timeout() {
				log.Printf("accept error: %v", err)
				continue
			}
			return err
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleConnection(c)
		}(conn)
	}
}

// Wait blocks until all connection goroutines exit.
func (s *Server) Wait() {
	s.wg.Wait()
}

// ListenAddress returns the actual listener address if the server has started.
func (s *Server) ListenAddress() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.Addr
}

func (s *Server) handleConnection(conn net.Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	var readerDone chan struct{}
	defer func() {
		cancel()
		_ = conn.Close()
		if readerDone != nil {
			<-readerDone
		}
	}()
	// Socket metadata is available even when no authentication hook is configured.
	ctx = ContextWithConnInfo(ctx, &ConnContext{RemoteAddr: conn.RemoteAddr().String()})
	if s.ConnContextFunc != nil {
		wrapped, info, err := s.ConnContextFunc(conn)
		if err != nil {
			log.Printf("connection context setup failed: %v", err)
			return
		}
		if wrapped != nil {
			conn = wrapped
		}
		if info != nil {
			copy := *info
			if copy.RemoteAddr == "" {
				copy.RemoteAddr = conn.RemoteAddr().String()
			}
			ctx = ContextWithConnInfo(ctx, &copy)
		}
	}
	// One reader owns the socket, including while a handler waits. Read-ahead
	// is bounded and handlers still run serially in request order.
	clientGone := make(chan struct{})
	ctx = protocol.ContextWithClientGone(ctx, clientGone)
	frames := make(chan *protocol.Frame, 1)
	readerDone = make(chan struct{})
	go func() {
		defer close(readerDone)
		defer close(frames)
		defer close(clientGone)
		for {
			frame, err := protocol.ReadFrame(conn)
			if err != nil {
				if !errors.Is(err, io.EOF) && ctx.Err() == nil {
					log.Printf("read frame: %v", err)
				}
				return
			}
			select {
			case frames <- frame:
			case <-ctx.Done():
				return
			}
		}
	}()
	writeFailed := false
	for {
		var frame *protocol.Frame
		select {
		case frame = <-frames:
			if frame == nil || ctx.Err() != nil {
				return
			}
		case <-ctx.Done():
			return
		}
		header, req, err := protocol.ParseRequest(frame.Payload)
		if err != nil {
			log.Printf("parse request: %v (payload bytes=%d)", err, len(frame.Payload))
			return
		}
		respPayload, err := s.Handler.Handle(ctx, header, req)
		if ctx.Err() != nil {
			return
		}
		select {
		case <-clientGone:
			continue
		default:
		}
		if err != nil {
			log.Printf("handle request api=%d v=%d: %v", header.APIKey, header.APIVersion, err)
			// Send an UNKNOWN_SERVER_ERROR response instead of dropping the
			// connection so the client can recover gracefully.
			if errResp := buildErrorResponse(header); errResp != nil && !writeFailed {
				if err := protocol.WriteFrameWithDeadline(conn, errResp, s.WriteTimeout); err != nil {
					writeFailed = true
					_ = conn.Close()
				}
			}
			continue
		}
		if respPayload == nil || writeFailed {
			continue
		}
		if err := protocol.WriteFrameWithDeadline(conn, respPayload, s.WriteTimeout); err != nil {
			writeFailed = true
			log.Printf("write frame: %v", err)
			_ = conn.Close()
			// Drain complete requests that were read before the failed response.
			continue
		}
	}
}

// buildErrorResponse creates a minimal Kafka error response for the given
// request header so the client receives a proper error instead of a closed
// connection. Returns nil if no suitable response can be constructed.
func buildErrorResponse(header *protocol.RequestHeader) []byte {
	resp := kmsg.ResponseForKey(header.APIKey)
	if resp == nil {
		return nil
	}
	resp.SetVersion(header.APIVersion)
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp)
}
