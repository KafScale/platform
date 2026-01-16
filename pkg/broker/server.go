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

	"github.com/novatechflow/kafscale/pkg/protocol"
)

// Handler processes parsed Kafka protocol requests and returns the response payload.
type Handler interface {
	Handle(ctx context.Context, header *protocol.RequestHeader, req protocol.Request) ([]byte, error)
}

// Server implements minimal Kafka TCP handling for milestone 1.
type Server struct {
	Addr     string
	Handler  Handler
	listener net.Listener
	wg       sync.WaitGroup
}

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
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Printf("accept temporary error: %v", err)
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
	defer cancel()
	defer conn.Close()
	for {
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			log.Printf("read frame: %v", err)
			return
		}
		header, req, err := protocol.ParseRequest(frame.Payload)
		if err != nil {
			log.Printf("parse request: %v (payload bytes=%d)", err, len(frame.Payload))
			return
		}
		respPayload, err := s.Handler.Handle(ctx, header, req)
		if err != nil {
			log.Printf("handle request: %v", err)
			return
		}
		if respPayload == nil {
			continue
		}
		if err := protocol.WriteFrame(conn, respPayload); err != nil {
			log.Printf("write frame: %v", err)
			return
		}
	}
}
