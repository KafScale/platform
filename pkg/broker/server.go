package broker

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"

	"github.com/alo/kafscale/pkg/protocol"
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
	log.Printf("broker listening on %s", s.Addr)

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
			log.Printf("parse request: %v", err)
			return
		}

		respPayload, err := s.Handler.Handle(ctx, header, req)
		if err != nil {
			log.Printf("handle request api=%d err=%v", header.APIKey, err)
			return
		}
		if err := protocol.WriteFrame(conn, respPayload); err != nil {
			log.Printf("write frame: %v", err)
			return
		}
	}
}
