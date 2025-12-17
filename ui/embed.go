package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed public/*
var content embed.FS

// StaticHandler returns an http.Handler that serves the embedded UI assets.
func StaticHandler() (http.Handler, error) {
	sub, err := fs.Sub(content, "public")
	if err != nil {
		return nil, err
	}
	return http.FileServer(http.FS(sub)), nil
}
