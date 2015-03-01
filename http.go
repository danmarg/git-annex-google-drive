package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

type logTransport struct {
	rt http.RoundTripper
}

func (t *logTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var buf bytes.Buffer

	os.Stderr.Write([]byte("\n[request]\n"))
	if req.Body != nil {
		req.Body = ioutil.NopCloser(&readButCopy{req.Body, &buf})
	}
	req.Write(os.Stderr)
	if req.Body != nil {
		req.Body = ioutil.NopCloser(&buf)
	}
	os.Stderr.Write([]byte("\n[/request]\n"))

	res, err := t.rt.RoundTrip(req)

	log.Printf("[response]\n")
	if err != nil {
		log.Printf("ERROR: %v", err)
	} else {
		body := res.Body
		res.Body = nil
		res.Write(os.Stderr)
		if body != nil {
			res.Body = ioutil.NopCloser(&echoAsRead{body})
		}
	}

	return res, err
}

type echoAsRead struct {
	src io.Reader
}

func (r *echoAsRead) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 {
		os.Stderr.Write(p[:n])
	}
	if err == io.EOF {
		log.Printf("\n[/response]\n")
	}
	return n, err
}

type readButCopy struct {
	src io.Reader
	dst io.Writer
}

func (r *readButCopy) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 {
		r.dst.Write(p[:n])
	}
	return n, err
}
