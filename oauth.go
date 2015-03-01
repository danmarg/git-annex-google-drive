package main

import (
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"time"
)

func tokenFromEnvOrWeb(ctx context.Context, config *oauth2.Config) (*oauth2.Token, error) {
	code := os.Getenv("OAUTH")
	if code == "NOBROWSER" {
		authURL := config.AuthCodeURL("")
		return nil, fmt.Errorf("Authorize this app at %s and rerun with environment variable 'OAUTH' set to the auth code!", authURL)
	} else if code == "" {
		var err error
		print("Launching browser for OAuth exchange. If running remotely or no browser is installed, rerun with environment varibale 'OAUTH' set to 'NOBROWSER'.\n")
		code, err = tokenFromWeb(ctx, config)
		if err != nil {
			config.RedirectURL = "urn:ietf:wg:oauth:2.0:oob"
			authURL := config.AuthCodeURL("")
			return nil, fmt.Errorf("Authorize this app at %s and rerun with environment variable 'OAUTH' set to the auth code!", authURL)
		}
	}
	token, err := config.Exchange(ctx, code)
	return token, err
}

func tokenFromWeb(ctx context.Context, config *oauth2.Config) (string, error) {
	ch := make(chan string)
	randState := fmt.Sprintf("st%d", time.Now().UnixNano())
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/favicon.ico" {
			http.Error(rw, "", 404)
			return
		}
		if req.FormValue("state") != randState {
			log.Printf("State doesn't match: req = %#v", req)
			http.Error(rw, "", 500)
			return
		}
		if code := req.FormValue("code"); code != "" {
			fmt.Fprintf(rw, "<h1>Success</h1>Authorized.")
			rw.(http.Flusher).Flush()
			ch <- code
			return
		}
		http.Error(rw, "", 500)
	}))
	defer ts.Close()
	config.RedirectURL = ts.URL
	authURL := config.AuthCodeURL(randState)
	errs := make(chan error)
	go func() {
		err := openURL(authURL)
		errs <- err
	}()
	err := <-errs
	if err == nil {
		code := <-ch
		return code, nil
	} else {
		return "", err
	}
}

func openURL(url string) error {
	try := []string{"xdg-open", "google-chrome", "open"}
	for _, bin := range try {
		err := exec.Command(bin, url).Run()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("Error opening URL in browser.")
}
