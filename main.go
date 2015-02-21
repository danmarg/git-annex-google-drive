package main

import (
	"bufio"
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	drive "google.golang.org/api/drive/v2"
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

const (
	// OAuth
	oauthClientId     = "1019961849531-cdd5lb3cum793l4v802f2vva3q622mmk.apps.googleusercontent.com"
	oauthClientSecret = "3ExqSKcqRGpTZDm0WRKhwCRl"
	// Other
	remoteRootDir = "annex"
	chunkSize     = 4096
)

var (
	// Input/output channels. We could write to stdin/stdout directly, but this abstracts that a little bit.
	input  <-chan string
	output chan<- string
	done   sync.WaitGroup
	// If true, we don't block on STDIN being closed. Makes testing easier.
	debug bool
	// GDrive client.
	svc      *drive.Service
	oauthCfg *oauth2.Config = &oauth2.Config{
		ClientID:     oauthClientId,
		ClientSecret: oauthClientSecret,
		Scopes:       []string{drive.DriveScope},
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://accounts.google.com/o/oauth2/auth",
			TokenURL: "https://accounts.google.com/o/oauth2/token",
		},
		RedirectURL: "urn:ietf:wg:oauth:2.0:oob",
	}
	// Cache what directories exist remotely.
	remoteCache = map[string]*drive.File{}
)

func print(s string, v interface{}) error {
	_, e := fmt.Fprintf(os.Stderr, s, v)
	return e
}

func logErr(err error) {
	log.Printf("%v", err)
	output <- fmt.Sprintf("ERROR %v", err)
}

func init() {
	flag.BoolVar(&debug, "debug", false, "Debug mode (don't block on STDIN)")
	flag.Parse()

	if debug {
		done.Add(1)
	} else {
		done.Add(2)
	}
	// Input.
	i := make(chan string)
	input = i
	go func() {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			i <- s.Text()
		}
		if err := s.Err(); err != nil {
			logErr(err)
		}
		close(i)
		done.Done()
	}()
	// Output.
	o := make(chan string)
	output = o
	go func() {
		defer os.Stdout.Close()
		defer done.Done()
		for i := range o {
			fmt.Printf("%v\n", i)
		}
	}()
}

type handler func(args []string) error

func main() {
	output <- "VERSION 1"

	handlers := map[string]handler{
		"INITREMOTE":     initremote,
		"PREPARE":        prepare,
		"TRANSFER STORE": transfer,
	}

	for msg := range input {
		parts := strings.Split(msg, " ")
		var hndlr handler
		var args []string
		for k, h := range handlers {
			pat := strings.Split(k, " ")
			if len(pat) > len(parts) {
				continue
			}
			match := true
			for i, _ := range pat {
				if pat[i] != parts[i] {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			hndlr = h
			args = parts[len(pat):]
		}
		if hndlr == nil {
			output <- "UNSUPPORTED-REQUEST"
		} else if err := hndlr(args); err != nil {
			logErr(err)
		}
	}

	close(output)
	done.Wait()
}

func getvalue(request string) ([]string, error) {
	output <- request
	r := <-input
	parts := strings.Split(r, " ")
	if len(parts) < 1 || parts[0] != "VALUE" {
		return []string{}, fmt.Errorf("protocol error: unexpected reply to %v", request)
	}
	return parts[1:], nil
}

// Initremote initializes the OAuth creds. Because we can't get input from the
// user except through env vars, we do a rather poor exchange, where we print
// the URL for auth and then exit with an error, then the user reruns with the
// auth code in the OAUTH env var.
func initremote(args []string) error {
	// If this is a second run, OAUTH will be set.
	code := os.Getenv("OAUTH")
	if code != "" {
		tok, err := oauthCfg.Exchange(oauth2.NoContext, code)
		if err != nil {
			output <- fmt.Sprintf("INITREMOTE-FAILURE %v", err)
			return nil
		}
		output <- fmt.Sprintf("SETCREDS oauth oauth %s", tok.RefreshToken)
		output <- "INITREMOTE-SUCCES"
	} else {
		url := oauthCfg.AuthCodeURL("state", oauth2.AccessTypeOffline)
		print("Visit the URL for the OAuth dialog: %v", url)
		output <- "INITREMOTE-FAILURE missing OAUTH env var"
	}
	return nil
}

func prepare(args []string) error {
	output <- "GETCREDS oauth"
	r := <-input
	parts := strings.Split(r, " ")
	if len(parts) < 3 || parts[0] != "CREDS" {
		return fmt.Errorf("protocol error: unexpected reply to GETCREDS")
	}
	// TODO: Does this work? Or do we have to store the access token and expiry as well?
	t := oauth2.Token{RefreshToken: parts[2]}

	var err error
	svc, err = drive.New(oauthCfg.Client(oauth2.NoContext, &t))
	if err != nil {
		output <- fmt.Sprintf("PREPARE-FAILURE %v", err)
	} else {
		output <- "PREPARE-SUCCESS"
	}
	return nil
}

func maybeCreateFile(parents string, pth string, parent *drive.File) (*drive.File, error) {
	h, tail := path.Split(pth)
	if h == "" {
		h, tail = tail, ""
	}
	p := path.Join(parents, h)
	f, exists := remoteCache[p]
	if !exists {
		f := &drive.File{Title: h}
		if parent != nil {
			f.Parents = []*drive.ParentReference{&drive.ParentReference{Id: parent.Id}}
		}
		f, err := svc.Files.Insert(f).Do()
		if err != nil {
			return nil, err
		}
		remoteCache[p] = f
	}
	if tail != "" {
		return maybeCreateFile(p, tail, f)
	} else {
		return f, nil
	}
}

func transfer(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("protocol error: unexpected args %v to TRANSFER STORE", args)
	}
	k := args[0]
	t := args[1]
	// Get a dirhash to use to write remote with.
	h, err := getvalue("DIRHASH " + k)
	if err != nil {
		return err
	}
	if len(h) != 1 {
		return fmt.Errorf("protocol error: unexpeted %v for DIRHASH", h)
	}
	// Create the file object.
	f, err := maybeCreateFile("", path.Join(h[0], k), nil)
	if err != nil {
		output <- fmt.Sprintf("TRANSFER-FAILURE STORE %v %v", k, err)
		return nil
	}
	// Upload the contents.
	local, err := os.Open(t)
	defer local.Close()
	if err != nil {
		output <- fmt.Sprintf("TRANSFER-FAILURE STORE %v %v", k, err)
		return nil
	}
	u := svc.Files.Update(f.Id, f).ResumableMedia(context.TODO(), local, chunkSize, "").ProgressUpdater(
		func(current, total int64) {
			output <- fmt.Sprintf("PROGRESS %d", current)
		})
	_, err = u.Do()
	if err != nil {
		output <- fmt.Sprintf("TRANSFER-FAILURE STORE %v, %v", k, err)
		return nil
	}
	output <- fmt.Sprintf("TRANSFER-SUCCESS STORE %v", k)
	return nil
}
