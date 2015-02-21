package main

import (
	"bufio"
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	drive "google.golang.org/api/drive/v2"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
)

const (
	// OAuth
	oauthClientId     = "1019961849531-cdd5lb3cum793l4v802f2vva3q622mmk.apps.googleusercontent.com"
	oauthClientSecret = "3ExqSKcqRGpTZDm0WRKhwCRl"
	// Other
	chunkSize = 4096
)

var (
	// Input/output channels. We could write to stdin/stdout directly, but this abstracts that a little bit.
	input  <-chan string
	output chan<- string
	done   sync.WaitGroup
	// If true, we don't block on STDIN being closed. Makes testing easier.
	debug bool
	// GDrive client.
	svc        *drive.Service
	httpClient *http.Client
	oauthCfg   *oauth2.Config = &oauth2.Config{
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
	remoteCache   = map[string]*drive.File{}
	remoteRootDir = "annex"
)

func print(s string, v ...interface{}) error {
	_, e := fmt.Fprintf(os.Stderr, s, v...)
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
		"INITREMOTE":        initremote,
		"PREPARE":           prepare,
		"TRANSFER STORE":    transfer,
		"TRANSFER RETRIEVE": retrieve,
		"CHECKPRESENT":      checkpresent,
		"REMOVE":            remove,
		"AVAILABILITY":      availability,
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

// Initremote initializes the OAuth creds. Because we can't get input from the
// user except through env vars, we do a rather poor exchange, where we print
// the URL for auth and then exit with an error, then the user reruns with the
// auth code in the OAUTH env var.
func initremote(args []string) error {
	// If this is a second run, OAUTH will be set.
	code := os.Getenv("OAUTH")
	if code != "" {
		print("here")
		tok, err := oauthCfg.Exchange(oauth2.NoContext, code)
		print("here done")
		if err != nil {
			print("TOKEN: [[%v]]", code)
			output <- fmt.Sprintf("INITREMOTE-FAILURE %v", err)
			return nil
		}
		print("here ok")
		output <- fmt.Sprintf("SETCREDS oauth oauth %s", tok.RefreshToken)
		output <- "INITREMOTE-SUCCESS"
		print("here done")
	} else {
		url := oauthCfg.AuthCodeURL("state", oauth2.AccessTypeOffline)
		print("***\nVisit the URL for the OAuth dialog: %v\n***\n", url)
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
	httpClient = oauthCfg.Client(oauth2.NoContext, &t)
	svc, err = drive.New(httpClient)
	if err != nil {
		output <- fmt.Sprintf("PREPARE-FAILURE %v", err)
		return nil
	}

	// Get the remote dir.
	output <- "GETCONFIG directory"
	r = <-input
	parts = strings.Split(r, " ")
	if len(parts) != 2 || parts[0] != "VALUE" {
		return fmt.Errorf("protocol error: unexpected reply to GETCONFIG")
	}
	remoteRootDir = parts[1]
	output <- "PREPARE-SUCCESS"
	return nil
}

func transfer(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("protocol error: unexpected args %v to TRANSFER STORE", args)
	}
	k := args[0]
	t := args[1]
	// Create the file object.
	f, err := getFile(k)
	if err == notfound {
		p, err := makeOrGetRoot()
		if err != nil {
			output <- fmt.Sprintf("TRANSFER-FAILURE STORE %s %v", k, err)
			return nil
		}
		f = &drive.File{
			Title:   k,
			Parents: []*drive.ParentReference{&drive.ParentReference{Id: p.Id}}}
	} else if err != nil {
		output <- fmt.Sprintf("TRANSFER-FAILURE STORE %s %v", k, err)
		return nil
	}
	// Upload the contents.
	local, err := os.Open(t)
	defer local.Close()
	if err != nil {
		output <- fmt.Sprintf("TRANSFER-FAILURE STORE %s %v", k, err)
		return nil
	}
	u := svc.Files.Insert(f).ResumableMedia(context.TODO(), local, chunkSize, "").ProgressUpdater(
		func(current, total int64) {
			output <- fmt.Sprintf("PROGRESS %d", current)
		})
	_, err = u.Do()
	if err != nil {
		output <- fmt.Sprintf("TRANSFER-FAILURE STORE %s, %v", k, err)
		return nil
	}
	remoteCache[k] = f
	output <- fmt.Sprintf("TRANSFER-SUCCESS STORE %v", k)
	return nil
}

var notfound error = fmt.Errorf("not found")

func getFile(k string) (*drive.File, error) {
	f, ok := remoteCache[k]
	if ok {
		return f, nil
	}
	fs, err := svc.Files.List().Q(fmt.Sprintf("title='%s' and parent='%s' and trashed=false", k, remoteRootDir)).Do()
	if err != nil {
		return nil, err
	}
	for _, f := range fs.Items {
		if f.Title == k {
			return f, nil
		}
	}
	return nil, notfound
}

func makeOrGetRoot() (*drive.File, error) {
	if f, ok := remoteCache[remoteRootDir]; ok {
		return f, nil
	}
	f := &drive.File{Title: remoteRootDir}
	f, err := svc.Files.Insert(f).Do()
	if err != nil {
		return nil, err
	}
	remoteCache[remoteRootDir] = f
	return f, nil
}

func retrieve(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("protocol error: unexpected args %v to TRANSFER STORE", args)
	}
	k := args[0]
	t := args[1]
	// Get the file ID.
	f, err := getFile(k)
	if err != nil {
		output <- fmt.Sprintf("TRANSFER-FAILURE RETRIEVE %s %v", k, err)
		return nil
	}
	r, err := httpClient.Get(f.DownloadUrl)
	if err != nil {
		output <- fmt.Sprintf("TRANSFER-FAILURE RETRIEVE %s %v", k, err)
		return nil
	}
	w, err := os.Open(t)
	defer w.Close()
	if err != nil {
		output <- fmt.Sprintf("TRANSFER-FAILURE RETRIEVE %s %v", k, err)
		return nil
	}
	c := 0
	for {
		b := make([]byte, chunkSize)
		n, err := r.Body.Read(b)
		if err == io.EOF {
			break
		} else if err != nil {
			output <- fmt.Sprintf("TRANSFER-FAILURE RETRIEVE %s %v", k, err)
			return nil
		}
		c += n
		output <- fmt.Sprintf("PROGRESS %d", c)
		_, err = w.Write(b[:n])
		if err != nil {
			output <- fmt.Sprintf("TRANSFER-FAILURE RETRIEVE %s %v", k, err)
			return nil
		}
	}
	output <- "TRANSFER-SUCCESS RETRIEVE " + k
	return nil
}

func checkpresent(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("protocol error: unexpected args %v to CHECKPRESENT", args)
	}
	k := args[0]
	_, err := getFile(k)
	if err == notfound {
		output <- fmt.Sprintf("CHECKPRESENT-FAILURE %s", k)

	} else if err != nil {
		output <- fmt.Sprintf("CHECKPRESENT-UNKNOWN %s, %v", k, err)
	} else {
		output <- fmt.Sprintf("CHECKPRESENT-SUCCESS %s", k)
	}
	return nil
}

func remove(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("protocol error: unexpected args %v to REMOVE", args)
	}
	k := args[0]
	f, err := getFile(k)
	if err != nil {
		output <- fmt.Sprintf("REMOVE-FAILURE %s %v", k, err)
		return nil
	}
	err = svc.Files.Delete(f.Id).Do()
	if err != nil {
		output <- fmt.Sprintf("REMOVE-FAILURE %s %v", k, err)
	} else {
		output <- fmt.Sprintf("REMOVE-SUCCESS %s", k)
	}
	return nil
}

func availability(args []string) error {
	output <- "AVAILABILITY REMOTE"
	return nil
}
