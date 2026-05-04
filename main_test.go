package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

// --- state.same ---

func TestStateSame_firstCallFalse(t *testing.T) {
	s := &state{}
	if s.same("hello") {
		t.Fatal("first call should return false")
	}
}

func TestStateSame_duplicateTrue(t *testing.T) {
	s := &state{}
	s.same("hello")
	if !s.same("hello") {
		t.Fatal("duplicate should return true")
	}
}

func TestStateSame_differentFalse(t *testing.T) {
	s := &state{}
	s.same("hello")
	if s.same("world") {
		t.Fatal("different text should return false")
	}
}

func TestStateSame_emptyString(t *testing.T) {
	s := &state{}
	if s.same("") {
		t.Fatal("first empty string should return false")
	}
	if !s.same("") {
		t.Fatal("second empty string should return true")
	}
}

func TestStateSame_concurrent(t *testing.T) {
	s := &state{}
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.same(fmt.Sprintf("text-%d", i))
		}(i)
	}
	wg.Wait()
}

// --- toWSURL ---

func TestToWSURL_https(t *testing.T) {
	got := toWSURL("https://ntfy.example.com", "clipboard")
	want := "wss://ntfy.example.com/clipboard/ws"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestToWSURL_http(t *testing.T) {
	got := toWSURL("http://ntfy.example.com", "clipboard")
	want := "ws://ntfy.example.com/clipboard/ws"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestToWSURL_trailingSlash(t *testing.T) {
	got := toWSURL("https://ntfy.example.com/", "clipboard")
	want := "wss://ntfy.example.com/clipboard/ws"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestToWSURL_customTopic(t *testing.T) {
	got := toWSURL("https://ntfy.example.com", "my-secret-topic")
	want := "wss://ntfy.example.com/my-secret-topic/ws"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// --- pushToNtfy ---

func TestPushToNtfy_correctURLAndBody(t *testing.T) {
	var gotPath, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	pushToNtfy(srv.URL, "clipboard", "hello world", false, false)

	if gotPath != "/clipboard" {
		t.Fatalf("path: got %q, want %q", gotPath, "/clipboard")
	}
	if gotBody != "hello world" {
		t.Fatalf("body: got %q, want %q", gotBody, "hello world")
	}
}

func TestPushToNtfy_trailingSlashInURL(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(200)
	}))
	defer srv.Close()

	pushToNtfy(srv.URL+"/", "clipboard", "x", false, false)

	if gotPath != "/clipboard" {
		t.Fatalf("path: got %q, want %q", gotPath, "/clipboard")
	}
}

// --- socket → ntfy integration ---

// socketDaemon spins up a Unix socket that accepts one connection,
// reads the text, checks dedup via s, then calls onPush if not a dup.
// Closes after receiving `count` connections.
func socketDaemon(t *testing.T, socketPath string, s *state, onPush func(string), count int) {
	t.Helper()
	os.Remove(socketPath)
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer l.Close()
		for range count {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			func() {
				defer conn.Close()
				data, _ := io.ReadAll(conn)
				if len(data) == 0 {
					return
				}
				text := string(data)
				if s.same(text) {
					return
				}
				onPush(text)
			}()
		}
	}()
	t.Cleanup(func() {
		l.Close()
		wg.Wait()
		os.Remove(socketPath)
	})
}

func sendToSocket(t *testing.T, socketPath, text string) {
	t.Helper()
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	fmt.Fprint(conn, text)
}

func ntfyCollector(t *testing.T, ch chan<- string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.WriteHeader(200)
		ch <- string(b)
	}))
}

func waitPushes(t *testing.T, ch <-chan string, n int) []string {
	t.Helper()
	results := make([]string, 0, n)
	deadline := make(chan struct{})
	go func() {
		<-time.After(2 * time.Second)
		close(deadline)
	}()
	for range n {
		select {
		case s := <-ch:
			results = append(results, s)
		case <-deadline:
			t.Fatalf("timeout waiting for push %d/%d", len(results)+1, n)
		}
	}
	return results
}

func TestSocket_pushesTextToNtfy(t *testing.T) {
	ch := make(chan string, 1)
	srv := ntfyCollector(t, ch)
	defer srv.Close()

	socket := t.TempDir() + "/clip-sync.sock"
	s := &state{}
	socketDaemon(t, socket, s, func(text string) {
		pushToNtfy(srv.URL, "clipboard", text, false, false)
	}, 1)

	sendToSocket(t, socket, "hello")

	got := waitPushes(t, ch, 1)
	if got[0] != "hello" {
		t.Fatalf("got %q, want %q", got[0], "hello")
	}
}

func TestSocket_dedup_sameTextNotPushedTwice(t *testing.T) {
	ch := make(chan string, 2)
	srv := ntfyCollector(t, ch)
	defer srv.Close()

	socket := t.TempDir() + "/clip-sync.sock"
	s := &state{}
	socketDaemon(t, socket, s, func(text string) {
		pushToNtfy(srv.URL, "clipboard", text, false, false)
	}, 2)

	sendToSocket(t, socket, "hello")
	sendToSocket(t, socket, "hello")

	// expect exactly 1 push; wait briefly to confirm the second doesn't arrive
	got := waitPushes(t, ch, 1)
	if got[0] != "hello" {
		t.Fatalf("got %q, want %q", got[0], "hello")
	}
	select {
	case extra := <-ch:
		t.Fatalf("unexpected second push: %q", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSocket_dedup_differentTextPushedTwice(t *testing.T) {
	ch := make(chan string, 2)
	srv := ntfyCollector(t, ch)
	defer srv.Close()

	socket := t.TempDir() + "/clip-sync.sock"
	s := &state{}
	socketDaemon(t, socket, s, func(text string) {
		pushToNtfy(srv.URL, "clipboard", text, false, false)
	}, 2)

	sendToSocket(t, socket, "hello")
	sendToSocket(t, socket, "world")

	got := waitPushes(t, ch, 2)
	if len(got) != 2 {
		t.Fatalf("expected 2 pushes, got %v", got)
	}
}
