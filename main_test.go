package main

import (
	"encoding/json"
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

// --- state gate ---

func TestState_falseByDefault(t *testing.T) {
	s := &state{}
	if s.checkAndClear() {
		t.Fatal("should not ignore by default")
	}
}

func TestState_setThenClear(t *testing.T) {
	s := &state{}
	s.setIgnoreNext()
	if !s.checkAndClear() {
		t.Fatal("should ignore within window")
	}
	if s.checkAndClear() {
		t.Fatal("flag should be cleared after first check")
	}
}

func TestState_expiresAfterWindow(t *testing.T) {
	s := &state{}
	s.setIgnoreNext()
	time.Sleep(echoWindow + 20*time.Millisecond)
	if s.checkAndClear() {
		t.Fatal("flag should have expired after echoWindow")
	}
}

func TestState_concurrent(t *testing.T) {
	s := &state{}
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.setIgnoreNext()
			s.checkAndClear()
		}()
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

// --- resolveSource ---

func TestResolveSource_usesConfigured(t *testing.T) {
	got := resolveSource("mydevice")
	if got != "mydevice" {
		t.Fatalf("got %q, want %q", got, "mydevice")
	}
}

func TestResolveSource_fallsBackToHostname(t *testing.T) {
	hostname, _ := os.Hostname()
	got := resolveSource("")
	if got != hostname {
		t.Fatalf("got %q, want hostname %q", got, hostname)
	}
}

// --- pushToNtfy ---

func TestPushToNtfy_jsonEnvelope(t *testing.T) {
	var got clipMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		json.Unmarshal(b, &got)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	pushToNtfy(srv.URL, "clipboard", "laptop", "hello world", false, false)

	if got.Source != "laptop" {
		t.Fatalf("source: got %q, want %q", got.Source, "laptop")
	}
	if got.Text != "hello world" {
		t.Fatalf("text: got %q, want %q", got.Text, "hello world")
	}
}

func TestPushToNtfy_correctPath(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(200)
	}))
	defer srv.Close()

	pushToNtfy(srv.URL+"/", "clipboard", "laptop", "x", false, false)

	if gotPath != "/clipboard" {
		t.Fatalf("path: got %q, want %q", gotPath, "/clipboard")
	}
}

func TestPushToNtfy_noCacheHeader(t *testing.T) {
	var cacheHdr, persistHdr string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cacheHdr = r.Header.Get("X-Cache")
		persistHdr = r.Header.Get("X-Persist")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	pushToNtfy(srv.URL, "clipboard", "laptop", "x", false, false)

	if cacheHdr != "no" {
		t.Fatalf("X-Cache: got %q, want %q", cacheHdr, "no")
	}
	if persistHdr != "no" {
		t.Fatalf("X-Persist: got %q, want %q", persistHdr, "no")
	}
}

func TestPushToNtfy_persistHeaders(t *testing.T) {
	var cacheHdr, persistHdr string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cacheHdr = r.Header.Get("X-Cache")
		persistHdr = r.Header.Get("X-Persist")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	pushToNtfy(srv.URL, "clipboard", "laptop", "x", true, true)

	if cacheHdr != "" {
		t.Fatalf("X-Cache should be absent when persist_server=true, got %q", cacheHdr)
	}
	if persistHdr != "" {
		t.Fatalf("X-Persist should be absent when persist_phone=true, got %q", persistHdr)
	}
}

// --- socket → ntfy integration ---

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
				if s.checkAndClear() {
					return
				}
				onPush(string(data))
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
		pushToNtfy(srv.URL, "clipboard", "laptop", text, false, false)
	}, 1)

	sendToSocket(t, socket, "hello")

	got := waitPushes(t, ch, 1)
	var msg clipMessage
	json.Unmarshal([]byte(got[0]), &msg)
	if msg.Text != "hello" {
		t.Fatalf("text: got %q, want %q", msg.Text, "hello")
	}
	if msg.Source != "laptop" {
		t.Fatalf("source: got %q, want %q", msg.Source, "laptop")
	}
}

func TestSocket_ignoreNextSkipsEcho(t *testing.T) {
	ch := make(chan string, 2)
	srv := ntfyCollector(t, ch)
	defer srv.Close()

	socket := t.TempDir() + "/clip-sync.sock"
	s := &state{}
	s.setIgnoreNext()
	socketDaemon(t, socket, s, func(text string) {
		pushToNtfy(srv.URL, "clipboard", "laptop", text, false, false)
	}, 2)

	sendToSocket(t, socket, "echo") // should be skipped
	sendToSocket(t, socket, "real") // should go through

	got := waitPushes(t, ch, 1)
	var msg clipMessage
	json.Unmarshal([]byte(got[0]), &msg)
	if msg.Text != "real" {
		t.Fatalf("expected only 'real' to be pushed, got %q", msg.Text)
	}
	select {
	case extra := <-ch:
		t.Fatalf("unexpected extra push: %q", extra)
	case <-time.After(100 * time.Millisecond):
	}
}
