package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/gorilla/websocket"
)

type fileConfig struct {
	Ntfy          string `toml:"ntfy"`
	Topic         string `toml:"topic"`
	Socket        string `toml:"socket"`
	Source        string `toml:"source"`        // identifies this device; defaults to hostname
	PersistServer bool   `toml:"persist_server"` // if false, server won't cache the message (X-Cache: no)
	PersistPhone  bool   `toml:"persist_phone"`  // if false, Android won't save to notification history (X-Persist: no)
}

func defaultConfigPath() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "clip-sync", "config.toml")
}

func loadFileConfig(path string) (fileConfig, error) {
	var cfg fileConfig
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func resolveSource(configured string) string {
	if configured != "" {
		return configured
	}
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

// clipMessage is the JSON envelope used for all clipboard transmissions.
// All devices on the same topic must use this format.
//
// Schema:
//
//	{ "source": "<device-name>", "text": "<clipboard content>" }
//
// source: identifies the sending device (e.g. "laptop", "pixel9", or hostname).
//
//	Receivers skip messages where source matches their own, preventing loops.
//
// text:   the raw clipboard content. JSON encoding preserves special characters,
//
//	newlines, and quotes exactly.
type clipMessage struct {
	Source string `json:"source"`
	Text   string `json:"text"`
}

// echoWindow is how long after a remote clipboard write we suppress the
// echo push. The clipboard-change event fires within tens of ms; 500ms
// gives ample margin on a loaded system while being short enough that a
// real user copy half a second later is never affected.
const echoWindow = 500 * time.Millisecond

// state holds the gate that suppresses the echo push after a remote
// clipboard write. When the daemon receives a message and sets the local
// clipboard, the resulting clipboard-change event triggers a push from
// the external hook — that push should be dropped. The gate expires
// automatically after echoWindow so a stuck flag never silently swallows
// a real user copy (e.g. when clipboard content was unchanged and no
// change event fires at all).
type state struct {
	mu          sync.Mutex
	ignoreUntil time.Time
}

func (s *state) setIgnoreNext() {
	s.mu.Lock()
	s.ignoreUntil = time.Now().Add(echoWindow)
	s.mu.Unlock()
}

func (s *state) checkAndClear() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Now().Before(s.ignoreUntil) {
		s.ignoreUntil = time.Time{}
		return true
	}
	return false
}

func getClipboard() string {
	out, err := exec.Command("xclip", "-o", "-selection", "clipboard").Output()
	if err != nil {
		return ""
	}
	return string(out)
}

func setClipboard(text string) {
	cmd := exec.Command("xclip", "-selection", "clipboard")
	cmd.Stdin = strings.NewReader(text)
	if err := cmd.Run(); err != nil {
		log.Printf("clipboard write: %v", err)
	}
}

func pushToNtfy(ntfyURL, topic, source, text string, persistServer, persistPhone bool) {
	body, err := json.Marshal(clipMessage{Source: source, Text: text})
	if err != nil {
		log.Printf("ntfy push: marshal: %v", err)
		return
	}
	url := strings.TrimRight(ntfyURL, "/") + "/" + topic
	req, err := http.NewRequest("POST", url, strings.NewReader(string(body)))
	if err != nil {
		log.Printf("ntfy push: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if !persistServer {
		req.Header.Set("X-Cache", "no")
	}
	if !persistPhone {
		req.Header.Set("X-Persist", "no")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("ntfy push: %v", err)
		return
	}
	resp.Body.Close()
}

func toWSURL(ntfyURL, topic string) string {
	u := strings.TrimRight(ntfyURL, "/")
	u = strings.Replace(u, "https://", "wss://", 1)
	u = strings.Replace(u, "http://", "ws://", 1)
	return u + "/" + topic + "/ws"
}

type ntfyEvent struct {
	Event   string `json:"event"`
	Message string `json:"message"`
}

func subscribeLoop(ntfyURL, topic, source string, s *state) {
	url := toWSURL(ntfyURL, topic)
	for {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			log.Printf("ws connect: %v — retry in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("ws: connected to %s", url)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				log.Printf("ws read: %v — reconnecting", err)
				conn.Close()
				time.Sleep(2 * time.Second)
				break
			}
			var ev ntfyEvent
			if err := json.Unmarshal(data, &ev); err != nil || ev.Event != "message" || ev.Message == "" {
				continue
			}
			var msg clipMessage
			if err := json.Unmarshal([]byte(ev.Message), &msg); err != nil || msg.Text == "" {
				log.Printf("ws: ignoring non-JSON or empty message")
				continue
			}
			if msg.Source == source {
				continue // own message echoed back, skip
			}
			setClipboard(msg.Text)
			s.setIgnoreNext()
			log.Printf("← %d bytes from %s", len(msg.Text), msg.Source)
		}
	}
}

func runDaemon(ntfyURL, topic, socket, source string, persistServer, persistPhone bool) {
	os.Remove(socket)
	l, err := net.Listen("unix", socket)
	if err != nil {
		log.Fatalf("listen %s: %v", socket, err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		l.Close()
		os.Remove(socket)
		os.Exit(0)
	}()

	s := &state{}

	go subscribeLoop(ntfyURL, topic, source, s)

	log.Printf("daemon: socket=%s ntfy=%s/%s source=%s", socket, ntfyURL, topic, source)

	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			data, err := io.ReadAll(c)
			if err != nil || len(data) == 0 {
				return
			}
			if s.checkAndClear() {
				log.Printf("skip: echo of received clipboard")
				return
			}
			text := string(data)
			pushToNtfy(ntfyURL, topic, source, text, persistServer, persistPhone)
			log.Printf("→ %d bytes", len(text))
		}(conn)
	}
}

func runPush(socket string) {
	text := getClipboard()
	if text == "" {
		return
	}
	conn, err := net.Dial("unix", socket)
	if err != nil {
		log.Fatalf("connect %s: %v — is the daemon running?", socket, err)
	}
	defer conn.Close()
	fmt.Fprint(conn, text)
}

func main() {
	configFile := flag.String("config", defaultConfigPath(), "path to TOML config file")
	ntfyFlag   := flag.String("ntfy", "", "ntfy base URL, e.g. https://ntfy.example.com")
	topicFlag  := flag.String("topic", "", "ntfy topic name")
	socketFlag := flag.String("socket", "", "Unix socket path")
	sourceFlag := flag.String("source", "", "device identifier (default: hostname)")
	pushMode   := flag.Bool("push", false, "push current clipboard to daemon and exit")
	flag.Parse()

	explicit := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	cfg := fileConfig{
		Topic:  "clipboard",
		Socket: "/tmp/clip-sync.sock",
	}

	if *configFile != "" {
		if fc, err := loadFileConfig(*configFile); err == nil {
			if fc.Ntfy != ""   { cfg.Ntfy = fc.Ntfy }
			if fc.Topic != ""  { cfg.Topic = fc.Topic }
			if fc.Socket != "" { cfg.Socket = fc.Socket }
			if fc.Source != "" { cfg.Source = fc.Source }
			cfg.PersistServer = fc.PersistServer
			cfg.PersistPhone  = fc.PersistPhone
		} else if explicit["config"] {
			fmt.Fprintf(os.Stderr, "clip-sync: config file: %v\n", err)
			os.Exit(1)
		}
	}

	if explicit["ntfy"]   { cfg.Ntfy = *ntfyFlag }
	if explicit["topic"]  { cfg.Topic = *topicFlag }
	if explicit["socket"] { cfg.Socket = *socketFlag }
	if explicit["source"] { cfg.Source = *sourceFlag }

	if *pushMode {
		runPush(cfg.Socket)
		return
	}

	if cfg.Ntfy == "" {
		fmt.Fprintln(os.Stderr, "clip-sync: ntfy URL required (--ntfy or config file)")
		flag.Usage()
		os.Exit(1)
	}

	source := resolveSource(cfg.Source)
	runDaemon(cfg.Ntfy, cfg.Topic, cfg.Socket, source, cfg.PersistServer, cfg.PersistPhone)
}
