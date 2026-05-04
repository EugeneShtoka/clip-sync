package main

import (
	"crypto/sha256"
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

type state struct {
	mu       sync.Mutex
	lastHash [32]byte
}

func (s *state) same(text string) bool {
	h := sha256.Sum256([]byte(text))
	s.mu.Lock()
	defer s.mu.Unlock()
	if h == s.lastHash {
		return true
	}
	s.lastHash = h
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

func pushToNtfy(ntfyURL, topic, text string, persistServer, persistPhone bool) {
	url := strings.TrimRight(ntfyURL, "/") + "/" + topic
	req, err := http.NewRequest("POST", url, strings.NewReader(text))
	if err != nil {
		log.Printf("ntfy push: %v", err)
		return
	}
	req.Header.Set("Content-Type", "text/plain")
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

func subscribeLoop(ntfyURL, topic string, s *state) {
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
			if s.same(ev.Message) {
				continue
			}
			setClipboard(ev.Message)
			log.Printf("← %d bytes", len(ev.Message))
		}
	}
}

func runDaemon(ntfyURL, topic, socket string, persistServer, persistPhone bool) {
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

	go subscribeLoop(ntfyURL, topic, s)

	log.Printf("daemon: socket=%s ntfy=%s/%s", socket, ntfyURL, topic)

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
			text := string(data)
			if s.same(text) {
				return
			}
			pushToNtfy(ntfyURL, topic, text, persistServer, persistPhone)
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
	pushMode   := flag.Bool("push", false, "push current clipboard to daemon and exit")
	flag.Parse()

	// track which flags were explicitly set on the command line
	explicit := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	// defaults
	cfg := fileConfig{
		Topic:  "clipboard",
		Socket: "/tmp/clip-sync.sock",
	}

	// load config file if it exists
	if *configFile != "" {
		if fc, err := loadFileConfig(*configFile); err == nil {
			if fc.Ntfy != ""   { cfg.Ntfy = fc.Ntfy }
			if fc.Topic != ""  { cfg.Topic = fc.Topic }
			if fc.Socket != "" { cfg.Socket = fc.Socket }
		} else if explicit["config"] {
			fmt.Fprintf(os.Stderr, "clip-sync: config file: %v\n", err)
			os.Exit(1)
		}
	}

	// CLI flags override config file
	if explicit["ntfy"]   { cfg.Ntfy = *ntfyFlag }
	if explicit["topic"]  { cfg.Topic = *topicFlag }
	if explicit["socket"] { cfg.Socket = *socketFlag }

	if *pushMode {
		runPush(cfg.Socket)
		return
	}

	if cfg.Ntfy == "" {
		fmt.Fprintln(os.Stderr, "clip-sync: ntfy URL required (--ntfy or config file)")
		flag.Usage()
		os.Exit(1)
	}

	runDaemon(cfg.Ntfy, cfg.Topic, cfg.Socket, cfg.PersistServer, cfg.PersistPhone)
}
