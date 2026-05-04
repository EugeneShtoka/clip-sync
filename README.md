# clip-sync

Clipboard sync daemon for Linux that syncs clipboard content across devices via [ntfy](https://ntfy.sh).

Clipboard changes are pushed to an ntfy topic and received from it in real time over WebSocket. Designed for use with the [ntfy Android app](https://github.com/binwiederhier/ntfy-android) for phone ↔ desktop clipboard sharing.

## How it works

- A daemon listens on a Unix socket and subscribes to an ntfy topic via WebSocket
- When the clipboard changes, a client sends the text to the daemon via the socket
- The daemon wraps it in a JSON envelope and publishes to ntfy
- Incoming messages are parsed, filtered by source, and written to the local clipboard
- A gate flag suppresses the echo: after writing a received clipboard, the next push is skipped

### Message format

All messages are JSON:

```json
{ "source": "laptop", "text": "clipboard content" }
```

- **source** — identifies the sending device. Receivers skip messages where `source` matches their own, preventing feedback loops. Configure per-device; defaults to hostname.
- **text** — raw clipboard content. JSON encoding preserves newlines, quotes, and special characters exactly.

This format is designed for interoperability — any device (Tasker, Python script, another clip-sync instance) can participate by producing and consuming the same JSON envelope.

## Installation

```sh
go install github.com/EugeneShtoka/clip-sync@latest
```

## Configuration

Copy `config.example.toml` to `~/.config/clip-sync/config.toml` and edit:

```toml
ntfy   = "https://ntfy.example.com"
topic  = "clipboard"

# Whether to persist messages on the server (ntfy cache)
persist_server = false

# Whether to persist messages in Android notification history
persist_phone = false
```

## Usage

Start the daemon:

```sh
clip-sync
```

Push current clipboard to the daemon (call this from a keybinding or clipboard manager hook):

```sh
clip-sync --push
```

## systemd

```ini
# ~/.config/systemd/user/clip-sync.service
[Unit]
Description=Clipboard sync daemon

[Service]
ExecStart=%h/.local/bin/clip-sync
Restart=on-failure

[Install]
WantedBy=default.target
```

```sh
systemctl --user enable --now clip-sync
```

## License

MIT
