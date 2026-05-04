# clip-sync

Clipboard sync daemon for Linux that syncs clipboard content across devices via [ntfy](https://ntfy.sh).

Clipboard changes are pushed to an ntfy topic and received from it in real time over WebSocket. Designed for use with the [ntfy Android app](https://github.com/binwiederhier/ntfy-android) for phone ↔ desktop clipboard sharing.

## How it works

- A daemon process listens on a Unix socket and subscribes to an ntfy topic via WebSocket
- When the clipboard changes, a client tool sends the text to the daemon via the socket
- The daemon deduplicates and publishes to ntfy
- Incoming ntfy messages are written back to the local clipboard

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
