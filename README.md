# h2a-go

A Go client for harnsy's **h2a v1** contract. Your service connects to a
harnsy node, gives a task to a project's lead or one of its roles, and gets
the agents' answers in the same conversation, through disconnects and
restarts.

SDK v1 talks h2a v1 (harnsy ≥ 0.11). Checked against harnsy at the commit
named in [`testdata/codes.json`](testdata/codes.json) (its first line).

## Install

```sh
go get github.com/harnesy/h2a-go@latest
```

Go 1.23 or newer. One dependency: `github.com/coder/websocket`.

## Before you start

- **A pass.** The node's owner makes it on the dashboard (Settings ›
  Services): the project, your service's name, and the posts it may write
  to (roles and/or «Project lead»). The token (`h2a_…`) is shown
  once: keep it as a secret.
- **The address.** `wss://<the node's gateway host>[:port]/h2a/v1`. The node
  needs harnsy Max and its gateway on.

## Quick start

```go
c, err := h2a.Dial(ctx, h2a.Config{
	URL:    "wss://node.example/h2a/v1",
	Token:  os.Getenv("H2A_TOKEN"),
	Cursor: h2a.FileCursor("h2a.seq"), // the last answer handled, across restarts
})
if err != nil {
	log.Fatal(err) // *h2a.Error: its Code says why (h2a_auth, h2a_off, …)
}
defer c.Close()

w := c.Welcome() // the project and the addresses of the pass
acc, err := c.Send(ctx, h2a.Message{To: w.To[0].Address, Text: "The video is ready, look at scene 2"})
// acc.Conversation: write into it with h2a.Message{Conversation: acc.Conversation, Text: "…"}

err = c.Run(ctx, func(ctx context.Context, m h2a.Reply) error {
	fmt.Println(m.From.Role, ":", m.Text)
	return nil // acknowledged; an error: Run returns it and the answer comes again
})
```

## h2a-bot

A small command on top of the SDK (`cmd/h2a-bot`, under 120 lines):

```sh
go install github.com/harnesy/h2a-go/cmd/h2a-bot@latest
H2A_URL=wss://node.example/h2a/v1 H2A_TOKEN=h2a_… h2a-bot                      # list the addresses
H2A_URL=… H2A_TOKEN=… h2a-bot -to lead "Check the release is built"           # a task, then the answers
H2A_URL=… H2A_TOKEN=… h2a-bot -conv <conversation> "And the notes?"           # into a conversation
H2A_URL=… H2A_TOKEN=… h2a-bot -listen                                          # only the answers
```

It prints `[role · project] text` for each answer, keeps the last seq in
`~/.h2a-bot.seq`, and survives disconnects; Ctrl+C loses nothing.

## Documentation

- [Connect and sign in](docs/connect.md)
- [Send and conversations](docs/send.md)
- [Answers and ack](docs/receive.md)
- [Disconnects and errors](docs/reconnect.md)
- [Security](docs/security.md)

The contract itself is harnsy's: <https://harnsy.dev/docs/h2a-v1>. This SDK
keeps no copy of it; it follows its «Client» column on every error and
close, and decides nothing the node decides.

## Versions

The major version of the SDK is the contract's: `v1.x.y` speaks contract
`"1"`; fields the contract adds within `1` come in minor versions, and
fields the SDK does not know are skipped. Contract `"2"` will be
`github.com/harnesy/h2a-go/v2`.

## Tests

```sh
go test -race ./...
H2A_TEST_URL=wss://… H2A_TEST_TOKEN=h2a_… H2A_TEST_TASK="ping" go test -run TestLiveNode -v   # against a node
```

The tests run a fake node, and check the SDK against harnsy's own frames and
codes (`testdata/`, copied by `scripts/update-vectors.sh <harnsy checkout>`;
never edited here).

## License

MIT
