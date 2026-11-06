# Connect and sign in

```go
c, err := h2a.Dial(ctx, h2a.Config{
	URL:    "wss://node.example/h2a/v1", // the node's gateway
	Token:  os.Getenv("H2A_TOKEN"),      // the pass's token
	Can:    []string{"text"},            // the default
	Cursor: h2a.FileCursor("h2a.seq"),   // where the last handled seq is kept
	Logf:   log.Printf,                  // optional: a line on each (re)connect
})
```

- `Dial` opens the WebSocket with the subprotocol `harnsy.h2a.v1`, sends
  `hello` at once with `resume_after` from the `Cursor`, and waits for the
  node's answer (15 s).
- A `welcome`: `Dial` returns the client. `c.Welcome()` has the project, the
  pass's addresses (`To`: `lead`, `role:<id>`, each with its name) and the
  node's limits.
- An `error`: `Dial` returns it as `*h2a.Error` — `Code` is the contract's
  (`h2a_auth`: the token is wrong or revoked; `h2a_off`: the pass is off or
  harnsy Max ended; `h2a_rate`: too many failed hellos, `RetryAfter`).
- A welcome of another version: `h2a.ErrVersion`.
- The token goes in the `hello` frame only: never in the URL or a header,
  never in a log line or an error of this package. No `Origin` header is
  sent (the node refuses one).

After `Dial` the client keeps the connection up by itself (see
[Disconnects and errors](reconnect.md)); `c.Close()` ends it.

## Cursor

`Cursor` keeps the `seq` of the last answer your handler took. `FileCursor`
writes it atomically to a file; `MemCursor` (the default) keeps it in
memory, so a restart gets every answer not yet acknowledged. Your own: any
type with `Load() (int64, error)` and `Save(int64) error`.
