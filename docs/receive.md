# Answers and ack

```go
err := c.Run(ctx, func(ctx context.Context, m h2a.Reply) error {
	fmt.Println(m.Seq, m.Conversation, m.From.Role, m.From.Project, m.At, m.Text)
	return nil
})
```

- Answers come in `seq` order, one call at a time.
- The handler returns `nil`: the SDK sends `ack {up_to: m.Seq}` and saves
  `m.Seq` in the `Cursor`. The node then forgets the answer and its text.
- The handler returns an error: no ack, `Run` returns that error, and the
  next `Run` gets the same answer first.
- After a reconnect the hello asks for what came after the `Cursor`
  (`resume_after`); an answer the SDK already handed over is not handed
  over twice.
- The node keeps an unacknowledged answer 24 h (500 per pass at most).
- `Run` returns when `ctx` ends, or when the client stops for good:
  `*h2a.Error` with `h2a_auth` / `h2a_off`, or `h2a.ErrReplaced`. A refusal
  of the connection without a frame's ID (`h2a_frame`) is returned too.
- The node's `ping` is answered with `pong` by the SDK, without a handler.
