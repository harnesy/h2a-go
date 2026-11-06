# Send and conversations

```go
acc, err := c.Send(ctx, h2a.Message{To: "lead", Text: "Check the release is built"})
// acc.Conversation is the new conversation; write into it:
_, err = c.Send(ctx, h2a.Message{Conversation: acc.Conversation, Text: "And the notes?"})
```

- `To` is an address of the welcome (`c.Welcome().To`); without
  `Conversation` the message starts a new conversation to it.
- `ID` is set by the SDK when empty (unique). Give your own to make a send
  safe to repeat: the node answers a repeated `ID` with the same `Accepted`
  for 24 h and delivers the message once.
- `Send` waits for the node's `accepted` (or its refusal) until `ctx` ends.
  A disconnect before it: the SDK sends the same frame, with the same `ID`,
  after the reconnect.
- A refusal is `*h2a.Error` with the contract's code: `h2a_address` (not an
  address of the pass), `h2a_conversation_unknown`, `h2a_no_holder` (nobody
  on the post now), `h2a_daily` (the day's messages are used; `Until` says
  when), `h2a_too_big`. `h2a_rate` is waited out (`RetryAfter`) and the
  frame sent once more; a second one is returned.
- The SDK does not send a text over the welcome's `Limits.TextBytes`: that
  is `h2a_too_big` here, without a frame.
- A conversation lives 30 days after its last message; who answers in it,
  and what a handover on the post does, is the node's (the contract, §3).
