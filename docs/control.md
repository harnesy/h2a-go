# Control: state, wake, sleep, items

Besides tasks, a pass may let a service see its posts' state, wake and put
to sleep a post's holder, and raise items. The person gives each right per
project on the dashboard; the node checks it on every frame.

| Method | Config.Can | Right in the post's project |
|---|---|---|
| `State`, `OnState` | `state` | `read` |
| `Wake`, `Sleep` | `wake` | `wake` |
| `CreateItem` | `item` | `items` |

```go
c, err := h2a.Dial(ctx, h2a.Config{URL: url, Token: token, Can: []string{"state", "wake", "item"}})
// "text" is sent too: the node takes no hello without it.
for _, p := range c.Welcome().Projects {
	fmt.Println(p.Project, p.Rights, p.To) // every project of the pass, its rights and posts
}
```

`Welcome().Projects` comes only to a service whose `Can` names `state`,
`wake` or `item`. On a pass of several projects a project's lead is
`lead:<project id>`; take the addresses from `Projects[].To`.

## State

```go
s, err := c.State(ctx, "") // every project with read; or a project's name
for _, p := range s.Projects {
	for _, post := range p.Posts {
		fmt.Println(p.Project, post.Address, post.Name, post.State, post.Counts.Asleep)
	}
}
c.OnState(func(s h2a.State) { /* only the posts that changed */ })
```

- `Post.State` is harnsy's word for the role: `vacant` (nobody on the
  post), `starting`, `up`, `partly_asleep`, `asleep`, `off`, `archived`
  (`h2a.PostUp`, …); `Counts` are its seats by what they do. Never an
  agent's name, a session or a node.
- `OnState` gets the node's events: a post whose state or counts changed,
  at most once per 5 s a post. It runs on one goroutine of the client, one
  event at a time: keep it short. The first look after the sign-in is not
  an event: call `State`.

## Wake and sleep

```go
err := c.Wake(ctx, "lead")
err = c.Sleep(ctx, "lead", "") // "" or h2a.SleepAfterStep: after its turn
```

- A service wakes only a holder this pass or harnsy put to sleep. A sleep
  the person or a lead chose, «do not wake», a team put to sleep whole, a
  holder that is off: `h2a_no_wake`. A holder awake already: `nil`.
- `Sleep` puts to sleep only the holder this pass woke last; else
  `h2a_not_yours`. The holder ends its turn first (`h2a.SleepNow` is taken
  the same way).
- The subscription at its soft stop: `h2a_soft_stop` with `Until`. The SDK
  waits until then and sends the frame once more; a `ctx` that ends before
  `Until` gets the error at once.

## Items

```go
ref, err := c.CreateItem(ctx, "role:11", "Scene 2", "Look at scene 2: the cut at 0:42")
fmt.Println("item", ref.Item)
```

- A new item of the post's role. Its title is the node's: `[service «…»]`
  and yours on one line, at most 120 characters. Your text is shown framed
  as data from outside, never as a command.
- The text with the node's frame around it is at most 8000 bytes
  (`h2a.ItemTextBytes`). The frame's words are the node's, so the SDK
  refuses here only a text over 8000 bytes by itself (`h2a_too_big`,
  without a frame); one a little under it may still get `h2a_too_big` from
  the node. The node never cuts it.

## Refusals and limits

Each method returns the node's refusal as `*h2a.Error`, as the contract's
«Client» column says (see [Disconnects and errors](reconnect.md)):
`h2a_forbidden` (no right to the project or the frame), `h2a_no_wake`,
`h2a_not_yours`, `h2a_no_holder`, `h2a_address`, `h2a_frame` (the frame
is not in `Can`) go to you; `h2a_rate` is waited out once (`RetryAfter`),
`h2a_soft_stop` once (`Until`). The connection stays.

Limits per pass: `wake` 10 an hour and 30 a day, `sleep` 30 a day, `item`
20 a day; a `state` request counts as a frame. Each frame has an ID; a
disconnect before its answer sends it again with the same ID, and the node
does it once.
