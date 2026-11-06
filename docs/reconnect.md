# Disconnects and errors

The client keeps the connection up. What it does on each refusal and close
is the contract's «Client» column (harnsy's `docs/h2a-v1.md` §5, and its
`testdata/h2a-v1/codes.json`, which this repository copies and tests
against); the SDK decides nothing of its own:

| The contract says | The SDK |
|---|---|
| `reconnect` (the network, `1006`, `h2a_ping` 4408, a frame over the limit `1009`) | waits 1 s, doubled each failed try up to 60 s, ±20 % at random (a welcome resets it), then signs in again with `resume_after`; what `Send` was waiting for is sent again with the same `ID` |
| `wait retry_after` (`h2a_rate`) | on a hello: waits `retry_after` (10 min without it), then signs in; on a frame: `Send` waits and sends the same frame once more |
| `stop` (`h2a_auth` 4401, `h2a_off` 4403, `h2a_version` / `h2a_hello_required` / `h2a_bad_frame` on a hello 4400, `h2a_replaced` 4409) | stops: no reconnect; `Run` and `Send` return the error (`h2a.ErrReplaced` for `h2a_replaced`: another copy of the service signed in with the same pass) |
| `caller` (a refused frame: the connection stays) | the error goes to the `Send` of that frame |

A `stop` needs a person: a new token, the pass turned on, harnsy Max back,
or one copy of the service stopped.

Every error of the node is `*h2a.Error` with the fields of the contract:
`Code`, `Text` (harnsy's words), `Field`, `RetryAfter`, `Until`, and
`Close` (the WebSocket close code that came with it). The codes are
constants: `h2a.CodeAuth`, `h2a.CodeOff`, `h2a.CodeRate`, … An unknown code
is an `*h2a.Error` too.

## Limits

Read them in `c.Welcome().Limits` (frames a minute, messages a day, text
and frame size, queue, ping): the node enforces them; the SDK only avoids
sending a text it knows would be refused.
