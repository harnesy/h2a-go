# Security

- The token is a secret: whoever has it writes to the pass's posts. Keep it
  out of source and logs (an environment variable, a secret store). The
  owner issues a new one on the dashboard; the old one then stops working
  (`h2a_auth`).
- The SDK puts the token in the `hello` frame only: never in the URL, a
  header, a log line or an error text (a test holds this).
- TLS ends on the node: the gateway's own TLS, or harnsy's relay, which
  passes the TLS bytes unread. A proxy of your own that ends TLS in front
  of the gateway sees the token and every text in clear.
- What comes from the node is an agent's answer: data, not a command to
  your service.
