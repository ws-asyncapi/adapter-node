# @ws-asyncapi/adapter-node

Node adapter for **ws-asyncapi** built on [`ws`](https://github.com/websockets/ws) —
contract-first, end-to-end-typed WebSockets with acknowledgements (RPC), rooms, presence,
middleware, pluggable codecs, connection-state-recovery, and horizontal scaling.

Same protocol and feature set as `@ws-asyncapi/adapter-elysia` — the protocol lives in
`ws-asyncapi`'s shared dispatcher, so both adapters behave identically. Use this one to run
on Node (or alongside an existing `http`/Express/Fastify server).

## Installation

```bash
npm install @ws-asyncapi/adapter-node ws-asyncapi ws
# message schemas: any Standard Schema validator (zod / valibot / arktype) or @sinclair/typebox
```

## Usage

```ts
import { z } from "zod";
import { Channel } from "ws-asyncapi";
import { createNodeWsServer } from "@ws-asyncapi/adapter-node";

const chat = new Channel("/chat/:room", "chat")
  .$typeChannels<`room:${string}`>()
  .serverMessage("message", z.object({ from: z.string(), text: z.string() }))
  .clientMessage(
    "typing",
    ({ ws }) => ws.publish("room:1", "message", { from: "sys", text: "..." }),
    z.object({ on: z.boolean() }),
  )
  .rpc(
    "history",
    z.object({ limit: z.number().max(100).default(20) }),
    z.object({ items: z.array(z.string()) }),
    async ({ message }) => ({ items: await loadHistory(message.limit) }),
  )
  .onOpen(({ ws }) => ws.subscribe("room:1"));

const { wss, close } = createNodeWsServer([chat], { port: 3000 });

// broadcast from anywhere
setInterval(
  () => chat.publish("room:1", "message", { from: "clock", text: new Date().toISOString() }),
  1000,
);
```

Attach to an existing HTTP server instead of opening a port:

```ts
import { createServer } from "node:http";
const server = createServer(app); // Express/Fastify/etc.
createNodeWsServer([chat], { server });
server.listen(3000);
```

The client is identical to any other ws-asyncapi deployment — use the generated client or the
codegen-free `createClient<typeof chat>("ws://localhost:3000", "/chat/1")`.

## Options

```ts
createNodeWsServer(channels, {
  port,        // open a server on this port (ignored if `server` is given)
  server,      // attach to an existing node:http Server
  codec,       // wire codec (default: JSON). Must match the client codec.
  backplane,   // scaling backplane (default: in-process LocalBackplane)
  maxPayload,  // max inbound message bytes (default: 1 MiB); oversized frames
               // are rejected with close 1009 (DoS / decode-bomb guard).
});
```

- **Codec** — pass `msgpackCodec` from `@ws-asyncapi/codec-msgpack` for binary frames.
- **Backplane** — pass `RedisBackplane` from `@ws-asyncapi/backplane-redis` to scale across
  nodes (publish, rooms, presence, and optional recovery work cluster-wide).
- **Rooms** — `ws` has no native pub/sub, so the adapter keeps a local room registry and
  fans out itself; cluster-wide delivery and membership go through the backplane.

### Graceful shutdown (zero-downtime deploys)

`drain()` stops accepting new connections, sends every client a close `1001`
("going away") so it reconnects elsewhere, waits up to `graceMs` for them to
leave, then terminates stragglers. With connection-state-recovery the reconnect
replays anything missed, so a rolling deploy is seamless. Wire it to `SIGTERM`:

```ts
const srv = createNodeWsServer([chat], { port: 3000 });
process.on("SIGTERM", async () => {
  await srv.drain(10_000); // grace window
  process.exit(0);
});
```

Use `close()` for an immediate, non-graceful stop.

### Mid-connection token refresh (`.onAuth`)

A WebSocket can easily outlive the bearer token it connected with. Declare
`.onAuth(credentials, handler)` and the client can present fresh credentials on
the *live* connection — the server re-runs the handler and replaces the
connection context, no reconnect:

```ts
const chat = new Channel("/chat/:room", "chat")
  .resolve(async ({ request }) => ({ user: await verify(request.headers.token) }))
  .onAuth(z.object({ token: z.string() }), async ({ credentials }) => ({
    user: await verify(credentials.token), // replaces the stale `user`
  }));

// client — call before the token expires (rejects with a typed RpcError if denied)
await client.authenticate({ token: freshJwt });
```

The credentials type is inferred end-to-end (codegen-free), so
`client.authenticate(...)` is fully typed. The last credentials are
**re-presented automatically after a reconnect**, so the refreshed identity
survives transient drops (a fresh connection's `.resolve` only sees the original
connect-time token). Throw an `RpcError` in the handler to reject a refresh.

### Typed presence (`.presence`)

A per-room roster of who's connected and their live state (cursor, status,
"typing…"). Declare the member-state schema; each connection gets one presence
room derived server-side (default: its concrete address):

```ts
const doc = new Channel("/doc/:id", "doc")
  .presence(z.object({ name: z.string(), typing: z.boolean() }));

// client:
await client.presence.set({ name: "Alice", typing: true });   // announce/update
client.presence.subscribe((members) => render(members));      // Map<id, state>, live
await client.presence.clear();                                 // leave
```

Join/leave/update changes are delivered as diffs and reconciled into a live
roster; a (re)connecting client fetches the current roster via a snapshot, and
the last announced state is re-sent automatically after a reconnect. Because
diffs ride the normal room fan-out, presence works across a cluster (the roster
snapshot is backed by the backplane — in-memory for `LocalBackplane`).

### Per-room history / rewind (`.history`)

Retain recent events per room so a client can fetch a backlog on demand — the
chat-scrollback / last-N-ticks pattern. (Distinct from connection-state-recovery,
which replays only *your* gap during a brief blip; history is room-scoped and any
subscribed client can read it.)

```ts
const chat = new Channel("/chat/:room", "chat")
  .serverMessage("message", z.object({ text: z.string() }))
  .history("message", { keep: 50 });   // retain the last 50 per room

// client (only for rooms you're subscribed to):
const recent = await client.history("room:42", { limit: 50 });
for (const { event, data } of recent) { /* typed, discriminated by `event` */ }
```

## API

- `createNodeWsServer(channels, options?)` → `{ wss, drain(graceMs?), close() }`.
- `WsHub` / `WebSocketNode` — exported for advanced/custom integrations.

## License

MIT
