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
  port,       // open a server on this port (ignored if `server` is given)
  server,     // attach to an existing node:http Server
  codec,      // wire codec (default: JSON). Must match the client codec.
  backplane,  // scaling backplane (default: in-process LocalBackplane)
});
```

- **Codec** — pass `msgpackCodec` from `@ws-asyncapi/codec-msgpack` for binary frames.
- **Backplane** — pass `RedisBackplane` from `@ws-asyncapi/backplane-redis` to scale across
  nodes (publish, rooms, presence, and optional recovery work cluster-wide).
- **Rooms** — `ws` has no native pub/sub, so the adapter keeps a local room registry and
  fans out itself; cluster-wide delivery and membership go through the backplane.

## API

- `createNodeWsServer(channels, options?)` → `{ wss, close() }`.
- `WsHub` / `WebSocketNode` — exported for advanced/custom integrations.

## License

MIT
