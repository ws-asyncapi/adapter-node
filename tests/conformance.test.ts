import { describe, expect, it } from "bun:test";
import { createClient } from "@ws-asyncapi/client";
import { Channel, RpcError } from "ws-asyncapi";
import { z } from "zod";
import { createNodeWsServer } from "../src/index.ts";

// End-to-end conformance over a real `ws` server + the real client. The harness
// covers the protocol via the in-memory hub; this verifies the actual socket
// transport and the adapter's per-connection state handling.
const chat = new Channel("/room/:id", "room")
	.derive(({ request }) => ({ room: `room:${request.params.id}` }))
	.onOpen(({ ws, data }) => {
		ws.subscribe(data.room);
	})
	.serverMessage("message", z.object({ text: z.string() }))
	.clientMessage(
		"say",
		async ({ ws, message, data }) => {
			ws.publish(data.room, "message", { text: message.text });
		},
		z.object({ text: z.string() }),
	)
	.rpc(
		"add",
		z.object({ a: z.number(), b: z.number() }),
		z.object({ sum: z.number() }),
		async ({ message }) => ({ sum: message.a + message.b }),
	)
	.rpc(
		"boom",
		z.object({}),
		z.object({ ok: z.boolean() }),
		async () => {
			throw new RpcError("FORBIDDEN", "nope");
		},
	)
	.presence(z.object({ name: z.string() }))
	.history("message", { keep: 50 });

type Server = { port: number; close: () => Promise<void> };

function serve(): Promise<Server> {
	return new Promise((resolve) => {
		const srv = createNodeWsServer([chat], { port: 0 });
		srv.wss.on("listening", () => {
			const addr = srv.wss.address();
			const port = typeof addr === "object" && addr ? addr.port : 0;
			resolve({ port, close: () => srv.close() });
		});
	});
}

function nextEvent<T>(
	client: { onEvent: (n: "message", cb: (d: T) => void) => () => void },
): Promise<T> {
	return new Promise((resolve) => {
		const off = client.onEvent("message", (d) => {
			off();
			resolve(d);
		});
	});
}

describe("adapter-node conformance", () => {
	it("opens (Welcome handshake) and answers an RPC", async () => {
		const { port, close } = await serve();
		try {
			const c = createClient<typeof chat>(`ws://localhost:${port}`, "/room/1");
			await c.opened;
			expect(c.connected).toBe(true);
			expect(await c.request("add", { a: 2, b: 3 })).toEqual({ sum: 5 });
			c.close();
		} finally {
			await close();
		}
	});

	it("surfaces a handler throw as a typed RpcError", async () => {
		const { port, close } = await serve();
		try {
			const c = createClient<typeof chat>(`ws://localhost:${port}`, "/room/1");
			await c.opened;
			await expect(c.request("boom", {})).rejects.toMatchObject({
				code: "FORBIDDEN",
			});
			c.close();
		} finally {
			await close();
		}
	});

	it("fans a command out to the room", async () => {
		const { port, close } = await serve();
		try {
			const a = createClient<typeof chat>(`ws://localhost:${port}`, "/room/1");
			const b = createClient<typeof chat>(`ws://localhost:${port}`, "/room/1");
			await Promise.all([a.opened, b.opened]);
			const onB = nextEvent<{ text: string }>(b);
			a.call("say", { text: "hi" });
			expect(await onB).toEqual({ text: "hi" });
			a.close();
			b.close();
		} finally {
			await close();
		}
	});

	it("presence works across messages (regression guard)", async () => {
		const { port, close } = await serve();
		try {
			const c = createClient<typeof chat>(`ws://localhost:${port}`, "/room/1");
			await c.opened;
			// would reject NOT_FOUND if presenceRoom were lost between handlers
			await c.presence.set({ name: "Alice" });
			expect(c.presence.self).not.toBeNull();
			c.close();
		} finally {
			await close();
		}
	});

	it("returns retained history for a subscribed room", async () => {
		const { port, close } = await serve();
		try {
			const c = createClient<typeof chat>(`ws://localhost:${port}`, "/room/1");
			await c.opened;
			c.call("say", { text: "one" });
			await nextEvent(c); // wait for the round-trip
			const entries = (await c.history("room:1")) as Array<{
				event: string;
				data: { text: string };
			}>;
			expect(entries.map((e) => e.data.text)).toContain("one");
			c.close();
		} finally {
			await close();
		}
	});
});
