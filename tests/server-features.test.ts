import { describe, expect, it } from "bun:test";
import { Channel } from "ws-asyncapi";
import { z } from "zod";
import { createNodeWsServer } from "../src/index.ts";

// Transport-specific server features that the protocol conformance suite can't
// reach: the oversized-frame guard and graceful drain.
const chat = new Channel("/room/:id", "room").serverMessage(
	"message",
	z.object({ text: z.string() }),
);

function serve(opts: { maxPayload?: number } = {}) {
	const srv = createNodeWsServer([chat], { port: 0, ...opts });
	const port = new Promise<number>((resolve) =>
		srv.wss.on("listening", () => {
			const addr = srv.wss.address();
			resolve(typeof addr === "object" && addr ? addr.port : 0);
		}),
	);
	return { srv, port };
}

describe("adapter-node server features", () => {
	it("rejects an oversized frame with close 1009", async () => {
		const { srv, port } = serve({ maxPayload: 100 });
		const p = await port;
		try {
			const ws = new WebSocket(`ws://localhost:${p}/room/1`);
			await new Promise<void>((res, rej) => {
				ws.onopen = () => res();
				ws.onerror = () => rej(new Error("connect failed"));
			});
			const closed = new Promise<number>((res) => {
				ws.onclose = (e) => res(e.code);
			});
			ws.send("x".repeat(5_000));
			expect(await closed).toBe(1009);
		} finally {
			await srv.close();
		}
	});

	it("drain() closes connected clients with 1001 (going away)", async () => {
		const { srv, port } = serve();
		const p = await port;
		const ws = new WebSocket(`ws://localhost:${p}/room/1`);
		await new Promise<void>((res, rej) => {
			ws.onopen = () => res();
			ws.onerror = () => rej(new Error("connect failed"));
		});
		const closed = new Promise<{ code: number; reason: string }>((res) => {
			ws.onclose = (e) => res({ code: e.code, reason: e.reason });
		});
		await srv.drain(500);
		const ev = await closed;
		// drain closes with 1001 + "server draining" (some ws clients, e.g. Bun,
		// normalize the observed code, so assert on the reliable reason).
		expect(ev.reason).toBe("server draining");
		expect([1000, 1001]).toContain(ev.code);
	});
});
