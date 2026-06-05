import type { IncomingMessage, Server as HttpServer } from "node:http";
import { type RawData, WebSocketServer } from "ws";
import {
	type AnyChannel,
	type AnyFrame,
	applyCommand,
	type Backplane,
	closeConnection,
	type Codec,
	COMMAND_TOPIC,
	type Connection,
	dispatchFrame,
	jsonCodec,
	LocalBackplane,
	type NodeCommand,
	openConnection,
	OutboundRpc,
	publishEvent,
	StreamRegistry,
} from "ws-asyncapi";
import { WebSocketNode, WsHub } from "./websocket.ts";

export { WebSocketNode, WsHub } from "./websocket.ts";

export interface NodeWsServerOptions {
	/** port to listen on (creates a server). Ignored if `server` is given. */
	port?: number;
	/** attach to an existing Node HTTP server instead of creating one */
	server?: HttpServer;
	/** wire codec (default: JSON). Must match the client codec. */
	codec?: Codec;
	/** scaling backplane (default: in-process LocalBackplane) */
	backplane?: Backplane;
	/**
	 * Max inbound message size in bytes; larger frames are rejected with close
	 * 1009 before they are buffered/decoded (DoS / decode-bomb guard). Default:
	 * 1 MiB. Raise it if you send large in-band payloads.
	 */
	maxPayload?: number;
}

export interface NodeWsServer {
	/** the underlying `ws` WebSocketServer */
	wss: WebSocketServer;
	/**
	 * Graceful shutdown for zero-downtime deploys: stop accepting new
	 * connections, send every client a close `1001` ("going away") so they
	 * reconnect elsewhere, and wait up to `graceMs` for them to disconnect (with
	 * connection-state-recovery, the reconnect replays anything missed). Falls
	 * back to terminating stragglers, then closes the backplane. Wire it to
	 * SIGTERM. Use {@link close} for an immediate, non-graceful stop.
	 */
	drain(graceMs?: number): Promise<void>;
	/** close the server and the backplane */
	close(): Promise<void>;
}

/** Match a request path to a channel, extracting `:params`. No wildcards (v1). */
function matchChannel(
	channels: AnyChannel[],
	pathname: string,
): { channel: AnyChannel; params: Record<string, string> } | undefined {
	const segs = pathname.split("/").filter(Boolean);
	for (const channel of channels) {
		const pat = channel.address.split("/").filter(Boolean);
		if (pat.length !== segs.length) continue;
		const params: Record<string, string> = {};
		let ok = true;
		for (let i = 0; i < pat.length; i++) {
			const p = pat[i];
			if (p.startsWith(":")) params[p.slice(1)] = decodeURIComponent(segs[i]);
			else if (p !== segs[i]) {
				ok = false;
				break;
			}
		}
		if (ok) return { channel, params };
	}
	return undefined;
}

/** Byte size of a raw inbound `ws` message (Buffer | ArrayBuffer | Buffer[]). */
function rawSize(raw: RawData): number {
	if (Array.isArray(raw)) return raw.reduce((n, b) => n + b.length, 0);
	if (raw instanceof ArrayBuffer) return raw.byteLength;
	return (raw as Buffer).length ?? 0;
}

function decodeFrame(codec: Codec, raw: RawData, isBinary: boolean): AnyFrame | undefined {
	try {
		if (isBinary) {
			const buf = Array.isArray(raw) ? Buffer.concat(raw) : (raw as Buffer);
			return codec.decode(buf);
		}
		const text = Array.isArray(raw)
			? Buffer.concat(raw).toString()
			: raw.toString();
		return codec.decode(text);
	} catch {
		return undefined;
	}
}

/**
 * Run ws-asyncapi channels on a Node `ws` server. Each channel is matched by its
 * path; rooms / presence / recovery / typed errors all work the same as the
 * Elysia adapter (the protocol lives in core's dispatcher).
 *
 * ```ts
 * const { close } = createNodeWsServer([chat], { port: 3000 });
 * ```
 */
export function createNodeWsServer(
	channels: AnyChannel[],
	options: NodeWsServerOptions = {},
): NodeWsServer {
	const codec = options.codec ?? jsonCodec;
	const backplane = options.backplane ?? new LocalBackplane();
	const hub = new WsHub();
	const channelsByName = new Map(channels.map((c) => [c.name, c]));

	// deliver every backplane message (local or cross-node) to local members
	backplane.onMessage((message) => {
		if (message.topic === COMMAND_TOPIC) {
			let cmd: NodeCommand | null = null;
			try {
				cmd = JSON.parse(
					typeof message.payload === "string"
						? message.payload
						: new TextDecoder().decode(message.payload),
				) as NodeCommand;
			} catch {}
			if (cmd)
				applyCommand(
					channelsByName.get(cmd.channel),
					cmd,
					message.origin === backplane.nodeId,
				);
			return;
		}
		hub.localPublish(message.topic, message.payload, message.except);
	});

	// route channel.publish(...) through the backplane (+ recovery offset)
	for (const channel of channels) {
		channel["~"].globalPublish = (
			topic: string,
			type: string,
			// biome-ignore lint/suspicious/noExplicitAny: type-erased seam
			data: any,
		) => void publishEvent(backplane, codec, topic, type, data);
		channel["~"].fetchSockets = async (room) => {
			const ids = room ? await backplane.roomMembers(room) : hub.ids();
			return Promise.all(
				ids.map(async (id) => ({
					id,
					rooms: (await backplane.rooms(id)).filter(
						(r) => !r.startsWith("#sid:"),
					),
				})),
			);
		};
		channel["~"].sendCommand = (cmd) => {
			void backplane.publish(COMMAND_TOPIC, JSON.stringify(cmd));
		};
		channel["~"].publishFrame = (topic, frame, except) =>
			void backplane.publish(topic, codec.encode(frame), undefined, except);
	}

	const maxPayload = options.maxPayload ?? 1_048_576; // 1 MiB
	const wss = options.server
		? new WebSocketServer({ server: options.server, maxPayload })
		: new WebSocketServer({ port: options.port, maxPayload });

	wss.on("connection", (raw, req: IncomingMessage) => {
		const url = new URL(req.url ?? "/", "http://localhost");
		const match = matchChannel(channels, url.pathname);
		if (!match) {
			raw.close(1008, "no matching channel");
			return;
		}
		const { channel, params } = match;
		const id = crypto.randomUUID();
		hub.add(id, raw);

		const request = {
			query: Object.fromEntries(url.searchParams) as Record<string, string>,
			headers: req.headers as Record<string, string>,
			params,
		};
		const outbound = new OutboundRpc();
		const conn: Connection = {
			ws: new WebSocketNode<any, any>(
				raw,
				id,
				hub,
				codec,
				backplane,
				outbound,
			),
			request,
			data: {},
			outbound,
			streams: new StreamRegistry(),
		};

		void (async () => {
			const result = await channel["~"].beforeUpgrade?.({
				query: request.query,
				headers: request.headers,
				params,
			});
			if (result instanceof Response) {
				raw.close(1008, "rejected");
				hub.remove(id);
				return;
			}
			if (result && typeof result === "object")
				conn.data = { ...conn.data, ...result };

			await openConnection(channel, conn);

			raw.on("message", (data, isBinary) => {
				// payload cap: reject oversized frames before decoding. `ws`'s
				// native maxPayload also covers this on real Node; this guard
				// makes it portable (some ws shims ignore maxPayload).
				if (rawSize(data) > maxPayload) {
					raw.close(1009, "message too large");
					return;
				}
				const frame = decodeFrame(codec, data, isBinary);
				if (frame) void dispatchFrame(channel, backplane, conn, frame);
			});
			raw.on("close", () => {
				void closeConnection(channel, backplane, conn);
				hub.remove(id);
			});
		})();
	});

	return {
		wss,
		drain: async (graceMs = 10_000) => {
			// tell every client we're going away so it reconnects elsewhere
			// (before wss.close so the 1001 code wins the race on runtimes whose
			// ws shim closes clients when the server stops listening)
			for (const client of wss.clients) {
				try {
					client.close(1001, "server draining");
				} catch {}
			}
			// stop accepting new upgrades
			wss.close();
			// wait for clients to leave, up to the grace window
			const deadline = Date.now() + graceMs;
			while (wss.clients.size > 0 && Date.now() < deadline)
				await new Promise((r) => setTimeout(r, 50).unref?.());
			// terminate any stragglers that ignored the close
			for (const client of wss.clients) client.terminate();
			await backplane.close();
		},
		close: async () => {
			// drop active sockets first so `wss.close` can complete promptly
			for (const client of wss.clients) client.terminate();
			await new Promise<void>((resolve) => {
				let done = false;
				const finish = () => {
					if (!done) {
						done = true;
						resolve();
					}
				};
				wss.close(finish);
				// fallback: a client reconnecting mid-shutdown can delay the
				// callback; don't hang shutdown on it.
				setTimeout(finish, 1000).unref?.();
			});
			await backplane.close();
		},
	};
}
