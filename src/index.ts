import type { IncomingMessage, Server as HttpServer } from "node:http";
import { type RawData, WebSocketServer } from "ws";
import {
	type AnyChannel,
	type AnyFrame,
	type Backplane,
	closeConnection,
	type Codec,
	type Connection,
	dispatchFrame,
	jsonCodec,
	LocalBackplane,
	openConnection,
	OutboundRpc,
} from "ws-asyncapi";
import { publishEvent } from "./emit.ts";
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
}

export interface NodeWsServer {
	/** the underlying `ws` WebSocketServer */
	wss: WebSocketServer;
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

	// deliver every backplane message (local or cross-node) to local members
	backplane.onMessage((message) =>
		hub.localPublish(message.topic, message.payload, message.except),
	);

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
	}

	const wss = options.server
		? new WebSocketServer({ server: options.server })
		: new WebSocketServer({ port: options.port });

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
