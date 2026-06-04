import type { WebSocket as WSWebSocket } from "ws";
import {
	type AnyFrame,
	type Backplane,
	type Codec,
	Frame,
	jsonCodec,
	publishEvent,
	type OutboundRpc,
	type WebSocketImplementation,
	type WebsocketDataType,
} from "ws-asyncapi";

/**
 * Local room registry + fan-out. The `ws` library has no native topic pub/sub
 * (unlike Bun/Elysia), so the adapter tracks which local sockets belong to which
 * topic and delivers backplane messages to them itself. Cluster-wide membership
 * and cross-node delivery are still the backplane's job.
 */
export class WsHub {
	#sockets = new Map<string, WSWebSocket>();
	#topics = new Map<string, Set<string>>();
	#socketTopics = new Map<string, Set<string>>();

	add(id: string, ws: WSWebSocket): void {
		this.#sockets.set(id, ws);
	}

	remove(id: string): void {
		const topics = this.#socketTopics.get(id);
		if (topics)
			for (const topic of topics) this.#topics.get(topic)?.delete(id);
		this.#socketTopics.delete(id);
		this.#sockets.delete(id);
	}

	join(id: string, topic: string): void {
		let members = this.#topics.get(topic);
		if (!members) {
			members = new Set();
			this.#topics.set(topic, members);
		}
		members.add(id);
		let topics = this.#socketTopics.get(id);
		if (!topics) {
			topics = new Set();
			this.#socketTopics.set(id, topics);
		}
		topics.add(topic);
	}

	leave(id: string, topic: string): void {
		this.#topics.get(topic)?.delete(id);
		this.#socketTopics.get(id)?.delete(topic);
	}

	isSubscribed(id: string, topic: string): boolean {
		return this.#topics.get(topic)?.has(id) ?? false;
	}

	/** Deliver an already-encoded payload to every local member of `topic`,
	 *  optionally skipping the given socket ids (e.g. the broadcast sender). */
	localPublish(
		topic: string,
		payload: string | Uint8Array,
		except?: string[],
	): void {
		const members = this.#topics.get(topic);
		if (!members) return;
		const skip = except && except.length ? new Set(except) : undefined;
		for (const id of members) {
			if (skip?.has(id)) continue;
			const ws = this.#sockets.get(id);
			// 1 === WebSocket.OPEN
			if (ws && ws.readyState === 1) ws.send(payload);
		}
	}

	/** Local socket ids, optionally filtered to a topic. */
	ids(topic?: string): string[] {
		if (topic) return [...(this.#topics.get(topic) ?? [])];
		return [...this.#sockets.keys()];
	}
}

export class WebSocketNode<WebsocketData extends WebsocketDataType, Topics>
	implements WebSocketImplementation<WebsocketData, Topics>
{
	constructor(
		private ws: WSWebSocket,
		readonly id: string,
		private hub: WsHub,
		private codec: Codec = jsonCodec,
		private backplane?: Backplane,
		private outbound?: OutboundRpc,
	) {}

	request<Name extends keyof NonNullable<WebsocketData["serverRpc"]>>(
		name: Name,
		input: NonNullable<WebsocketData["serverRpc"]>[Name]["input"],
		options?: { timeout?: number },
	): Promise<NonNullable<WebsocketData["serverRpc"]>[Name]["output"]> {
		if (!this.outbound)
			return Promise.reject(
				new Error("server→client RPC not available on this connection"),
			);
		return this.outbound.request(
			(frame) => this.sendFrame(frame),
			name as string,
			input,
			options?.timeout ?? 30_000,
		) as Promise<NonNullable<WebsocketData["serverRpc"]>[Name]["output"]>;
	}

	sendFrame(frame: AnyFrame): void {
		this.sendRaw(this.codec.encode(frame));
	}

	sendRaw(data: string | Uint8Array): void {
		// ws sends strings as text frames and binary (Buffer/TypedArray) as
		// binary frames — no JSON-stringify surprise, unlike Elysia's send.
		if (this.ws.readyState === 1) this.ws.send(data);
	}

	send<T extends keyof WebsocketData["server"]>(
		type: T,
		...data: WebsocketData["server"][T] extends never
			? []
			: [WebsocketData["server"][T]]
	): void {
		this.sendFrame([Frame.Event, type as string, data[0]]);
	}

	subscribe(topic: Topics): void {
		if (typeof topic !== "string") return;
		this.hub.join(this.id, topic);
		void this.backplane?.addToRoom(topic, this.id);
	}

	unsubscribe(topic: Topics): void {
		if (typeof topic !== "string") return;
		this.hub.leave(this.id, topic);
		void this.backplane?.removeFromRoom(topic, this.id);
	}

	isSubscribed(topic: Topics): boolean {
		if (typeof topic !== "string") return false;
		return this.hub.isSubscribed(this.id, topic);
	}

	publish<T extends keyof WebsocketData["server"]>(
		topic: Topics,
		type: T,
		...data: WebsocketData["server"][T] extends never
			? []
			: [WebsocketData["server"][T]]
	): void {
		if (typeof topic !== "string" || !this.backplane) return;
		// fan out (+ recovery offset) via the backplane, which delivers back to
		// this node's hub through onMessage.
		void publishEvent(
			this.backplane,
			this.codec,
			topic,
			type as string,
			data[0],
		);
	}

	broadcast<T extends keyof WebsocketData["server"]>(
		topic: Topics,
		type: T,
		...data: WebsocketData["server"][T] extends never
			? []
			: [WebsocketData["server"][T]]
	): void {
		if (typeof topic !== "string" || !this.backplane) return;
		// exclude this socket from delivery cluster-wide
		void publishEvent(
			this.backplane,
			this.codec,
			topic,
			type as string,
			data[0],
			[this.id],
		);
	}

	async roomMembers(topic: Topics): Promise<string[]> {
		if (typeof topic === "string" && this.backplane)
			return this.backplane.roomMembers(topic);
		return [];
	}

	close(code?: number, reason?: string): void {
		this.ws.close(code, reason);
	}
}
