import { describe, expect, it } from "bun:test";
import { createClient } from "@ws-asyncapi/client";
import { msgpackCodec } from "@ws-asyncapi/codec-msgpack";
import {
	type ConformanceDriver,
	runConformance,
} from "@ws-asyncapi/testing/conformance";
import { jsonCodec, LocalBackplane } from "ws-asyncapi";
import { createNodeWsServer } from "../src/index.ts";

// Run the shared protocol conformance contract over a REAL `ws` server + the
// real client — across JSON and msgpack codecs. This is the layer the in-memory
// harness can't reach (per-connection state marshalling), and it includes the
// connection-state-recovery scenario (the client genuinely reconnects).
const nodeDriver: ConformanceDriver = {
	name: "node",
	capabilities: { crossNode: true, recovery: true },
	async setup(channels, { codec, backplane, plugins }) {
		const bp = backplane ?? new LocalBackplane();
		const srv = createNodeWsServer(channels, {
			port: 0,
			codec,
			backplane: bp,
			plugins,
		});
		const port = await new Promise<number>((resolve) =>
			srv.wss.on("listening", () => {
				const addr = srv.wss.address();
				resolve(typeof addr === "object" && addr ? addr.port : 0);
			}),
		);
		const path = channels[0].address.replace(/:[^/]+/g, "1");
		return {
			connect: (opts) =>
				createClient<never>(
					`ws://localhost:${port}`,
					(opts?.path ?? path) as never,
					{
						codec,
						query: opts?.query as never,
						headers: opts?.headers as never,
					},
				),
			backplane: bp,
			close: () => srv.close(),
		};
	},
};

runConformance(
	nodeDriver,
	{ describe, it, expect },
	{
		codecs: [
			["json", jsonCodec],
			["msgpack", msgpackCodec],
		],
		backplanes: [
			{
				name: "local",
				create: () => new LocalBackplane(),
				crossNode: false,
				recovery: true,
			},
		],
	},
);
