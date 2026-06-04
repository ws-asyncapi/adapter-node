import { type AnyFrame, type Backplane, type Codec, Frame } from "ws-asyncapi";

/**
 * Assign a recovery offset (when the backplane supports it), encode the Event
 * frame once, and publish it through the backplane — which fans it out to every
 * node and appends it to the replay log under that offset.
 */
export async function publishEvent(
	backplane: Backplane,
	codec: Codec,
	topic: string,
	type: string,
	data: unknown,
	except?: string[],
): Promise<void> {
	const offset = backplane.assignOffset
		? await backplane.assignOffset()
		: undefined;
	const frame: AnyFrame =
		offset !== undefined
			? [Frame.Event, type, data, offset]
			: [Frame.Event, type, data];
	await backplane.publish(topic, codec.encode(frame), offset, except);
}
