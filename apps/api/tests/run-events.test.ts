import { SSE_DONE } from "@arch-flow/shared";
import { describe, expect, it, vi } from "vitest";
import { RunEvents } from "../src/modules/run/run-events.js";
import type { RunSnapshot } from "../src/modules/run/run-store.js";

const RUN_ID = "run_1";
const ASSISTANT_ID = "assistant_1";
const TRACE_ID = "trace-123";
const CREATED_AT = "2026-11-27T10:00:00.000Z";

type Listener = Parameters<RunEvents["subscribe"]>[3];

const RUNNING_SNAPSHOT: RunSnapshot = {
  run: {
    id: RUN_ID,
    conversationId: "conversation_1",
    userMessageId: "user_1",
    assistantMessageId: ASSISTANT_ID,
    status: "running",
    finishReason: null,
    createdAt: CREATED_AT,
    finishedAt: null,
    error: null,
  },
  message: {
    id: ASSISTANT_ID,
    role: "assistant",
    content: "",
    graphSnapshot: null,
    metadata: null,
    createdAt: CREATED_AT,
  },
  interrupt: null,
  draftVersion: 0,
  traceId: TRACE_ID,
  streamSequence: 0,
};

const SUCCEEDED_SNAPSHOT: RunSnapshot = {
  ...RUNNING_SNAPSHOT,
  run: {
    ...RUNNING_SNAPSHOT.run,
    status: "succeeded",
    finishReason: "stop",
    finishedAt: CREATED_AT,
  },
  message: { ...RUNNING_SNAPSHOT.message, content: "第一段。尾段。" },
  streamSequence: 10,
};

const SUCCEEDED_FRAMES = [
  { type: "text-delta", messageId: ASSISTANT_ID, content: "第一段。尾段。", replace: true },
  { type: "done", assistantMessageId: ASSISTANT_ID, finishReason: "stop" },
  SSE_DONE,
];

describe("formal run event replay", () => {
  it("settles a live journal from the persisted terminal snapshot when a client reconnects", () => {
    const events = new RunEvents();
    const connected = vi.fn<Listener>();
    const reconnect = vi.fn<Listener>();
    events.begin(RUNNING_SNAPSHOT, TRACE_ID);
    events.subscribe(RUNNING_SNAPSHOT, TRACE_ID, undefined, connected);
    events.publish(RUN_ID, { type: "text-delta", messageId: ASSISTANT_ID, content: "第一段。" });
    const cursor = events.sequence(RUN_ID);

    const { backlog } = events.subscribe(SUCCEEDED_SNAPSHOT, TRACE_ID, cursor, reconnect);

    expect(backlog).toEqual(SUCCEEDED_FRAMES.map((frame, index) => ({ id: cursor + index + 1, frame })));
    expect(connected.mock.calls.slice(1).map(([event]) => event)).toEqual(backlog);
    events.settle(SUCCEEDED_SNAPSHOT, TRACE_ID);
    expect(connected).toHaveBeenCalledTimes(1 + SUCCEEDED_FRAMES.length);
    expect(reconnect).not.toHaveBeenCalled();
  });

  it("rebuilds the final replay if a stale active snapshot arrives after settlement and buffered prefix loss", () => {
    const events = new RunEvents();
    events.begin(RUNNING_SNAPSHOT, TRACE_ID);
    const cursor = events.sequence(RUN_ID);
    for (let index = 0; index < 520; index += 1) {
      events.publish(RUN_ID, { type: "status", stage: "thinking" });
    }
    events.settle(SUCCEEDED_SNAPSHOT, TRACE_ID);
    const settledCursor = events.sequence(RUN_ID);

    const { backlog } = events.subscribe(RUNNING_SNAPSHOT, TRACE_ID, cursor, vi.fn<Listener>());

    expect(backlog[0]?.frame).toMatchObject({ type: "run-start", runId: RUN_ID });
    expect(backlog.slice(1).map(({ frame }) => frame)).toEqual(SUCCEEDED_FRAMES);
    expect(backlog.map(({ id }) => id)).toEqual(backlog.map((_, index) => settledCursor + index + 1));
  });

  it("replays cancellation after a waiting interrupt expires", () => {
    const events = new RunEvents();
    const paused: RunSnapshot = {
      ...RUNNING_SNAPSHOT,
      run: { ...RUNNING_SNAPSHOT.run, status: "waiting_input" },
      message: { ...RUNNING_SNAPSHOT.message, content: "请确认目标系统。" },
      interrupt: {
        interruptId: "interrupt_1",
        kind: "clarification",
        node: "router",
        payload: {},
        expiresAt: CREATED_AT,
      },
    };
    const canceled: RunSnapshot = {
      ...paused,
      run: { ...paused.run, status: "canceled", finishReason: "canceled", finishedAt: CREATED_AT },
      interrupt: null,
      streamSequence: 11,
    };
    events.begin(RUNNING_SNAPSHOT, TRACE_ID);
    events.settle(paused, TRACE_ID);
    const cursor = events.sequence(RUN_ID);

    const { backlog } = events.subscribe(canceled, TRACE_ID, cursor, vi.fn<Listener>());

    expect(backlog.every(({ id }) => id > cursor)).toBe(true);
    expect(backlog.map(({ frame }) => frame)).toEqual([
      expect.objectContaining({ type: "run-start", runId: RUN_ID }),
      { type: "text-delta", messageId: ASSISTANT_ID, content: paused.message.content, replace: true },
      { type: "done", assistantMessageId: ASSISTANT_ID, finishReason: "canceled" },
      SSE_DONE,
    ]);
  });
});
