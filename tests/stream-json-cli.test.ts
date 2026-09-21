/**
 * Tests for stream-json-cli.ts — subprocess lifecycle management.
 *
 * Uses real child_process.spawn (echo + node -e scripts) to produce
 * controllable NDJSON output without mocking.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  extractCursorAgentDelta,
  iterStreamJsonEvents,
  TextAssembler,
} from "../src/providers/stream-json-cli.js";

/** Mimics `cursor-agent --output-format stream-json --stream-partial-output`. */
function cursorAgentEvents(chunks: string[]): Record<string, unknown>[] {
  const events = chunks.map((text, i) => ({
    type: "assistant",
    message: { role: "assistant", content: [{ type: "text", text }] },
    timestamp_ms: 1700000000000 + i,
  }));
  events.push({
    type: "assistant",
    message: { role: "assistant", content: [{ type: "text", text: chunks.join("") }] },
  } as (typeof events)[number]);
  return events;
}

function streamCursorAgent(chunks: string[]): { streamed: string; assembled: string } {
  const assembler = new TextAssembler();
  let streamed = "";
  for (const evt of cursorAgentEvents(chunks)) {
    streamed += extractCursorAgentDelta(evt, assembler);
  }
  return { streamed, assembled: assembler.text };
}

test("iterStreamJsonEvents yields parsed NDJSON events from subprocess stdout", async () => {
  const events: Record<string, unknown>[] = [];
  const lines = [
    `process.stdout.write('{"type":"test","n":1}\\n');`,
    `setTimeout(() => { process.stdout.write('{"type":"test","n":2}\\n'); }, 10);`,
  ].join("");
  for await (const evt of iterStreamJsonEvents({
    cmd: ["node", "-e", lines],
    timeoutMs: 5000,
  })) {
    events.push(evt);
  }
  assert.equal(events.length, 2);
  assert.equal(events[0].type, "test");
  assert.equal(events[0].n, 1);
  assert.equal(events[1].type, "test");
  assert.equal(events[1].n, 2);
});

test("iterStreamJsonEvents keeps every line when a chunk carries many of them", async () => {
  const n = 200;
  const script = [
    `let s = "";`,
    `for (let i = 0; i < ${n}; i++) s += JSON.stringify({ type: "assistant", n: i }) + "\\n";`,
    // One write, so readline emits all ${n} lines back-to-back.
    `process.stdout.write(s);`,
  ].join(" ");

  const received: number[] = [];
  for await (const evt of iterStreamJsonEvents({
    cmd: ["node", "-e", script],
    timeoutMs: 5000,
  })) {
    received.push(evt.n as number);
    // The SSE loop awaits between events; the reader must not lose lines meanwhile.
    await new Promise((r) => setImmediate(r));
  }

  assert.deepEqual(
    received,
    Array.from({ length: n }, (_, i) => i),
  );
});

test("iterStreamJsonEvents handles killOnResult", async () => {
  const events: Record<string, unknown>[] = [];
  const script = [
    `process.stdout.write('{"type":"assistant","message":{"content":"hello"}}\\n');`,
    `setTimeout(() => { process.stdout.write('{"type":"result","result":"done"}\\n'); }, 10);`,
    // Would output after result but process should be killed before this
    `setTimeout(() => process.stdout.write('{"type":"after","x":1}\\n'), 15000);`,
  ].join(" ");
  for await (const evt of iterStreamJsonEvents({
    cmd: ["node", "-e", script],
    timeoutMs: 5000,
    killOnResult: true,
  })) {
    events.push(evt);
  }
  assert.equal(events.length, 2);
  assert.equal(events[0].type, "assistant");
  assert.equal(events[1].type, "result");
});

test("iterStreamJsonEvents propagates subprocess non-zero exit as error", async () => {
  try {
    for await (const _evt of iterStreamJsonEvents({
      cmd: ["node", "-e", `process.stderr.write('something broke'); process.exit(1);`],
      timeoutMs: 5000,
    })) {
      // should throw before yielding anything useful
    }
    assert.fail("Expected error was not thrown");
  } catch (e) {
    const msg = String(e);
    assert.ok(
      msg.includes("something broke") || msg.includes("subprocess failed") || msg.includes("1"),
      `error message should indicate subprocess failure, got: ${msg}`,
    );
  }
});

test("iterStreamJsonEvents with AbortSignal kills subprocess early", async () => {
  const ac = new AbortController();
  const events: Record<string, unknown>[] = [];

  const script = [
    `process.stdout.write('{"type":"start"}\\n');`,
    // Sleep 30 seconds — abort should kill before this
    `setTimeout(() => process.stdout.write('{"type":"never"}\\n'), 30000);`,
  ].join(" ");

  const iter = iterStreamJsonEvents({
    cmd: ["node", "-e", script],
    timeoutMs: 3000,
    signal: ac.signal,
  });

  // Abort after a short delay
  setTimeout(() => ac.abort(), 50);

  try {
    for await (const evt of iter) {
      events.push(evt);
    }
  } catch (_) {
    // Expected — generator should throw after abort kills the process
  }

  const types = events.map((e) => e.type);
  assert.ok(types.includes("start"), "should have received start event");
  assert.ok(!types.includes("never"), "should NOT have received never event");
});

test("TextAssembler produces clean deltas from partial/full text", () => {
  const a = new TextAssembler();
  assert.equal(a.feed("Hello"), "Hello");
  assert.equal(a.text, "Hello");
  assert.equal(a.feed("Hello World"), " World");
  assert.equal(a.text, "Hello World");
  assert.equal(a.feed("New"), "New");
  assert.equal(a.text, "Hello WorldNew");
  assert.equal(a.feed(""), "");
  assert.equal(a.text, "Hello WorldNew");
});

test("TextAssembler.feedDelta never drops a chunk that repeats or extends the text", () => {
  const a = new TextAssembler();
  assert.equal(a.feedDelta("的"), "的");
  assert.equal(a.feedDelta("的"), "的");
  assert.equal(a.feedDelta("的时候"), "的时候");
  assert.equal(a.text, "的的的时候");
  assert.equal(a.feedDelta(""), "");
  assert.equal(a.text, "的的的时候");
});

test("extractCursorAgentDelta keeps partial chunks that look like snapshots", () => {
  for (const chunks of [
    ["的", "的", "时候"],
    ["我", "我们", "都在"],
    ["\n\n", "\n\n", "总结"],
    ["对", "对话框", "坏了"],
    ["##", "## ", "标题"],
  ]) {
    const truth = chunks.join("");
    const { streamed, assembled } = streamCursorAgent(chunks);
    assert.equal(streamed, truth, `streamed text for ${JSON.stringify(chunks)}`);
    assert.equal(assembled, truth, `assembled text for ${JSON.stringify(chunks)}`);
  }
});

test("extractCursorAgentDelta does not re-emit the terminal full-message event", () => {
  const chunks = ["对", "对话框", "坏了", "，", "原因是", "超时"];
  const { streamed } = streamCursorAgent(chunks);
  assert.equal(streamed, chunks.join(""));
  assert.equal(streamed.indexOf("对对话框"), streamed.lastIndexOf("对对话框"));
});

test("extractCursorAgentDelta ignores divergent terminal snapshot instead of appending", () => {
  const assembler = new TextAssembler();
  let streamed = "";
  for (const text of ["Hello", " world"]) {
    streamed += extractCursorAgentDelta(
      {
        type: "assistant",
        timestamp_ms: 1,
        message: { role: "assistant", content: [{ type: "text", text }] },
      },
      assembler,
    );
  }
  // Terminal full message differs (e.g. capitalization) — old feed() would append the
  // whole string and duplicate the answer in the SSE stream.
  streamed += extractCursorAgentDelta(
    {
      type: "assistant",
      message: { role: "assistant", content: [{ type: "text", text: "Hello World" }] },
    },
    assembler,
  );
  assert.equal(streamed, "Hello world");
  assert.equal(streamed.includes("Hello World"), false);
  assert.equal(streamed.indexOf("Hello world"), streamed.lastIndexOf("Hello world"));
});

test("extractCursorAgentDelta still assembles snapshot-only streams", () => {
  const assembler = new TextAssembler();
  let streamed = "";
  // No timestamp_ms: cumulative snapshots, as emitted without --stream-partial-output.
  for (const text of ["窗外", "窗外的雨", "窗外的雨细"]) {
    streamed += extractCursorAgentDelta(
      { type: "assistant", message: { role: "assistant", content: [{ type: "text", text }] } },
      assembler,
    );
  }
  assert.equal(streamed, "窗外的雨细");
  assert.equal(assembler.text, "窗外的雨细");
});

test("extractCursorAgentDelta drops timestamp_ms full-message replay after deltas", () => {
  // Real cursor-agent --stream-partial-output: incremental deltas, then the full
  // current message again WITH timestamp_ms (not only a terminal without it).
  const assembler = new TextAssembler();
  let streamed = "";
  const deltas = ["这是", " Pi", " 的", "配置"];
  for (const text of deltas) {
    streamed += extractCursorAgentDelta(
      {
        type: "assistant",
        timestamp_ms: 1,
        message: { role: "assistant", content: [{ type: "text", text }] },
      },
      assembler,
    );
  }
  const full = deltas.join("");
  streamed += extractCursorAgentDelta(
    {
      type: "assistant",
      timestamp_ms: 2,
      message: { role: "assistant", content: [{ type: "text", text: full }] },
    },
    assembler,
  );
  assert.equal(streamed, full);
  assert.equal(streamed.indexOf(full), streamed.lastIndexOf(full));
  assert.equal(assembler.text, full);
});

test("extractCursorAgentDelta drops whitespace-trimmed full-message replay", () => {
  const assembler = new TextAssembler();
  let streamed = "";
  for (const text of ["Hello", " world"]) {
    streamed += extractCursorAgentDelta(
      {
        type: "assistant",
        timestamp_ms: 1,
        message: { role: "assistant", content: [{ type: "text", text }] },
      },
      assembler,
    );
  }
  streamed += extractCursorAgentDelta(
    {
      type: "assistant",
      timestamp_ms: 2,
      message: { role: "assistant", content: [{ type: "text", text: "Hello world\n" }] },
    },
    assembler,
  );
  assert.equal(streamed, "Hello world");
});

test("extractCursorAgentDelta resets on tool_call so later segments are not dropped", () => {
  const assembler = new TextAssembler();
  let streamed = "";
  const feed = (text: string, ts?: number) => {
    streamed += extractCursorAgentDelta(
      {
        type: "assistant",
        ...(ts != null ? { timestamp_ms: ts } : {}),
        message: { role: "assistant", content: [{ type: "text", text }] },
      },
      assembler,
    );
  };

  feed("STEP", 1);
  feed("_", 2);
  feed("ONE", 3);
  feed("STEP_ONE", 4); // full-message replay with timestamp_ms
  extractCursorAgentDelta({ type: "tool_call", subtype: "started" }, assembler);
  extractCursorAgentDelta({ type: "tool_call", subtype: "completed" }, assembler);
  feed("STEP", 5);
  feed("_", 6);
  feed("TWO", 7);
  feed("STEP_TWO", 8); // replay of second message
  feed("STEP_TWO", undefined); // terminal without timestamp

  assert.equal(streamed, "STEP_ONESTEP_TWO");
  assert.equal(assembler.text, "STEP_ONESTEP_TWO");
  assert.equal(streamed.indexOf("STEP_ONE"), streamed.lastIndexOf("STEP_ONE"));
  assert.equal(streamed.indexOf("STEP_TWO"), streamed.lastIndexOf("STEP_TWO"));
});
