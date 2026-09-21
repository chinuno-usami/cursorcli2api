import test from "node:test";
import assert from "node:assert/strict";

import {
  compatChatRequestToChatRequest,
  normalizeToolingRequest,
  responsesRequestToChatRequest,
  parseToolCallResponse,
  ToolCallMarkerStreamFilter,
  toolCallContentRemainder,
  TOOL_CALL_MARKER,
  type ChatCompletionRequest,
} from "../src/lib/openai-compat.js";

test("normalizeToolingRequest mirrors top-level tool fields into model_extra", () => {
  const request = normalizeToolingRequest({
    model: "claude:sonnet",
    stream: false,
    max_tokens: null,
    tools: [
      {
        type: "function",
        function: {
          name: "get_weather",
          description: "Get weather",
          parameters: { type: "object", properties: { city: { type: "string" } } },
        },
      },
    ],
    tool_choice: "auto",
    parallel_tool_calls: false,
    messages: [
      {
        role: "assistant",
        content: "",
        tool_calls: [
          {
            id: "call_1",
            type: "function",
            function: { name: "get_weather", arguments: "{\"city\":\"Beijing\"}" },
          },
        ],
      },
      {
        role: "tool",
        tool_call_id: "call_1",
        content: "sunny",
      },
    ],
  } as ChatCompletionRequest & Record<string, unknown>);

  const extra = (request as Record<string, unknown>).model_extra as Record<string, unknown>;
  assert.deepEqual(extra.tools, (request as Record<string, unknown>).tools);
  assert.equal(extra.tool_choice, "auto");
  assert.equal(extra.parallel_tool_calls, false);

  const assistantExtra = ((request.messages[0] as Record<string, unknown>).model_extra ?? {}) as Record<string, unknown>;
  const toolExtra = ((request.messages[1] as Record<string, unknown>).model_extra ?? {}) as Record<string, unknown>;
  assert.deepEqual(assistantExtra.tool_calls, (request.messages[0] as Record<string, unknown>).tool_calls);
  assert.equal(toolExtra.tool_call_id, "call_1");
});

test("normalizeToolingRequest preserves existing model_extra precedence", () => {
  const topLevelTools = [{ type: "function", function: { name: "wrong" } }];
  const modelExtraTools = [{ type: "function", function: { name: "right" } }];

  const request = normalizeToolingRequest({
    model: "codex:auto",
    stream: false,
    max_tokens: null,
    tools: topLevelTools,
    messages: [
      {
        role: "assistant",
        content: "",
        tool_calls: [{ id: "call_1", type: "function", function: { name: "wrong", arguments: "{}" } }],
        model_extra: {
          tool_calls: [{ id: "call_1", type: "function", function: { name: "right", arguments: "{}" } }],
        },
      },
    ],
    model_extra: {
      tools: modelExtraTools,
      tool_choice: "required",
    },
  } as ChatCompletionRequest & Record<string, unknown>);

  const extra = (request as Record<string, unknown>).model_extra as Record<string, unknown>;
  assert.deepEqual(extra.tools, modelExtraTools);
  assert.equal(extra.tool_choice, "required");

  const assistantExtra = ((request.messages[0] as Record<string, unknown>).model_extra ?? {}) as Record<string, unknown>;
  assert.deepEqual(assistantExtra.tool_calls, [{ id: "call_1", type: "function", function: { name: "right", arguments: "{}" } }]);
});

test("compat and responses request conversion keep top-level tool fields available in model_extra", () => {
  const compatRequest = compatChatRequestToChatRequest({
    model: "claude:sonnet",
    stream: false,
    messages: [{ role: "user", content: "hello" }],
    tools: [{ type: "function", function: { name: "lookup" } }],
    tool_choice: { type: "function", function: { name: "lookup" } },
  } as ChatCompletionRequest & Record<string, unknown>);

  const compatExtra = (compatRequest as Record<string, unknown>).model_extra as Record<string, unknown>;
  assert.deepEqual(compatExtra.tools, [{ type: "function", function: { name: "lookup" } }]);
  assert.deepEqual(compatExtra.tool_choice, { type: "function", function: { name: "lookup" } });

  const responsesRequest = responsesRequestToChatRequest({
    model: "gemini:flash",
    input: [{ role: "user", content: [{ type: "input_text", text: "hi" }] }],
    stream: false,
    tools: [{ type: "function", function: { name: "lookup" } }],
    parallel_tool_calls: true,
  } as Record<string, unknown>);

  const responsesExtra = (responsesRequest as Record<string, unknown>).model_extra as Record<string, unknown>;
  assert.deepEqual(responsesExtra.tools, [{ type: "function", function: { name: "lookup" } }]);
  assert.equal(responsesExtra.parallel_tool_calls, true);
});

test("ToolCallMarkerStreamFilter streams plain text and holds marker blocks", () => {
  const f = new ToolCallMarkerStreamFilter();
  const block =
    `${TOOL_CALL_MARKER}\n` +
    `{"name":"bash","arguments":{"command":"ls"}}\n` +
    `${TOOL_CALL_MARKER}`;

  assert.equal(f.feed("Hello "), "Hello ");
  assert.equal(f.feed("world"), "world");
  // Split marker across chunks
  assert.equal(f.feed("___TOOL"), "");
  assert.equal(f.feed("_CALL___"), "");
  assert.equal(f.isHolding, true);
  assert.equal(f.feed('\n{"name":"bash","arguments":{"command":"ls"}}\n'), "");
  assert.equal(f.feed(TOOL_CALL_MARKER), "");
  assert.equal(f.flush(), "");
  assert.equal(f.streamedContent, "Hello world");

  const full = "Hello world" + block;
  const parsedFull = parseToolCallResponse(full);
  assert.ok(parsedFull.toolCalls);
  assert.equal(parsedFull.toolCalls![0].function.name, "bash");
  assert.equal(parsedFull.text, "Hello world");
  assert.equal(toolCallContentRemainder(f.streamedContent, parsedFull.text), "");
});

test("ToolCallMarkerStreamFilter emits post-marker clean text via remainder", () => {
  const f = new ToolCallMarkerStreamFilter();
  const full =
    `${TOOL_CALL_MARKER}\n` +
    `{"name":"read","arguments":{"path":"/tmp/a"}}\n` +
    `${TOOL_CALL_MARKER}\n` +
    `note after`;

  for (const ch of full) {
    assert.equal(f.feed(ch), "");
  }
  assert.equal(f.flush(), "");
  assert.equal(f.streamedContent, "");
  assert.equal(f.isHolding, true);

  const parsed = parseToolCallResponse(full);
  assert.ok(parsed.toolCalls);
  assert.equal(parsed.toolCalls![0].function.name, "read");
  assert.equal(toolCallContentRemainder(f.streamedContent, parsed.text), "note after");
});

test("ToolCallMarkerStreamFilter flush releases partial-marker lookahead for plain text", () => {
  const f = new ToolCallMarkerStreamFilter();
  // Ends with a proper prefix of the marker but never completes it
  assert.equal(f.feed("ok ___TOOL_CAL"), "ok ");
  assert.equal(f.flush(), "___TOOL_CAL");
  assert.equal(f.streamedContent, "ok ___TOOL_CAL");
});

test("toolCallContentRemainder avoids re-emitting trimmed pre-marker text", () => {
  assert.equal(toolCallContentRemainder("Hello\n", "Hello"), "");
  assert.equal(toolCallContentRemainder("", "only clean"), "only clean");
  assert.equal(toolCallContentRemainder("Hi", "Hi there"), " there");
});
