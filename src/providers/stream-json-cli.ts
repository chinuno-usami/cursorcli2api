/**
 * Runs CLI subprocesses and yields NDJSON events from stdout.
 * Ported from Python stream_json_cli.py.
 */

import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import type { Readable } from 'node:stream';

/**
 * Normalize message content to plain text (OpenAI-style parts).
 */
function normalizeMessageContent(content: unknown): string {
  if (content == null) return '';
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    const parts: string[] = [];
    for (const part of content) {
      if (part && typeof part === 'object' && 'type' in part && part.type === 'text' && typeof part.text === 'string') {
        parts.push(part.text);
      }
    }
    return parts.join('');
  }
  if (typeof content === 'object' && 'type' in content && (content as { type: string }).type === 'text' && 'text' in content && typeof (content as { text: string }).text === 'string') {
    return (content as { text: string }).text;
  }
  return String(content);
}

/**
 * Handles mixed partial/full text streams.
 * Turns mixed streams into clean deltas (and a final assembled text).
 */
export class TextAssembler {
  text = '';
  /**
   * Text of the in-flight assistant message only (between tool boundaries).
   * Used to detect cursor-agent's full-message replay that still carries
   * `timestamp_ms` after incremental deltas.
   */
  currentMessage = '';
  /** Number of feedDelta calls for the in-flight message. */
  currentDeltaCount = 0;

  /** Start a new assistant message (e.g. after a tool_call). */
  resetCurrentMessage(): void {
    this.currentMessage = '';
    this.currentDeltaCount = 0;
  }

  /**
   * Feed a chunk of unknown kind (full snapshot or delta) and guess from the text.
   * Only safe when the caller cannot tell the two apart: a delta that repeats or
   * extends everything seen so far is misread as a snapshot and loses characters.
   * Prefer feedDelta() whenever the stream marks its incremental chunks.
   */
  feed(incoming: string): string {
    const s = incoming ?? '';
    if (!s) return '';
    if (s === this.text) return '';
    if (s.startsWith(this.text)) {
      const delta = s.slice(this.text.length);
      this.text = s;
      return delta;
    }
    // Fallback: treat as delta chunk
    this.text += s;
    return s;
  }

  /** Feed a known-incremental chunk. Always appended, never deduplicated. */
  feedDelta(chunk: string): string {
    const s = chunk ?? '';
    if (!s) return '';
    this.currentMessage += s;
    this.currentDeltaCount += 1;
    this.text += s;
    return s;
  }

  /**
   * Feed a known full-message snapshot (e.g. cursor-agent terminal assistant
   * event without timestamp_ms). Never falls back to append: a divergent
   * snapshot after partial deltas would otherwise re-stream the whole answer.
   */
  feedSnapshot(incoming: string): string {
    const s = incoming ?? '';
    if (!s) return '';
    if (s === this.text) return '';
    if (s.startsWith(this.text)) {
      const delta = s.slice(this.text.length);
      this.text = s;
      return delta;
    }
    // Snapshot is a prefix of what partials already built — ignore.
    if (this.text.startsWith(s)) return '';
    // Multi-turn: terminal snapshot of the latest message only (already streamed).
    if (this.text.endsWith(s)) return '';
    // First content is a snapshot (no prior partials).
    if (!this.text) {
      this.text = s;
      return s;
    }
    // Divergent terminal after partials: keep partial assembly, do not re-emit.
    return '';
  }
}

/** Stop reading stdout once this many unconsumed lines pile up. */
const LINE_BUFFER_HIGH_WATER = 1024;
/** Resume reading stdout once the backlog drains back to this. */
const LINE_BUFFER_LOW_WATER = 256;

export interface IterStreamJsonEventsOptions {
  cmd: string[];
  env?: Record<string, string>;
  /** Per-line idle timeout (resets on each line). */
  timeoutMs?: number;
  /** Absolute wall-clock timeout for the entire subprocess. */
  totalTimeoutMs?: number;
  /** Kill the subprocess after seeing a "result" event (for CLIs that don't exit on their own). */
  killOnResult?: boolean;
  /** Data to write to the subprocess stdin before closing it. */
  stdinData?: string | null;
  /** AbortSignal to cancel the subprocess (e.g. client disconnect). */
  signal?: AbortSignal;
  eventCallback?: (evt: Record<string, unknown>) => void;
  stderrCallback?: (line: string) => void;
}

/**
 * Async generator that spawns a subprocess, reads stdout line-by-line as NDJSON,
 * drains stderr, and yields parsed JSON events.
 */
export async function* iterStreamJsonEvents(
  opts: IterStreamJsonEventsOptions
): AsyncGenerator<Record<string, unknown>> {
  const {
    cmd,
    env,
    timeoutMs = 60_000,
    totalTimeoutMs,
    killOnResult = false,
    stdinData = null,
    signal,
    eventCallback,
    stderrCallback,
  } = opts;

  const proc = spawn(cmd[0], cmd.slice(1), {
    stdio: [stdinData != null ? 'pipe' : 'ignore', 'pipe', 'pipe'],
    env: { ...process.env, ...env },
  });

  if (stdinData != null && proc.stdin) {
    proc.stdin.write(stdinData);
    proc.stdin.end();
  }

  // AbortSignal: check immediately-aborted, defer listener until rl is created
  if (signal) {
    if (signal.aborted) {
      proc.kill("SIGKILL");
      throw new Error("aborted");
    }
  }

  const stderrBuf: Buffer[] = [];
  let lastHint: string | null = null;
  let totalTimedOut = false;
  const totalTimer = totalTimeoutMs
    ? setTimeout(() => {
        totalTimedOut = true;
        proc.kill("SIGKILL");
      }, totalTimeoutMs)
    : null;

  const drainStderr = (): Promise<void> =>
    new Promise((resolve) => {
      if (!proc.stderr) {
        resolve();
        return;
      }
      let textBuf = '';
      proc.stderr.on('data', (chunk: Buffer) => {
        stderrBuf.push(chunk);
        if (stderrCallback) {
          textBuf += chunk.toString('utf8');
          const lines = textBuf.split('\n');
          if (!textBuf.endsWith('\n')) {
            textBuf = lines.pop() ?? '';
          } else {
            textBuf = '';
          }
          for (const line of lines) {
            const trimmed = line.trim();
            if (trimmed) stderrCallback(trimmed);
          }
        }
      });
      proc.stderr.on('end', () => {
        if (stderrCallback && textBuf.trim()) {
          for (const line of textBuf.split('\n')) {
            const trimmed = line.trim();
            if (trimmed) stderrCallback(trimmed);
          }
        }
        resolve();
      });
    });

  const drainPromise = drainStderr();

  let rl: ReturnType<typeof createInterface> | null = null;

  try {
    if (!proc.stdout) {
      throw new Error('subprocess stdout not available');
    }

    rl = createInterface({
      input: proc.stdout as Readable,
      crlfDelay: Infinity,
    });

    // Register abort listener now that rl is assigned
    if (signal && !signal.aborted) {
      const onAbort = () => {
        rl?.close();
        proc.kill("SIGKILL");
      };
      signal.addEventListener("abort", onAbort, { once: true });
    }

    const iface = rl; // non-null: assigned above, only reached inside try block

    // readline emits every line of a stdout chunk synchronously, so a listener
    // attached per await only ever sees the first one and the rest are dropped.
    // Keep one permanent listener and queue the lines instead.
    const pending: string[] = [];
    let inputClosed = false;
    let paused = false;
    let notify: (() => void) | null = null;

    const wake = () => {
      const fn = notify;
      notify = null;
      fn?.();
    };

    iface.on('line', (line: string) => {
      pending.push(line);
      if (!paused && pending.length >= LINE_BUFFER_HIGH_WATER) {
        paused = true;
        iface.pause();
      }
      wake();
    });
    iface.on('close', () => {
      inputClosed = true;
      wake();
    });

    const nextLine = async (): Promise<string | null> => {
      for (;;) {
        const line = pending.shift();
        if (line !== undefined) {
          if (paused && pending.length <= LINE_BUFFER_LOW_WATER) {
            paused = false;
            iface.resume();
          }
          return line;
        }
        if (inputClosed) return null;

        let timer: ReturnType<typeof setTimeout> | undefined;
        const timedOut = await new Promise<boolean>((resolve) => {
          notify = () => {
            clearTimeout(timer);
            resolve(false);
          };
          timer = setTimeout(() => {
            notify = null;
            resolve(true);
          }, timeoutMs);
        });

        if (timedOut) {
          proc.kill("SIGKILL");
          throw new Error(`subprocess timeout after ${timeoutMs}ms`);
        }
      }
    };

    while (true) {
      if (totalTimedOut) {
        await drainPromise;
        throw new Error(`subprocess total timeout after ${totalTimeoutMs}ms`);
      }

      let line: string | null;
      try {
        line = await nextLine();
      } catch (err) {
        await drainPromise;
        throw err;
      }

      if (line === null) break;

      const raw = line.trim();
      if (!raw) continue;

      let evt: Record<string, unknown>;
      try {
        evt = JSON.parse(raw) as Record<string, unknown>;
      } catch {
        continue;
      }

      if (evt.type === 'result' && typeof evt.result === 'string' && evt.result) {
        lastHint = String(evt.result).trim() || lastHint;
      }
      if (evt.type === 'error' && typeof evt.message === 'string' && evt.message) {
        lastHint = String(evt.message).trim() || lastHint;
      }

      if (eventCallback) eventCallback(evt);
      yield evt;

      if (killOnResult && evt.type === 'result') {
        proc.kill("SIGKILL");
        return;
      }
    }

    await drainPromise;

    await new Promise<void>((resolve, reject) => {
      const finish = (code: number | null) => {
        if (code !== 0) {
          const msg = Buffer.concat(stderrBuf).toString('utf8').trim();
          const exitInfo = code != null ? `${code}` : `signal ${proc.signalCode ?? "unknown"}`;
          reject(new Error(msg || lastHint || `subprocess failed: ${exitInfo}`));
        } else {
          resolve();
        }
      };
      if (proc.exitCode != null) {
        finish(proc.exitCode);
      } else {
        proc.once('exit', finish);
      }
    });
  } finally {
    if (totalTimer) clearTimeout(totalTimer);
    rl?.close();
    // exitCode is null for signal-killed processes, so also check signalCode
    if (proc.exitCode === null && proc.signalCode === null) {
      proc.kill("SIGKILL");
      await new Promise<void>((r) => proc.once("exit", () => r()));
    }
  }
}

/**
 * Extract text delta from cursor-agent events.
 */
export function extractCursorAgentDelta(
  evt: Record<string, unknown>,
  assembler: TextAssembler
): string {
  // Tool boundaries end the current assistant message; subsequent text is new.
  const evtType = typeof evt.type === 'string' ? evt.type : '';
  if (
    evtType === 'tool_call' ||
    evtType === 'tool_result' ||
    evtType.startsWith('tool_call') ||
    evtType.startsWith('tool_result')
  ) {
    assembler.resetCurrentMessage();
    return '';
  }
  if (evt.type !== 'assistant') return '';
  const message = evt.message;
  if (!message || typeof message !== 'object') return '';
  const content = normalizeMessageContent((message as Record<string, unknown>).content);
  // Under --stream-partial-output each incremental chunk carries timestamp_ms while
  // the terminal full-message event omits it. Without that signal, chunks that
  // repeat or extend the assembled text are mistaken for a snapshot and dropped.
  // Terminal events must use feedSnapshot (never append-fallback), or a slightly
  // different full message re-streams the entire answer as a second copy.
  //
  // Additionally, cursor-agent often re-emits the full current message as one more
  // timestamp_ms event after the incremental deltas (still with timestamp_ms).
  // Treating that as feedDelta doubles every narration segment in the SSE stream.
  // Do NOT treat "content.startsWith(currentMessage)" as cumulative: Chinese
  // token deltas like "我" then "我们都在" must still append in full.
  if (typeof evt.timestamp_ms === 'number') {
    const cur = assembler.currentMessage;
    // Full-message replay after 2+ deltas (allow trailing whitespace drift).
    if (
      content &&
      cur &&
      assembler.currentDeltaCount >= 2 &&
      (content === cur || content.trimEnd() === cur.trimEnd())
    ) {
      assembler.resetCurrentMessage();
      return '';
    }
    return assembler.feedDelta(content);
  }
  const delta = assembler.feedSnapshot(content);
  assembler.resetCurrentMessage();
  return delta;
}

/**
 * Extract text delta from claude events.
 */
export function extractClaudeDelta(
  evt: Record<string, unknown>,
  assembler: TextAssembler
): string {
  if (evt.type !== 'assistant') return '';
  const message = evt.message;
  if (!message || typeof message !== 'object') return '';
  const content = (message as Record<string, unknown>).content;
  const incoming = normalizeMessageContent(content);
  return assembler.feed(incoming);
}

/**
 * Extract text delta from gemini events.
 */
export function extractGeminiDelta(
  evt: Record<string, unknown>,
  assembler: TextAssembler
): string {
  if (evt.type !== 'message') return '';
  if (evt.role !== 'assistant') return '';
  const content = evt.content;
  const incoming = normalizeMessageContent(content);
  return assembler.feed(incoming);
}

/**
 * Extract usage from claude result events.
 */
export function extractUsageFromClaudeResult(
  evt: Record<string, unknown>
): { prompt_tokens: number; completion_tokens: number; total_tokens: number } | null {
  if (evt.type !== 'result') return null;
  const usage = evt.usage;
  if (!usage || typeof usage !== 'object') return null;
  const u = usage as Record<string, unknown>;
  const inTokens = Math.floor(Number(u.input_tokens) || 0);
  const outTokens = Math.floor(Number(u.output_tokens) || 0);
  return {
    prompt_tokens: inTokens,
    completion_tokens: outTokens,
    total_tokens: inTokens + outTokens,
  };
}

/**
 * Extract usage from gemini result events.
 */
export function extractUsageFromGeminiResult(
  evt: Record<string, unknown>
): { prompt_tokens: number; completion_tokens: number; total_tokens: number } | null {
  if (evt.type !== 'result') return null;
  const stats = evt.stats;
  if (!stats || typeof stats !== 'object') return null;
  const s = stats as Record<string, unknown>;
  const inTokens = Math.floor(Number(s.input_tokens) || 0);
  const outTokens = Math.floor(Number(s.output_tokens) || 0);
  const total = Math.floor(Number(s.total_tokens) || inTokens + outTokens);
  return {
    prompt_tokens: inTokens,
    completion_tokens: outTokens,
    total_tokens: total,
  };
}
