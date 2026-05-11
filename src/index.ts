/**
 * Voice Coordinator
 *
 * Real-time pipeline:
 *   Mic → Deepgram Flux (STT + turn detection)
 *       → Groq Llama-4 (LLM, streaming)
 *       → ElevenLabs Turbo v2.5 (TTS, sentence-boundary streaming)
 *       → Speaker (PCM via `speaker`)
 *
 * Key design decisions:
 *  - A single persistent Flux v2 WebSocket receives all mic audio.
 *    Flux handles its own turn detection: StartOfTurn / EndOfTurn replace VAD.
 *  - LLM tokens are accumulated in a buffer and flushed sentence-by-sentence to TTS,
 *    minimising Time-To-First-Audio (TTFA) without chopping mid-sentence.
 *  - Barge-in: when Flux fires StartOfTurn while the assistant is responding,
 *    we abort the active AbortController (LLM stream + TTS pipeline) immediately.
 */

import "dotenv/config";
import { spawn } from "node:child_process";
import { Readable } from "node:stream";
import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { DeepgramClient, ListenV2Model, ListenV2Encoding } from "@deepgram/sdk";

// Minimal shape of the TurnInfo message Flux emits on the socket
interface TurnInfo {
  type:       "TurnInfo";
  event:      "Update" | "StartOfTurn" | "EagerEndOfTurn" | "TurnResumed" | "EndOfTurn" | string;
  transcript: string;
}
import Groq from "groq-sdk";
import { ElevenLabsClient } from "@elevenlabs/elevenlabs-js";
import Speaker from "speaker";

// ─────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────

function requireEnv(key: string): string {
  const val = process.env[key];
  if (!val) throw new Error(`Missing required environment variable: ${key}`);
  return val;
}

const DEEPGRAM_API_KEY   = requireEnv("DEEPGRAM_API_KEY");
const GROQ_API_KEY       = requireEnv("GROQ_API_KEY");
const ELEVENLABS_API_KEY = requireEnv("ELEVENLABS_API_KEY");

const VOICE_ID   = process.env.ELEVENLABS_VOICE_ID ?? "21m00Tcm4TlvDq8ikWAM";
const GROQ_MODEL = process.env.GROQ_MODEL         ?? "meta-llama/llama-4-maverick-17b-128e-instruct";
// Priority: prompts/system_prompt.txt (edited locally) → SYSTEM_PROMPT env var → built-in default.
const SYSTEM_PROMPT_FILE = join(process.cwd(), "prompts", "system_prompt_demo.txt");
const SYSTEM_PROMPT_DEFAULT = "You are a helpful, concise voice assistant. Keep answers brief and natural for spoken conversation. Avoid markdown and lists — just speak plainly.";
let systemPromptSource: string;
let SYSTEM_PROMPT: string;
if (existsSync(SYSTEM_PROMPT_FILE)) {
  SYSTEM_PROMPT = readFileSync(SYSTEM_PROMPT_FILE, "utf8").trim();
  systemPromptSource = `file:${SYSTEM_PROMPT_FILE}`;
} else {
  SYSTEM_PROMPT = SYSTEM_PROMPT_DEFAULT;
  systemPromptSource = "default";
}
const MAX_HISTORY_TURNS = parseInt(process.env.MAX_HISTORY_TURNS ?? "10", 10);

// When true, mic audio is dropped while the assistant is speaking — prevents the
// app from picking up its own TTS output through laptop speakers (echo loop).
// Trade-off: barge-in is disabled while this is on. Set to "true" for laptop
// speaker use; leave off (default) when wearing headphones.
const MUTE_MIC_WHILE_SPEAKING = process.env.MUTE_MIC_WHILE_SPEAKING === "true";
// Extra ms to keep the mic muted after the last sentence's `close` event, to
// cover CoreAudio drain + room reverb. Only relevant when MUTE_MIC_WHILE_SPEAKING.
const SPEAK_COOLDOWN_MS = parseInt(process.env.SPEAK_COOLDOWN_MS ?? "500", 10);

// ElevenLabs PCM format — must stay in sync with Speaker constructor below
const TTS_FORMAT      = "pcm_22050" as const;
const TTS_SAMPLE_RATE = 22050;

// ─────────────────────────────────────────────────────────
// SDK clients
// ─────────────────────────────────────────────────────────

const deepgram = new DeepgramClient({ apiKey: DEEPGRAM_API_KEY });
const groq     = new Groq({ apiKey: GROQ_API_KEY });
const eleven   = new ElevenLabsClient({ apiKey: ELEVENLABS_API_KEY });

// ─────────────────────────────────────────────────────────
// Conversation state
// ─────────────────────────────────────────────────────────

type Phase   = "listening" | "thinking" | "speaking";
type Message = { role: "user" | "assistant"; content: string };

let phase: Phase = "listening";

/** The abort controller for the current respond() call. */
let responseAbort: AbortController | null = null;

/** The currently active Speaker instance — destroyed on barge-in. */
let activeSpeaker: Speaker | null = null;

/** Rolling conversation history (trimmed to MAX_HISTORY_TURNS pairs). */
const history: Message[] = [];

/** Re-entrancy guard so rapid Flux turn-end events don't stack. */
let processing = false;

/** Timestamp until which the mic stays muted after speaking ends. */
let mutedUntil = 0;

// ─────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────


function extractSentences(text: string): { sentences: string[]; remainder: string } {
  const re = /(.+?[.!?]+)(?:\s+|$)/g;
  const sentences: string[] = [];
  let lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    const s = m[1].trim();
    if (s) sentences.push(s);
    lastIndex = re.lastIndex;
  }
  return { sentences, remainder: text.slice(lastIndex) };
}

function pushHistory(role: Message["role"], content: string): void {
  history.push({ role, content });
  const maxMessages = MAX_HISTORY_TURNS * 2;
  if (history.length > maxMessages) history.splice(0, history.length - maxMessages);
}

// ─────────────────────────────────────────────────────────
// Barge-in
// ─────────────────────────────────────────────────────────

function bargeIn(reason = "barge-in"): void {
  if (phase === "listening") return;
  console.log(`\n[barge-in] ${reason}`);
  if (activeSpeaker) {
    try { activeSpeaker.destroy(); } catch { /* ignore write-after-destroy */ }
    activeSpeaker = null;
  }
  responseAbort?.abort();
  responseAbort = null;
  phase = "listening";
  processing = false;
}

// ─────────────────────────────────────────────────────────
// TTS — stream one sentence to ElevenLabs and play via Speaker
// ─────────────────────────────────────────────────────────

async function speakSentence(text: string, signal: AbortSignal): Promise<void> {
  if (!text.trim() || signal.aborted) return;

  const spk = new Speaker({ channels: 1, bitDepth: 16, sampleRate: TTS_SAMPLE_RATE });
  activeSpeaker = spk;

  await new Promise<void>((resolve, reject) => {
    spk.once("close", () => {
      if (MUTE_MIC_WHILE_SPEAKING) mutedUntil = Date.now() + SPEAK_COOLDOWN_MS;
      resolve();
    });
    spk.once("error", (err) => {
      if (!signal.aborted) reject(err);
      else resolve();
    });

    const onAbort = () => {
      try { spk.destroy(); } catch { /* ignore */ }
      resolve();
    };
    signal.addEventListener("abort", onAbort, { once: true });

    eleven.textToSpeech
      .stream(VOICE_ID, { text, modelId: "eleven_turbo_v2_5", outputFormat: TTS_FORMAT })
      .then((webStream) => {
        if (signal.aborted) { spk.destroy(); return; }
        const audioStream = Readable.fromWeb(webStream as Parameters<typeof Readable.fromWeb>[0]);
        audioStream.on("error", (err: Error) => {
          if (!signal.aborted) reject(err);
          else resolve();
        });
        audioStream.pipe(spk);
      })
      .catch((err: Error) => {
        if (!signal.aborted) reject(err);
        else resolve();
      });
  });

  if (activeSpeaker === spk) activeSpeaker = null;
}

// ─────────────────────────────────────────────────────────
// LLM → sentence-boundary TTS pipeline
// ─────────────────────────────────────────────────────────

async function respond(userText: string): Promise<void> {
  phase = "thinking";
  console.log(`\n[user]       ${userText}`);
  pushHistory("user", userText);

  const abort = new AbortController();
  responseAbort = abort;
  const { signal } = abort;

  let buffer       = "";
  let fullResponse = "";

  try {
    const stream = await groq.chat.completions.create(
      {
        model:    GROQ_MODEL,
        messages: [{ role: "system", content: SYSTEM_PROMPT }, ...history],
        stream:   true,
      },
      { signal }
    );

    phase = "speaking";

    for await (const chunk of stream) {
      if (signal.aborted) break;
      const token = chunk.choices[0]?.delta?.content ?? "";
      if (!token) continue;
      buffer       += token;
      fullResponse += token;
      const { sentences, remainder } = extractSentences(buffer);
      buffer = remainder;
      for (const sentence of sentences) {
        if (signal.aborted) break;
        process.stdout.write(`[assistant]  ${sentence}\n`);
        await speakSentence(sentence, signal);
      }
    }

    const tail = buffer.trim();
    if (tail && !signal.aborted) {
      process.stdout.write(`[assistant]  ${tail}\n`);
      await speakSentence(tail, signal);
    }

    if (!signal.aborted && fullResponse.trim()) pushHistory("assistant", fullResponse.trim());
  } catch (err: unknown) {
    const isAbort = err instanceof Error &&
      (err.name === "AbortError" || err.message.includes("aborted"));
    if (!isAbort) console.error("[LLM/TTS] error:", err);
  } finally {
    if (!signal.aborted) {
      responseAbort = null;
      phase = "listening";
      processing = false;
      console.log("\n[ready]      listening…");
    }
  }
}

// ─────────────────────────────────────────────────────────
// Main — open Flux connection and stream mic audio
// ─────────────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log("🎙  Voice coordinator starting…");
  console.log(`    STT   Deepgram Flux (flux-general-multi)`);
  console.log(`    LLM   Groq / ${GROQ_MODEL}`);
  console.log(`    TTS   ElevenLabs eleven_turbo_v2_5  (voice: ${VOICE_ID})`);
  console.log(`    PROMPT  ${systemPromptSource}  (${SYSTEM_PROMPT.length} chars)`);
  console.log(`            "${SYSTEM_PROMPT.slice(0, 80).replace(/\n/g, " ")}${SYSTEM_PROMPT.length > 80 ? "…" : ""}"`);
  console.log(`    MIC     ${MUTE_MIC_WHILE_SPEAKING ? `muted while speaking + ${SPEAK_COOLDOWN_MS}ms cooldown (no barge-in)` : "always live (barge-in enabled)"}`);
  console.log();

  // ── Flux v2 WebSocket ─────────────────────────────────
  const socket = await deepgram.listen.v2.connect({
    model:         ListenV2Model.FluxGeneralMulti,
    encoding:      ListenV2Encoding.Mulaw,
    sample_rate:   8000,
    Authorization: `Token ${DEEPGRAM_API_KEY}`,
    queryParams:   { language_hint: "en", mip_opt_out: "false" },
  });

  socket.on("open", () => console.log("[Flux]       connected\n"));

  socket.on("message", (msg: { type: string }) => {
    if (msg.type !== "TurnInfo") return;
    const turn = msg as TurnInfo;

    if (turn.event === "StartOfTurn") {
      process.stdout.write("\n[Flux]       start of turn\n");
      if (phase === "thinking" || phase === "speaking") {
        bargeIn("user spoke during response");
      }
      return;
    }

    if (turn.event === "Update") {
      process.stdout.write(`\r[Flux]       ${turn.transcript}`);
      return;
    }

    if (turn.event === "EndOfTurn") {
      process.stdout.write(`\n[Flux]       end of turn\n`);
      const transcript = turn.transcript.trim();
      if (!transcript || processing || phase !== "listening") return;
      processing = true;
      console.log(`[transcript] ${transcript}`);
      respond(transcript).catch(console.error);
    }
  });

  socket.on("error", (err: Error) => console.error("[Flux] error:", err));
  socket.on("close", () => console.log("[Flux]       connection closed"));

  socket.connect();
  await socket.waitForOpen();

  // ── Microphone via sox ────────────────────────────────
  const mic = spawn("sox", [
    "-d",       // default audio input device
    "-r", "8000",
    "-c", "1",  // mono
    "-e", "u-law",
    "-b", "8",
    "-t", "raw",
    "-",
  ]);

  mic.stderr.on("data", () => { /* suppress sox status lines */ });
  mic.on("error", (err) => console.error("[mic] sox error:", err));

  mic.stdout.on("data", (chunk: Buffer) => {
    if (MUTE_MIC_WHILE_SPEAKING && (phase === "speaking" || Date.now() < mutedUntil)) return;
    socket.sendMedia(chunk);
  });

  console.log("✅  Ready — speak to begin!\n");

  // ── Graceful shutdown ─────────────────────────────────
  process.on("SIGINT", () => {
    console.log("\n👋  Shutting down…");
    bargeIn("shutdown");
    mic.kill();
    socket.close();
    process.exit(0);
  });
}

main().catch((err: unknown) => {
  console.error("Fatal:", err);
  process.exit(1);
});
