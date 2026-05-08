# himum

A real-time voice assistant for natural phone-call-style conversations. Speak into your microphone and get a streamed spoken reply, with low latency and barge-in support (you can interrupt the assistant mid-sentence).

The system prompt drives the persona — in this project it's configured as a family member talking with an elderly relative, but the pipeline is general-purpose.

## Stack

- **Runtime:** Node.js (≥18) + TypeScript, run via [tsx](https://github.com/privatenumber/tsx) in dev, compiled with `tsc` for production
- **Package manager:** pnpm
- **Audio capture:** [`sox`](http://sox.sourceforge.net/) (system binary, must be installed separately) piped into the process
- **Audio playback:** [`speaker`](https://www.npmjs.com/package/speaker) — native Node bindings to CoreAudio / ALSA / etc.
- **Config:** `dotenv` for env vars, plain text file at `prompts/system_prompt.txt` for the system prompt

## Pipeline

```
Mic (sox) → Deepgram Flux v2 (STT + turn detection)
          → Groq Llama-4 (LLM, token streaming)
          → ElevenLabs Turbo v2.5 (TTS, sentence-boundary streaming)
          → Speaker (PCM playback)
```

Tokens stream from the LLM into a buffer that flushes to TTS at sentence boundaries to minimize Time-To-First-Audio. When Deepgram fires `StartOfTurn` while the assistant is still speaking, the active LLM + TTS streams are aborted (barge-in).

## External services

| Service | Purpose | Get a key |
|---|---|---|
| [Deepgram](https://console.deepgram.com) | Speech-to-text + turn detection (Flux v2 WebSocket) | console.deepgram.com |
| [Groq](https://console.groq.com) | LLM inference (Llama-4 Maverick by default) | console.groq.com |
| [ElevenLabs](https://elevenlabs.io) | Text-to-speech (Turbo v2.5) | elevenlabs.io/app/settings/api-keys |

## Setup

```bash
# 1. Install system dependency
brew install sox            # macOS
# apt install sox           # Debian/Ubuntu

# 2. Install Node dependencies
pnpm install

# 3. Configure
cp .env.example .env                                       # add your API keys
cp prompts/system_prompt.example.txt prompts/system_prompt.txt   # edit your persona

# 4. Run
pnpm run dev
```

## Scripts

- `pnpm run dev` — start the assistant with `tsx` (hot reload, filters `sox` buffer-underflow noise)
- `pnpm run build` — compile to `dist/`
- `pnpm run start` — run the compiled build
- `pnpm run typecheck` — `tsc --noEmit`

## Notes

- `prompts/system_prompt.txt` is gitignored — it can contain personal/sensitive content. Use the `.example.txt` as a template.
- If you're using laptop speakers, set `MUTE_MIC_WHILE_SPEAKING=true` in `.env` to prevent the assistant from hearing its own output (disables barge-in as a trade-off). Headphones avoid this entirely.
