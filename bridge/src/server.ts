/**
 * Pixel Agents Bridge
 *
 * Watches Claude Code's JSONL transcripts (under ~/.claude/projects/** by default)
 * and broadcasts agent-lifecycle + tool-call events to WebSocket clients using the
 * same message shape the upstream VS Code extension posts to its webview.
 *
 * Events emitted to clients:
 *   - existingAgents        (on connect)
 *   - agentCreated
 *   - agentClosed
 *   - agentToolStart
 *   - agentToolDone
 *   - agentStatus
 *
 * The webview-ui consumes these via window `message` events; a tiny client
 * adapter (see webview-ui/src/bridgeClient.ts) pipes WS → window.
 */

import chokidar from 'chokidar';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { WebSocket, WebSocketServer } from 'ws';

// ── Config ──────────────────────────────────────────────────────────────────
const BASE = process.env.CLAUDE_PROJECTS_DIR || path.join(os.homedir(), '.claude/projects');
const PORT = Number(process.env.PORT || 8787);
const IDLE_MS = Number(process.env.IDLE_MS || 15 * 60 * 1000); // 15 min → closed

// ── State ──────────────────────────────────────────────────────────────────
interface AgentState {
  id: number;
  sessionId: string;
  folderName: string;
  filePath: string;
  offset: number;
  pendingLine: string;
  activeTools: Map<string, string>;
  completedToolIds: Set<string>; // tool_result seen (possibly before tool_use)
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  lastThought: string | null;
  lastActivity: number;
}

let nextAgentId = 1;
const agentsBySession = new Map<string, AgentState>();
const agentsByFile = new Map<string, AgentState>();

const clients = new Set<WebSocket>();

function broadcast(msg: unknown): void {
  const payload = JSON.stringify(msg);
  for (const c of clients) {
    if (c.readyState === c.OPEN) c.send(payload);
  }
}

function deriveFolderName(filePath: string): string {
  // ~/.claude/projects/-Users-guilherme-andrade-code-foo/session.jsonl
  // → "foo" (last path segment of decoded project dir).
  const projectDir = path.basename(path.dirname(filePath));
  const decoded = projectDir.replace(/^-/, '').replace(/-/g, '/');
  return decoded.split('/').filter(Boolean).pop() || projectDir;
}

function ensureAgent(sessionId: string, filePath: string): AgentState {
  const existing = agentsBySession.get(sessionId);
  if (existing) return existing;
  const agent: AgentState = {
    id: nextAgentId++,
    sessionId,
    folderName: deriveFolderName(filePath),
    filePath,
    offset: 0,
    pendingLine: '',
    activeTools: new Map(),
    completedToolIds: new Set(),
    inputTokens: 0,
    outputTokens: 0,
    cacheReadTokens: 0,
    lastThought: null,
    lastActivity: Date.now(),
  };
  agentsBySession.set(sessionId, agent);
  agentsByFile.set(filePath, agent);
  broadcast({ type: 'agentCreated', id: agent.id, folderName: agent.folderName });
  return agent;
}

// ── JSONL tailing ──────────────────────────────────────────────────────────
function emitTokenUsage(agent: AgentState): void {
  broadcast({
    type: 'agentTokenUsage',
    id: agent.id,
    inputTokens: agent.inputTokens,
    outputTokens: agent.outputTokens,
    cacheReadTokens: agent.cacheReadTokens,
  });
}

function processEntry(
  agent: AgentState,
  entry: Record<string, unknown>,
  emit: boolean,
): void {
  agent.lastActivity = Date.now();

  const type = entry.type as string | undefined;
  if (type !== 'assistant' && type !== 'user') return;

  const message = entry.message as
    | { content?: unknown[]; usage?: Record<string, number> }
    | undefined;

  // Accumulate tokens from usage blocks on assistant messages.
  if (type === 'assistant' && message?.usage) {
    const u = message.usage;
    agent.inputTokens += Number(u.input_tokens || 0);
    agent.outputTokens += Number(u.output_tokens || 0);
    agent.cacheReadTokens += Number(u.cache_read_input_tokens || 0);
    if (emit) emitTokenUsage(agent);
  }

  const content = Array.isArray(message?.content) ? message!.content : [];

  // Capture the latest text block as a "thought" bubble (assistant messages only).
  if (type === 'assistant') {
    const lastText = content
      .filter(
        (c): c is { type: string; text: string } =>
          !!c && typeof c === 'object' && (c as { type?: string }).type === 'text',
      )
      .map((c) => c.text)
      .filter(Boolean)
      .pop();
    if (lastText) {
      agent.lastThought = lastText.slice(0, 240);
      if (emit) {
        broadcast({ type: 'agentThought', id: agent.id, text: agent.lastThought });
      }
    }
  }

  for (const item of content) {
    if (!item || typeof item !== 'object') continue;
    const it = item as Record<string, unknown>;

    if (it.type === 'tool_use') {
      const toolId = String(it.id ?? '');
      const toolName = String(it.name ?? 'Unknown');
      if (!toolId) continue;
      // If the result already arrived in an earlier line (happens on session
      // resume where tool_result precedes tool_use), the tool is already done.
      if (agent.completedToolIds.has(toolId)) continue;
      agent.activeTools.set(toolId, toolName);
      if (emit) {
        broadcast({ type: 'agentToolStart', id: agent.id, toolId, status: toolName });
      }
    } else if (it.type === 'tool_result') {
      const toolId = String(it.tool_use_id ?? '');
      if (!toolId) continue;
      agent.completedToolIds.add(toolId);
      const existed = agent.activeTools.delete(toolId);
      if (emit && existed) {
        broadcast({ type: 'agentToolDone', id: agent.id, toolId });
        if (agent.activeTools.size === 0) {
          broadcast({ type: 'agentToolsClear', id: agent.id });
          broadcast({ type: 'agentStatus', id: agent.id, status: 'waiting' });
        }
      }
    }
  }
}

// Simple, correct approach: read the tail of the file as a string and split by
// newline. On initial full-file scans, we parse in two passes:
//   1) collect every tool_result id (so reverse-order pairs are resolved),
//   2) emit events.
// Incremental reads (file grew) use single-pass streaming and emit as they go.
function readNewLines(filePath: string): void {
  let agent = agentsByFile.get(filePath);
  try {
    const stat = fs.statSync(filePath);
    const startOffset = agent?.offset ?? 0;
    if (stat.size < startOffset) {
      if (agent) agent.offset = 0;
    }
    if (stat.size <= (agent?.offset ?? 0)) return;

    const isInitialScan = !agent || agent.offset === 0;

    const fd = fs.openSync(filePath, 'r');
    try {
      const length = stat.size - (agent?.offset ?? 0);
      const buf = Buffer.alloc(length);
      fs.readSync(fd, buf, 0, length, agent?.offset ?? 0);
      const text = (agent?.pendingLine ?? '') + buf.toString('utf8');
      const lines = text.split('\n');
      const trailing = lines.pop() ?? '';

      const entries: Record<string, unknown>[] = [];
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          entries.push(JSON.parse(line));
        } catch {
          /* ignore unparseable */
        }
      }

      // First pass (initial scan only): resolve sessionId, pre-populate
      // completedToolIds so out-of-order tool_use/tool_result pairs match.
      if (isInitialScan) {
        for (const entry of entries) {
          const sid = entry.sessionId as string | undefined;
          if (!sid) continue;
          agent ??= ensureAgent(sid, filePath);
          if (sid !== agent.sessionId) agent = ensureAgent(sid, filePath);
          const msg = entry.message as { content?: unknown[] } | undefined;
          const content = Array.isArray(msg?.content) ? msg!.content : [];
          for (const item of content) {
            if (!item || typeof item !== 'object') continue;
            const it = item as Record<string, unknown>;
            if (it.type === 'tool_result') {
              const tid = String(it.tool_use_id ?? '');
              if (tid) agent.completedToolIds.add(tid);
            }
          }
        }
      }

      // Second pass (or only pass for incremental): emit events.
      for (const entry of entries) {
        const sid = entry.sessionId as string | undefined;
        if (!sid) continue;
        agent ??= ensureAgent(sid, filePath);
        if (sid !== agent.sessionId) agent = ensureAgent(sid, filePath);
        processEntry(agent, entry, !isInitialScan);
      }
      // On initial scan we don't stream — flush token total, last thought, and
      // status once.
      if (isInitialScan && agent) {
        emitTokenUsage(agent);
        if (agent.lastThought) {
          broadcast({ type: 'agentThought', id: agent.id, text: agent.lastThought });
        }
        broadcast({ type: 'agentStatus', id: agent.id, status: 'waiting' });
      }

      if (agent) {
        agent.offset = stat.size;
        agent.pendingLine = trailing;
      }
    } finally {
      fs.closeSync(fd);
    }
  } catch (err) {
    console.error(`[bridge] read ${filePath}:`, err);
  }
}

// ── Idle GC: close agents that stopped writing ─────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [sid, agent] of agentsBySession) {
    if (now - agent.lastActivity > IDLE_MS) {
      agentsBySession.delete(sid);
      agentsByFile.delete(agent.filePath);
      broadcast({ type: 'agentClosed', id: agent.id });
    }
  }
}, 60_000);

// ── File watcher ───────────────────────────────────────────────────────────
// chokidar with a glob and deep nested paths can be flaky on macOS fsevents.
// Watch the base dir directly (recursive) and filter for .jsonl in handlers.
const watcher = chokidar.watch(BASE, {
  ignoreInitial: false,
  persistent: true,
  awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 50 },
});

let addedCount = 0;
const ACTIVE_WINDOW_MS = Number(process.env.ACTIVE_WINDOW_MS || 2 * 60 * 1000);
watcher.on('add', (p) => {
  if (!p.endsWith('.jsonl')) return;
  addedCount++;
  // Skip historical sessions — only show transcripts touched recently. Changes
  // to older files (someone resuming a stale session) still fire 'change' and
  // will be picked up then.
  try {
    const mtime = fs.statSync(p).mtimeMs;
    if (Date.now() - mtime > ACTIVE_WINDOW_MS) return;
  } catch {
    return;
  }
  if (addedCount <= 20) console.log(`[bridge] active add: ${path.basename(p)}`);
  readNewLines(p);
});
watcher.on('change', (p) => {
  if (!p.endsWith('.jsonl')) return;
  readNewLines(p);
});
watcher.on('ready', () => console.log(`[bridge] initial scan done, ${addedCount} .jsonl files added, watching ${BASE}`));
watcher.on('error', (err) => console.error('[bridge] watcher error:', err));

// ── WebSocket server ───────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: PORT });
wss.on('connection', (ws) => {
  clients.add(ws);
  console.log(`[bridge] client connected (${clients.size} total)`);

  // UI buffers 'existingAgents' until layoutLoaded fires — but with
  // browserMock, layoutLoaded has usually already fired by the time we connect,
  // so the buffer never drains. Sending discrete 'agentCreated' messages
  // instead (which the UI adds to the scene immediately).
  const snapshot = [...agentsBySession.values()];
  for (const a of snapshot) {
    ws.send(JSON.stringify({ type: 'agentCreated', id: a.id, folderName: a.folderName }));
    ws.send(
      JSON.stringify({
        type: 'agentTokenUsage',
        id: a.id,
        inputTokens: a.inputTokens,
        outputTokens: a.outputTokens,
        cacheReadTokens: a.cacheReadTokens,
      }),
    );
    if (a.lastThought) {
      ws.send(JSON.stringify({ type: 'agentThought', id: a.id, text: a.lastThought }));
    }
    for (const [toolId, toolName] of a.activeTools) {
      ws.send(JSON.stringify({ type: 'agentToolStart', id: a.id, toolId, status: toolName }));
    }
    if (a.activeTools.size === 0) {
      ws.send(JSON.stringify({ type: 'agentStatus', id: a.id, status: 'waiting' }));
    }
  }

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[bridge] client disconnected (${clients.size} remaining)`);
  });
});

console.log(`[bridge] ws://0.0.0.0:${PORT}`);
