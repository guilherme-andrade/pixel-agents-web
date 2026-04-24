/**
 * Bridge client: connects to the pixel-agents-bridge WebSocket and relays
 * every message into the window message bus — same shape the VS Code
 * extension would post.
 *
 * Only active in browser (non-VS Code) runtime.
 */

// Production: same-origin at /pixel/ws (Caddy path-routes to pixel-bridge).
// Dev (localhost): fall back to the standalone bridge on :8787.
function defaultWsUrl(): string {
  if (typeof window !== 'undefined') {
    const overridden = (window as unknown as { __BRIDGE_WS__?: string }).__BRIDGE_WS__;
    if (overridden) return overridden;
  }
  if (typeof import.meta !== 'undefined') {
    const env = (import.meta as { env?: Record<string, string> }).env;
    if (env?.VITE_BRIDGE_WS) return env.VITE_BRIDGE_WS;
  }
  if (typeof location === 'undefined') return 'ws://localhost:8787';
  // Same-origin deployed: wss on /pixel/ws
  if (location.hostname !== 'localhost' && location.hostname !== '127.0.0.1') {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    return `${proto}://${location.host}/pixel/ws`;
  }
  return `ws://${location.hostname}:8787`;
}

const DEFAULT_URL = defaultWsUrl();

let ws: WebSocket | null = null;
let reconnectDelay = 500;

function connect(url: string): void {
  try {
    ws = new WebSocket(url);
  } catch (err) {
    console.error('[bridgeClient] failed to construct WebSocket:', err);
    scheduleReconnect(url);
    return;
  }

  ws.addEventListener('open', () => {
    console.log(`[bridgeClient] connected to ${url}`);
    reconnectDelay = 500;
  });

  ws.addEventListener('message', (event) => {
    let data: unknown;
    try {
      data = typeof event.data === 'string' ? JSON.parse(event.data) : event.data;
    } catch (err) {
      console.warn('[bridgeClient] non-JSON message:', err);
      return;
    }
    window.dispatchEvent(new MessageEvent('message', { data }));
  });

  ws.addEventListener('close', () => {
    console.log('[bridgeClient] disconnected, retrying...');
    scheduleReconnect(url);
  });

  ws.addEventListener('error', (err) => {
    console.error('[bridgeClient] error:', err);
  });
}

function scheduleReconnect(url: string): void {
  setTimeout(() => connect(url), reconnectDelay);
  reconnectDelay = Math.min(reconnectDelay * 2, 10_000);
}

export function startBridgeClient(url: string = DEFAULT_URL): void {
  console.log(`[bridgeClient] will connect to ${url}`);
  connect(url);
}
