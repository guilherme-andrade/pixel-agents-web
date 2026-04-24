/**
 * Bridge client: connects to the pixel-agents-bridge WebSocket and relays
 * every message into the window message bus — same shape the VS Code
 * extension would post.
 *
 * Only active in browser (non-VS Code) runtime.
 */

const DEFAULT_URL =
  (typeof window !== 'undefined' && (window as unknown as { __BRIDGE_WS__?: string }).__BRIDGE_WS__) ||
  (typeof import.meta !== 'undefined' && (import.meta as { env?: Record<string, string> }).env?.VITE_BRIDGE_WS) ||
  `ws://${typeof location !== 'undefined' ? location.hostname : 'localhost'}:8787`;

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
