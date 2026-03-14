type WSEventHandler = (event: any) => void

function resolveWSBase(): string {
  if (process.env.NEXT_PUBLIC_WS_URL) {
    return process.env.NEXT_PUBLIC_WS_URL.replace(/\/$/, "")
  }
  if (typeof window !== "undefined") {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:"
    const port = process.env.NEXT_PUBLIC_API_PORT || "8080"
    return `${protocol}//${window.location.hostname}:${port}`
  }
  return "ws://ota-api:8080"
}

class WebSocketManager {
  private ws: WebSocket | null = null
  private url: string
  private handlers: Map<string, Set<WSEventHandler>> = new Map()
  private reconnectTimer: NodeJS.Timeout | null = null
  private reconnectDelay = 1000

  private authFailures = 0

  constructor() {
    const base = resolveWSBase()
    const token = process.env.NEXT_PUBLIC_API_KEY
    this.url = base + '/ws' + (token ? `?token=${encodeURIComponent(token)}` : '')
  }

  connect() {
    if (typeof window === 'undefined') return
    if (this.ws?.readyState === WebSocket.OPEN) return
    if (this.authFailures >= 3) return // stop reconnecting after repeated auth failures

    this.ws = new WebSocket(this.url)

    this.ws.onopen = () => {
      this.reconnectDelay = 1000
      this.authFailures = 0
    }

    this.ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        const type = data.type as string
        this.handlers.get(type)?.forEach(handler => handler(data))
        this.handlers.get('*')?.forEach(handler => handler(data))
      } catch {}
    }

    this.ws.onclose = (event) => {
      if (event.code === 1008 || event.code === 4401) {
        this.authFailures++
      }
      this.scheduleReconnect()
    }

    this.ws.onerror = () => {
      this.ws?.close()
    }
  }

  private scheduleReconnect() {
    if (this.reconnectTimer) return
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000)
      this.connect()
    }, this.reconnectDelay)
  }

  subscribe(topic: string) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ action: 'subscribe', topic }))
    }
  }

  on(eventType: string, handler: WSEventHandler) {
    if (!this.handlers.has(eventType)) {
      this.handlers.set(eventType, new Set())
    }
    this.handlers.get(eventType)!.add(handler)
    return () => { this.handlers.get(eventType)?.delete(handler) }
  }

  disconnect() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    this.ws?.close()
    this.ws = null
  }
}

export const wsManager = new WebSocketManager()
