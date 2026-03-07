"use client"

import React, { useEffect, useState, useRef, useCallback } from "react"
import { useQuery } from "@tanstack/react-query"
import { dashboardAPI } from "@/lib/api"
import { wsManager } from "@/lib/ws"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { cn } from "@/lib/utils"
import { Activity } from "lucide-react"
import type { ActivityEvent } from "@/lib/types"

function formatTimestamp(ts: string): string {
  const date = new Date(ts)
  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffSeconds = Math.floor(diffMs / 1000)
  const diffMinutes = Math.floor(diffSeconds / 60)
  const diffHours = Math.floor(diffMinutes / 60)

  if (diffSeconds < 60) return `${diffSeconds}s ago`
  if (diffMinutes < 60) return `${diffMinutes}m ago`
  if (diffHours < 24) return `${diffHours}h ago`
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  })
}

function getEventBadgeVariant(type: string): "default" | "secondary" | "destructive" | "outline" {
  switch (type) {
    case "error":
    case "failed":
      return "destructive"
    case "campaign_started":
    case "campaign_completed":
    case "success":
      return "default"
    case "warning":
      return "outline"
    default:
      return "secondary"
  }
}

function getEventLabel(type: string): string {
  return type
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
}

interface ActivityItemProps {
  event: ActivityEvent
  isNew: boolean
}

function ActivityItem({ event, isNew }: ActivityItemProps) {
  return (
    <div
      className={cn(
        "flex items-start gap-3 rounded-md border-b px-3 py-3 transition-all duration-500",
        isNew && "bg-primary/5"
      )}
    >
      <div className="flex flex-col gap-1 flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <Badge variant={getEventBadgeVariant(event.type)} className="text-[10px] px-1.5 py-0">
            {getEventLabel(event.type)}
          </Badge>
          {event.entity_type && (
            <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
              {event.entity_type}
            </span>
          )}
        </div>
        <p className="text-sm text-foreground truncate">{event.description}</p>
        <div className="flex items-center gap-2">
          <span className="text-xs text-muted-foreground">
            {formatTimestamp(event.timestamp)}
          </span>
          {event.user && (
            <span className="text-xs text-muted-foreground">
              by {event.user}
            </span>
          )}
        </div>
      </div>
    </div>
  )
}

export function ActivityFeed() {
  const [liveEvents, setLiveEvents] = useState<ActivityEvent[]>([])
  const [newEventIds, setNewEventIds] = useState<Set<string>>(new Set())
  const scrollRef = useRef<HTMLDivElement>(null)

  const { data: initialEvents } = useQuery({
    queryKey: ["dashboard", "activity"],
    queryFn: dashboardAPI.getActivity,
    refetchInterval: 60000,
  })

  const handleWSEvent = useCallback((wsEvent: any) => {
    const newEvent: ActivityEvent = {
      id: wsEvent.id || `ws-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      type: wsEvent.type || "unknown",
      description: wsEvent.description || wsEvent.message || JSON.stringify(wsEvent),
      entity_type: wsEvent.entity_type || "",
      entity_id: wsEvent.entity_id || "",
      user: wsEvent.user || "system",
      timestamp: wsEvent.timestamp || new Date().toISOString(),
      metadata: wsEvent.metadata || {},
    }

    setLiveEvents((prev) => [newEvent, ...prev].slice(0, 50))
    setNewEventIds((prev) => {
      const next = new Set(prev)
      next.add(newEvent.id)
      return next
    })

    setTimeout(() => {
      setNewEventIds((prev) => {
        const next = new Set(prev)
        next.delete(newEvent.id)
        return next
      })
    }, 3000)
  }, [])

  useEffect(() => {
    wsManager.connect()
    const unsubscribe = wsManager.on("*", handleWSEvent)
    return () => {
      unsubscribe()
    }
  }, [handleWSEvent])

  const allEvents: ActivityEvent[] = [
    ...liveEvents,
    ...(initialEvents || []).filter(
      (e) => !liveEvents.some((le) => le.id === e.id)
    ),
  ].slice(0, 100)

  return (
    <Card className="h-full">
      <CardHeader className="pb-3">
        <div className="flex items-center gap-2">
          <Activity className="h-4 w-4 text-muted-foreground" />
          <CardTitle className="text-base">Recent Activity</CardTitle>
          {liveEvents.length > 0 && (
            <div className="ml-auto flex items-center gap-1.5">
              <span className="relative flex h-2 w-2">
                <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-green-400 opacity-75" />
                <span className="relative inline-flex h-2 w-2 rounded-full bg-green-500" />
              </span>
              <span className="text-xs text-muted-foreground">Live</span>
            </div>
          )}
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <div ref={scrollRef} className="max-h-[500px] overflow-y-auto">
          {allEvents.length === 0 ? (
            <div className="flex items-center justify-center py-12 text-sm text-muted-foreground">
              No activity yet
            </div>
          ) : (
            allEvents.map((event) => (
              <ActivityItem
                key={event.id}
                event={event}
                isNew={newEventIds.has(event.id)}
              />
            ))
          )}
        </div>
      </CardContent>
    </Card>
  )
}
