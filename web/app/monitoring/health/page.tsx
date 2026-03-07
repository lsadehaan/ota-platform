"use client"

import React, { useEffect } from "react"
import { useQuery } from "@tanstack/react-query"
import { monitoringAPI } from "@/lib/api"
import type { SystemHealth, HealthComponent } from "@/lib/types"
import {
  Card as CardUI,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Skeleton } from "@/components/ui/skeleton"
import { Activity, Database, Radio, MessageSquare } from "lucide-react"

function getIconForComponent(name: string) {
  const lower = name.toLowerCase()
  if (lower.includes("database") || lower.includes("db")) return Database
  if (lower.includes("smpp") || lower.includes("sms")) return Radio
  if (lower.includes("kafka") || lower.includes("broker") || lower.includes("queue")) return MessageSquare
  return Activity
}

function getStatusColor(status: string): string {
  switch (status?.toLowerCase()) {
    case "up":
    case "healthy":
    case "connected":
    case "ok":
      return "bg-green-500"
    case "degraded":
    case "warning":
      return "bg-yellow-500"
    case "down":
    case "unhealthy":
    case "disconnected":
    case "error":
      return "bg-red-500"
    default:
      return "bg-gray-400"
  }
}

function getStatusBadgeVariant(status: string): "default" | "secondary" | "destructive" | "outline" {
  switch (status?.toLowerCase()) {
    case "up":
    case "healthy":
    case "connected":
    case "ok":
      return "default"
    case "degraded":
    case "warning":
      return "secondary"
    case "down":
    case "unhealthy":
    case "disconnected":
    case "error":
      return "destructive"
    default:
      return "outline"
  }
}

function formatUptime(seconds: number): string {
  if (!seconds) return "-"
  const days = Math.floor(seconds / 86400)
  const hours = Math.floor((seconds % 86400) / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  const parts: string[] = []
  if (days > 0) parts.push(`${days}d`)
  if (hours > 0) parts.push(`${hours}h`)
  if (minutes > 0) parts.push(`${minutes}m`)
  return parts.length > 0 ? parts.join(" ") : "<1m"
}

function ServiceCard({ component }: { component: HealthComponent }) {
  const Icon = getIconForComponent(component.name)
  const statusColor = getStatusColor(component.status)

  return (
    <CardUI>
      <CardContent className="pt-6">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className="rounded-md bg-muted p-2">
              <Icon className="h-5 w-5 text-muted-foreground" />
            </div>
            <div>
              <p className="font-semibold">{component.name}</p>
              <div className="flex items-center gap-2 mt-1">
                <div className={`h-2.5 w-2.5 rounded-full ${statusColor}`} />
                <Badge variant={getStatusBadgeVariant(component.status)}>
                  {component.status}
                </Badge>
              </div>
            </div>
          </div>
          {component.latency_ms !== undefined && component.latency_ms !== null && (
            <div className="text-right">
              <p className="text-xs text-muted-foreground">Response Time</p>
              <p className="text-sm font-mono font-semibold">{component.latency_ms}ms</p>
            </div>
          )}
        </div>
        {component.details && Object.keys(component.details).length > 0 && (
          <div className="mt-4 space-y-1 border-t pt-3">
            {Object.entries(component.details).map(([key, value]) => (
              <div key={key} className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">{key}</span>
                <span className="font-mono text-xs">{String(value)}</span>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </CardUI>
  )
}

export default function HealthPage() {
  const { data: health, isLoading, dataUpdatedAt } = useQuery<SystemHealth>({
    queryKey: ["health"],
    queryFn: () => monitoringAPI.getHealth(),
    refetchInterval: 10000,
  })

  const lastUpdated = dataUpdatedAt
    ? new Date(dataUpdatedAt).toLocaleTimeString()
    : "-"

  if (isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-48" />
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-40" />
          ))}
        </div>
      </div>
    )
  }

  const overallStatusColor = getStatusColor(health?.status ?? "unknown")

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">System Health</h1>
          <p className="text-muted-foreground mt-1">
            Real-time service status monitoring
          </p>
        </div>
        <div className="text-right text-sm text-muted-foreground">
          <p>Auto-refreshes every 10s</p>
          <p>Last updated: {lastUpdated}</p>
        </div>
      </div>

      {health && (
        <CardUI>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className={`h-4 w-4 rounded-full ${overallStatusColor}`} />
                <div>
                  <span className="font-semibold text-lg">Overall Status</span>
                  <Badge variant={getStatusBadgeVariant(health.status)} className="ml-3">
                    {health.status}
                  </Badge>
                </div>
              </div>
              <div className="flex items-center gap-6 text-sm">
                {health.version && (
                  <div>
                    <span className="text-muted-foreground">Version: </span>
                    <span className="font-mono">{health.version}</span>
                  </div>
                )}
                {health.uptime_seconds > 0 && (
                  <div>
                    <span className="text-muted-foreground">Uptime: </span>
                    <span className="font-semibold">{formatUptime(health.uptime_seconds)}</span>
                  </div>
                )}
              </div>
            </div>
          </CardContent>
        </CardUI>
      )}

      <div className="grid gap-4 md:grid-cols-2">
        {health?.components?.map((component) => (
          <ServiceCard key={component.name} component={component} />
        ))}
        {(!health?.components || health.components.length === 0) && (
          <p className="text-muted-foreground col-span-full text-center py-8">
            No component status data available.
          </p>
        )}
      </div>
    </div>
  )
}
