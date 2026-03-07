"use client"

import React from "react"
import { useQuery } from "@tanstack/react-query"
import { monitoringAPI } from "@/lib/api"
import type { ErrorSummary, RecentError, ErrorsByHour } from "@/lib/types"
import {
  Card as CardUI,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts"
import {
  AlertTriangle,
  RefreshCw,
  ShieldAlert,
  Radio,
  Clock,
  Zap,
} from "lucide-react"

function getErrorIcon(type: string) {
  const lower = type.toLowerCase()
  if (lower.includes("counter") || lower.includes("replay")) return RefreshCw
  if (lower.includes("tar")) return ShieldAlert
  if (lower.includes("dlr")) return Radio
  if (lower.includes("por") || lower.includes("timeout")) return Clock
  if (lower.includes("smpp")) return Zap
  return AlertTriangle
}

function getErrorColor(type: string): string {
  const lower = type.toLowerCase()
  if (lower.includes("counter") || lower.includes("replay")) return "text-orange-600 bg-orange-50"
  if (lower.includes("tar")) return "text-purple-600 bg-purple-50"
  if (lower.includes("dlr")) return "text-red-600 bg-red-50"
  if (lower.includes("por") || lower.includes("timeout")) return "text-yellow-600 bg-yellow-50"
  if (lower.includes("smpp")) return "text-blue-600 bg-blue-50"
  return "text-gray-600 bg-gray-50"
}

function formatDate(dateStr: string): string {
  if (!dateStr) return "-"
  return new Date(dateStr).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  })
}

function ErrorTypeCard({ type, count }: { type: string; count: number }) {
  const Icon = getErrorIcon(type)
  const colorClass = getErrorColor(type)

  return (
    <CardUI>
      <CardContent className="pt-6">
        <div className="flex items-center gap-3">
          <div className={`rounded-md p-2 ${colorClass}`}>
            <Icon className="h-5 w-5" />
          </div>
          <div className="flex-1">
            <p className="text-sm font-medium text-muted-foreground">{type}</p>
            <p className="text-2xl font-bold">{count}</p>
          </div>
        </div>
      </CardContent>
    </CardUI>
  )
}

export default function ErrorsPage() {
  const { data: errorSummary, isLoading } = useQuery<ErrorSummary>({
    queryKey: ["errors"],
    queryFn: () => monitoringAPI.getErrors(),
    refetchInterval: 30000,
  })

  if (isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-48" />
        <div className="grid gap-4 md:grid-cols-5">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-24" />
          ))}
        </div>
        <Skeleton className="h-64" />
        <Skeleton className="h-96" />
      </div>
    )
  }

  const errorsByType = errorSummary?.errors_by_type ?? {}
  const errorsByHour = errorSummary?.errors_by_hour ?? []
  const recentErrors = errorSummary?.recent_errors ?? []
  const totalErrors = errorSummary?.total_errors_24h ?? 0

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Error Dashboard</h1>
          <p className="text-muted-foreground mt-1">
            Error monitoring and analysis (last 24 hours)
          </p>
        </div>
        <Badge variant={totalErrors > 0 ? "destructive" : "secondary"} className="text-lg px-4 py-1">
          {totalErrors} error{totalErrors !== 1 ? "s" : ""} (24h)
        </Badge>
      </div>

      <div className="grid gap-4 md:grid-cols-3 lg:grid-cols-5">
        {Object.entries(errorsByType).length > 0 ? (
          Object.entries(errorsByType).map(([type, count]) => (
            <ErrorTypeCard key={type} type={type} count={count} />
          ))
        ) : (
          <>
            <ErrorTypeCard type="Counter Replay" count={0} />
            <ErrorTypeCard type="TAR Unknown" count={0} />
            <ErrorTypeCard type="DLR Failed" count={0} />
            <ErrorTypeCard type="PoR Timeout" count={0} />
            <ErrorTypeCard type="SMPP Error" count={0} />
          </>
        )}
      </div>

      <CardUI>
        <CardHeader>
          <CardTitle>Error Trend (24h)</CardTitle>
        </CardHeader>
        <CardContent>
          {errorsByHour.length > 0 ? (
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={errorsByHour}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis
                  dataKey="hour"
                  tick={{ fontSize: 12 }}
                  tickFormatter={(value: string) => {
                    try {
                      const d = new Date(value)
                      return `${d.getHours().toString().padStart(2, "0")}:00`
                    } catch {
                      return value
                    }
                  }}
                />
                <YAxis tick={{ fontSize: 12 }} allowDecimals={false} />
                <Tooltip
                  labelFormatter={(label: string) => {
                    try {
                      return new Date(label).toLocaleString()
                    } catch {
                      return label
                    }
                  }}
                />
                <Bar dataKey="count" fill="hsl(0, 72%, 51%)" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex items-center justify-center h-64 text-muted-foreground">
              No error data available for the trend chart.
            </div>
          )}
        </CardContent>
      </CardUI>

      <CardUI>
        <CardHeader>
          <CardTitle>Recent Errors</CardTitle>
        </CardHeader>
        <CardContent>
          {recentErrors.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Timestamp</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Card ID</TableHead>
                  <TableHead>Campaign</TableHead>
                  <TableHead>Description</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {recentErrors.map((error) => (
                  <TableRow key={error.id}>
                    <TableCell className="text-sm text-muted-foreground whitespace-nowrap">
                      {formatDate(error.timestamp)}
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline" className="text-xs">
                        {error.type}
                      </Badge>
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {error.card_id ? error.card_id.slice(0, 12) + "..." : "-"}
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {error.campaign_id ? error.campaign_id.slice(0, 8) + "..." : "-"}
                    </TableCell>
                    <TableCell className="text-sm max-w-md truncate">
                      {error.message}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <div className="flex items-center justify-center h-24 text-muted-foreground">
              No recent errors.
            </div>
          )}
        </CardContent>
      </CardUI>
    </div>
  )
}
