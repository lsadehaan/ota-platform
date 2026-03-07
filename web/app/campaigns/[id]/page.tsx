"use client"

import React, { useState, useEffect, useCallback } from "react"
import { useParams, useRouter } from "next/navigation"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { campaignsAPI } from "@/lib/api"
import { wsManager } from "@/lib/ws"
import type { Campaign, CampaignCard, CampaignCommand, PaginatedResponse } from "@/lib/types"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Skeleton } from "@/components/ui/skeleton"
import { Separator } from "@/components/ui/separator"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  ArrowLeft,
  Play,
  Pause,
  RotateCcw,
  XCircle,
  RefreshCw,
  ChevronLeft,
  ChevronRight,
  Clock,
  CheckCircle2,
  AlertCircle,
  Loader2,
  MinusCircle,
  Timer,
} from "lucide-react"

function getStatusBadge(status: string) {
  switch (status) {
    case "draft":
    case "scheduled":
    case "pending":
      return <Badge variant="secondary">{status}</Badge>
    case "running":
    case "in_progress":
      return <Badge className="bg-blue-600 text-white hover:bg-blue-600/80 border-transparent">{status}</Badge>
    case "completed":
    case "succeeded":
    case "success":
      return <Badge variant="outline" className="border-green-600 text-green-600">{status}</Badge>
    case "failed":
    case "error":
      return <Badge variant="destructive">{status}</Badge>
    case "paused":
      return <Badge variant="outline" className="border-yellow-500 text-yellow-600">{status}</Badge>
    case "aborted":
      return <Badge variant="destructive">{status}</Badge>
    case "skipped":
      return <Badge variant="outline" className="text-muted-foreground">{status}</Badge>
    default:
      return <Badge variant="secondary">{status}</Badge>
  }
}

function formatDate(dateStr: string | null | undefined) {
  if (!dateStr) return "-"
  return new Date(dateStr).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  })
}

function StatCard({
  title,
  value,
  icon,
  color,
}: {
  title: string
  value: number
  icon: React.ReactNode
  color?: string
}) {
  return (
    <Card>
      <CardContent className="p-4">
        <div className="flex items-center gap-3">
          <div className={color}>{icon}</div>
          <div>
            <p className="text-2xl font-bold">{value.toLocaleString()}</p>
            <p className="text-xs text-muted-foreground">{title}</p>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

export default function CampaignDetailPage() {
  const params = useParams()
  const router = useRouter()
  const queryClient = useQueryClient()
  const id = params.id as string

  const [cardStatusFilter, setCardStatusFilter] = useState("all")
  const [cardPage, setCardPage] = useState(1)
  const cardPageSize = 20
  const [actionLoading, setActionLoading] = useState<string | null>(null)

  const cardParams: Record<string, string> = {
    page: cardPage.toString(),
    page_size: cardPageSize.toString(),
  }
  if (cardStatusFilter !== "all") {
    cardParams.status = cardStatusFilter
  }

  const { data: campaign, isLoading } = useQuery<Campaign & { cards?: PaginatedResponse<CampaignCard> }>({
    queryKey: ["campaign", id, cardStatusFilter, cardPage],
    queryFn: () =>
      campaignsAPI.get(id + "?" + new URLSearchParams(cardParams).toString()),
    refetchInterval: 10000,
  })

  const invalidate = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: ["campaign", id] })
  }, [queryClient, id])

  useEffect(() => {
    wsManager.connect()
    wsManager.subscribe(`campaign:${id}`)

    const unsubProgress = wsManager.on("campaign_progress", (event: any) => {
      if (event.campaign_id === id) {
        invalidate()
      }
    })

    const unsubStatus = wsManager.on("campaign_status", (event: any) => {
      if (event.campaign_id === id) {
        invalidate()
      }
    })

    const unsubCard = wsManager.on("card_status", (event: any) => {
      if (event.campaign_id === id) {
        invalidate()
      }
    })

    return () => {
      unsubProgress()
      unsubStatus()
      unsubCard()
    }
  }, [id, invalidate])

  async function handleAction(action: string, fn: () => Promise<void>) {
    setActionLoading(action)
    try {
      await fn()
      invalidate()
    } catch (err) {
      console.error(`Campaign ${action} failed:`, err)
    } finally {
      setActionLoading(null)
    }
  }

  if (isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-64" />
        <div className="grid gap-4 grid-cols-2 lg:grid-cols-6">
          {Array.from({ length: 6 }).map((_, i) => (
            <Card key={i}>
              <CardContent className="p-4">
                <Skeleton className="h-12 w-full" />
              </CardContent>
            </Card>
          ))}
        </div>
        <Skeleton className="h-64 w-full" />
      </div>
    )
  }

  if (!campaign) {
    return (
      <div className="flex flex-col items-center justify-center py-20">
        <p className="text-muted-foreground mb-4">Campaign not found.</p>
        <Button variant="outline" onClick={() => router.push("/campaigns")}>
          Back to Campaigns
        </Button>
      </div>
    )
  }

  const status = campaign.status
  const cards = campaign.cards?.data ?? []
  const cardsTotalPages = campaign.cards?.total_pages ?? 1
  const cardsTotal = campaign.cards?.total ?? 0

  const canStart = status === "draft" || status === "scheduled"
  const canPause = status === "running"
  const canResume = status === "paused"
  const canAbort = status === "running" || status === "paused"
  const canRetry = campaign.failed_cards > 0

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div className="space-y-1">
          <div className="flex items-center gap-3">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => router.push("/campaigns")}
            >
              <ArrowLeft className="h-4 w-4 mr-1" />
              Back
            </Button>
            <h1 className="text-3xl font-bold tracking-tight">{campaign.name}</h1>
            {getStatusBadge(status)}
          </div>
          {campaign.description && (
            <p className="text-muted-foreground ml-[72px]">{campaign.description}</p>
          )}
          <div className="flex gap-4 ml-[72px] text-sm text-muted-foreground">
            <span>Created: {formatDate(campaign.created_at)}</span>
            {campaign.started_at && <span>Started: {formatDate(campaign.started_at)}</span>}
            {campaign.completed_at && <span>Completed: {formatDate(campaign.completed_at)}</span>}
          </div>
        </div>

        <div className="flex items-center gap-2">
          {canStart && (
            <Button
              onClick={() => handleAction("start", () => campaignsAPI.start(id))}
              disabled={actionLoading !== null}
            >
              {actionLoading === "start" ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <Play className="mr-2 h-4 w-4" />
              )}
              Start
            </Button>
          )}
          {canPause && (
            <Button
              variant="outline"
              onClick={() => handleAction("pause", () => campaignsAPI.pause(id))}
              disabled={actionLoading !== null}
            >
              {actionLoading === "pause" ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <Pause className="mr-2 h-4 w-4" />
              )}
              Pause
            </Button>
          )}
          {canResume && (
            <Button
              onClick={() => handleAction("resume", () => campaignsAPI.resume(id))}
              disabled={actionLoading !== null}
            >
              {actionLoading === "resume" ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <Play className="mr-2 h-4 w-4" />
              )}
              Resume
            </Button>
          )}
          {canAbort && (
            <Button
              variant="destructive"
              onClick={() => handleAction("abort", () => campaignsAPI.abort(id))}
              disabled={actionLoading !== null}
            >
              {actionLoading === "abort" ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <XCircle className="mr-2 h-4 w-4" />
              )}
              Abort
            </Button>
          )}
          {canRetry && (
            <Button
              variant="outline"
              onClick={() =>
                handleAction("retry", () => campaignsAPI.retryFailed(id))
              }
              disabled={actionLoading !== null}
            >
              {actionLoading === "retry" ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <RotateCcw className="mr-2 h-4 w-4" />
              )}
              Retry Failed
            </Button>
          )}
        </div>
      </div>

      <div className="space-y-2">
        <div className="flex items-center justify-between text-sm">
          <span className="text-muted-foreground">Overall Progress</span>
          <span className="font-medium">{campaign.progress_percent}%</span>
        </div>
        <Progress value={campaign.progress_percent} className="h-3" />
      </div>

      <div className="grid gap-4 grid-cols-2 md:grid-cols-3 lg:grid-cols-6">
        <StatCard
          title="Total Cards"
          value={campaign.total_cards}
          icon={<RefreshCw className="h-5 w-5" />}
          color="text-foreground"
        />
        <StatCard
          title="Pending"
          value={campaign.pending_cards}
          icon={<Clock className="h-5 w-5" />}
          color="text-muted-foreground"
        />
        <StatCard
          title="In Progress"
          value={campaign.in_progress_cards}
          icon={<Timer className="h-5 w-5" />}
          color="text-blue-600"
        />
        <StatCard
          title="Succeeded"
          value={campaign.success_cards}
          icon={<CheckCircle2 className="h-5 w-5" />}
          color="text-green-600"
        />
        <StatCard
          title="Failed"
          value={campaign.failed_cards}
          icon={<AlertCircle className="h-5 w-5" />}
          color="text-red-600"
        />
        <StatCard
          title="Skipped"
          value={
            campaign.total_cards -
            campaign.pending_cards -
            campaign.in_progress_cards -
            campaign.success_cards -
            campaign.failed_cards
          }
          icon={<MinusCircle className="h-5 w-5" />}
          color="text-muted-foreground"
        />
      </div>

      <Separator />

      {campaign.commands && campaign.commands.length > 0 && (
        <div className="space-y-3">
          <h2 className="text-lg font-semibold">Command Sequence</h2>
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-16">#</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Description</TableHead>
                  <TableHead>APDU</TableHead>
                  <TableHead>Expect Response</TableHead>
                  <TableHead className="text-right">Timeout (s)</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {campaign.commands
                  .sort((a: CampaignCommand, b: CampaignCommand) => a.sequence - b.sequence)
                  .map((cmd: CampaignCommand) => (
                    <TableRow key={cmd.id}>
                      <TableCell className="font-mono">{cmd.sequence}</TableCell>
                      <TableCell>{cmd.type}</TableCell>
                      <TableCell>{cmd.description || "-"}</TableCell>
                      <TableCell>
                        <code className="rounded bg-muted px-1.5 py-0.5 text-xs font-mono break-all">
                          {cmd.apdu.length > 60
                            ? cmd.apdu.substring(0, 60) + "..."
                            : cmd.apdu}
                        </code>
                      </TableCell>
                      <TableCell>
                        {cmd.expect_response ? (
                          <Badge variant="outline" className="border-green-600 text-green-600">
                            Yes
                          </Badge>
                        ) : (
                          <Badge variant="secondary">No</Badge>
                        )}
                      </TableCell>
                      <TableCell className="text-right">{cmd.timeout_seconds}</TableCell>
                    </TableRow>
                  ))}
              </TableBody>
            </Table>
          </div>
        </div>
      )}

      <Separator />

      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <h2 className="text-lg font-semibold">Per-Card Progress</h2>
          <Select
            value={cardStatusFilter}
            onValueChange={(value) => {
              setCardStatusFilter(value)
              setCardPage(1)
            }}
          >
            <SelectTrigger className="w-[180px]">
              <SelectValue placeholder="Filter by status" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All statuses</SelectItem>
              <SelectItem value="pending">Pending</SelectItem>
              <SelectItem value="in_progress">In Progress</SelectItem>
              <SelectItem value="succeeded">Succeeded</SelectItem>
              <SelectItem value="failed">Failed</SelectItem>
              <SelectItem value="skipped">Skipped</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>ICCID</TableHead>
                <TableHead>MSISDN</TableHead>
                <TableHead>Current Step</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Last Error</TableHead>
                <TableHead className="text-right">Retries</TableHead>
                <TableHead>Updated</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {cards.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} className="h-24 text-center text-muted-foreground">
                    No card records found.
                  </TableCell>
                </TableRow>
              ) : (
                cards.map((card: CampaignCard) => (
                  <TableRow key={card.id}>
                    <TableCell className="font-mono text-xs">{card.iccid}</TableCell>
                    <TableCell>{card.msisdn || "-"}</TableCell>
                    <TableCell>
                      {card.current_command}/{card.total_commands}
                    </TableCell>
                    <TableCell>{getStatusBadge(card.status)}</TableCell>
                    <TableCell className="max-w-[200px] truncate text-xs text-red-600">
                      {card.error_message || "-"}
                    </TableCell>
                    <TableCell className="text-right">{card.attempts}</TableCell>
                    <TableCell className="text-muted-foreground text-xs">
                      {formatDate(card.updated_at)}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>

        {cardsTotalPages > 1 && (
          <div className="flex items-center justify-between">
            <p className="text-sm text-muted-foreground">
              Showing {(cardPage - 1) * cardPageSize + 1} to{" "}
              {Math.min(cardPage * cardPageSize, cardsTotal)} of {cardsTotal} cards
            </p>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setCardPage((p) => Math.max(1, p - 1))}
                disabled={cardPage <= 1}
              >
                <ChevronLeft className="h-4 w-4" />
                Previous
              </Button>
              <span className="text-sm text-muted-foreground px-2">
                Page {cardPage} of {cardsTotalPages}
              </span>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setCardPage((p) => Math.min(cardsTotalPages, p + 1))}
                disabled={cardPage >= cardsTotalPages}
              >
                Next
                <ChevronRight className="h-4 w-4" />
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
