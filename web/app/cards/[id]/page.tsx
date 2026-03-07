"use client"

import React, { useState } from "react"
import { useParams, useRouter } from "next/navigation"
import Link from "next/link"
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query"
import { cardsAPI, monitoringAPI } from "@/lib/api"
import type { Card, CardCounter, MessageLog, PaginatedResponse } from "@/lib/types"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Skeleton } from "@/components/ui/skeleton"
import { Separator } from "@/components/ui/separator"
import {
  Card as CardUI,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  ArrowLeft,
  Send,
  ShieldOff,
  ChevronDown,
  ChevronUp,
} from "lucide-react"

function getStatusBadge(status: string) {
  switch (status) {
    case "active":
      return (
        <Badge variant="outline" className="border-green-600 text-green-600">
          active
        </Badge>
      )
    case "inactive":
      return <Badge variant="secondary">inactive</Badge>
    case "blocked":
      return <Badge variant="destructive">blocked</Badge>
    default:
      return <Badge variant="secondary">{status}</Badge>
  }
}

function getDirectionBadge(direction: string) {
  if (direction === "outbound") {
    return (
      <Badge className="bg-blue-600 text-white hover:bg-blue-600/80 border-transparent">
        MT
      </Badge>
    )
  }
  return (
    <Badge variant="outline" className="border-purple-600 text-purple-600">
      MO
    </Badge>
  )
}

function getMessageStatusBadge(status: string) {
  switch (status) {
    case "delivered":
      return (
        <Badge variant="outline" className="border-green-600 text-green-600">
          {status}
        </Badge>
      )
    case "failed":
      return <Badge variant="destructive">{status}</Badge>
    case "pending":
    case "sent":
      return <Badge variant="secondary">{status}</Badge>
    default:
      return <Badge variant="secondary">{status}</Badge>
  }
}

function formatDate(dateStr: string) {
  if (!dateStr) return "-"
  return new Date(dateStr).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  })
}

function ExpandablePayload({ payload }: { payload: string }) {
  const [expanded, setExpanded] = useState(false)
  if (!payload) return <span className="text-muted-foreground">-</span>

  const truncated = payload.length > 40 ? payload.slice(0, 40) + "..." : payload
  const needsExpansion = payload.length > 40

  return (
    <div className="max-w-[300px]">
      <code className="text-xs font-mono break-all">
        {expanded ? payload : truncated}
      </code>
      {needsExpansion && (
        <button
          onClick={(e) => {
            e.stopPropagation()
            setExpanded(!expanded)
          }}
          className="ml-1 inline-flex items-center text-xs text-muted-foreground hover:text-foreground"
        >
          {expanded ? (
            <ChevronUp className="h-3 w-3" />
          ) : (
            <ChevronDown className="h-3 w-3" />
          )}
        </button>
      )}
    </div>
  )
}

function SendTestCommandDialog({ cardId, msisdn }: { cardId: string; msisdn: string }) {
  const [open, setOpen] = useState(false)
  const [command, setCommand] = useState("")
  const queryClient = useQueryClient()

  const sendCommand = useMutation({
    mutationFn: () =>
      cardsAPI.update(cardId, { action: "send_command", command }),
    onSuccess: () => {
      setOpen(false)
      setCommand("")
      queryClient.invalidateQueries({ queryKey: ["card-messages", cardId] })
    },
  })

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline">
          <Send className="mr-2 h-4 w-4" />
          Send Test Command
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Send Test Command</DialogTitle>
          <DialogDescription>
            Send an APDU command to card {msisdn}
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="command">APDU Command (hex)</Label>
            <Input
              id="command"
              placeholder="00A40400..."
              value={command}
              onChange={(e) => setCommand(e.target.value)}
              className="font-mono"
            />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button
            onClick={() => sendCommand.mutate()}
            disabled={!command.trim() || sendCommand.isPending}
          >
            {sendCommand.isPending ? "Sending..." : "Send Command"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

export default function CardDetailPage() {
  const params = useParams()
  const router = useRouter()
  const queryClient = useQueryClient()
  const cardId = params.id as string

  const { data: card, isLoading: cardLoading } = useQuery<Card>({
    queryKey: ["card", cardId],
    queryFn: () => cardsAPI.get(cardId),
  })

  const { data: counters, isLoading: countersLoading } = useQuery<CardCounter[]>({
    queryKey: ["card-counters", cardId],
    queryFn: () => cardsAPI.getCounters(cardId),
  })

  const { data: messagesData, isLoading: messagesLoading } = useQuery<
    PaginatedResponse<MessageLog>
  >({
    queryKey: ["card-messages", cardId],
    queryFn: () =>
      monitoringAPI.listMessages({
        card_id: cardId,
        page: "1",
        page_size: "20",
      }),
  })

  const blockCard = useMutation({
    mutationFn: () => cardsAPI.update(cardId, { status: "blocked" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["card", cardId] })
    },
  })

  if (cardLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-4 w-48" />
        <div className="grid gap-4 grid-cols-1 md:grid-cols-2">
          <Skeleton className="h-48" />
          <Skeleton className="h-48" />
        </div>
      </div>
    )
  }

  if (!card) {
    return (
      <div className="space-y-4">
        <Button variant="ghost" onClick={() => router.back()}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <p className="text-muted-foreground">Card not found.</p>
      </div>
    )
  }

  const messages = messagesData?.data ?? []

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Button variant="ghost" size="sm" onClick={() => router.push("/cards")}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Cards
        </Button>
      </div>

      <div className="flex items-start justify-between">
        <div className="space-y-1">
          <div className="flex items-center gap-3">
            <h1 className="text-3xl font-bold tracking-tight font-mono">
              {card.iccid}
            </h1>
            {getStatusBadge(card.status)}
          </div>
          <div className="flex items-center gap-4 text-sm text-muted-foreground">
            <span>
              IMSI: <span className="font-mono">{card.imsi}</span>
            </span>
            <span>
              MSISDN: <span className="font-mono">{card.msisdn}</span>
            </span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <SendTestCommandDialog cardId={card.id} msisdn={card.msisdn} />
          <Button
            variant="destructive"
            onClick={() => blockCard.mutate()}
            disabled={card.status === "blocked" || blockCard.isPending}
          >
            <ShieldOff className="mr-2 h-4 w-4" />
            {blockCard.isPending ? "Blocking..." : "Block Card"}
          </Button>
        </div>
      </div>

      <div className="grid gap-6 grid-cols-1 lg:grid-cols-3">
        <CardUI>
          <CardHeader>
            <CardTitle className="text-base">Card Information</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex justify-between text-sm">
              <span className="text-muted-foreground">Profile</span>
              <Link
                href={`/profiles/${card.profile_id}`}
                className="text-primary hover:underline"
              >
                {card.profile_name}
              </Link>
            </div>
            <Separator />
            <div className="flex justify-between text-sm">
              <span className="text-muted-foreground">Created</span>
              <span>{formatDate(card.created_at)}</span>
            </div>
            <Separator />
            <div className="flex justify-between text-sm">
              <span className="text-muted-foreground">Updated</span>
              <span>{formatDate(card.updated_at)}</span>
            </div>
            {card.tags && card.tags.length > 0 && (
              <>
                <Separator />
                <div className="space-y-1">
                  <span className="text-sm text-muted-foreground">Tags</span>
                  <div className="flex flex-wrap gap-1">
                    {card.tags.map((tag) => (
                      <Badge key={tag} variant="secondary" className="text-xs">
                        {tag}
                      </Badge>
                    ))}
                  </div>
                </div>
              </>
            )}
          </CardContent>
        </CardUI>

        <CardUI className="lg:col-span-2">
          <CardHeader>
            <CardTitle className="text-base">Counters</CardTitle>
          </CardHeader>
          <CardContent>
            {countersLoading ? (
              <div className="space-y-2">
                {Array.from({ length: 3 }).map((_, i) => (
                  <Skeleton key={i} className="h-8 w-full" />
                ))}
              </div>
            ) : !counters || counters.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No counters recorded yet.
              </p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>TAR</TableHead>
                    <TableHead className="text-right">Counter Value</TableHead>
                    <TableHead className="text-right">
                      Last Response Counter
                    </TableHead>
                    <TableHead>Updated</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {counters.map((counter) => (
                    <TableRow key={counter.id}>
                      <TableCell className="font-mono text-sm">
                        {counter.tar}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {counter.counter_value}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {counter.last_response_counter}
                      </TableCell>
                      <TableCell className="text-muted-foreground text-sm">
                        {formatDate(counter.updated_at)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </CardUI>
      </div>

      <CardUI>
        <CardHeader>
          <CardTitle className="text-base">Recent Messages</CardTitle>
        </CardHeader>
        <CardContent>
          {messagesLoading ? (
            <div className="space-y-2">
              {Array.from({ length: 5 }).map((_, i) => (
                <Skeleton key={i} className="h-8 w-full" />
              ))}
            </div>
          ) : messages.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No messages recorded yet.
            </p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Direction</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>DLR Status</TableHead>
                  <TableHead>PoR Status</TableHead>
                  <TableHead>Payload</TableHead>
                  <TableHead>Created</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {messages.map((msg) => (
                  <TableRow key={msg.id}>
                    <TableCell>
                      {getDirectionBadge(msg.direction)}
                    </TableCell>
                    <TableCell>{getMessageStatusBadge(msg.status)}</TableCell>
                    <TableCell>
                      {msg.dlr_status ? (
                        <Badge variant="secondary" className="text-xs">
                          {msg.dlr_status}
                        </Badge>
                      ) : (
                        <span className="text-muted-foreground">-</span>
                      )}
                    </TableCell>
                    <TableCell>
                      {msg.response_status_word ? (
                        <Badge variant="secondary" className="text-xs font-mono">
                          {msg.response_status_word}
                        </Badge>
                      ) : (
                        <span className="text-muted-foreground">-</span>
                      )}
                    </TableCell>
                    <TableCell>
                      <ExpandablePayload payload={msg.raw_payload} />
                    </TableCell>
                    <TableCell className="text-muted-foreground text-sm">
                      {formatDate(msg.created_at)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </CardUI>

      <CardUI>
        <CardHeader>
          <CardTitle className="text-base">Campaign History</CardTitle>
        </CardHeader>
        <CardContent>
          {cardLoading ? (
            <Skeleton className="h-24 w-full" />
          ) : card.metadata?.campaigns ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Campaign</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Started</TableHead>
                  <TableHead>Completed</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(
                  (card as any).campaign_history ?? []
                ).map(
                  (entry: {
                    campaign_id: string
                    campaign_name: string
                    status: string
                    started_at: string
                    completed_at: string
                  }) => (
                    <TableRow key={entry.campaign_id}>
                      <TableCell>
                        <Link
                          href={`/campaigns/${entry.campaign_id}`}
                          className="text-primary hover:underline"
                        >
                          {entry.campaign_name}
                        </Link>
                      </TableCell>
                      <TableCell>
                        {getMessageStatusBadge(entry.status)}
                      </TableCell>
                      <TableCell className="text-muted-foreground text-sm">
                        {formatDate(entry.started_at)}
                      </TableCell>
                      <TableCell className="text-muted-foreground text-sm">
                        {formatDate(entry.completed_at)}
                      </TableCell>
                    </TableRow>
                  )
                )}
              </TableBody>
            </Table>
          ) : (
            <p className="text-sm text-muted-foreground">
              This card has not participated in any campaigns.
            </p>
          )}
        </CardContent>
      </CardUI>
    </div>
  )
}
