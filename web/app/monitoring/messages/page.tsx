"use client"

import React, { useState, useEffect, useCallback } from "react"
import { useQuery } from "@tanstack/react-query"
import { monitoringAPI } from "@/lib/api"
import type { MessageLog, PaginatedResponse } from "@/lib/types"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Label } from "@/components/ui/label"
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
import { ChevronDown, ChevronUp, ChevronLeft, ChevronRight, Download, Search } from "lucide-react"
import { HexViewer } from "@/components/hex-viewer/hex-viewer"
import Papa from "papaparse"

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

function DirectionBadge({ direction }: { direction: string }) {
  if (direction === "outbound") {
    return (
      <Badge variant="outline" className="border-blue-600 text-blue-600">
        MT
      </Badge>
    )
  }
  return (
    <Badge variant="outline" className="border-emerald-600 text-emerald-600">
      MO
    </Badge>
  )
}

function StatusBadge({ status }: { status: string }) {
  switch (status?.toLowerCase()) {
    case "sent":
    case "delivered":
    case "success":
      return (
        <Badge variant="outline" className="border-green-600 text-green-600">
          {status}
        </Badge>
      )
    case "failed":
    case "error":
      return <Badge variant="destructive">{status}</Badge>
    case "pending":
    case "queued":
      return <Badge variant="secondary">{status}</Badge>
    default:
      return <Badge variant="outline">{status || "-"}</Badge>
  }
}

export default function MessagesPage() {
  const [direction, setDirection] = useState("all")
  const [msisdn, setMsisdn] = useState("")
  const [campaignId, setCampaignId] = useState("")
  const [status, setStatus] = useState("")
  const [dateFrom, setDateFrom] = useState("")
  const [dateTo, setDateTo] = useState("")
  const [page, setPage] = useState(1)
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set())
  const pageSize = 20

  const params: Record<string, string> = {
    page: page.toString(),
    page_size: pageSize.toString(),
  }
  if (direction !== "all") params.direction = direction
  if (msisdn.trim()) params.msisdn = msisdn.trim()
  if (campaignId.trim()) params.campaign_id = campaignId.trim()
  if (status.trim()) params.status = status.trim()
  if (dateFrom) params.date_from = dateFrom
  if (dateTo) params.date_to = dateTo

  const { data, isLoading } = useQuery<PaginatedResponse<MessageLog>>({
    queryKey: ["messages", direction, msisdn, campaignId, status, dateFrom, dateTo, page],
    queryFn: () => monitoringAPI.listMessages(params),
  })

  const messages = data?.data ?? []
  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / pageSize))

  function toggleRow(id: string) {
    setExpandedRows((prev) => {
      const next = new Set(prev)
      if (next.has(id)) {
        next.delete(id)
      } else {
        next.add(id)
      }
      return next
    })
  }

  function handleExportCSV() {
    if (messages.length === 0) return
    const csvData = messages.map((msg) => ({
      Direction: msg.direction === "outbound" ? "MT" : "MO",
      MSISDN: msg.msisdn,
      Campaign_ID: msg.campaign_id || "",
      Status: msg.status,
      DLR_Status: msg.dlr_status || "",
      Response_SW: msg.response_status_word || "",
      Raw_Payload: msg.raw_payload || "",
      Decoded_Payload: msg.decoded_payload || "",
      Sent_At: msg.sent_at || "",
      Delivered_At: msg.delivered_at || "",
      Created_At: msg.created_at || "",
    }))
    const csv = Papa.unparse(csvData)
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" })
    const url = URL.createObjectURL(blob)
    const link = document.createElement("a")
    link.href = url
    link.download = `messages-export-${new Date().toISOString().slice(0, 10)}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Message Explorer</h1>
          <p className="text-muted-foreground mt-1">
            Search and inspect OTA message traffic
          </p>
        </div>
        <Button variant="outline" onClick={handleExportCSV} disabled={messages.length === 0}>
          <Download className="mr-2 h-4 w-4" />
          Export CSV
        </Button>
      </div>

      <div className="grid gap-4 md:grid-cols-3 lg:grid-cols-6">
        <div className="space-y-1">
          <Label className="text-xs">Direction</Label>
          <Select value={direction} onValueChange={(v) => { setDirection(v); setPage(1) }}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All</SelectItem>
              <SelectItem value="outbound">MT (Outbound)</SelectItem>
              <SelectItem value="inbound">MO (Inbound)</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-1">
          <Label className="text-xs">MSISDN</Label>
          <Input
            placeholder="e.g. 93700..."
            value={msisdn}
            onChange={(e) => { setMsisdn(e.target.value); setPage(1) }}
            className="font-mono"
          />
        </div>
        <div className="space-y-1">
          <Label className="text-xs">Campaign ID</Label>
          <Input
            placeholder="Campaign ID"
            value={campaignId}
            onChange={(e) => { setCampaignId(e.target.value); setPage(1) }}
          />
        </div>
        <div className="space-y-1">
          <Label className="text-xs">Status</Label>
          <Input
            placeholder="e.g. delivered"
            value={status}
            onChange={(e) => { setStatus(e.target.value); setPage(1) }}
          />
        </div>
        <div className="space-y-1">
          <Label className="text-xs">Date From</Label>
          <Input
            type="date"
            value={dateFrom}
            onChange={(e) => { setDateFrom(e.target.value); setPage(1) }}
          />
        </div>
        <div className="space-y-1">
          <Label className="text-xs">Date To</Label>
          <Input
            type="date"
            value={dateTo}
            onChange={(e) => { setDateTo(e.target.value); setPage(1) }}
          />
        </div>
      </div>

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-8" />
              <TableHead>Direction</TableHead>
              <TableHead>MSISDN</TableHead>
              <TableHead>Campaign</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>DLR Status</TableHead>
              <TableHead>PoR Status</TableHead>
              <TableHead>Created</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <TableRow key={`skeleton-${i}`}>
                  {Array.from({ length: 8 }).map((_, j) => (
                    <TableCell key={`skeleton-cell-${j}`}>
                      <div className="h-4 w-full animate-pulse rounded bg-muted" />
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : messages.length === 0 ? (
              <TableRow>
                <TableCell colSpan={8} className="h-24 text-center text-muted-foreground">
                  No messages found matching the filters.
                </TableCell>
              </TableRow>
            ) : (
              messages.map((msg) => (
                <React.Fragment key={msg.id}>
                  <TableRow
                    className="cursor-pointer"
                    onClick={() => toggleRow(msg.id)}
                  >
                    <TableCell>
                      {expandedRows.has(msg.id) ? (
                        <ChevronUp className="h-4 w-4 text-muted-foreground" />
                      ) : (
                        <ChevronDown className="h-4 w-4 text-muted-foreground" />
                      )}
                    </TableCell>
                    <TableCell>
                      <DirectionBadge direction={msg.direction} />
                    </TableCell>
                    <TableCell className="font-mono text-sm">{msg.msisdn}</TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {msg.campaign_id ? msg.campaign_id.slice(0, 8) + "..." : "-"}
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={msg.status} />
                    </TableCell>
                    <TableCell>
                      <span className="text-sm text-muted-foreground">{msg.dlr_status || "-"}</span>
                    </TableCell>
                    <TableCell>
                      <span className="text-sm text-muted-foreground">{msg.response_status_word || "-"}</span>
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {formatDate(msg.created_at)}
                    </TableCell>
                  </TableRow>
                  {expandedRows.has(msg.id) && (
                    <TableRow>
                      <TableCell colSpan={8} className="bg-muted/50 p-4">
                        <div className="grid gap-4 md:grid-cols-2">
                          <div className="space-y-1">
                            <p className="text-xs font-medium text-muted-foreground uppercase">Raw Payload</p>
                            <div className="max-h-32 overflow-auto">
                              <HexViewer data={msg.raw_payload || ""} maxPreviewLength={128} />
                            </div>
                          </div>
                          <div className="space-y-1">
                            <p className="text-xs font-medium text-muted-foreground uppercase">Decoded Payload</p>
                            <div className="max-h-32 overflow-auto">
                              <HexViewer data={msg.decoded_payload || ""} maxPreviewLength={128} />
                            </div>
                          </div>
                          {msg.response_payload && (
                            <div className="space-y-1 md:col-span-2">
                              <p className="text-xs font-medium text-muted-foreground uppercase">Response Payload</p>
                              <div className="max-h-32 overflow-auto">
                                <HexViewer data={msg.response_payload} maxPreviewLength={128} />
                              </div>
                            </div>
                          )}
                          <div className="md:col-span-2 grid grid-cols-4 gap-4 text-sm">
                            <div>
                              <span className="text-muted-foreground">Transport: </span>
                              <span className="font-mono">{msg.transport || "-"}</span>
                            </div>
                            <div>
                              <span className="text-muted-foreground">Sent: </span>
                              <span>{formatDate(msg.sent_at)}</span>
                            </div>
                            <div>
                              <span className="text-muted-foreground">Delivered: </span>
                              <span>{formatDate(msg.delivered_at)}</span>
                            </div>
                            <div>
                              <span className="text-muted-foreground">Responded: </span>
                              <span>{formatDate(msg.responded_at)}</span>
                            </div>
                          </div>
                        </div>
                      </TableCell>
                    </TableRow>
                  )}
                </React.Fragment>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            Showing {(page - 1) * pageSize + 1} to{" "}
            {Math.min(page * pageSize, total)} of {total} messages
          </p>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPage(page - 1)}
              disabled={page <= 1}
            >
              <ChevronLeft className="h-4 w-4" />
              Previous
            </Button>
            <span className="text-sm text-muted-foreground px-2">
              Page {page} of {totalPages}
            </span>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPage(page + 1)}
              disabled={page >= totalPages}
            >
              Next
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  )
}
