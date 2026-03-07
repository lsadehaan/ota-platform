"use client"

import React, { useState } from "react"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { useQuery } from "@tanstack/react-query"
import { campaignsAPI } from "@/lib/api"
import type { Campaign, CampaignStatus, PaginatedResponse } from "@/lib/types"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Skeleton } from "@/components/ui/skeleton"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Plus, Search, ChevronLeft, ChevronRight } from "lucide-react"

const STATUS_TABS = [
  { value: "all", label: "All" },
  { value: "running", label: "Running" },
  { value: "scheduled", label: "Pending" },
  { value: "completed", label: "Completed" },
  { value: "failed", label: "Failed" },
] as const

function getStatusBadge(status: CampaignStatus) {
  switch (status) {
    case "draft":
    case "scheduled":
      return <Badge variant="secondary">{status}</Badge>
    case "running":
      return <Badge className="bg-blue-600 text-white hover:bg-blue-600/80 border-transparent">{status}</Badge>
    case "completed":
      return <Badge variant="outline" className="border-green-600 text-green-600">{status}</Badge>
    case "failed":
      return <Badge variant="destructive">{status}</Badge>
    case "paused":
      return <Badge variant="outline" className="border-yellow-500 text-yellow-600">{status}</Badge>
    case "aborted":
      return <Badge variant="destructive">{status}</Badge>
    default:
      return <Badge variant="secondary">{status}</Badge>
  }
}

function formatCampaignType(type: string) {
  return type
    .split("_")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ")
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

export default function CampaignsPage() {
  const router = useRouter()
  const [statusFilter, setStatusFilter] = useState("all")
  const [search, setSearch] = useState("")
  const [page, setPage] = useState(1)
  const pageSize = 20

  const params: Record<string, string> = {
    page: page.toString(),
    page_size: pageSize.toString(),
  }
  if (statusFilter !== "all") {
    params.status = statusFilter
  }
  if (search.trim()) {
    params.search = search.trim()
  }

  const { data, isLoading } = useQuery<PaginatedResponse<Campaign>>({
    queryKey: ["campaigns", statusFilter, search, page],
    queryFn: () => campaignsAPI.list(params),
  })

  const campaigns = data?.data ?? []
  const totalPages = data?.total_pages ?? 1
  const total = data?.total ?? 0

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Campaigns</h1>
          <p className="text-muted-foreground mt-1">
            Manage OTA campaign deployments
          </p>
        </div>
        <Link href="/campaigns/new">
          <Button>
            <Plus className="mr-2 h-4 w-4" />
            New Campaign
          </Button>
        </Link>
      </div>

      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <Tabs
          value={statusFilter}
          onValueChange={(value) => {
            setStatusFilter(value)
            setPage(1)
          }}
        >
          <TabsList>
            {STATUS_TABS.map((tab) => (
              <TabsTrigger key={tab.value} value={tab.value}>
                {tab.label}
              </TabsTrigger>
            ))}
          </TabsList>
        </Tabs>

        <div className="relative w-full sm:w-72">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Search campaigns..."
            value={search}
            onChange={(e) => {
              setSearch(e.target.value)
              setPage(1)
            }}
            className="pl-9"
          />
        </div>
      </div>

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Type</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Progress</TableHead>
              <TableHead className="text-right">Cards</TableHead>
              <TableHead className="text-right">Success</TableHead>
              <TableHead className="text-right">Failed</TableHead>
              <TableHead>Created</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <TableRow key={i}>
                  {Array.from({ length: 8 }).map((_, j) => (
                    <TableCell key={j}>
                      <Skeleton className="h-4 w-full" />
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : campaigns.length === 0 ? (
              <TableRow>
                <TableCell colSpan={8} className="h-24 text-center text-muted-foreground">
                  No campaigns found.
                </TableCell>
              </TableRow>
            ) : (
              campaigns.map((campaign) => (
                <TableRow
                  key={campaign.id}
                  className="cursor-pointer"
                  onClick={() => router.push(`/campaigns/${campaign.id}`)}
                >
                  <TableCell className="font-medium">{campaign.name}</TableCell>
                  <TableCell>{formatCampaignType(campaign.type)}</TableCell>
                  <TableCell>{getStatusBadge(campaign.status)}</TableCell>
                  <TableCell className="min-w-[120px]">
                    <div className="flex items-center gap-2">
                      <Progress value={campaign.progress_percent} className="h-2 flex-1" />
                      <span className="text-xs text-muted-foreground w-10 text-right">
                        {campaign.progress_percent}%
                      </span>
                    </div>
                  </TableCell>
                  <TableCell className="text-right">{campaign.total_cards}</TableCell>
                  <TableCell className="text-right text-green-600">
                    {campaign.success_cards}
                  </TableCell>
                  <TableCell className="text-right text-red-600">
                    {campaign.failed_cards}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {formatDate(campaign.created_at)}
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            Showing {(page - 1) * pageSize + 1} to{" "}
            {Math.min(page * pageSize, total)} of {total} campaigns
          </p>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPage((p) => Math.max(1, p - 1))}
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
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
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
