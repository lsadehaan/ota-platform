"use client"

import React, { useState, useEffect, useCallback } from "react"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { useQuery } from "@tanstack/react-query"
import { cardsAPI, profilesAPI } from "@/lib/api"
import type { Card, Profile, PaginatedResponse } from "@/lib/types"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { DataTable } from "@/components/card-table/data-table"
import { Upload, Users, Search } from "lucide-react"
import type { ColumnDef } from "@tanstack/react-table"

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

function formatDate(dateStr: string) {
  if (!dateStr) return "-"
  return new Date(dateStr).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  })
}

const columns: ColumnDef<Card, any>[] = [
  {
    accessorKey: "iccid",
    header: "ICCID",
    cell: ({ row }) => (
      <span className="font-mono text-sm">{row.original.iccid}</span>
    ),
  },
  {
    accessorKey: "imsi",
    header: "IMSI",
    cell: ({ row }) => (
      <span className="font-mono text-sm">{row.original.imsi}</span>
    ),
  },
  {
    accessorKey: "msisdn",
    header: "MSISDN",
    cell: ({ row }) => (
      <span className="font-mono text-sm">{row.original.msisdn}</span>
    ),
  },
  {
    accessorKey: "profile_name",
    header: "Profile",
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ row }) => getStatusBadge(row.original.status),
  },
  {
    accessorKey: "created_at",
    header: "Created",
    cell: ({ row }) => (
      <span className="text-muted-foreground">
        {formatDate(row.original.created_at)}
      </span>
    ),
  },
]

export default function CardsPage() {
  const router = useRouter()
  const [searchInput, setSearchInput] = useState("")
  const [debouncedSearch, setDebouncedSearch] = useState("")
  const [profileFilter, setProfileFilter] = useState("all")
  const [statusFilter, setStatusFilter] = useState("all")
  const [page, setPage] = useState(1)
  const pageSize = 20

  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(searchInput)
      setPage(1)
    }, 300)
    return () => clearTimeout(timer)
  }, [searchInput])

  const params: Record<string, string> = {
    page: page.toString(),
    page_size: pageSize.toString(),
  }
  if (debouncedSearch.trim()) {
    params.q = debouncedSearch.trim()
  }
  if (profileFilter !== "all") {
    params.profile_id = profileFilter
  }
  if (statusFilter !== "all") {
    params.status = statusFilter
  }

  const { data, isLoading } = useQuery<PaginatedResponse<Card>>({
    queryKey: ["cards", debouncedSearch, profileFilter, statusFilter, page],
    queryFn: () => cardsAPI.list(params),
  })

  const { data: profilesData } = useQuery<PaginatedResponse<Profile>>({
    queryKey: ["profiles-list"],
    queryFn: () => profilesAPI.list({ page_size: "100" }),
  })

  const cards = data?.data ?? []
  const total = data?.total ?? 0
  const totalPages = data?.total_pages ?? 1
  const profiles = profilesData?.data ?? []

  const handleRowClick = useCallback(
    (card: Card) => {
      router.push(`/cards/${card.id}`)
    },
    [router]
  )

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Card Inventory</h1>
          <p className="text-muted-foreground mt-1">
            Manage SIM cards and their security configurations
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Link href="/cards/groups">
            <Button variant="outline">
              <Users className="mr-2 h-4 w-4" />
              Card Groups
            </Button>
          </Link>
          <Link href="/cards/import">
            <Button>
              <Upload className="mr-2 h-4 w-4" />
              Import Cards
            </Button>
          </Link>
        </div>
      </div>

      <div className="flex flex-col gap-4 sm:flex-row sm:items-center">
        <div className="relative flex-1 sm:max-w-sm">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Search ICCID, IMSI, MSISDN..."
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            className="pl-9"
          />
        </div>
        <Select
          value={profileFilter}
          onValueChange={(value) => {
            setProfileFilter(value)
            setPage(1)
          }}
        >
          <SelectTrigger className="w-[200px]">
            <SelectValue placeholder="All Profiles" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Profiles</SelectItem>
            {profiles.map((profile) => (
              <SelectItem key={profile.id} value={profile.id}>
                {profile.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select
          value={statusFilter}
          onValueChange={(value) => {
            setStatusFilter(value)
            setPage(1)
          }}
        >
          <SelectTrigger className="w-[160px]">
            <SelectValue placeholder="All Statuses" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Statuses</SelectItem>
            <SelectItem value="active">Active</SelectItem>
            <SelectItem value="inactive">Inactive</SelectItem>
            <SelectItem value="blocked">Blocked</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <DataTable
        columns={columns}
        data={cards}
        isLoading={isLoading}
        onRowClick={handleRowClick}
        emptyMessage="No cards found."
        pagination={{
          page,
          pageSize,
          total,
          onPageChange: setPage,
        }}
      />
    </div>
  )
}
