"use client"

import React, { useState, useCallback } from "react"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { useQuery } from "@tanstack/react-query"
import { profilesAPI } from "@/lib/api"
import type { Profile, PaginatedResponse } from "@/lib/types"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { DataTable } from "@/components/card-table/data-table"
import { Plus } from "lucide-react"
import type { ColumnDef } from "@tanstack/react-table"

function formatDate(dateStr: string) {
  if (!dateStr) return "-"
  return new Date(dateStr).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  })
}

const columns: ColumnDef<Profile, any>[] = [
  {
    accessorKey: "name",
    header: "Name",
    cell: ({ row }) => (
      <span className="font-medium">{row.original.name}</span>
    ),
  },
  {
    id: "card_count",
    header: "Card Count",
    cell: ({ row }) => {
      const count = (row.original as any).card_count ?? 0
      return <span>{count}</span>
    },
  },
  {
    id: "application_count",
    header: "Applications",
    cell: ({ row }) => (
      <span>{row.original.applications?.length ?? 0}</span>
    ),
  },
  {
    id: "max_concat",
    header: "Max Concat",
    cell: ({ row }) => {
      const maxConcat = (row.original as any).max_concat_sms ?? "-"
      return <span>{maxConcat}</span>
    },
  },
  {
    id: "security_bytes_type",
    header: "Security Bytes",
    cell: ({ row }) => {
      const sbType = (row.original as any).security_bytes_type
      return sbType ? (
        <Badge variant="secondary">{sbType}</Badge>
      ) : (
        <span className="text-muted-foreground">-</span>
      )
    },
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

export default function ProfilesPage() {
  const router = useRouter()
  const [page, setPage] = useState(1)
  const pageSize = 20

  const { data, isLoading } = useQuery<PaginatedResponse<Profile>>({
    queryKey: ["profiles", page],
    queryFn: () =>
      profilesAPI.list({
        page: page.toString(),
        page_size: pageSize.toString(),
      }),
  })

  const profiles = data?.data ?? []
  const total = data?.total ?? 0

  const handleRowClick = useCallback(
    (profile: Profile) => {
      router.push(`/profiles/${profile.id}`)
    },
    [router]
  )

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Profiles</h1>
          <p className="text-muted-foreground mt-1">
            Manage OTA security profiles and application configurations
          </p>
        </div>
        <Link href="/profiles/new">
          <Button>
            <Plus className="mr-2 h-4 w-4" />
            New Profile
          </Button>
        </Link>
      </div>

      <DataTable
        columns={columns}
        data={profiles}
        isLoading={isLoading}
        onRowClick={handleRowClick}
        emptyMessage="No profiles found. Create one to get started."
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
