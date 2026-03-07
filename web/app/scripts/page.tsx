"use client"

import React, { useCallback, useState } from "react"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query"
import { scriptsAPI } from "@/lib/api"
import type { Script, PaginatedResponse } from "@/lib/types"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { DataTable } from "@/components/card-table/data-table"
import { Plus, Trash2, FileCode, Loader2 } from "lucide-react"
import { useToast } from "@/components/ui/use-toast"
import type { ColumnDef } from "@tanstack/react-table"

function formatDate(dateStr: string): string {
  if (!dateStr) return "-"
  return new Date(dateStr).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  })
}

function truncateText(text: string, maxLen: number): string {
  if (!text) return "-"
  return text.length > maxLen ? `${text.slice(0, maxLen)}...` : text
}

function countCommands(content: string): number {
  if (!content) return 0
  return content
    .split("\n")
    .filter((line) => {
      const trimmed = line.trim()
      return trimmed.length > 0 && !trimmed.startsWith("//") && !trimmed.startsWith("#")
    }).length
}

export default function ScriptsPage() {
  const router = useRouter()
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [deleteId, setDeleteId] = useState<string | null>(null)
  const [deleteOpen, setDeleteOpen] = useState(false)

  const { data, isLoading } = useQuery<PaginatedResponse<Script>>({
    queryKey: ["scripts"],
    queryFn: () => scriptsAPI.list(),
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => scriptsAPI.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["scripts"] })
      toast({ title: "Script deleted successfully." })
      setDeleteOpen(false)
      setDeleteId(null)
    },
    onError: (err: Error) => {
      toast({ title: "Failed to delete script", description: err.message, variant: "destructive" })
    },
  })

  const scripts = data?.data ?? []

  function handleDeleteClick(e: React.MouseEvent, id: string) {
    e.stopPropagation()
    setDeleteId(id)
    setDeleteOpen(true)
  }

  const handleRowClick = useCallback(
    (script: Script) => {
      router.push(`/scripts/${script.id}`)
    },
    [router]
  )

  const columns: ColumnDef<Script, any>[] = [
    {
      accessorKey: "name",
      header: "Name",
      cell: ({ row }) => (
        <div className="flex items-center gap-2">
          <FileCode className="h-4 w-4 text-muted-foreground" />
          <span className="font-medium">{row.original.name}</span>
        </div>
      ),
    },
    {
      accessorKey: "description",
      header: "Description",
      cell: ({ row }) => (
        <span className="text-muted-foreground text-sm">
          {truncateText(row.original.description, 60)}
        </span>
      ),
    },
    {
      id: "tar",
      header: "Target TAR",
      cell: ({ row }) => {
        const tarParam = row.original.parameters?.find((p) => p.name === "tar")
        return tarParam ? (
          <Badge variant="outline" className="font-mono text-xs">
            {tarParam.default_value}
          </Badge>
        ) : (
          <span className="text-muted-foreground">-</span>
        )
      },
    },
    {
      id: "command_count",
      header: "Commands",
      cell: ({ row }) => (
        <Badge variant="secondary">
          {countCommands(row.original.content)}
        </Badge>
      ),
    },
    {
      accessorKey: "updated_at",
      header: "Modified",
      cell: ({ row }) => (
        <span className="text-muted-foreground">
          {formatDate(row.original.updated_at || row.original.created_at)}
        </span>
      ),
    },
    {
      id: "actions",
      header: "",
      cell: ({ row }) => (
        <Button
          variant="ghost"
          size="icon"
          className="h-8 w-8 text-muted-foreground hover:text-destructive"
          onClick={(e) => handleDeleteClick(e, row.original.id)}
        >
          <Trash2 className="h-4 w-4" />
        </Button>
      ),
    },
  ]

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Script Library</h1>
          <p className="text-muted-foreground mt-1">
            Manage APDU command scripts for SIM card operations
          </p>
        </div>
        <Link href="/scripts/new">
          <Button>
            <Plus className="mr-2 h-4 w-4" />
            New Script
          </Button>
        </Link>
      </div>

      <DataTable
        columns={columns}
        data={scripts}
        isLoading={isLoading}
        onRowClick={handleRowClick}
        emptyMessage="No scripts created yet."
      />

      <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Script</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this script? This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={() => deleteId && deleteMutation.mutate(deleteId)}
              disabled={deleteMutation.isPending}
            >
              {deleteMutation.isPending ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Deleting...
                </>
              ) : (
                "Delete"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
