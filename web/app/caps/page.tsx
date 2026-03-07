"use client"

import React, { useState, useCallback, useRef } from "react"
import { useRouter } from "next/navigation"
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query"
import { capsAPI } from "@/lib/api"
import type { CAPFile, PaginatedResponse } from "@/lib/types"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { DataTable } from "@/components/card-table/data-table"
import { Upload, Trash2, FileBox, Loader2 } from "lucide-react"
import { useToast } from "@/components/ui/use-toast"
import type { ColumnDef } from "@tanstack/react-table"

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`
}

function formatDate(dateStr: string): string {
  if (!dateStr) return "-"
  return new Date(dateStr).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  })
}

function truncateHash(hash: string): string {
  if (!hash) return "-"
  return hash.length > 16 ? `${hash.slice(0, 16)}...` : hash
}

export default function CapsPage() {
  const router = useRouter()
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [uploadOpen, setUploadOpen] = useState(false)
  const [dragOver, setDragOver] = useState(false)
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [uploading, setUploading] = useState(false)
  const [uploadProgress, setUploadProgress] = useState(0)
  const [deleteId, setDeleteId] = useState<string | null>(null)
  const [deleteOpen, setDeleteOpen] = useState(false)

  const { data, isLoading } = useQuery<PaginatedResponse<CAPFile>>({
    queryKey: ["caps"],
    queryFn: () => capsAPI.list(),
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => capsAPI.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["caps"] })
      toast({ title: "CAP file deleted successfully." })
      setDeleteOpen(false)
      setDeleteId(null)
    },
    onError: (err: Error) => {
      toast({ title: "Failed to delete CAP file", description: err.message, variant: "destructive" })
    },
  })

  const caps = data?.data ?? []

  function handleDragOver(e: React.DragEvent) {
    e.preventDefault()
    setDragOver(true)
  }

  function handleDragLeave(e: React.DragEvent) {
    e.preventDefault()
    setDragOver(false)
  }

  function handleDrop(e: React.DragEvent) {
    e.preventDefault()
    setDragOver(false)
    const file = e.dataTransfer.files?.[0]
    if (file && (file.name.endsWith(".cap") || file.name.endsWith(".CAP"))) {
      setSelectedFile(file)
    } else {
      toast({ title: "Invalid file", description: "Please select a .cap file.", variant: "destructive" })
    }
  }

  function handleFileSelect(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (file) {
      setSelectedFile(file)
    }
  }

  async function handleUpload() {
    if (!selectedFile) return
    setUploading(true)
    setUploadProgress(10)
    try {
      const formData = new FormData()
      formData.append("file", selectedFile)
      setUploadProgress(30)
      await capsAPI.upload(formData)
      setUploadProgress(100)
      queryClient.invalidateQueries({ queryKey: ["caps"] })
      toast({ title: "CAP file uploaded successfully." })
      setUploadOpen(false)
      setSelectedFile(null)
      setUploadProgress(0)
    } catch (err: any) {
      toast({ title: "Upload failed", description: err.message, variant: "destructive" })
    } finally {
      setUploading(false)
    }
  }

  function handleDeleteClick(e: React.MouseEvent, id: string) {
    e.stopPropagation()
    setDeleteId(id)
    setDeleteOpen(true)
  }

  const handleRowClick = useCallback(
    (cap: CAPFile) => {
      router.push(`/caps/${cap.id}`)
    },
    [router]
  )

  const columns: ColumnDef<CAPFile, any>[] = [
    {
      accessorKey: "filename",
      header: "Filename",
      cell: ({ row }) => (
        <div className="flex items-center gap-2">
          <FileBox className="h-4 w-4 text-muted-foreground" />
          <span className="font-medium">{row.original.filename}</span>
        </div>
      ),
    },
    {
      accessorKey: "package_aid",
      header: "AID",
      cell: ({ row }) => (
        <Badge variant="outline" className="font-mono text-xs">
          {row.original.package_aid || "-"}
        </Badge>
      ),
    },
    {
      accessorKey: "size",
      header: "File Size",
      cell: ({ row }) => (
        <span className="text-muted-foreground">{formatFileSize(row.original.size)}</span>
      ),
    },
    {
      accessorKey: "sha256",
      header: "SHA-256",
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground">
          {truncateHash(row.original.sha256)}
        </span>
      ),
    },
    {
      accessorKey: "uploaded_at",
      header: "Upload Date",
      cell: ({ row }) => (
        <span className="text-muted-foreground">
          {formatDate(row.original.uploaded_at || row.original.created_at)}
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
          <h1 className="text-3xl font-bold tracking-tight">CAP File Library</h1>
          <p className="text-muted-foreground mt-1">
            Manage Java Card CAP files for applet deployment
          </p>
        </div>
        <Dialog open={uploadOpen} onOpenChange={(open) => { setUploadOpen(open); if (!open) { setSelectedFile(null); setUploadProgress(0) } }}>
          <DialogTrigger asChild>
            <Button>
              <Upload className="mr-2 h-4 w-4" />
              Upload CAP
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Upload CAP File</DialogTitle>
              <DialogDescription>
                Drag and drop a .cap file or click to browse.
              </DialogDescription>
            </DialogHeader>
            <div
              className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors cursor-pointer ${
                dragOver
                  ? "border-primary bg-primary/5"
                  : "border-muted-foreground/25 hover:border-primary/50"
              }`}
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onDrop={handleDrop}
              onClick={() => fileInputRef.current?.click()}
            >
              <input
                ref={fileInputRef}
                type="file"
                accept=".cap,.CAP"
                className="hidden"
                onChange={handleFileSelect}
              />
              <FileBox className="mx-auto h-10 w-10 text-muted-foreground mb-3" />
              {selectedFile ? (
                <div>
                  <p className="font-medium">{selectedFile.name}</p>
                  <p className="text-sm text-muted-foreground mt-1">
                    {formatFileSize(selectedFile.size)}
                  </p>
                </div>
              ) : (
                <div>
                  <p className="text-sm text-muted-foreground">
                    Drop .cap file here or click to browse
                  </p>
                </div>
              )}
            </div>
            {uploading && (
              <Progress value={uploadProgress} className="mt-2" />
            )}
            <DialogFooter>
              <Button
                variant="outline"
                onClick={() => { setUploadOpen(false); setSelectedFile(null) }}
                disabled={uploading}
              >
                Cancel
              </Button>
              <Button
                onClick={handleUpload}
                disabled={!selectedFile || uploading}
              >
                {uploading ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Uploading...
                  </>
                ) : (
                  "Upload"
                )}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      <DataTable
        columns={columns}
        data={caps}
        isLoading={isLoading}
        onRowClick={handleRowClick}
        emptyMessage="No CAP files uploaded yet."
      />

      <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete CAP File</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this CAP file? This action cannot be undone.
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
