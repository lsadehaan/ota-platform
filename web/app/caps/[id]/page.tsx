"use client"

import React, { useState } from "react"
import { useParams, useRouter } from "next/navigation"
import { useQuery } from "@tanstack/react-query"
import { capsAPI } from "@/lib/api"
import type { CAPFile, CAPComponent } from "@/lib/types"
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
import { ArrowLeft, Copy, Check, Loader2, FileBox, Hash, HardDrive, Calendar } from "lucide-react"
import { HexViewer } from "@/components/hex-viewer/hex-viewer"

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
    hour: "2-digit",
    minute: "2-digit",
  })
}

interface APDUPreview {
  install_for_load: string
  load_blocks: string[]
  total_apdus: number
}

function CopyableAPDU({ label, value }: { label: string; value: string }) {
  const [copied, setCopied] = useState(false)

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(value)
    } catch {
      const textarea = document.createElement("textarea")
      textarea.value = value
      document.body.appendChild(textarea)
      textarea.select()
      document.execCommand("copy")
      document.body.removeChild(textarea)
    }
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium text-muted-foreground">{label}</span>
        <Button variant="ghost" size="icon" className="h-6 w-6" onClick={handleCopy}>
          {copied ? <Check className="h-3 w-3 text-green-600" /> : <Copy className="h-3 w-3" />}
        </Button>
      </div>
      <code className="block font-mono text-xs bg-muted px-3 py-2 rounded break-all whitespace-pre-wrap">
        {value}
      </code>
    </div>
  )
}

export default function CapDetailPage() {
  const params = useParams()
  const router = useRouter()
  const id = params.id as string

  const [maxBlockSize, setMaxBlockSize] = useState(216)
  const [previewRequested, setPreviewRequested] = useState(false)

  const { data: cap, isLoading } = useQuery<CAPFile>({
    queryKey: ["cap", id],
    queryFn: () => capsAPI.get(id),
  })

  const {
    data: apduPreview,
    isLoading: previewLoading,
    refetch: fetchPreview,
  } = useQuery<APDUPreview>({
    queryKey: ["cap-apdu-preview", id, maxBlockSize],
    queryFn: () => capsAPI.previewAPDUs(id, maxBlockSize),
    enabled: previewRequested,
  })

  function handleGeneratePreview() {
    setPreviewRequested(true)
    fetchPreview()
  }

  if (isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-32 w-full" />
        <Skeleton className="h-64 w-full" />
      </div>
    )
  }

  if (!cap) {
    return (
      <div className="space-y-6">
        <Button variant="ghost" onClick={() => router.push("/caps")}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back to CAP Files
        </Button>
        <p className="text-muted-foreground">CAP file not found.</p>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <Button variant="ghost" onClick={() => router.push("/caps")}>
        <ArrowLeft className="mr-2 h-4 w-4" />
        Back to CAP Files
      </Button>

      <div>
        <div className="flex items-center gap-3">
          <FileBox className="h-8 w-8 text-muted-foreground" />
          <div>
            <h1 className="text-3xl font-bold tracking-tight">{cap.filename}</h1>
            <div className="flex items-center gap-2 mt-1">
              {cap.package_aid && (
                <Badge variant="outline" className="font-mono text-xs">
                  AID: {cap.package_aid}
                </Badge>
              )}
              {cap.version && (
                <Badge variant="secondary" className="text-xs">
                  v{cap.version}
                </Badge>
              )}
            </div>
          </div>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-4">
        <CardUI>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
              <HardDrive className="h-4 w-4" />
              File Size
            </div>
            <p className="text-lg font-semibold">{formatFileSize(cap.size)}</p>
          </CardContent>
        </CardUI>
        <CardUI>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
              <Hash className="h-4 w-4" />
              Components
            </div>
            <p className="text-lg font-semibold">{cap.component_count ?? cap.components?.length ?? 0}</p>
          </CardContent>
        </CardUI>
        <CardUI>
          <CardContent className="pt-6">
            <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
              <Calendar className="h-4 w-4" />
              Uploaded
            </div>
            <p className="text-lg font-semibold">{formatDate(cap.uploaded_at || cap.created_at)}</p>
          </CardContent>
        </CardUI>
        <CardUI>
          <CardContent className="pt-6">
            <div className="text-sm text-muted-foreground mb-1">SHA-256</div>
            <p className="font-mono text-xs break-all">{cap.sha256 || "-"}</p>
          </CardContent>
        </CardUI>
      </div>

      {cap.components && cap.components.length > 0 && (
        <CardUI>
          <CardHeader>
            <CardTitle>CAP Components</CardTitle>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Component</TableHead>
                  <TableHead>Tag</TableHead>
                  <TableHead className="text-right">Size (bytes)</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {cap.components.map((comp, idx) => (
                  <TableRow key={idx}>
                    <TableCell className="font-medium">{comp.name}</TableCell>
                    <TableCell>
                      <code className="font-mono text-xs bg-muted px-1.5 py-0.5 rounded">
                        {comp.tag}
                      </code>
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {comp.size.toLocaleString()}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </CardUI>
      )}

      <CardUI>
        <CardHeader>
          <CardTitle>APDU Preview</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-end gap-4">
            <div className="space-y-2">
              <Label htmlFor="max-block-size">Max Block Size (bytes)</Label>
              <Input
                id="max-block-size"
                type="number"
                min={1}
                max={255}
                value={maxBlockSize}
                onChange={(e) => {
                  setMaxBlockSize(parseInt(e.target.value) || 216)
                  setPreviewRequested(false)
                }}
                className="w-32 font-mono"
              />
              <p className="text-xs text-muted-foreground">
                Default: 0xD8 (216)
              </p>
            </div>
            <Button onClick={handleGeneratePreview} disabled={previewLoading}>
              {previewLoading ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Generating...
                </>
              ) : (
                "Generate Preview"
              )}
            </Button>
          </div>

          {apduPreview && (
            <>
              <Separator />
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium">
                    Total APDUs: {apduPreview.total_apdus}
                  </span>
                </div>

                {apduPreview.install_for_load && (
                  <CopyableAPDU
                    label="INSTALL [for LOAD]"
                    value={apduPreview.install_for_load}
                  />
                )}

                {apduPreview.load_blocks && apduPreview.load_blocks.length > 0 && (
                  <div className="space-y-3">
                    <span className="text-sm font-medium text-muted-foreground">
                      Load Blocks ({apduPreview.load_blocks.length})
                    </span>
                    <div className="max-h-96 overflow-y-auto space-y-2">
                      {apduPreview.load_blocks.map((block, idx) => (
                        <CopyableAPDU
                          key={idx}
                          label={`Block #${idx}`}
                          value={block}
                        />
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </>
          )}
        </CardContent>
      </CardUI>
    </div>
  )
}
