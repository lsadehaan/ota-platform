"use client"

import React, { useState } from "react"
import { Button } from "@/components/ui/button"
import { Check, Copy } from "lucide-react"

interface HexViewerProps {
  data: string
  maxPreviewLength?: number
}

export function HexViewer({ data, maxPreviewLength = 64 }: HexViewerProps) {
  const [expanded, setExpanded] = useState(false)
  const [copied, setCopied] = useState(false)

  const normalized = data.replace(/\s/g, "").toUpperCase()
  const needsTruncation = normalized.length > maxPreviewLength
  const displayValue = expanded || !needsTruncation
    ? normalized
    : normalized.slice(0, maxPreviewLength)

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(normalized)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      // fallback
      const textarea = document.createElement("textarea")
      textarea.value = normalized
      document.body.appendChild(textarea)
      textarea.select()
      document.execCommand("copy")
      document.body.removeChild(textarea)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }

  if (!data) {
    return <span className="text-muted-foreground text-sm">No data</span>
  }

  return (
    <div className="space-y-1">
      <div className="flex items-start gap-2">
        <code className="font-mono text-xs break-all bg-muted px-2 py-1 rounded flex-1">
          {displayValue}
          {needsTruncation && !expanded && "..."}
        </code>
        <Button
          variant="ghost"
          size="icon"
          className="h-7 w-7 shrink-0"
          onClick={handleCopy}
        >
          {copied ? (
            <Check className="h-3.5 w-3.5 text-green-600" />
          ) : (
            <Copy className="h-3.5 w-3.5" />
          )}
        </Button>
      </div>
      {needsTruncation && (
        <button
          onClick={() => setExpanded(!expanded)}
          className="text-xs text-primary hover:underline"
        >
          {expanded ? "Show less" : `Show more (${normalized.length} chars)`}
        </button>
      )}
    </div>
  )
}
