"use client"

import React, { useState, useEffect, useMemo } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import {
  Card as CardUI,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Save, Loader2 } from "lucide-react"
import Editor from "@monaco-editor/react"
import type { Script } from "@/lib/types"

interface ScriptEditorProps {
  initialData?: Script
  onSave: (data: {
    name: string
    description: string
    language: string
    content: string
    parameters: Script["parameters"]
  }) => Promise<void>
}

export function ScriptEditor({ initialData, onSave }: ScriptEditorProps) {
  const [name, setName] = useState(initialData?.name ?? "")
  const [description, setDescription] = useState(initialData?.description ?? "")
  const [content, setContent] = useState(initialData?.content ?? "")
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    if (initialData) {
      setName(initialData.name)
      setDescription(initialData.description)
      setContent(initialData.content)
    }
  }, [initialData])

  const commandCount = useMemo(() => {
    if (!content.trim()) return 0
    return content
      .split("\n")
      .filter((line) => {
        const trimmed = line.trim()
        return trimmed.length > 0 && !trimmed.startsWith("//") && !trimmed.startsWith("#")
      }).length
  }, [content])

  const [prefersDark, setPrefersDark] = useState(false)

  useEffect(() => {
    const mq = window.matchMedia("(prefers-color-scheme: dark)")
    setPrefersDark(mq.matches)
    function handler(e: MediaQueryListEvent) {
      setPrefersDark(e.matches)
    }
    mq.addEventListener("change", handler)
    return () => mq.removeEventListener("change", handler)
  }, [])

  async function handleSave() {
    if (!name.trim()) return
    setSaving(true)
    try {
      await onSave({
        name: name.trim(),
        description: description.trim(),
        language: "apdu_hex",
        content,
        parameters: initialData?.parameters ?? [],
      })
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-2">
        <div className="space-y-2">
          <Label htmlFor="script-name">Name</Label>
          <Input
            id="script-name"
            placeholder="Script name"
            value={name}
            onChange={(e) => setName(e.target.value)}
          />
        </div>
        <div className="space-y-2">
          <Label htmlFor="script-tar">Target TAR (hex)</Label>
          <Input
            id="script-tar"
            placeholder="e.g. B00010"
            className="font-mono"
            value={
              initialData?.parameters?.find((p) => p.name === "tar")
                ?.default_value ?? ""
            }
            readOnly={!!initialData}
            disabled={!!initialData}
          />
        </div>
      </div>

      <div className="space-y-2">
        <Label htmlFor="script-description">Description</Label>
        <Textarea
          id="script-description"
          placeholder="Describe what this script does..."
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          rows={3}
        />
      </div>

      <CardUI>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">APDU Commands</CardTitle>
            <Badge variant="secondary">
              {commandCount} command{commandCount !== 1 ? "s" : ""}
            </Badge>
          </div>
          <p className="text-sm text-muted-foreground">
            One hex APDU command per line. Lines starting with // or # are comments.
          </p>
        </CardHeader>
        <CardContent>
          <div className="rounded-md border overflow-hidden" style={{ minHeight: 400 }}>
            <Editor
              height="400px"
              language="plaintext"
              theme={prefersDark ? "vs-dark" : "light"}
              value={content}
              onChange={(value) => setContent(value ?? "")}
              options={{
                minimap: { enabled: false },
                lineNumbers: "on",
                fontSize: 13,
                fontFamily: "monospace",
                wordWrap: "on",
                scrollBeyondLastLine: false,
                renderWhitespace: "boundary",
                padding: { top: 8, bottom: 8 },
              }}
            />
          </div>
        </CardContent>
      </CardUI>

      <div className="flex items-center justify-end gap-3">
        <Button onClick={handleSave} disabled={saving || !name.trim()}>
          {saving ? (
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          ) : (
            <Save className="mr-2 h-4 w-4" />
          )}
          {saving ? "Saving..." : "Save Script"}
        </Button>
      </div>
    </div>
  )
}
