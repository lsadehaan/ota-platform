"use client"

import React from "react"
import { useRouter } from "next/navigation"
import { useQueryClient } from "@tanstack/react-query"
import { scriptsAPI } from "@/lib/api"
import type { Script } from "@/lib/types"
import { Button } from "@/components/ui/button"
import { ArrowLeft } from "lucide-react"
import { useToast } from "@/components/ui/use-toast"
import { ScriptEditor } from "@/components/apdu-editor/script-editor"

export default function NewScriptPage() {
  const router = useRouter()
  const queryClient = useQueryClient()
  const { toast } = useToast()

  async function handleSave(data: {
    name: string
    description: string
    language: string
    content: string
    parameters: Script["parameters"]
  }) {
    try {
      await scriptsAPI.create(data)
      queryClient.invalidateQueries({ queryKey: ["scripts"] })
      toast({ title: "Script created successfully." })
      router.push("/scripts")
    } catch (err: any) {
      toast({ title: "Failed to create script", description: err.message, variant: "destructive" })
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Button variant="ghost" onClick={() => router.push("/scripts")}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <div>
          <h1 className="text-3xl font-bold tracking-tight">New Script</h1>
          <p className="text-muted-foreground mt-1">
            Create a new APDU command script
          </p>
        </div>
      </div>

      <ScriptEditor onSave={handleSave} />
    </div>
  )
}
