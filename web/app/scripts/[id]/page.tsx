"use client"

import React from "react"
import { useParams, useRouter } from "next/navigation"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { scriptsAPI } from "@/lib/api"
import type { Script } from "@/lib/types"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { ArrowLeft } from "lucide-react"
import { useToast } from "@/components/ui/use-toast"
import { ScriptEditor } from "@/components/apdu-editor/script-editor"

export default function ScriptEditPage() {
  const params = useParams()
  const router = useRouter()
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const id = params.id as string

  const { data: script, isLoading } = useQuery<Script>({
    queryKey: ["script", id],
    queryFn: () => scriptsAPI.get(id),
  })

  async function handleSave(data: {
    name: string
    description: string
    language: string
    content: string
    parameters: Script["parameters"]
  }) {
    try {
      await scriptsAPI.update(id, data)
      queryClient.invalidateQueries({ queryKey: ["script", id] })
      queryClient.invalidateQueries({ queryKey: ["scripts"] })
      toast({ title: "Script updated successfully." })
      router.push("/scripts")
    } catch (err: any) {
      toast({ title: "Failed to update script", description: err.message, variant: "destructive" })
    }
  }

  if (isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-10 w-full" />
        <Skeleton className="h-24 w-full" />
        <Skeleton className="h-96 w-full" />
      </div>
    )
  }

  if (!script) {
    return (
      <div className="space-y-6">
        <Button variant="ghost" onClick={() => router.push("/scripts")}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back to Scripts
        </Button>
        <p className="text-muted-foreground">Script not found.</p>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Button variant="ghost" onClick={() => router.push("/scripts")}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Edit Script</h1>
          <p className="text-muted-foreground mt-1">{script.name}</p>
        </div>
      </div>

      <ScriptEditor initialData={script} onSave={handleSave} />
    </div>
  )
}
