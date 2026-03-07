"use client"

import React, { useState } from "react"
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query"
import { cardGroupsAPI } from "@/lib/api"
import type { CardGroup, PaginatedResponse } from "@/lib/types"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Skeleton } from "@/components/ui/skeleton"
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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { Plus, ArrowLeft, Trash2, Users } from "lucide-react"
import { useRouter } from "next/navigation"

function formatDate(dateStr: string) {
  if (!dateStr) return "-"
  return new Date(dateStr).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  })
}

function CreateGroupDialog({ onCreated }: { onCreated: () => void }) {
  const [open, setOpen] = useState(false)
  const [name, setName] = useState("")
  const [description, setDescription] = useState("")
  const queryClient = useQueryClient()

  const createGroup = useMutation({
    mutationFn: () => cardGroupsAPI.create({ name, description }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["card-groups"] })
      setOpen(false)
      setName("")
      setDescription("")
      onCreated()
    },
  })

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          New Group
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Create Card Group</DialogTitle>
          <DialogDescription>
            Create a new group to organize SIM cards.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="group-name">Name</Label>
            <Input
              id="group-name"
              placeholder="e.g., Production Cards - Region A"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="group-description">Description</Label>
            <Input
              id="group-description"
              placeholder="Optional description..."
              value={description}
              onChange={(e) => setDescription(e.target.value)}
            />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button
            onClick={() => createGroup.mutate()}
            disabled={!name.trim() || createGroup.isPending}
          >
            {createGroup.isPending ? "Creating..." : "Create Group"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function GroupDetailPanel({
  group,
  onClose,
}: {
  group: CardGroup
  onClose: () => void
}) {
  const queryClient = useQueryClient()

  const { data: groupDetail, isLoading } = useQuery<CardGroup & { members?: any[] }>({
    queryKey: ["card-group", group.id],
    queryFn: () => cardGroupsAPI.get(group.id),
  })

  const deleteGroup = useMutation({
    mutationFn: () => cardGroupsAPI.delete(group.id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["card-groups"] })
      onClose()
    },
  })

  const [confirmDelete, setConfirmDelete] = useState(false)

  const members = (groupDetail as any)?.members ?? []

  return (
    <CardUI>
      <CardHeader className="flex flex-row items-center justify-between">
        <div>
          <CardTitle className="text-base">{group.name}</CardTitle>
          {group.description && (
            <p className="text-sm text-muted-foreground mt-1">
              {group.description}
            </p>
          )}
        </div>
        <div className="flex items-center gap-2">
          {confirmDelete ? (
            <>
              <span className="text-sm text-destructive">Are you sure?</span>
              <Button
                variant="destructive"
                size="sm"
                onClick={() => deleteGroup.mutate()}
                disabled={deleteGroup.isPending}
              >
                {deleteGroup.isPending ? "Deleting..." : "Yes, Delete"}
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setConfirmDelete(false)}
              >
                Cancel
              </Button>
            </>
          ) : (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setConfirmDelete(true)}
            >
              <Trash2 className="h-4 w-4 text-destructive" />
            </Button>
          )}
        </div>
      </CardHeader>
      <CardContent>
        <div className="flex items-center gap-4 text-sm mb-4">
          <span className="text-muted-foreground">
            Members: <strong>{group.card_count}</strong>
          </span>
          {group.is_dynamic && (
            <Badge variant="secondary">Dynamic</Badge>
          )}
          <span className="text-muted-foreground">
            Created: {formatDate(group.created_at)}
          </span>
        </div>
        {isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 3 }).map((_, i) => (
              <Skeleton key={i} className="h-8 w-full" />
            ))}
          </div>
        ) : members.length === 0 ? (
          <p className="text-sm text-muted-foreground py-4">
            No members in this group.
          </p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>ICCID</TableHead>
                <TableHead>IMSI</TableHead>
                <TableHead>MSISDN</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {members.map(
                (member: {
                  id: string
                  iccid: string
                  imsi: string
                  msisdn: string
                  status: string
                }) => (
                  <TableRow key={member.id}>
                    <TableCell className="font-mono text-sm">
                      {member.iccid}
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {member.imsi}
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {member.msisdn}
                    </TableCell>
                    <TableCell>
                      <Badge
                        variant={
                          member.status === "active"
                            ? "outline"
                            : member.status === "blocked"
                            ? "destructive"
                            : "secondary"
                        }
                        className={
                          member.status === "active"
                            ? "border-green-600 text-green-600"
                            : undefined
                        }
                      >
                        {member.status}
                      </Badge>
                    </TableCell>
                  </TableRow>
                )
              )}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </CardUI>
  )
}

export default function CardGroupsPage() {
  const router = useRouter()
  const [selectedGroup, setSelectedGroup] = useState<CardGroup | null>(null)

  const { data, isLoading } = useQuery<PaginatedResponse<CardGroup>>({
    queryKey: ["card-groups"],
    queryFn: () => cardGroupsAPI.list(),
  })

  const groups = data?.data ?? []

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => router.push("/cards")}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Cards
        </Button>
      </div>

      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Card Groups</h1>
          <p className="text-muted-foreground mt-1">
            Organize SIM cards into groups for campaign targeting
          </p>
        </div>
        <CreateGroupDialog onCreated={() => {}} />
      </div>

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Description</TableHead>
              <TableHead className="text-right">Members</TableHead>
              <TableHead>Created</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <TableRow key={i}>
                  {Array.from({ length: 4 }).map((_, j) => (
                    <TableCell key={j}>
                      <Skeleton className="h-4 w-full" />
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : groups.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={4}
                  className="h-24 text-center text-muted-foreground"
                >
                  No card groups found. Create one to get started.
                </TableCell>
              </TableRow>
            ) : (
              groups.map((group) => (
                <TableRow
                  key={group.id}
                  className="cursor-pointer"
                  onClick={() => setSelectedGroup(group)}
                  data-state={
                    selectedGroup?.id === group.id ? "selected" : undefined
                  }
                >
                  <TableCell className="font-medium">
                    <div className="flex items-center gap-2">
                      <Users className="h-4 w-4 text-muted-foreground" />
                      {group.name}
                    </div>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {group.description || "-"}
                  </TableCell>
                  <TableCell className="text-right">
                    {group.card_count}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {formatDate(group.created_at)}
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {selectedGroup && (
        <GroupDetailPanel
          group={selectedGroup}
          onClose={() => setSelectedGroup(null)}
        />
      )}
    </div>
  )
}
