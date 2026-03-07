"use client"

import React, { useState } from "react"
import { useParams, useRouter } from "next/navigation"
import Link from "next/link"
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query"
import { profilesAPI, cardsAPI } from "@/lib/api"
import type { Profile, Application, Card, PaginatedResponse } from "@/lib/types"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Checkbox } from "@/components/ui/checkbox"
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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  ArrowLeft,
  Edit,
  Plus,
  Trash2,
  ChevronLeft,
  ChevronRight,
} from "lucide-react"

function formatDate(dateStr: string) {
  if (!dateStr) return "-"
  return new Date(dateStr).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  })
}

const ALGORITHMS = ["DES_CBC", "DES_ECB", "AES_CBC", "TRIPLE_DES_CBC_2_KEYS", "TRIPLE_DES_CBC_3_KEYS"]
const CERT_MODES = ["NO_SECURITY", "RC", "CC"]
const COUNTER_MODES = ["NO_COUNTER", "COUNTER_AVAILABLE", "COUNTER_MUST_BE_HIGHER", "COUNTER_ONE_HIGHER"]
const POR_MODES = ["NO_REPLY", "REPLY_REQUIRED", "REPLY_ALWAYS"]
const POR_PROTOCOLS = ["SMS_DELIVER_REPORT", "SMS_SUBMIT"]
const POR_CERT_MODES = ["NO_SECURITY", "RC", "CC"]

interface ApplicationFormData {
  name: string
  tar: string
  kic_algorithm: string
  kic_mode: string
  kic_keyset_id: string
  kid_algorithm: string
  kid_mode: string
  kid_keyset_id: string
  certification_mode: string
  ciphered: boolean
  counter_mode: string
  por_mode: string
  por_protocol: string
  por_ciphered: boolean
  por_cert_mode: string
}

const defaultFormData: ApplicationFormData = {
  name: "",
  tar: "",
  kic_algorithm: "DES_CBC",
  kic_mode: "",
  kic_keyset_id: "1",
  kid_algorithm: "DES_CBC",
  kid_mode: "",
  kid_keyset_id: "1",
  certification_mode: "NO_SECURITY",
  ciphered: false,
  counter_mode: "NO_COUNTER",
  por_mode: "NO_REPLY",
  por_protocol: "SMS_DELIVER_REPORT",
  por_ciphered: false,
  por_cert_mode: "NO_SECURITY",
}

function ApplicationDialog({
  profileId,
  existingApp,
  onClose,
}: {
  profileId: string
  existingApp?: Application | null
  onClose: () => void
}) {
  const queryClient = useQueryClient()
  const isEditing = !!existingApp

  const [form, setForm] = useState<ApplicationFormData>(() => {
    if (existingApp) {
      return {
        name: existingApp.name || "",
        tar: (existingApp as any).tar || "",
        kic_algorithm: (existingApp as any).kic_algorithm || "DES_CBC",
        kic_mode: (existingApp as any).kic_mode || "",
        kic_keyset_id: String((existingApp as any).kic_keyset_id ?? "1"),
        kid_algorithm: (existingApp as any).kid_algorithm || "DES_CBC",
        kid_mode: (existingApp as any).kid_mode || "",
        kid_keyset_id: String((existingApp as any).kid_keyset_id ?? "1"),
        certification_mode: (existingApp as any).certification_mode || "NO_SECURITY",
        ciphered: (existingApp as any).ciphered ?? false,
        counter_mode: (existingApp as any).counter_mode || "NO_COUNTER",
        por_mode: (existingApp as any).por_mode || "NO_REPLY",
        por_protocol: (existingApp as any).por_protocol || "SMS_DELIVER_REPORT",
        por_ciphered: (existingApp as any).por_ciphered ?? false,
        por_cert_mode: (existingApp as any).por_cert_mode || "NO_SECURITY",
      }
    }
    return { ...defaultFormData }
  })

  const updateField = (field: keyof ApplicationFormData, value: any) => {
    setForm((prev) => ({ ...prev, [field]: value }))
  }

  const saveMutation = useMutation({
    mutationFn: () => {
      const payload = {
        ...form,
        kic_keyset_id: parseInt(form.kic_keyset_id, 10),
        kid_keyset_id: parseInt(form.kid_keyset_id, 10),
      }
      if (isEditing) {
        return profilesAPI.updateApplication(profileId, existingApp!.id, payload)
      }
      return profilesAPI.createApplication(profileId, payload)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["profile", profileId] })
      onClose()
    },
  })

  return (
    <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
      <DialogHeader>
        <DialogTitle>
          {isEditing ? "Edit Application" : "Add Application"}
        </DialogTitle>
        <DialogDescription>
          Configure application security parameters for this profile.
        </DialogDescription>
      </DialogHeader>
      <div className="grid gap-4 py-4">
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <Label htmlFor="app-name">Name</Label>
            <Input
              id="app-name"
              value={form.name}
              onChange={(e) => updateField("name", e.target.value)}
              placeholder="e.g., RFM Application"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="app-tar">TAR (hex)</Label>
            <Input
              id="app-tar"
              value={form.tar}
              onChange={(e) => updateField("tar", e.target.value)}
              placeholder="e.g., B00010"
              className="font-mono"
            />
          </div>
        </div>

        <Separator />
        <p className="text-sm font-medium">KIc Configuration</p>
        <div className="grid grid-cols-3 gap-4">
          <div className="space-y-2">
            <Label>Algorithm</Label>
            <Select
              value={form.kic_algorithm}
              onValueChange={(v) => updateField("kic_algorithm", v)}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {ALGORITHMS.map((a) => (
                  <SelectItem key={a} value={a}>
                    {a}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>Mode</Label>
            <Input
              value={form.kic_mode}
              onChange={(e) => updateField("kic_mode", e.target.value)}
              placeholder="Mode"
            />
          </div>
          <div className="space-y-2">
            <Label>Keyset ID</Label>
            <Input
              value={form.kic_keyset_id}
              onChange={(e) => updateField("kic_keyset_id", e.target.value)}
              type="number"
              min={1}
            />
          </div>
        </div>

        <Separator />
        <p className="text-sm font-medium">KID Configuration</p>
        <div className="grid grid-cols-3 gap-4">
          <div className="space-y-2">
            <Label>Algorithm</Label>
            <Select
              value={form.kid_algorithm}
              onValueChange={(v) => updateField("kid_algorithm", v)}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {ALGORITHMS.map((a) => (
                  <SelectItem key={a} value={a}>
                    {a}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>Mode</Label>
            <Input
              value={form.kid_mode}
              onChange={(e) => updateField("kid_mode", e.target.value)}
              placeholder="Mode"
            />
          </div>
          <div className="space-y-2">
            <Label>Keyset ID</Label>
            <Input
              value={form.kid_keyset_id}
              onChange={(e) => updateField("kid_keyset_id", e.target.value)}
              type="number"
              min={1}
            />
          </div>
        </div>

        <Separator />
        <p className="text-sm font-medium">Security Settings</p>
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <Label>Certification Mode</Label>
            <Select
              value={form.certification_mode}
              onValueChange={(v) => updateField("certification_mode", v)}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {CERT_MODES.map((m) => (
                  <SelectItem key={m} value={m}>
                    {m}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>Counter Mode</Label>
            <Select
              value={form.counter_mode}
              onValueChange={(v) => updateField("counter_mode", v)}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {COUNTER_MODES.map((m) => (
                  <SelectItem key={m} value={m}>
                    {m}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Checkbox
            id="ciphered"
            checked={form.ciphered}
            onCheckedChange={(checked) =>
              updateField("ciphered", checked === true)
            }
          />
          <Label htmlFor="ciphered">Ciphered</Label>
        </div>

        <Separator />
        <p className="text-sm font-medium">Proof of Receipt (PoR)</p>
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <Label>PoR Mode</Label>
            <Select
              value={form.por_mode}
              onValueChange={(v) => updateField("por_mode", v)}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {POR_MODES.map((m) => (
                  <SelectItem key={m} value={m}>
                    {m}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>PoR Protocol</Label>
            <Select
              value={form.por_protocol}
              onValueChange={(v) => updateField("por_protocol", v)}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {POR_PROTOCOLS.map((p) => (
                  <SelectItem key={p} value={p}>
                    {p}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div className="flex items-center gap-2">
            <Checkbox
              id="por-ciphered"
              checked={form.por_ciphered}
              onCheckedChange={(checked) =>
                updateField("por_ciphered", checked === true)
              }
            />
            <Label htmlFor="por-ciphered">PoR Ciphered</Label>
          </div>
          <div className="space-y-2">
            <Label>PoR Cert Mode</Label>
            <Select
              value={form.por_cert_mode}
              onValueChange={(v) => updateField("por_cert_mode", v)}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {POR_CERT_MODES.map((m) => (
                  <SelectItem key={m} value={m}>
                    {m}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>
      <DialogFooter>
        <Button variant="outline" onClick={onClose}>
          Cancel
        </Button>
        <Button
          onClick={() => saveMutation.mutate()}
          disabled={!form.name.trim() || !form.tar.trim() || saveMutation.isPending}
        >
          {saveMutation.isPending
            ? "Saving..."
            : isEditing
            ? "Update Application"
            : "Add Application"}
        </Button>
      </DialogFooter>
    </DialogContent>
  )
}

export default function ProfileDetailPage() {
  const params = useParams()
  const router = useRouter()
  const queryClient = useQueryClient()
  const profileId = params.id as string

  const [appDialogOpen, setAppDialogOpen] = useState(false)
  const [editingApp, setEditingApp] = useState<Application | null>(null)
  const [cardsPage, setCardsPage] = useState(1)
  const cardsPageSize = 10

  const { data: profile, isLoading } = useQuery<Profile & { card_count?: number }>({
    queryKey: ["profile", profileId],
    queryFn: () => profilesAPI.get(profileId),
  })

  const { data: cardsData, isLoading: cardsLoading } = useQuery<
    PaginatedResponse<Card>
  >({
    queryKey: ["profile-cards", profileId, cardsPage],
    queryFn: () =>
      cardsAPI.list({
        profile_id: profileId,
        page: cardsPage.toString(),
        page_size: cardsPageSize.toString(),
      }),
  })

  const deleteApp = useMutation({
    mutationFn: (appId: string) =>
      profilesAPI.deleteApplication(profileId, appId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["profile", profileId] })
    },
  })

  const [confirmDeleteAppId, setConfirmDeleteAppId] = useState<string | null>(null)

  if (isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-4 w-48" />
        <Skeleton className="h-48 w-full" />
      </div>
    )
  }

  if (!profile) {
    return (
      <div className="space-y-4">
        <Button variant="ghost" onClick={() => router.back()}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <p className="text-muted-foreground">Profile not found.</p>
      </div>
    )
  }

  const applications = profile.applications ?? []
  const cards = cardsData?.data ?? []
  const cardsTotalPages = cardsData?.total_pages ?? 1
  const cardsTotal = cardsData?.total ?? 0

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => router.push("/profiles")}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Profiles
        </Button>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">{profile.name}</h1>
          {profile.description && (
            <p className="text-muted-foreground mt-1">{profile.description}</p>
          )}
        </div>
        <Button
          variant="outline"
          onClick={() => router.push(`/profiles/${profileId}/edit`)}
        >
          <Edit className="mr-2 h-4 w-4" />
          Edit Profile
        </Button>
      </div>

      <CardUI>
        <CardHeader>
          <CardTitle className="text-base">General Information</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
            <div>
              <p className="text-sm text-muted-foreground">Max Concat SMS</p>
              <p className="text-sm font-medium">
                {(profile as any).max_concat_sms ?? "-"}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Buffer Size</p>
              <p className="text-sm font-medium">
                {(profile as any).buffer_size ?? "-"}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">PID</p>
              <p className="text-sm font-mono">
                {(profile as any).pid ?? "-"}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">DCS</p>
              <p className="text-sm font-mono">
                {(profile as any).dcs ?? "-"}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">
                Security Bytes Type
              </p>
              <p className="text-sm font-medium">
                {(profile as any).security_bytes_type ? (
                  <Badge variant="secondary">
                    {(profile as any).security_bytes_type}
                  </Badge>
                ) : (
                  "-"
                )}
              </p>
            </div>
          </div>
        </CardContent>
      </CardUI>

      <CardUI>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-base">Applications</CardTitle>
          <Dialog
            open={appDialogOpen}
            onOpenChange={(open) => {
              setAppDialogOpen(open)
              if (!open) setEditingApp(null)
            }}
          >
            <DialogTrigger asChild>
              <Button
                size="sm"
                onClick={() => {
                  setEditingApp(null)
                  setAppDialogOpen(true)
                }}
              >
                <Plus className="mr-2 h-4 w-4" />
                Add Application
              </Button>
            </DialogTrigger>
            {appDialogOpen && (
              <ApplicationDialog
                profileId={profileId}
                existingApp={editingApp}
                onClose={() => {
                  setAppDialogOpen(false)
                  setEditingApp(null)
                }}
              />
            )}
          </Dialog>
        </CardHeader>
        <CardContent>
          {applications.length === 0 ? (
            <p className="text-sm text-muted-foreground py-4">
              No applications configured. Add one to define OTA security
              parameters.
            </p>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>TAR</TableHead>
                    <TableHead>KIc</TableHead>
                    <TableHead>KID</TableHead>
                    <TableHead>Cert Mode</TableHead>
                    <TableHead>Ciphered</TableHead>
                    <TableHead>Counter</TableHead>
                    <TableHead>PoR</TableHead>
                    <TableHead className="w-24">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {applications.map((app) => {
                    const appAny = app as any
                    return (
                      <TableRow key={app.id}>
                        <TableCell className="font-medium">
                          {app.name}
                        </TableCell>
                        <TableCell className="font-mono text-sm">
                          {appAny.tar || "-"}
                        </TableCell>
                        <TableCell className="text-xs">
                          {appAny.kic_algorithm || "-"}
                          {appAny.kic_keyset_id
                            ? ` / KS${appAny.kic_keyset_id}`
                            : ""}
                        </TableCell>
                        <TableCell className="text-xs">
                          {appAny.kid_algorithm || "-"}
                          {appAny.kid_keyset_id
                            ? ` / KS${appAny.kid_keyset_id}`
                            : ""}
                        </TableCell>
                        <TableCell>
                          <Badge variant="secondary" className="text-xs">
                            {appAny.certification_mode || "-"}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          {appAny.ciphered ? (
                            <Badge
                              variant="outline"
                              className="border-green-600 text-green-600 text-xs"
                            >
                              Yes
                            </Badge>
                          ) : (
                            <span className="text-muted-foreground text-xs">
                              No
                            </span>
                          )}
                        </TableCell>
                        <TableCell className="text-xs">
                          {appAny.counter_mode || "-"}
                        </TableCell>
                        <TableCell className="text-xs">
                          {appAny.por_mode || "-"}
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center gap-1">
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-8 w-8 p-0"
                              onClick={() => {
                                setEditingApp(app)
                                setAppDialogOpen(true)
                              }}
                            >
                              <Edit className="h-3.5 w-3.5" />
                            </Button>
                            {confirmDeleteAppId === app.id ? (
                              <div className="flex items-center gap-1">
                                <Button
                                  variant="destructive"
                                  size="sm"
                                  className="h-8 text-xs px-2"
                                  onClick={() => {
                                    deleteApp.mutate(app.id)
                                    setConfirmDeleteAppId(null)
                                  }}
                                  disabled={deleteApp.isPending}
                                >
                                  Confirm
                                </Button>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-8 text-xs px-2"
                                  onClick={() =>
                                    setConfirmDeleteAppId(null)
                                  }
                                >
                                  Cancel
                                </Button>
                              </div>
                            ) : (
                              <Button
                                variant="ghost"
                                size="sm"
                                className="h-8 w-8 p-0"
                                onClick={() =>
                                  setConfirmDeleteAppId(app.id)
                                }
                              >
                                <Trash2 className="h-3.5 w-3.5 text-destructive" />
                              </Button>
                            )}
                          </div>
                        </TableCell>
                      </TableRow>
                    )
                  })}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </CardUI>

      <CardUI>
        <CardHeader>
          <CardTitle className="text-base">
            Cards Using This Profile ({cardsTotal})
          </CardTitle>
        </CardHeader>
        <CardContent>
          {cardsLoading ? (
            <div className="space-y-2">
              {Array.from({ length: 5 }).map((_, i) => (
                <Skeleton key={i} className="h-8 w-full" />
              ))}
            </div>
          ) : cards.length === 0 ? (
            <p className="text-sm text-muted-foreground py-4">
              No cards are using this profile.
            </p>
          ) : (
            <>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>ICCID</TableHead>
                    <TableHead>MSISDN</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Created</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {cards.map((card) => (
                    <TableRow
                      key={card.id}
                      className="cursor-pointer"
                      onClick={() => router.push(`/cards/${card.id}`)}
                    >
                      <TableCell className="font-mono text-sm">
                        {card.iccid}
                      </TableCell>
                      <TableCell className="font-mono text-sm">
                        {card.msisdn}
                      </TableCell>
                      <TableCell>
                        <Badge
                          variant={
                            card.status === "active"
                              ? "outline"
                              : card.status === "blocked"
                              ? "destructive"
                              : "secondary"
                          }
                          className={
                            card.status === "active"
                              ? "border-green-600 text-green-600"
                              : undefined
                          }
                        >
                          {card.status}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-muted-foreground text-sm">
                        {formatDate(card.created_at)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              {cardsTotalPages > 1 && (
                <div className="flex items-center justify-between mt-4">
                  <p className="text-sm text-muted-foreground">
                    Showing{" "}
                    {(cardsPage - 1) * cardsPageSize + 1} to{" "}
                    {Math.min(cardsPage * cardsPageSize, cardsTotal)} of{" "}
                    {cardsTotal} cards
                  </p>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        setCardsPage((p) => Math.max(1, p - 1))
                      }
                      disabled={cardsPage <= 1}
                    >
                      <ChevronLeft className="h-4 w-4" />
                      Previous
                    </Button>
                    <span className="text-sm text-muted-foreground px-2">
                      Page {cardsPage} of {cardsTotalPages}
                    </span>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        setCardsPage((p) =>
                          Math.min(cardsTotalPages, p + 1)
                        )
                      }
                      disabled={cardsPage >= cardsTotalPages}
                    >
                      Next
                      <ChevronRight className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              )}
            </>
          )}
        </CardContent>
      </CardUI>
    </div>
  )
}
