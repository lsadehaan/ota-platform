"use client"

import React, { useState, useRef } from "react"
import { useRouter } from "next/navigation"
import { useQuery } from "@tanstack/react-query"
import { useForm, Controller } from "react-hook-form"
import { zodResolver } from "@hookform/resolvers/zod"
import { z } from "zod"
import {
  campaignsAPI,
  profilesAPI,
  cardGroupsAPI,
  capsAPI,
  scriptsAPI,
} from "@/lib/api"
import type {
  Profile,
  CardGroup,
  CAPFile,
  Script,
  PaginatedResponse,
} from "@/lib/types"
import { StepIndicator } from "@/components/campaign-wizard/step-indicator"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Checkbox } from "@/components/ui/checkbox"
import { Separator } from "@/components/ui/separator"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  ArrowLeft,
  ArrowRight,
  Upload,
  Rocket,
  Save,
  Loader2,
  Package,
  Download,
  Trash2,
  FileCode,
} from "lucide-react"

const campaignSchema = z.object({
  name: z.string().min(1, "Campaign name is required").max(200),
  type: z.enum(["install_applet", "delete_applet", "send_script", "custom_apdu"], {
    required_error: "Select a campaign type",
  }),
  target_method: z.enum(["profile", "card_group", "csv", "manual"]),
  profile_id: z.string().optional(),
  card_group_id: z.string().optional(),
  csv_card_ids: z.array(z.string()).optional(),
  cap_file_id: z.string().optional(),
  max_block_size: z.number().min(1).max(255).optional(),
  load_file_aid: z.string().optional(),
  module_aid: z.string().optional(),
  app_aid: z.string().optional(),
  install_privileges: z.string().optional(),
  stk_params: z.string().optional(),
  delete_aid: z.string().optional(),
  delete_related: z.boolean().optional(),
  script_id: z.string().optional(),
  script_hex: z.string().optional(),
  tar: z.string().optional(),
  max_retries: z.number().min(0).max(10).default(3),
  throttle_sms_per_sec: z.number().min(1).max(1000).default(10),
  max_concat_override: z.number().min(1).max(255).optional(),
  start_immediately: z.boolean().default(true),
  scheduled_at: z.string().optional(),
})

type CampaignFormValues = z.infer<typeof campaignSchema>

const CAMPAIGN_TYPES = [
  {
    value: "custom_apdu" as const,
    label: "CAP Load",
    description: "Load a CAP file onto SIM cards via OTA",
    icon: <Upload className="h-6 w-6" />,
  },
  {
    value: "install_applet" as const,
    label: "Applet Install",
    description: "Install an applet on SIM cards from a loaded package",
    icon: <Download className="h-6 w-6" />,
  },
  {
    value: "delete_applet" as const,
    label: "Applet Delete",
    description: "Delete an applet or package from SIM cards",
    icon: <Trash2 className="h-6 w-6" />,
  },
  {
    value: "send_script" as const,
    label: "Script Execution",
    description: "Execute a custom APDU script on SIM cards",
    icon: <FileCode className="h-6 w-6" />,
  },
]

const TARGET_METHODS = [
  { value: "profile", label: "By Profile" },
  { value: "card_group", label: "By Card Group" },
  { value: "csv", label: "By CSV Upload" },
  { value: "manual", label: "Manual Selection" },
]

export default function NewCampaignPage() {
  const router = useRouter()
  const [currentStep, setCurrentStep] = useState(1)
  const [completedSteps, setCompletedSteps] = useState<number[]>([])
  const [submitting, setSubmitting] = useState(false)
  const [csvFileName, setCsvFileName] = useState("")
  const fileInputRef = useRef<HTMLInputElement>(null)

  const form = useForm<CampaignFormValues>({
    resolver: zodResolver(campaignSchema),
    defaultValues: {
      name: "",
      type: undefined,
      target_method: "profile",
      profile_id: "",
      card_group_id: "",
      csv_card_ids: [],
      cap_file_id: "",
      max_block_size: 240,
      load_file_aid: "",
      module_aid: "",
      app_aid: "",
      install_privileges: "00",
      stk_params: "",
      delete_aid: "",
      delete_related: false,
      script_id: "",
      script_hex: "",
      tar: "",
      max_retries: 3,
      throttle_sms_per_sec: 10,
      max_concat_override: undefined,
      start_immediately: true,
      scheduled_at: "",
    },
  })

  const watchType = form.watch("type")
  const watchTargetMethod = form.watch("target_method")
  const watchStartImmediately = form.watch("start_immediately")
  const watchProfileId = form.watch("profile_id")
  const watchCardGroupId = form.watch("card_group_id")
  const watchCsvCardIds = form.watch("csv_card_ids")
  const watchCapFileId = form.watch("cap_file_id")
  const watchScriptId = form.watch("script_id")

  const { data: profilesData } = useQuery<PaginatedResponse<Profile>>({
    queryKey: ["profiles"],
    queryFn: () => profilesAPI.list({ page_size: "100" }),
    enabled: watchTargetMethod === "profile",
  })

  const { data: cardGroupsData } = useQuery<PaginatedResponse<CardGroup>>({
    queryKey: ["cardGroups"],
    queryFn: () => cardGroupsAPI.list(),
    enabled: watchTargetMethod === "card_group",
  })

  const { data: capsData } = useQuery<PaginatedResponse<CAPFile>>({
    queryKey: ["caps"],
    queryFn: () => capsAPI.list(),
    enabled: watchType === "custom_apdu",
  })

  const { data: scriptsData } = useQuery<PaginatedResponse<Script>>({
    queryKey: ["scripts"],
    queryFn: () => scriptsAPI.list(),
    enabled: watchType === "send_script",
  })

  const profiles = profilesData?.data ?? []
  const cardGroups = cardGroupsData?.data ?? []
  const capFiles = capsData?.data ?? []
  const scripts = scriptsData?.data ?? []

  const selectedProfile = profiles.find((p) => p.id === watchProfileId)
  const selectedGroup = cardGroups.find((g) => g.id === watchCardGroupId)
  const selectedCap = capFiles.find((c) => c.id === watchCapFileId)
  const selectedScript = scripts.find((s) => s.id === watchScriptId)

  function getTargetCount(): number {
    if (watchTargetMethod === "profile" && selectedProfile) {
      return 0 // Count unknown until server resolves
    }
    if (watchTargetMethod === "card_group" && selectedGroup) {
      return selectedGroup.card_count
    }
    if (watchTargetMethod === "csv" && watchCsvCardIds) {
      return watchCsvCardIds.length
    }
    return 0
  }

  function handleCsvUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (!file) return
    setCsvFileName(file.name)
    const reader = new FileReader()
    reader.onload = (event) => {
      const text = event.target?.result as string
      const lines = text.split(/\r?\n/).filter((line) => line.trim())
      const cardIds: string[] = []
      for (const line of lines) {
        const parts = line.split(",")
        const id = parts[0]?.trim()
        if (id && id !== "iccid" && id !== "id" && id !== "card_id") {
          cardIds.push(id)
        }
      }
      form.setValue("csv_card_ids", cardIds)
    }
    reader.readAsText(file)
  }

  function validateStep(step: number): boolean {
    switch (step) {
      case 1:
        return !!form.getValues("type") && !!form.getValues("name").trim()
      case 2: {
        const method = form.getValues("target_method")
        if (method === "profile") return !!form.getValues("profile_id")
        if (method === "card_group") return !!form.getValues("card_group_id")
        if (method === "csv") return (form.getValues("csv_card_ids")?.length ?? 0) > 0
        return false
      }
      case 3: {
        const type = form.getValues("type")
        if (type === "custom_apdu") return !!form.getValues("cap_file_id")
        if (type === "install_applet")
          return (
            !!form.getValues("load_file_aid") &&
            !!form.getValues("module_aid") &&
            !!form.getValues("app_aid")
          )
        if (type === "delete_applet") return !!form.getValues("delete_aid")
        if (type === "send_script")
          return !!form.getValues("script_id") || !!form.getValues("script_hex")?.trim()
        return false
      }
      case 4:
        return true
      case 5:
        return true
      default:
        return false
    }
  }

  function goNext() {
    if (!validateStep(currentStep)) return
    setCompletedSteps((prev) =>
      prev.includes(currentStep) ? prev : [...prev, currentStep]
    )
    setCurrentStep((s) => Math.min(5, s + 1))
  }

  function goBack() {
    setCurrentStep((s) => Math.max(1, s - 1))
  }

  async function handleSubmit(andStart: boolean) {
    const values = form.getValues()
    setSubmitting(true)
    try {
      const payload: Record<string, any> = {
        name: values.name,
        type: values.type,
        max_retries: values.max_retries,
        throttle_sms_per_sec: values.throttle_sms_per_sec,
      }

      if (values.max_concat_override) {
        payload.max_concat_override = values.max_concat_override
      }

      if (!values.start_immediately && values.scheduled_at) {
        payload.scheduled_at = values.scheduled_at
      }

      if (values.target_method === "profile") {
        payload.profile_id = values.profile_id
      } else if (values.target_method === "card_group") {
        payload.card_group_id = values.card_group_id
      } else if (values.target_method === "csv") {
        payload.card_ids = values.csv_card_ids
      }

      if (values.type === "custom_apdu") {
        payload.cap_file_id = values.cap_file_id
        payload.max_block_size = values.max_block_size
      } else if (values.type === "install_applet") {
        payload.load_file_aid = values.load_file_aid
        payload.module_aid = values.module_aid
        payload.app_aid = values.app_aid
        payload.install_privileges = values.install_privileges
        if (values.stk_params) payload.stk_params = values.stk_params
      } else if (values.type === "delete_applet") {
        payload.delete_aid = values.delete_aid
        payload.delete_related = values.delete_related
      } else if (values.type === "send_script") {
        if (values.script_id) payload.script_id = values.script_id
        if (values.script_hex) payload.script_hex = values.script_hex
        if (values.tar) payload.tar = values.tar
      }

      const created = await campaignsAPI.create(payload)

      if (andStart) {
        await campaignsAPI.start(created.data.id)
      }

      router.push(`/campaigns/${created.data.id}`)
    } catch (err) {
      console.error("Failed to create campaign:", err)
    } finally {
      setSubmitting(false)
    }
  }

  function getCampaignTypeLabel(type: string) {
    return CAMPAIGN_TYPES.find((t) => t.value === type)?.label ?? type
  }

  function getTargetMethodLabel(method: string) {
    return TARGET_METHODS.find((m) => m.value === method)?.label ?? method
  }

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div className="flex items-center gap-3">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => router.push("/campaigns")}
        >
          <ArrowLeft className="h-4 w-4 mr-1" />
          Back
        </Button>
        <div>
          <h1 className="text-3xl font-bold tracking-tight">New Campaign</h1>
          <p className="text-muted-foreground mt-1">
            Create a new OTA campaign deployment
          </p>
        </div>
      </div>

      <StepIndicator currentStep={currentStep} completedSteps={completedSteps} />

      {/* Step 1: Campaign Type */}
      {currentStep === 1 && (
        <div className="space-y-6">
          <div className="space-y-2">
            <Label htmlFor="name">Campaign Name</Label>
            <Input
              id="name"
              placeholder="Enter campaign name"
              {...form.register("name")}
            />
            {form.formState.errors.name && (
              <p className="text-sm text-destructive">
                {form.formState.errors.name.message}
              </p>
            )}
          </div>

          <div className="space-y-2">
            <Label>Campaign Type</Label>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {CAMPAIGN_TYPES.map((ct) => (
                <Card
                  key={ct.value}
                  className={`cursor-pointer transition-colors ${
                    watchType === ct.value
                      ? "border-primary ring-2 ring-primary/20"
                      : "hover:border-muted-foreground/50"
                  }`}
                  onClick={() => form.setValue("type", ct.value)}
                >
                  <CardContent className="p-4 flex items-start gap-3">
                    <div
                      className={`p-2 rounded-md ${
                        watchType === ct.value
                          ? "bg-primary/10 text-primary"
                          : "bg-muted text-muted-foreground"
                      }`}
                    >
                      {ct.icon}
                    </div>
                    <div>
                      <p className="font-medium">{ct.label}</p>
                      <p className="text-sm text-muted-foreground">
                        {ct.description}
                      </p>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Step 2: Target Selection */}
      {currentStep === 2 && (
        <div className="space-y-6">
          <div className="space-y-2">
            <Label>Selection Method</Label>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              {TARGET_METHODS.map((tm) => (
                <Card
                  key={tm.value}
                  className={`cursor-pointer transition-colors text-center ${
                    watchTargetMethod === tm.value
                      ? "border-primary ring-2 ring-primary/20"
                      : "hover:border-muted-foreground/50"
                  }`}
                  onClick={() =>
                    form.setValue("target_method", tm.value as any)
                  }
                >
                  <CardContent className="p-4">
                    <p className="text-sm font-medium">{tm.label}</p>
                  </CardContent>
                </Card>
              ))}
            </div>
          </div>

          <Separator />

          {watchTargetMethod === "profile" && (
            <div className="space-y-2">
              <Label>Select Profile</Label>
              <Controller
                control={form.control}
                name="profile_id"
                render={({ field }) => (
                  <Select value={field.value} onValueChange={field.onChange}>
                    <SelectTrigger>
                      <SelectValue placeholder="Choose a profile" />
                    </SelectTrigger>
                    <SelectContent>
                      {profiles.map((profile) => (
                        <SelectItem key={profile.id} value={profile.id}>
                          {profile.name} - {profile.card_profile_type}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                )}
              />
              {selectedProfile && (
                <p className="text-sm text-muted-foreground">
                  Profile: {selectedProfile.name} ({selectedProfile.card_profile_type})
                </p>
              )}
            </div>
          )}

          {watchTargetMethod === "card_group" && (
            <div className="space-y-2">
              <Label>Select Card Group</Label>
              <Controller
                control={form.control}
                name="card_group_id"
                render={({ field }) => (
                  <Select value={field.value} onValueChange={field.onChange}>
                    <SelectTrigger>
                      <SelectValue placeholder="Choose a card group" />
                    </SelectTrigger>
                    <SelectContent>
                      {cardGroups.map((group) => (
                        <SelectItem key={group.id} value={group.id}>
                          {group.name} ({group.card_count} cards)
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                )}
              />
              {selectedGroup && (
                <p className="text-sm text-muted-foreground">
                  {selectedGroup.card_count} cards in this group
                </p>
              )}
            </div>
          )}

          {watchTargetMethod === "csv" && (
            <div className="space-y-2">
              <Label>Upload CSV File</Label>
              <p className="text-sm text-muted-foreground">
                CSV should have ICCIDs or card IDs in the first column.
              </p>
              <input
                ref={fileInputRef}
                type="file"
                accept=".csv,.txt"
                onChange={handleCsvUpload}
                className="hidden"
              />
              <Button
                variant="outline"
                onClick={() => fileInputRef.current?.click()}
              >
                <Upload className="mr-2 h-4 w-4" />
                {csvFileName || "Select CSV file"}
              </Button>
              {watchCsvCardIds && watchCsvCardIds.length > 0 && (
                <p className="text-sm text-green-600">
                  {watchCsvCardIds.length} card IDs loaded from CSV
                </p>
              )}
            </div>
          )}

          {watchTargetMethod === "manual" && (
            <div className="space-y-2">
              <Label>Enter Card IDs</Label>
              <Textarea
                placeholder="Paste ICCIDs, one per line"
                rows={6}
                onChange={(e) => {
                  const ids = e.target.value
                    .split(/\r?\n/)
                    .map((l) => l.trim())
                    .filter(Boolean)
                  form.setValue("csv_card_ids", ids)
                }}
              />
              {watchCsvCardIds && watchCsvCardIds.length > 0 && (
                <p className="text-sm text-green-600">
                  {watchCsvCardIds.length} card IDs entered
                </p>
              )}
            </div>
          )}

          {getTargetCount() > 0 && (
            <Card>
              <CardContent className="p-4 flex items-center gap-3">
                <Package className="h-5 w-5 text-primary" />
                <span className="font-medium">
                  {getTargetCount()} cards selected
                </span>
              </CardContent>
            </Card>
          )}
        </div>
      )}

      {/* Step 3: Command Configuration */}
      {currentStep === 3 && (
        <div className="space-y-6">
          <h2 className="text-lg font-semibold">
            Command Configuration - {getCampaignTypeLabel(watchType)}
          </h2>

          {watchType === "custom_apdu" && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label>Select CAP File</Label>
                <Controller
                  control={form.control}
                  name="cap_file_id"
                  render={({ field }) => (
                    <Select value={field.value} onValueChange={field.onChange}>
                      <SelectTrigger>
                        <SelectValue placeholder="Choose a CAP file" />
                      </SelectTrigger>
                      <SelectContent>
                        {capFiles.map((cap) => (
                          <SelectItem key={cap.id} value={cap.id}>
                            {cap.name} (v{cap.version}) - {cap.applet_aid}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  )}
                />
              </div>

              {selectedCap && (
                <Card>
                  <CardContent className="p-4 space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium">{selectedCap.name}</span>
                      <Badge variant="secondary">v{selectedCap.version}</Badge>
                    </div>
                    <div className="grid grid-cols-2 gap-2 text-sm">
                      <div>
                        <span className="text-muted-foreground">Package AID: </span>
                        <code className="text-xs">{selectedCap.package_aid}</code>
                      </div>
                      <div>
                        <span className="text-muted-foreground">Applet AID: </span>
                        <code className="text-xs">{selectedCap.applet_aid}</code>
                      </div>
                      <div>
                        <span className="text-muted-foreground">Size: </span>
                        {selectedCap.size} bytes
                      </div>
                      <div>
                        <span className="text-muted-foreground">Components: </span>
                        {selectedCap.component_count}
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}

              <div className="space-y-2">
                <Label htmlFor="max_block_size">Max Block Size (bytes)</Label>
                <Input
                  id="max_block_size"
                  type="number"
                  min={1}
                  max={255}
                  {...form.register("max_block_size", { valueAsNumber: true })}
                />
                <p className="text-xs text-muted-foreground">
                  Maximum APDU block size for LOAD commands (default: 240)
                </p>
              </div>
            </div>
          )}

          {watchType === "install_applet" && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="load_file_aid">Load File AID (Package AID)</Label>
                <Input
                  id="load_file_aid"
                  placeholder="e.g. A0000000031010"
                  {...form.register("load_file_aid")}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="module_aid">Module AID (Applet Class AID)</Label>
                <Input
                  id="module_aid"
                  placeholder="e.g. A000000003101001"
                  {...form.register("module_aid")}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="app_aid">Application AID (Instance AID)</Label>
                <Input
                  id="app_aid"
                  placeholder="e.g. A000000003101001"
                  {...form.register("app_aid")}
                />
              </div>

              <div className="space-y-2">
                <Label>Install Privileges</Label>
                <Controller
                  control={form.control}
                  name="install_privileges"
                  render={({ field }) => (
                    <Select value={field.value} onValueChange={field.onChange}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select privilege" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="00">None (00)</SelectItem>
                        <SelectItem value="04">
                          Security Domain (04)
                        </SelectItem>
                        <SelectItem value="02">
                          DAP Verification (02)
                        </SelectItem>
                        <SelectItem value="C0">
                          Delegated Management (C0)
                        </SelectItem>
                        <SelectItem value="80">
                          Card Lock (80)
                        </SelectItem>
                      </SelectContent>
                    </Select>
                  )}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="stk_params">
                  STK Parameters (optional, hex)
                </Label>
                <Input
                  id="stk_params"
                  placeholder="e.g. 00000000"
                  {...form.register("stk_params")}
                />
                <p className="text-xs text-muted-foreground">
                  SIM Toolkit access parameters in hex, leave empty if not needed
                </p>
              </div>
            </div>
          )}

          {watchType === "delete_applet" && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="delete_aid">AID to Delete</Label>
                <Input
                  id="delete_aid"
                  placeholder="e.g. A0000000031010"
                  {...form.register("delete_aid")}
                />
                <p className="text-xs text-muted-foreground">
                  The AID of the applet or package to delete
                </p>
              </div>

              <div className="flex items-center space-x-2">
                <Controller
                  control={form.control}
                  name="delete_related"
                  render={({ field }) => (
                    <Checkbox
                      id="delete_related"
                      checked={field.value}
                      onCheckedChange={field.onChange}
                    />
                  )}
                />
                <Label htmlFor="delete_related" className="font-normal">
                  Delete related objects (cascade delete of package and all
                  applet instances)
                </Label>
              </div>
            </div>
          )}

          {watchType === "send_script" && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label>Script Source</Label>
                <div className="space-y-4">
                  <div className="space-y-2">
                    <Label className="text-sm text-muted-foreground">
                      Select from library
                    </Label>
                    <Controller
                      control={form.control}
                      name="script_id"
                      render={({ field }) => (
                        <Select
                          value={field.value}
                          onValueChange={(val) => {
                            field.onChange(val)
                            form.setValue("script_hex", "")
                          }}
                        >
                          <SelectTrigger>
                            <SelectValue placeholder="Choose a script" />
                          </SelectTrigger>
                          <SelectContent>
                            {scripts.map((script) => (
                              <SelectItem key={script.id} value={script.id}>
                                {script.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      )}
                    />
                    {selectedScript && (
                      <p className="text-sm text-muted-foreground">
                        {selectedScript.description}
                      </p>
                    )}
                  </div>

                  <div className="flex items-center gap-3">
                    <Separator className="flex-1" />
                    <span className="text-xs text-muted-foreground">OR</span>
                    <Separator className="flex-1" />
                  </div>

                  <div className="space-y-2">
                    <Label className="text-sm text-muted-foreground">
                      Paste hex APDU commands
                    </Label>
                    <Textarea
                      placeholder="Enter hex APDU commands, one per line"
                      rows={6}
                      {...form.register("script_hex")}
                      onChange={(e) => {
                        form.setValue("script_hex", e.target.value)
                        if (e.target.value.trim()) {
                          form.setValue("script_id", "")
                        }
                      }}
                    />
                  </div>
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="tar">TAR (Toolkit Application Reference)</Label>
                <Input
                  id="tar"
                  placeholder="e.g. B00010"
                  maxLength={6}
                  {...form.register("tar")}
                />
                <p className="text-xs text-muted-foreground">
                  6-character hex TAR value for addressing the target application
                </p>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Step 4: Execution Settings */}
      {currentStep === 4 && (
        <div className="space-y-6">
          <h2 className="text-lg font-semibold">Execution Settings</h2>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
            <div className="space-y-2">
              <Label htmlFor="max_retries">Max Retries</Label>
              <Input
                id="max_retries"
                type="number"
                min={0}
                max={10}
                {...form.register("max_retries", { valueAsNumber: true })}
              />
              <p className="text-xs text-muted-foreground">
                Number of retry attempts per card on failure (0-10)
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor="throttle_sms_per_sec">Throttle (SMS/sec)</Label>
              <Input
                id="throttle_sms_per_sec"
                type="number"
                min={1}
                max={1000}
                {...form.register("throttle_sms_per_sec", {
                  valueAsNumber: true,
                })}
              />
              <p className="text-xs text-muted-foreground">
                Maximum SMS send rate per second
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor="max_concat_override">
                Max Concatenation Override
              </Label>
              <Input
                id="max_concat_override"
                type="number"
                min={1}
                max={255}
                placeholder="Auto"
                {...form.register("max_concat_override", {
                  setValueAs: (v) => (v === "" ? undefined : Number(v)),
                })}
              />
              <p className="text-xs text-muted-foreground">
                Override maximum concatenated SMS parts (leave empty for auto)
              </p>
            </div>
          </div>

          <Separator />

          <div className="space-y-4">
            <h3 className="font-medium">Schedule</h3>

            <div className="flex items-center space-x-2">
              <Controller
                control={form.control}
                name="start_immediately"
                render={({ field }) => (
                  <Checkbox
                    id="start_immediately"
                    checked={field.value}
                    onCheckedChange={field.onChange}
                  />
                )}
              />
              <Label htmlFor="start_immediately" className="font-normal">
                Start immediately after creation
              </Label>
            </div>

            {!watchStartImmediately && (
              <div className="space-y-2">
                <Label htmlFor="scheduled_at">Scheduled Start</Label>
                <Input
                  id="scheduled_at"
                  type="datetime-local"
                  {...form.register("scheduled_at")}
                />
                <p className="text-xs text-muted-foreground">
                  Campaign will start at the specified date and time
                </p>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Step 5: Review & Launch */}
      {currentStep === 5 && (
        <div className="space-y-6">
          <h2 className="text-lg font-semibold">Review Campaign</h2>

          <div className="grid gap-4">
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base">General</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Name</span>
                  <span className="font-medium">{form.getValues("name")}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Type</span>
                  <Badge variant="secondary">
                    {getCampaignTypeLabel(form.getValues("type"))}
                  </Badge>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Target Cards</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Method</span>
                  <span className="font-medium">
                    {getTargetMethodLabel(form.getValues("target_method"))}
                  </span>
                </div>
                {watchTargetMethod === "profile" && selectedProfile && (
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Profile</span>
                    <span className="font-medium">{selectedProfile.name}</span>
                  </div>
                )}
                {watchTargetMethod === "card_group" && selectedGroup && (
                  <>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Group</span>
                      <span className="font-medium">{selectedGroup.name}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Card Count</span>
                      <span className="font-medium">
                        {selectedGroup.card_count}
                      </span>
                    </div>
                  </>
                )}
                {(watchTargetMethod === "csv" ||
                  watchTargetMethod === "manual") &&
                  watchCsvCardIds && (
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Cards</span>
                      <span className="font-medium">
                        {watchCsvCardIds.length} cards
                      </span>
                    </div>
                  )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Command Configuration</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                {watchType === "custom_apdu" && selectedCap && (
                  <>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">CAP File</span>
                      <span className="font-medium">{selectedCap.name}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">
                        Package AID
                      </span>
                      <code className="text-xs">{selectedCap.package_aid}</code>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">
                        Max Block Size
                      </span>
                      <span>{form.getValues("max_block_size")} bytes</span>
                    </div>
                  </>
                )}
                {watchType === "install_applet" && (
                  <>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">
                        Load File AID
                      </span>
                      <code className="text-xs">
                        {form.getValues("load_file_aid")}
                      </code>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Module AID</span>
                      <code className="text-xs">
                        {form.getValues("module_aid")}
                      </code>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">App AID</span>
                      <code className="text-xs">
                        {form.getValues("app_aid")}
                      </code>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Privileges</span>
                      <code className="text-xs">
                        {form.getValues("install_privileges")}
                      </code>
                    </div>
                    {form.getValues("stk_params") && (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">
                          STK Params
                        </span>
                        <code className="text-xs">
                          {form.getValues("stk_params")}
                        </code>
                      </div>
                    )}
                  </>
                )}
                {watchType === "delete_applet" && (
                  <>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Delete AID</span>
                      <code className="text-xs">
                        {form.getValues("delete_aid")}
                      </code>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">
                        Delete Related
                      </span>
                      <span>
                        {form.getValues("delete_related") ? "Yes" : "No"}
                      </span>
                    </div>
                  </>
                )}
                {watchType === "send_script" && (
                  <>
                    {selectedScript ? (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">Script</span>
                        <span className="font-medium">
                          {selectedScript.name}
                        </span>
                      </div>
                    ) : (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">Script</span>
                        <span className="font-medium">Custom hex input</span>
                      </div>
                    )}
                    {form.getValues("tar") && (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">TAR</span>
                        <code className="text-xs">{form.getValues("tar")}</code>
                      </div>
                    )}
                  </>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Execution Settings</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Max Retries</span>
                  <span>{form.getValues("max_retries")}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Throttle</span>
                  <span>{form.getValues("throttle_sms_per_sec")} SMS/sec</span>
                </div>
                {form.getValues("max_concat_override") && (
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">
                      Max Concat Override
                    </span>
                    <span>{form.getValues("max_concat_override")}</span>
                  </div>
                )}
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Schedule</span>
                  <span>
                    {form.getValues("start_immediately")
                      ? "Start immediately"
                      : form.getValues("scheduled_at")
                      ? `Scheduled: ${new Date(form.getValues("scheduled_at")!).toLocaleString()}`
                      : "Pending"}
                  </span>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      )}

      <Separator />

      {/* Navigation */}
      <div className="flex items-center justify-between pb-8">
        <Button
          variant="outline"
          onClick={goBack}
          disabled={currentStep === 1 || submitting}
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back
        </Button>

        <div className="flex items-center gap-2">
          {currentStep < 5 ? (
            <Button
              onClick={goNext}
              disabled={!validateStep(currentStep)}
            >
              Next
              <ArrowRight className="ml-2 h-4 w-4" />
            </Button>
          ) : (
            <>
              <Button
                variant="outline"
                onClick={() => handleSubmit(false)}
                disabled={submitting}
              >
                {submitting ? (
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                ) : (
                  <Save className="mr-2 h-4 w-4" />
                )}
                Create Campaign
              </Button>
              <Button
                onClick={() => handleSubmit(true)}
                disabled={submitting}
              >
                {submitting ? (
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                ) : (
                  <Rocket className="mr-2 h-4 w-4" />
                )}
                Create & Start
              </Button>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
