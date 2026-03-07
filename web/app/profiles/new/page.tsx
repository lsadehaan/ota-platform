"use client"

import React, { useState } from "react"
import { useRouter } from "next/navigation"
import { useMutation } from "@tanstack/react-query"
import { useForm, useFieldArray } from "react-hook-form"
import { zodResolver } from "@hookform/resolvers/zod"
import { z } from "zod"
import { profilesAPI } from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Checkbox } from "@/components/ui/checkbox"
import { Separator } from "@/components/ui/separator"
import {
  Card as CardUI,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { ArrowLeft, Plus, Trash2 } from "lucide-react"

const ALGORITHMS = [
  "DES_CBC",
  "DES_ECB",
  "AES_CBC",
  "TRIPLE_DES_CBC_2_KEYS",
  "TRIPLE_DES_CBC_3_KEYS",
]
const CERT_MODES = ["NO_SECURITY", "RC", "CC"]
const COUNTER_MODES = [
  "NO_COUNTER",
  "COUNTER_AVAILABLE",
  "COUNTER_MUST_BE_HIGHER",
  "COUNTER_ONE_HIGHER",
]
const POR_MODES = ["NO_REPLY", "REPLY_REQUIRED", "REPLY_ALWAYS"]
const POR_PROTOCOLS = ["SMS_DELIVER_REPORT", "SMS_SUBMIT"]
const SECURITY_BYTES_TYPES = ["COMPACT", "EXTENDED"]

const applicationSchema = z.object({
  name: z.string().min(1, "Name is required"),
  tar: z
    .string()
    .min(1, "TAR is required")
    .regex(/^[0-9A-Fa-f]+$/, "TAR must be hexadecimal"),
  kic_algorithm: z.string().min(1),
  kic_mode: z.string().optional(),
  kic_keyset_id: z.coerce.number().min(1),
  kid_algorithm: z.string().min(1),
  kid_mode: z.string().optional(),
  kid_keyset_id: z.coerce.number().min(1),
  certification_mode: z.string().min(1),
  ciphered: z.boolean(),
  counter_mode: z.string().min(1),
  por_mode: z.string().min(1),
  por_protocol: z.string().min(1),
  por_ciphered: z.boolean(),
  por_cert_mode: z.string().min(1),
})

const profileSchema = z.object({
  name: z.string().min(1, "Profile name is required"),
  max_concat_sms: z.coerce.number().min(1).max(255),
  buffer_size: z.coerce.number().min(1),
  pid: z.string().optional(),
  dcs: z.string().optional(),
  security_bytes_type: z.string().min(1, "Security bytes type is required"),
  applications: z.array(applicationSchema),
})

type ProfileFormValues = z.infer<typeof profileSchema>

const defaultApplication = {
  name: "",
  tar: "",
  kic_algorithm: "DES_CBC",
  kic_mode: "",
  kic_keyset_id: 1,
  kid_algorithm: "DES_CBC",
  kid_mode: "",
  kid_keyset_id: 1,
  certification_mode: "NO_SECURITY",
  ciphered: false,
  counter_mode: "NO_COUNTER",
  por_mode: "NO_REPLY",
  por_protocol: "SMS_DELIVER_REPORT",
  por_ciphered: false,
  por_cert_mode: "NO_SECURITY",
}

export default function NewProfilePage() {
  const router = useRouter()

  const {
    register,
    handleSubmit,
    control,
    formState: { errors },
    setValue,
    watch,
  } = useForm<ProfileFormValues>({
    resolver: zodResolver(profileSchema),
    defaultValues: {
      name: "",
      max_concat_sms: 3,
      buffer_size: 254,
      pid: "",
      dcs: "",
      security_bytes_type: "COMPACT",
      applications: [],
    },
  })

  const { fields, append, remove } = useFieldArray({
    control,
    name: "applications",
  })

  const createProfile = useMutation({
    mutationFn: (data: ProfileFormValues) => profilesAPI.create(data),
    onSuccess: (result: any) => {
      router.push(`/profiles/${result.id}`)
    },
  })

  const onSubmit = (data: ProfileFormValues) => {
    createProfile.mutate(data)
  }

  const watchedApps = watch("applications")

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

      <div>
        <h1 className="text-3xl font-bold tracking-tight">New Profile</h1>
        <p className="text-muted-foreground mt-1">
          Create a new OTA security profile
        </p>
      </div>

      <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
        <CardUI>
          <CardHeader>
            <CardTitle className="text-base">General Settings</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="name">Profile Name</Label>
                <Input
                  id="name"
                  {...register("name")}
                  placeholder="e.g., Production SEGAM Profile"
                />
                {errors.name && (
                  <p className="text-xs text-destructive">
                    {errors.name.message}
                  </p>
                )}
              </div>
              <div className="space-y-2">
                <Label htmlFor="security_bytes_type">Security Bytes Type</Label>
                <Select
                  value={watch("security_bytes_type")}
                  onValueChange={(v) => setValue("security_bytes_type", v)}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {SECURITY_BYTES_TYPES.map((t) => (
                      <SelectItem key={t} value={t}>
                        {t}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {errors.security_bytes_type && (
                  <p className="text-xs text-destructive">
                    {errors.security_bytes_type.message}
                  </p>
                )}
              </div>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div className="space-y-2">
                <Label htmlFor="max_concat_sms">Max Concat SMS</Label>
                <Input
                  id="max_concat_sms"
                  type="number"
                  {...register("max_concat_sms")}
                  min={1}
                  max={255}
                />
                {errors.max_concat_sms && (
                  <p className="text-xs text-destructive">
                    {errors.max_concat_sms.message}
                  </p>
                )}
              </div>
              <div className="space-y-2">
                <Label htmlFor="buffer_size">Buffer Size</Label>
                <Input
                  id="buffer_size"
                  type="number"
                  {...register("buffer_size")}
                  min={1}
                />
                {errors.buffer_size && (
                  <p className="text-xs text-destructive">
                    {errors.buffer_size.message}
                  </p>
                )}
              </div>
              <div className="space-y-2">
                <Label htmlFor="pid">PID</Label>
                <Input
                  id="pid"
                  {...register("pid")}
                  placeholder="e.g., 00"
                  className="font-mono"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="dcs">DCS</Label>
                <Input
                  id="dcs"
                  {...register("dcs")}
                  placeholder="e.g., F6"
                  className="font-mono"
                />
              </div>
            </div>
          </CardContent>
        </CardUI>

        <CardUI>
          <CardHeader className="flex flex-row items-center justify-between">
            <div>
              <CardTitle className="text-base">Applications</CardTitle>
              <CardDescription>
                Define the OTA applications and their security parameters.
              </CardDescription>
            </div>
            <Button
              type="button"
              size="sm"
              variant="outline"
              onClick={() => append({ ...defaultApplication })}
            >
              <Plus className="mr-2 h-4 w-4" />
              Add Application
            </Button>
          </CardHeader>
          <CardContent className="space-y-6">
            {fields.length === 0 && (
              <p className="text-sm text-muted-foreground py-4 text-center">
                No applications added yet. Click "Add Application" to configure
                one.
              </p>
            )}
            {fields.map((field, index) => (
              <div key={field.id} className="rounded-lg border p-4 space-y-4">
                <div className="flex items-center justify-between">
                  <h4 className="text-sm font-medium">
                    Application {index + 1}
                    {watchedApps?.[index]?.name
                      ? `: ${watchedApps[index].name}`
                      : ""}
                  </h4>
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => remove(index)}
                  >
                    <Trash2 className="h-4 w-4 text-destructive" />
                  </Button>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label>Name</Label>
                    <Input
                      {...register(`applications.${index}.name`)}
                      placeholder="Application Name"
                    />
                    {errors.applications?.[index]?.name && (
                      <p className="text-xs text-destructive">
                        {errors.applications[index]!.name!.message}
                      </p>
                    )}
                  </div>
                  <div className="space-y-2">
                    <Label>TAR (hex)</Label>
                    <Input
                      {...register(`applications.${index}.tar`)}
                      placeholder="B00010"
                      className="font-mono"
                    />
                    {errors.applications?.[index]?.tar && (
                      <p className="text-xs text-destructive">
                        {errors.applications[index]!.tar!.message}
                      </p>
                    )}
                  </div>
                </div>

                <Separator />
                <p className="text-xs font-medium text-muted-foreground">
                  KIc Configuration
                </p>
                <div className="grid grid-cols-3 gap-4">
                  <div className="space-y-2">
                    <Label className="text-xs">Algorithm</Label>
                    <Select
                      value={watchedApps?.[index]?.kic_algorithm || "DES_CBC"}
                      onValueChange={(v) =>
                        setValue(`applications.${index}.kic_algorithm`, v)
                      }
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
                    <Label className="text-xs">Mode</Label>
                    <Input
                      {...register(`applications.${index}.kic_mode`)}
                      placeholder="Mode"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label className="text-xs">Keyset ID</Label>
                    <Input
                      type="number"
                      {...register(`applications.${index}.kic_keyset_id`)}
                      min={1}
                    />
                  </div>
                </div>

                <p className="text-xs font-medium text-muted-foreground">
                  KID Configuration
                </p>
                <div className="grid grid-cols-3 gap-4">
                  <div className="space-y-2">
                    <Label className="text-xs">Algorithm</Label>
                    <Select
                      value={watchedApps?.[index]?.kid_algorithm || "DES_CBC"}
                      onValueChange={(v) =>
                        setValue(`applications.${index}.kid_algorithm`, v)
                      }
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
                    <Label className="text-xs">Mode</Label>
                    <Input
                      {...register(`applications.${index}.kid_mode`)}
                      placeholder="Mode"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label className="text-xs">Keyset ID</Label>
                    <Input
                      type="number"
                      {...register(`applications.${index}.kid_keyset_id`)}
                      min={1}
                    />
                  </div>
                </div>

                <Separator />
                <p className="text-xs font-medium text-muted-foreground">
                  Security Settings
                </p>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label className="text-xs">Certification Mode</Label>
                    <Select
                      value={
                        watchedApps?.[index]?.certification_mode || "NO_SECURITY"
                      }
                      onValueChange={(v) =>
                        setValue(
                          `applications.${index}.certification_mode`,
                          v
                        )
                      }
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
                    <Label className="text-xs">Counter Mode</Label>
                    <Select
                      value={
                        watchedApps?.[index]?.counter_mode || "NO_COUNTER"
                      }
                      onValueChange={(v) =>
                        setValue(`applications.${index}.counter_mode`, v)
                      }
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
                    id={`ciphered-${index}`}
                    checked={watchedApps?.[index]?.ciphered ?? false}
                    onCheckedChange={(checked) =>
                      setValue(
                        `applications.${index}.ciphered`,
                        checked === true
                      )
                    }
                  />
                  <Label htmlFor={`ciphered-${index}`} className="text-xs">
                    Ciphered
                  </Label>
                </div>

                <Separator />
                <p className="text-xs font-medium text-muted-foreground">
                  Proof of Receipt (PoR)
                </p>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label className="text-xs">PoR Mode</Label>
                    <Select
                      value={watchedApps?.[index]?.por_mode || "NO_REPLY"}
                      onValueChange={(v) =>
                        setValue(`applications.${index}.por_mode`, v)
                      }
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
                    <Label className="text-xs">PoR Protocol</Label>
                    <Select
                      value={
                        watchedApps?.[index]?.por_protocol ||
                        "SMS_DELIVER_REPORT"
                      }
                      onValueChange={(v) =>
                        setValue(`applications.${index}.por_protocol`, v)
                      }
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
                      id={`por-ciphered-${index}`}
                      checked={watchedApps?.[index]?.por_ciphered ?? false}
                      onCheckedChange={(checked) =>
                        setValue(
                          `applications.${index}.por_ciphered`,
                          checked === true
                        )
                      }
                    />
                    <Label
                      htmlFor={`por-ciphered-${index}`}
                      className="text-xs"
                    >
                      PoR Ciphered
                    </Label>
                  </div>
                  <div className="space-y-2">
                    <Label className="text-xs">PoR Cert Mode</Label>
                    <Select
                      value={
                        watchedApps?.[index]?.por_cert_mode || "NO_SECURITY"
                      }
                      onValueChange={(v) =>
                        setValue(`applications.${index}.por_cert_mode`, v)
                      }
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
                </div>
              </div>
            ))}
          </CardContent>
        </CardUI>

        <div className="flex items-center justify-end gap-2">
          <Button
            type="button"
            variant="outline"
            onClick={() => router.push("/profiles")}
          >
            Cancel
          </Button>
          <Button type="submit" disabled={createProfile.isPending}>
            {createProfile.isPending ? "Creating..." : "Create Profile"}
          </Button>
        </div>

        {createProfile.isError && (
          <p className="text-sm text-destructive">
            Failed to create profile: {createProfile.error.message}
          </p>
        )}
      </form>
    </div>
  )
}
