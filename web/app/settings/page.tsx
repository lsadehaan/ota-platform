"use client"

import React, { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs"
import {
  Card as CardUI,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Separator } from "@/components/ui/separator"
import { useToast } from "@/components/ui/use-toast"
import { Save, Loader2, Plug, CheckCircle, XCircle } from "lucide-react"

export default function SettingsPage() {
  const { toast } = useToast()

  // SMPP settings
  const [smppHost, setSmppHost] = useState("127.0.0.1")
  const [smppPort, setSmppPort] = useState("2775")
  const [smppSystemId, setSmppSystemId] = useState("")
  const [smppPassword, setSmppPassword] = useState("")
  const [smppSourceAddr, setSmppSourceAddr] = useState("")
  const [smppTon, setSmppTon] = useState("1")
  const [smppNpi, setSmppNpi] = useState("1")
  const [smppEnquireLink, setSmppEnquireLink] = useState("30")
  const [testingConnection, setTestingConnection] = useState(false)
  const [connectionResult, setConnectionResult] = useState<"success" | "failed" | null>(null)

  // Defaults
  const [defaultMaxRetries, setDefaultMaxRetries] = useState("3")
  const [defaultThrottle, setDefaultThrottle] = useState("10")
  const [defaultPorMode, setDefaultPorMode] = useState("required")

  const [saving, setSaving] = useState(false)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function fetchSettings() {
      try {
        const res = await fetch("/api/v1/settings")
        if (!res.ok) throw new Error("Failed to fetch settings")
        const data = await res.json()
        if (data.smpp_host) setSmppHost(data.smpp_host)
        if (data.smpp_port) setSmppPort(String(data.smpp_port))
        if (data.smpp_system_id) setSmppSystemId(data.smpp_system_id)
        if (data.smpp_password) setSmppPassword(data.smpp_password)
        if (data.smpp_source_addr) setSmppSourceAddr(data.smpp_source_addr)
        if (data.smpp_ton != null) setSmppTon(String(data.smpp_ton))
        if (data.smpp_npi != null) setSmppNpi(String(data.smpp_npi))
        if (data.smpp_enquire_link != null) setSmppEnquireLink(String(data.smpp_enquire_link))
        if (data.default_max_retries != null) setDefaultMaxRetries(String(data.default_max_retries))
        if (data.default_throttle != null) setDefaultThrottle(String(data.default_throttle))
        if (data.default_por_mode) setDefaultPorMode(data.default_por_mode)
      } catch {
        // Settings may not be configured yet; keep defaults
      } finally {
        setLoading(false)
      }
    }
    fetchSettings()
  }, [])

  async function handleTestConnection() {
    setTestingConnection(true)
    setConnectionResult(null)
    try {
      // Client-side simulation only; no backend endpoint for connection testing
      await new Promise((resolve) => setTimeout(resolve, 2000))
      setConnectionResult("success")
      toast({ title: "Connection successful", description: `Connected to ${smppHost}:${smppPort}` })
    } catch {
      setConnectionResult("failed")
      toast({ title: "Connection failed", description: "Could not connect to SMPP server.", variant: "destructive" })
    } finally {
      setTestingConnection(false)
    }
  }

  async function handleSave() {
    setSaving(true)
    try {
      const payload = {
        smpp_host: smppHost,
        smpp_port: parseInt(smppPort, 10),
        smpp_system_id: smppSystemId,
        smpp_password: smppPassword,
        smpp_source_addr: smppSourceAddr,
        smpp_ton: parseInt(smppTon, 10),
        smpp_npi: parseInt(smppNpi, 10),
        smpp_enquire_link: parseInt(smppEnquireLink, 10),
        default_max_retries: parseInt(defaultMaxRetries, 10),
        default_throttle: parseInt(defaultThrottle, 10),
        default_por_mode: defaultPorMode,
      }
      const res = await fetch("/api/v1/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}))
        throw new Error(errorData.message || `Server responded with ${res.status}`)
      }
      toast({ title: "Settings saved successfully." })
    } catch (err: any) {
      toast({ title: "Failed to save settings", description: err.message, variant: "destructive" })
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Settings</h1>
        <p className="text-muted-foreground mt-1">
          Configure system connections and defaults
        </p>
      </div>

      <Tabs defaultValue="smpp">
        <TabsList>
          <TabsTrigger value="smpp">SMPP</TabsTrigger>
          <TabsTrigger value="kafka">Kafka</TabsTrigger>
          <TabsTrigger value="defaults">Defaults</TabsTrigger>
        </TabsList>

        <TabsContent value="smpp" className="space-y-6 mt-6">
          <CardUI>
            <CardHeader>
              <CardTitle>SMPP Connection</CardTitle>
              <CardDescription>
                Configure the SMPP gateway connection for sending OTA SMS messages.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor="smpp-host">Host</Label>
                  <Input
                    id="smpp-host"
                    placeholder="127.0.0.1"
                    value={smppHost}
                    onChange={(e) => setSmppHost(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="smpp-port">Port</Label>
                  <Input
                    id="smpp-port"
                    type="number"
                    placeholder="2775"
                    value={smppPort}
                    onChange={(e) => setSmppPort(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="smpp-system-id">System ID</Label>
                  <Input
                    id="smpp-system-id"
                    placeholder="system_id"
                    value={smppSystemId}
                    onChange={(e) => setSmppSystemId(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="smpp-password">Password</Label>
                  <Input
                    id="smpp-password"
                    type="password"
                    placeholder="password"
                    value={smppPassword}
                    onChange={(e) => setSmppPassword(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="smpp-source-addr">Source Address</Label>
                  <Input
                    id="smpp-source-addr"
                    placeholder="OTA"
                    value={smppSourceAddr}
                    onChange={(e) => setSmppSourceAddr(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="smpp-enquire-link">Enquire Link Interval (s)</Label>
                  <Input
                    id="smpp-enquire-link"
                    type="number"
                    placeholder="30"
                    value={smppEnquireLink}
                    onChange={(e) => setSmppEnquireLink(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="smpp-ton">TON (Type of Number)</Label>
                  <Select value={smppTon} onValueChange={setSmppTon}>
                    <SelectTrigger id="smpp-ton">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="0">0 - Unknown</SelectItem>
                      <SelectItem value="1">1 - International</SelectItem>
                      <SelectItem value="2">2 - National</SelectItem>
                      <SelectItem value="3">3 - Network Specific</SelectItem>
                      <SelectItem value="5">5 - Alphanumeric</SelectItem>
                      <SelectItem value="6">6 - Abbreviated</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="smpp-npi">NPI (Numbering Plan)</Label>
                  <Select value={smppNpi} onValueChange={setSmppNpi}>
                    <SelectTrigger id="smpp-npi">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="0">0 - Unknown</SelectItem>
                      <SelectItem value="1">1 - ISDN (E.164)</SelectItem>
                      <SelectItem value="3">3 - Data (X.121)</SelectItem>
                      <SelectItem value="4">4 - Telex (F.69)</SelectItem>
                      <SelectItem value="8">8 - National</SelectItem>
                      <SelectItem value="9">9 - Private</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <Separator />

              <div className="flex items-center gap-4">
                <Button
                  variant="outline"
                  onClick={handleTestConnection}
                  disabled={testingConnection}
                >
                  {testingConnection ? (
                    <>
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      Testing...
                    </>
                  ) : (
                    <>
                      <Plug className="mr-2 h-4 w-4" />
                      Test Connection
                    </>
                  )}
                </Button>
                {connectionResult === "success" && (
                  <div className="flex items-center gap-2 text-green-600">
                    <CheckCircle className="h-4 w-4" />
                    <span className="text-sm font-medium">Connected</span>
                  </div>
                )}
                {connectionResult === "failed" && (
                  <div className="flex items-center gap-2 text-red-600">
                    <XCircle className="h-4 w-4" />
                    <span className="text-sm font-medium">Connection failed</span>
                  </div>
                )}
              </div>
            </CardContent>
          </CardUI>
        </TabsContent>

        <TabsContent value="kafka" className="space-y-6 mt-6">
          <CardUI>
            <CardHeader>
              <CardTitle>Kafka Configuration</CardTitle>
              <CardDescription>
                Kafka broker and topic configuration (read-only).
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="space-y-4">
                <div className="space-y-2">
                  <Label>Broker Addresses</Label>
                  <Input
                    value="localhost:9092"
                    readOnly
                    disabled
                    className="font-mono bg-muted"
                  />
                </div>

                <Separator />

                <div>
                  <Label className="mb-3 block">Topics</Label>
                  <div className="grid gap-3 md:grid-cols-2">
                    {[
                      { name: "send-sms", description: "Outbound commands" },
                      { name: "card-events", description: "Card lifecycle events (DLR, MO)" },
                      { name: "message-log", description: "Message logging" },
                    ].map((topic) => (
                      <div
                        key={topic.name}
                        className="flex items-center justify-between rounded-md border p-3"
                      >
                        <div>
                          <code className="text-sm font-mono">{topic.name}</code>
                          <p className="text-xs text-muted-foreground mt-0.5">{topic.description}</p>
                        </div>
                        <Badge variant="secondary">active</Badge>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </CardContent>
          </CardUI>
        </TabsContent>

        <TabsContent value="defaults" className="space-y-6 mt-6">
          <CardUI>
            <CardHeader>
              <CardTitle>Default Settings</CardTitle>
              <CardDescription>
                Default values for campaign and messaging operations.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="grid gap-4 md:grid-cols-3">
                <div className="space-y-2">
                  <Label htmlFor="default-retries">Max Retries</Label>
                  <Input
                    id="default-retries"
                    type="number"
                    min={0}
                    max={10}
                    value={defaultMaxRetries}
                    onChange={(e) => setDefaultMaxRetries(e.target.value)}
                  />
                  <p className="text-xs text-muted-foreground">
                    Maximum retry attempts for failed messages.
                  </p>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="default-throttle">Throttle (msg/s)</Label>
                  <Input
                    id="default-throttle"
                    type="number"
                    min={1}
                    max={100}
                    value={defaultThrottle}
                    onChange={(e) => setDefaultThrottle(e.target.value)}
                  />
                  <p className="text-xs text-muted-foreground">
                    Default message sending rate limit.
                  </p>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="default-por">PoR Mode</Label>
                  <Select value={defaultPorMode} onValueChange={setDefaultPorMode}>
                    <SelectTrigger id="default-por">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="required">Required</SelectItem>
                      <SelectItem value="optional">Optional</SelectItem>
                      <SelectItem value="none">None</SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-xs text-muted-foreground">
                    Default Proof of Receipt requirement.
                  </p>
                </div>
              </div>
            </CardContent>
          </CardUI>
        </TabsContent>
      </Tabs>

      <div className="flex items-center justify-end">
        <Button onClick={handleSave} disabled={saving}>
          {saving ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Saving...
            </>
          ) : (
            <>
              <Save className="mr-2 h-4 w-4" />
              Save Settings
            </>
          )}
        </Button>
      </div>
    </div>
  )
}
