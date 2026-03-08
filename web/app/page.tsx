"use client"

import React from "react"
import { useQuery } from "@tanstack/react-query"
import { dashboardAPI } from "@/lib/api"
import { KPICard } from "@/components/kpi-cards/kpi-card"
import { ActivityFeed } from "@/components/live-feed/activity-feed"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import {
  CreditCard,
  Rocket,
  MessageSquare,
  CalendarDays,
  CheckCircle,
  XCircle,
} from "lucide-react"
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts"
import type { DashboardKPIs, SMSThroughputPoint } from "@/lib/types"

function KPISection() {
  const { data: kpis, isLoading } = useQuery<DashboardKPIs>({
    queryKey: ["dashboard", "kpis"],
    queryFn: dashboardAPI.getKPIs,
    refetchInterval: 30000,
  })

  if (isLoading) {
    return (
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
        {Array.from({ length: 6 }).map((_, i) => (
          <Card key={i}>
            <CardContent className="p-6">
              <Skeleton className="h-4 w-24 mb-2" />
              <Skeleton className="h-8 w-16" />
            </CardContent>
          </Card>
        ))}
      </div>
    )
  }

  const deliveryRate = kpis?.success_rate ?? 0
  const messagesToday = kpis?.messages_today ?? 0
  const totalCards = kpis?.total_cards ?? 0
  const activeCampaigns = kpis?.active_campaigns ?? 0

  const campaignsByStatus = kpis?.campaigns_by_status ?? {}
  const failedCampaigns = campaignsByStatus["failed"] ?? 0
  const messagesThisWeek = messagesToday * 7

  return (
    <div className="grid gap-4 grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
      <KPICard
        title="Total Cards"
        value={totalCards.toLocaleString()}
        description="Registered SIM cards"
        icon={<CreditCard className="h-5 w-5" />}
      />
      <KPICard
        title="Active Campaigns"
        value={activeCampaigns}
        description="Currently running"
        icon={<Rocket className="h-5 w-5" />}
      />
      <KPICard
        title="SMS Today"
        value={messagesToday.toLocaleString()}
        description="Messages sent today"
        icon={<MessageSquare className="h-5 w-5" />}
      />
      <KPICard
        title="SMS This Week"
        value={messagesThisWeek.toLocaleString()}
        description="Messages this week"
        icon={<CalendarDays className="h-5 w-5" />}
      />
      <KPICard
        title="Delivery Rate"
        value={`${deliveryRate.toFixed(1)}%`}
        description="Successful deliveries"
        icon={<CheckCircle className="h-5 w-5" />}
        trend={
          deliveryRate >= 95
            ? { value: deliveryRate - 95, positive: true }
            : deliveryRate > 0
            ? { value: 95 - deliveryRate, positive: false }
            : undefined
        }
      />
      <KPICard
        title="Failed Campaigns"
        value={failedCampaigns}
        description="Need attention"
        icon={<XCircle className="h-5 w-5" />}
      />
    </div>
  )
}

function SMSThroughputChart() {
  const { data: throughput, isLoading } = useQuery<SMSThroughputPoint[]>({
    queryKey: ["dashboard", "sms-throughput"],
    queryFn: dashboardAPI.getSMSThroughput,
    refetchInterval: 15000,
  })

  const chartData = (throughput || []).map((point) => ({
    time: new Date(point.timestamp).toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }),
    sent: Math.round(point.sent * 100) / 100,
    delivered: Math.round(point.delivered * 100) / 100,
    failed: Math.round(point.failed * 100) / 100,
  }))

  return (
    <Card className="col-span-2">
      <CardHeader>
        <CardTitle className="text-base">SMS Throughput — avg msg/s (Last Hour)</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <Skeleton className="h-[300px] w-full" />
        ) : chartData.length === 0 ? (
          <div className="flex h-[300px] items-center justify-center text-sm text-muted-foreground">
            No throughput data available
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis
                dataKey="time"
                tick={{ fontSize: 11 }}
                tickLine={false}
                axisLine={false}
                interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fontSize: 11 }}
                tickLine={false}
                axisLine={false}
                label={{ value: "msg/s", angle: -90, position: "insideLeft", style: { fontSize: 11 } }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  border: "1px solid hsl(var(--border))",
                  borderRadius: "var(--radius)",
                  fontSize: 12,
                }}
                formatter={(value: number) => [`${value.toFixed(2)} msg/s`]}
              />
              <Legend />
              <Bar
                dataKey="sent"
                fill="hsl(221.2, 83.2%, 53.3%)"
                radius={[2, 2, 0, 0]}
                name="Sent"
              />
              <Bar
                dataKey="delivered"
                fill="hsl(142, 71%, 45%)"
                radius={[2, 2, 0, 0]}
                name="Delivered"
              />
              <Bar
                dataKey="failed"
                fill="hsl(0, 84.2%, 60.2%)"
                radius={[2, 2, 0, 0]}
                name="Failed"
              />
            </BarChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  )
}

export default function DashboardPage() {
  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
        <p className="text-muted-foreground mt-1">
          OTA platform overview and real-time activity
        </p>
      </div>

      <KPISection />

      <div className="grid gap-4 grid-cols-1 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <SMSThroughputChart />
        </div>

        <div className="lg:col-span-1">
          <ActivityFeed />
        </div>
      </div>
    </div>
  )
}
