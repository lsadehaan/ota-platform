import React from "react"
import { Card, CardContent } from "@/components/ui/card"
import { cn } from "@/lib/utils"
import { ArrowUp, ArrowDown } from "lucide-react"

export interface KPICardProps {
  title: string
  value: string | number
  description?: string
  icon: React.ReactNode
  trend?: { value: number; positive: boolean }
}

export function KPICard({ title, value, description, icon, trend }: KPICardProps) {
  return (
    <Card>
      <CardContent className="p-6">
        <div className="flex items-center justify-between">
          <div className="space-y-1">
            <p className="text-sm font-medium text-muted-foreground">{title}</p>
            <div className="flex items-baseline gap-2">
              <p className="text-2xl font-bold">{value}</p>
              {trend && (
                <span
                  className={cn(
                    "flex items-center text-xs font-medium",
                    trend.positive ? "text-green-600" : "text-red-600"
                  )}
                >
                  {trend.positive ? (
                    <ArrowUp className="mr-0.5 h-3 w-3" />
                  ) : (
                    <ArrowDown className="mr-0.5 h-3 w-3" />
                  )}
                  {Math.abs(trend.value)}%
                </span>
              )}
            </div>
            {description && (
              <p className="text-xs text-muted-foreground">{description}</p>
            )}
          </div>
          <div className="flex h-10 w-10 items-center justify-center rounded-md bg-primary/10 text-primary">
            {icon}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
