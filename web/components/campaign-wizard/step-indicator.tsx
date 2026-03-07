"use client"

import React from "react"
import { cn } from "@/lib/utils"
import { Check } from "lucide-react"

const STEPS = [
  { label: "Type", step: 1 },
  { label: "Targets", step: 2 },
  { label: "Commands", step: 3 },
  { label: "Settings", step: 4 },
  { label: "Review", step: 5 },
]

interface StepIndicatorProps {
  currentStep: number
  completedSteps: number[]
}

export function StepIndicator({ currentStep, completedSteps }: StepIndicatorProps) {
  return (
    <nav aria-label="Campaign creation steps" className="mb-8">
      <ol className="flex items-center w-full">
        {STEPS.map(({ label, step }, index) => {
          const isActive = step === currentStep
          const isCompleted = completedSteps.includes(step)
          const isLast = index === STEPS.length - 1

          return (
            <li
              key={step}
              className={cn("flex items-center", !isLast && "flex-1")}
            >
              <div className="flex flex-col items-center gap-1.5">
                <div
                  className={cn(
                    "flex h-9 w-9 shrink-0 items-center justify-center rounded-full border-2 text-sm font-semibold transition-colors",
                    isCompleted
                      ? "border-primary bg-primary text-primary-foreground"
                      : isActive
                      ? "border-primary bg-background text-primary"
                      : "border-muted-foreground/30 bg-background text-muted-foreground"
                  )}
                >
                  {isCompleted ? (
                    <Check className="h-4 w-4" />
                  ) : (
                    step
                  )}
                </div>
                <span
                  className={cn(
                    "text-xs font-medium whitespace-nowrap",
                    isActive
                      ? "text-primary"
                      : isCompleted
                      ? "text-foreground"
                      : "text-muted-foreground"
                  )}
                >
                  {label}
                </span>
              </div>
              {!isLast && (
                <div
                  className={cn(
                    "h-0.5 w-full mx-2 mt-[-1.25rem]",
                    isCompleted
                      ? "bg-primary"
                      : "bg-muted-foreground/30"
                  )}
                />
              )}
            </li>
          )
        })}
      </ol>
    </nav>
  )
}
