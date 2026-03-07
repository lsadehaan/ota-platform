"use client"

import "./globals.css"
import React, { useState } from "react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { Toaster } from "@/components/ui/toaster"
import { cn } from "@/lib/utils"
import {
  LayoutDashboard,
  Rocket,
  CreditCard,
  Shield,
  FileBox,
  FileCode,
  Activity,
  Settings,
} from "lucide-react"

const navItems = [
  { label: "Dashboard", href: "/", icon: LayoutDashboard },
  { label: "Campaigns", href: "/campaigns", icon: Rocket },
  { label: "Cards", href: "/cards", icon: CreditCard },
  { label: "Profiles", href: "/profiles", icon: Shield },
  { label: "CAP Files", href: "/caps", icon: FileBox },
  { label: "Scripts", href: "/scripts", icon: FileCode },
  { label: "Monitoring", href: "/monitoring/health", icon: Activity },
  { label: "Settings", href: "/settings", icon: Settings },
]

function Sidebar() {
  const pathname = usePathname()

  function isActive(href: string): boolean {
    if (href === "/") {
      return pathname === "/"
    }
    return pathname.startsWith(href)
  }

  return (
    <aside className="fixed left-0 top-0 z-40 h-screen w-64 border-r bg-card">
      <div className="flex h-14 items-center border-b px-6">
        <Link href="/" className="flex items-center gap-2 font-semibold text-lg">
          <div className="flex h-8 w-8 items-center justify-center rounded-md bg-primary text-primary-foreground text-sm font-bold">
            OTA
          </div>
          <span>OTA Platform</span>
        </Link>
      </div>
      <nav className="flex flex-col gap-1 p-4">
        {navItems.map((item) => {
          const Icon = item.icon
          const active = isActive(item.href)
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                active
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:bg-muted hover:text-foreground"
              )}
            >
              <Icon className="h-4 w-4" />
              {item.label}
            </Link>
          )
        })}
      </nav>
    </aside>
  )
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 30 * 1000,
            retry: 1,
          },
        },
      })
  )

  return (
    <html lang="en" suppressHydrationWarning>
      <body className="min-h-screen">
        <QueryClientProvider client={queryClient}>
          <Sidebar />
          <main className="pl-64">
            <div className="p-8">{children}</div>
          </main>
          <Toaster />
        </QueryClientProvider>
      </body>
    </html>
  )
}
