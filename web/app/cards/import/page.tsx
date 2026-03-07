"use client"

import React, { useState, useCallback, useRef } from "react"
import { useRouter } from "next/navigation"
import { useMutation } from "@tanstack/react-query"
import { cardsAPI } from "@/lib/api"
import Papa from "papaparse"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import {
  Card as CardUI,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
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
  ArrowLeft,
  Upload,
  FileSpreadsheet,
  CheckCircle2,
  XCircle,
  AlertTriangle,
} from "lucide-react"

const REQUIRED_COLUMNS = ["iccid", "imsi", "msisdn", "profile_name"]
const OPTIONAL_COLUMNS = ["enc_key", "auth_key"]
const ALL_COLUMNS = [...REQUIRED_COLUMNS, ...OPTIONAL_COLUMNS]

interface ParsedRow {
  rowIndex: number
  data: Record<string, string>
  errors: string[]
}

interface ImportResult {
  imported: number
  failed: number
  errors: string[]
}

function validateRows(rows: Record<string, string>[]): ParsedRow[] {
  const seenIccids = new Set<string>()
  const seenImsis = new Set<string>()

  return rows.map((data, index) => {
    const errors: string[] = []

    for (const col of REQUIRED_COLUMNS) {
      if (!data[col] || !data[col].trim()) {
        errors.push(`Missing required field: ${col}`)
      }
    }

    if (data.iccid) {
      if (seenIccids.has(data.iccid)) {
        errors.push("Duplicate ICCID in file")
      }
      seenIccids.add(data.iccid)
    }

    if (data.imsi) {
      if (seenImsis.has(data.imsi)) {
        errors.push("Duplicate IMSI in file")
      }
      seenImsis.add(data.imsi)
    }

    if (data.iccid && !/^\d{18,22}$/.test(data.iccid)) {
      errors.push("ICCID should be 18-22 digits")
    }

    if (data.imsi && !/^\d{14,15}$/.test(data.imsi)) {
      errors.push("IMSI should be 14-15 digits")
    }

    return { rowIndex: index + 1, data, errors }
  })
}

export default function ImportCardsPage() {
  const router = useRouter()
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [file, setFile] = useState<File | null>(null)
  const [parsedRows, setParsedRows] = useState<ParsedRow[]>([])
  const [allRows, setAllRows] = useState<Record<string, string>[]>([])
  const [parseError, setParseError] = useState<string | null>(null)
  const [detectedColumns, setDetectedColumns] = useState<string[]>([])
  const [importResult, setImportResult] = useState<ImportResult | null>(null)
  const [isDragging, setIsDragging] = useState(false)

  const importMutation = useMutation({
    mutationFn: (formData: FormData) => cardsAPI.import(formData),
    onSuccess: (data: any) => {
      setImportResult({
        imported: data.imported ?? data.success_count ?? 0,
        failed: data.failed ?? data.error_count ?? 0,
        errors: data.errors ?? [],
      })
    },
    onError: (error: Error) => {
      setImportResult({
        imported: 0,
        failed: allRows.length,
        errors: [error.message],
      })
    },
  })

  const handleFile = useCallback((selectedFile: File) => {
    setFile(selectedFile)
    setParseError(null)
    setImportResult(null)

    Papa.parse(selectedFile, {
      header: true,
      skipEmptyLines: true,
      transformHeader: (header: string) => header.trim().toLowerCase(),
      complete: (results) => {
        const headers = results.meta.fields ?? []
        setDetectedColumns(headers)

        const missingRequired = REQUIRED_COLUMNS.filter(
          (col) => !headers.includes(col)
        )
        if (missingRequired.length > 0) {
          setParseError(
            `Missing required columns: ${missingRequired.join(", ")}`
          )
          setParsedRows([])
          setAllRows([])
          return
        }

        const rows = results.data as Record<string, string>[]
        setAllRows(rows)
        const validated = validateRows(rows)
        setParsedRows(validated)
      },
      error: (error) => {
        setParseError(`Failed to parse CSV: ${error.message}`)
        setParsedRows([])
        setAllRows([])
      },
    })
  }, [])

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      setIsDragging(false)
      const droppedFile = e.dataTransfer.files[0]
      if (droppedFile && droppedFile.name.endsWith(".csv")) {
        handleFile(droppedFile)
      } else {
        setParseError("Please upload a .csv file")
      }
    },
    [handleFile]
  )

  const handleFileInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const selectedFile = e.target.files?.[0]
      if (selectedFile) {
        handleFile(selectedFile)
      }
    },
    [handleFile]
  )

  const handleImport = useCallback(() => {
    if (!file) return
    const formData = new FormData()
    formData.append("file", file)
    importMutation.mutate(formData)
  }, [file, importMutation])

  const previewRows = parsedRows.slice(0, 10)
  const errorCount = parsedRows.filter((r) => r.errors.length > 0).length
  const hasErrors = errorCount > 0

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

      <div>
        <h1 className="text-3xl font-bold tracking-tight">Import Cards</h1>
        <p className="text-muted-foreground mt-1">
          Upload a CSV file to bulk import SIM cards
        </p>
      </div>

      {!importResult && (
        <>
          <CardUI>
            <CardHeader>
              <CardTitle className="text-base">Upload CSV File</CardTitle>
              <CardDescription>
                Required columns: <code>iccid</code>, <code>imsi</code>,{" "}
                <code>msisdn</code>, <code>profile_name</code>. Optional:{" "}
                <code>enc_key</code>, <code>auth_key</code>.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div
                className={`border-2 border-dashed rounded-lg p-12 text-center transition-colors ${
                  isDragging
                    ? "border-primary bg-primary/5"
                    : "border-muted-foreground/25 hover:border-muted-foreground/50"
                }`}
                onDragOver={(e) => {
                  e.preventDefault()
                  setIsDragging(true)
                }}
                onDragLeave={() => setIsDragging(false)}
                onDrop={handleDrop}
                onClick={() => fileInputRef.current?.click()}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    fileInputRef.current?.click()
                  }
                }}
              >
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv"
                  className="hidden"
                  onChange={handleFileInput}
                />
                <div className="flex flex-col items-center gap-3">
                  {file ? (
                    <>
                      <FileSpreadsheet className="h-10 w-10 text-primary" />
                      <div>
                        <p className="font-medium">{file.name}</p>
                        <p className="text-sm text-muted-foreground">
                          {(file.size / 1024).toFixed(1)} KB -{" "}
                          {allRows.length} rows detected
                        </p>
                      </div>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={(e) => {
                          e.stopPropagation()
                          setFile(null)
                          setParsedRows([])
                          setAllRows([])
                          setDetectedColumns([])
                          setParseError(null)
                          if (fileInputRef.current) {
                            fileInputRef.current.value = ""
                          }
                        }}
                      >
                        Choose Different File
                      </Button>
                    </>
                  ) : (
                    <>
                      <Upload className="h-10 w-10 text-muted-foreground" />
                      <div>
                        <p className="font-medium">
                          Drop your CSV file here or click to browse
                        </p>
                        <p className="text-sm text-muted-foreground">
                          Accepts .csv files
                        </p>
                      </div>
                    </>
                  )}
                </div>
              </div>
            </CardContent>
          </CardUI>

          {parseError && (
            <CardUI className="border-destructive">
              <CardContent className="pt-6">
                <div className="flex items-center gap-2 text-destructive">
                  <XCircle className="h-5 w-5" />
                  <span className="font-medium">{parseError}</span>
                </div>
              </CardContent>
            </CardUI>
          )}

          {parsedRows.length > 0 && (
            <>
              <CardUI>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <div>
                      <CardTitle className="text-base">
                        Preview ({previewRows.length} of {parsedRows.length} rows)
                      </CardTitle>
                      <CardDescription>
                        Detected columns:{" "}
                        {detectedColumns.map((col) => (
                          <Badge
                            key={col}
                            variant={
                              ALL_COLUMNS.includes(col) ? "secondary" : "outline"
                            }
                            className="mr-1 text-xs"
                          >
                            {col}
                          </Badge>
                        ))}
                      </CardDescription>
                    </div>
                    {hasErrors && (
                      <div className="flex items-center gap-2 text-destructive">
                        <AlertTriangle className="h-4 w-4" />
                        <span className="text-sm font-medium">
                          {errorCount} row{errorCount > 1 ? "s" : ""} with
                          errors
                        </span>
                      </div>
                    )}
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="w-12">#</TableHead>
                          {ALL_COLUMNS.filter((col) =>
                            detectedColumns.includes(col)
                          ).map((col) => (
                            <TableHead key={col}>{col}</TableHead>
                          ))}
                          <TableHead>Validation</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {previewRows.map((row) => (
                          <TableRow
                            key={row.rowIndex}
                            className={
                              row.errors.length > 0
                                ? "bg-destructive/5"
                                : undefined
                            }
                          >
                            <TableCell className="text-muted-foreground text-xs">
                              {row.rowIndex}
                            </TableCell>
                            {ALL_COLUMNS.filter((col) =>
                              detectedColumns.includes(col)
                            ).map((col) => (
                              <TableCell
                                key={col}
                                className="font-mono text-xs"
                              >
                                {row.data[col] || (
                                  <span className="text-destructive">
                                    missing
                                  </span>
                                )}
                              </TableCell>
                            ))}
                            <TableCell>
                              {row.errors.length === 0 ? (
                                <CheckCircle2 className="h-4 w-4 text-green-600" />
                              ) : (
                                <div className="flex flex-col gap-0.5">
                                  {row.errors.map((err, i) => (
                                    <span
                                      key={i}
                                      className="text-xs text-destructive"
                                    >
                                      {err}
                                    </span>
                                  ))}
                                </div>
                              )}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </CardUI>

              <div className="flex items-center justify-between">
                <div className="text-sm text-muted-foreground">
                  {allRows.length} total rows, {errorCount} with validation
                  errors
                </div>
                <Button
                  onClick={handleImport}
                  disabled={importMutation.isPending}
                  size="lg"
                >
                  {importMutation.isPending ? (
                    <>Importing...</>
                  ) : (
                    <>
                      <Upload className="mr-2 h-4 w-4" />
                      Import {allRows.length} Cards
                    </>
                  )}
                </Button>
              </div>

              {importMutation.isPending && (
                <CardUI>
                  <CardContent className="pt-6">
                    <div className="space-y-2">
                      <div className="flex items-center justify-between text-sm">
                        <span>Importing cards...</span>
                        <span className="text-muted-foreground">
                          Please wait
                        </span>
                      </div>
                      <Progress className="h-2" />
                    </div>
                  </CardContent>
                </CardUI>
              )}
            </>
          )}
        </>
      )}

      {importResult && (
        <CardUI
          className={
            importResult.failed === 0 ? "border-green-600" : "border-yellow-500"
          }
        >
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              {importResult.failed === 0 ? (
                <>
                  <CheckCircle2 className="h-5 w-5 text-green-600" />
                  Import Successful
                </>
              ) : (
                <>
                  <AlertTriangle className="h-5 w-5 text-yellow-500" />
                  Import Completed with Errors
                </>
              )}
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="rounded-lg border p-4 text-center">
                <p className="text-2xl font-bold text-green-600">
                  {importResult.imported}
                </p>
                <p className="text-sm text-muted-foreground">
                  Successfully Imported
                </p>
              </div>
              <div className="rounded-lg border p-4 text-center">
                <p className="text-2xl font-bold text-destructive">
                  {importResult.failed}
                </p>
                <p className="text-sm text-muted-foreground">Failed</p>
              </div>
            </div>
            {importResult.errors.length > 0 && (
              <div className="space-y-1">
                <p className="text-sm font-medium">Errors:</p>
                <div className="max-h-48 overflow-y-auto rounded border p-3 bg-muted/50">
                  {importResult.errors.map((err, i) => (
                    <p key={i} className="text-xs text-destructive font-mono">
                      {err}
                    </p>
                  ))}
                </div>
              </div>
            )}
            <div className="flex items-center gap-2">
              <Button onClick={() => router.push("/cards")}>
                View Card Inventory
              </Button>
              <Button
                variant="outline"
                onClick={() => {
                  setFile(null)
                  setParsedRows([])
                  setAllRows([])
                  setDetectedColumns([])
                  setParseError(null)
                  setImportResult(null)
                  if (fileInputRef.current) {
                    fileInputRef.current.value = ""
                  }
                }}
              >
                Import Another File
              </Button>
            </div>
          </CardContent>
        </CardUI>
      )}
    </div>
  )
}
