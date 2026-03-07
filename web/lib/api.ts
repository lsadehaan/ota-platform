function resolveAPIBase(): string {
  if (process.env.NEXT_PUBLIC_API_URL) {
    return process.env.NEXT_PUBLIC_API_URL.replace(/\/$/, '')
  }
  if (typeof window !== 'undefined') {
    return ''
  }
  return (process.env.INTERNAL_API_URL || 'http://ota-api:8080').replace(/\/$/, '')
}

interface PaginatedResponse<T> {
  data: T[]
  total: number
  page: number
  page_size: number
  total_pages: number
}

interface FetchOptions {
  method?: string
  body?: any
  params?: Record<string, string>
}

async function fetchAPI<T>(path: string, options: FetchOptions = {}): Promise<T> {
  const base = resolveAPIBase()
  const url = new URL(base ? `${base}${path}` : path, typeof window !== 'undefined' ? window.location.origin : undefined)
  if (options.params) {
    Object.entries(options.params).forEach(([k, v]) => url.searchParams.set(k, v))
  }
  const headers: Record<string, string> = options.body instanceof FormData ? {} : { 'Content-Type': 'application/json' }
  const apiKey = process.env.NEXT_PUBLIC_API_KEY
  if (apiKey) {
    headers['Authorization'] = `Bearer ${apiKey}`
  }
  const res = await fetch(url.toString(), {
    method: options.method || 'GET',
    headers,
    body: options.body instanceof FormData ? options.body : options.body ? JSON.stringify(options.body) : undefined,
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }))
    throw new Error(err.error || res.statusText)
  }
  const json = await res.json()
  // Unwrap {data: ...} envelope for non-paginated responses.
  // Paginated responses have 'total' alongside 'data' and are kept intact.
  if (json && typeof json === 'object' && 'data' in json && !('total' in json)) {
    return json.data as T
  }
  return json as T
}

// Profiles
export const profilesAPI = {
  list: (params?: Record<string, string>) => fetchAPI<PaginatedResponse<any>>('/api/v1/profiles', { params }),
  get: (id: string) => fetchAPI<any>(`/api/v1/profiles/${id}`),
  create: (data: any) => fetchAPI<any>('/api/v1/profiles', { method: 'POST', body: data }),
  update: (id: string, data: any) => fetchAPI<any>(`/api/v1/profiles/${id}`, { method: 'PUT', body: data }),
  delete: (id: string) => fetchAPI<void>(`/api/v1/profiles/${id}`, { method: 'DELETE' }),
  createApplication: (profileId: string, data: any) => fetchAPI<any>(`/api/v1/profiles/${profileId}/applications`, { method: 'POST', body: data }),
  updateApplication: (profileId: string, appId: string, data: any) => fetchAPI<any>(`/api/v1/profiles/${profileId}/applications/${appId}`, { method: 'PUT', body: data }),
  deleteApplication: (profileId: string, appId: string) => fetchAPI<void>(`/api/v1/profiles/${profileId}/applications/${appId}`, { method: 'DELETE' }),
}

// Cards
export const cardsAPI = {
  list: (params?: Record<string, string>) => fetchAPI<PaginatedResponse<any>>('/api/v1/cards', { params }),
  get: (id: string) => fetchAPI<any>(`/api/v1/cards/${id}`),
  create: (data: any) => fetchAPI<any>('/api/v1/cards', { method: 'POST', body: data }),
  update: (id: string, data: any) => fetchAPI<any>(`/api/v1/cards/${id}`, { method: 'PUT', body: data }),
  delete: (id: string) => fetchAPI<void>(`/api/v1/cards/${id}`, { method: 'DELETE' }),
  getCounters: (id: string) => fetchAPI<any[]>(`/api/v1/cards/${id}/counters`),
  import: (formData: FormData) => fetchAPI<any>('/api/v1/cards/import', { method: 'POST', body: formData }),
}

// Card Groups
export const cardGroupsAPI = {
  list: () => fetchAPI<PaginatedResponse<any>>('/api/v1/card-groups'),
  get: (id: string) => fetchAPI<any>(`/api/v1/card-groups/${id}`),
  create: (data: any) => fetchAPI<any>('/api/v1/card-groups', { method: 'POST', body: data }),
  update: (id: string, data: any) => fetchAPI<any>(`/api/v1/card-groups/${id}`, { method: 'PUT', body: data }),
  delete: (id: string) => fetchAPI<void>(`/api/v1/card-groups/${id}`, { method: 'DELETE' }),
  addMembers: (id: string, cardIds: string[]) => fetchAPI<void>(`/api/v1/card-groups/${id}/members`, { method: 'POST', body: { card_ids: cardIds } }),
  removeMembers: (id: string, cardIds: string[]) => fetchAPI<void>(`/api/v1/card-groups/${id}/members`, { method: 'DELETE', body: { card_ids: cardIds } }),
}

// Campaigns
export const campaignsAPI = {
  list: (params?: Record<string, string>) => fetchAPI<PaginatedResponse<any>>('/api/v1/campaigns', { params }),
  get: (id: string) => fetchAPI<any>(`/api/v1/campaigns/${id}`),
  create: (data: any) => fetchAPI<any>('/api/v1/campaigns', { method: 'POST', body: data }),
  start: (id: string) => fetchAPI<void>(`/api/v1/campaigns/${id}/start`, { method: 'POST' }),
  pause: (id: string) => fetchAPI<void>(`/api/v1/campaigns/${id}/pause`, { method: 'POST' }),
  resume: (id: string) => fetchAPI<void>(`/api/v1/campaigns/${id}/resume`, { method: 'POST' }),
  abort: (id: string) => fetchAPI<void>(`/api/v1/campaigns/${id}/abort`, { method: 'POST' }),
  retryFailed: (id: string) => fetchAPI<void>(`/api/v1/campaigns/${id}/retry-failed`, { method: 'POST' }),
}

// CAP Files
export const capsAPI = {
  list: () => fetchAPI<PaginatedResponse<any>>('/api/v1/caps'),
  get: (id: string) => fetchAPI<any>(`/api/v1/caps/${id}`),
  upload: (formData: FormData) => fetchAPI<any>('/api/v1/caps/upload', { method: 'POST', body: formData }),
  delete: (id: string) => fetchAPI<void>(`/api/v1/caps/${id}`, { method: 'DELETE' }),
  previewAPDUs: (id: string, maxBlockSize?: number) => fetchAPI<any>(`/api/v1/caps/${id}/apdu-preview`, { params: maxBlockSize ? { max_block_size: maxBlockSize.toString() } : undefined }),
}

// Scripts
export const scriptsAPI = {
  list: () => fetchAPI<PaginatedResponse<any>>('/api/v1/scripts'),
  get: (id: string) => fetchAPI<any>(`/api/v1/scripts/${id}`),
  create: (data: any) => fetchAPI<any>('/api/v1/scripts', { method: 'POST', body: data }),
  update: (id: string, data: any) => fetchAPI<any>(`/api/v1/scripts/${id}`, { method: 'PUT', body: data }),
  delete: (id: string) => fetchAPI<void>(`/api/v1/scripts/${id}`, { method: 'DELETE' }),
}

// Dashboard
export const dashboardAPI = {
  getKPIs: () => fetchAPI<any>('/api/v1/dashboard/kpis'),
  getActivity: () => fetchAPI<any[]>('/api/v1/dashboard/activity'),
  getSMSThroughput: () => fetchAPI<any[]>('/api/v1/dashboard/sms-throughput'),
}

// Monitoring
export const monitoringAPI = {
  getHealth: () => fetchAPI<any>('/api/v1/monitoring/health'),
  listMessages: (params?: Record<string, string>) => fetchAPI<PaginatedResponse<any>>('/api/v1/monitoring/messages', { params }),
  getMessage: (id: string) => fetchAPI<any>(`/api/v1/monitoring/messages/${id}`),
  getErrors: () => fetchAPI<any>('/api/v1/monitoring/errors'),
}
