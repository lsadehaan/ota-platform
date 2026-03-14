// Security configuration for OTA profiles
export interface Profile {
  id: string
  name: string
  description: string
  card_profile_type: string
  kic_key_id: number
  kid_key_id: number
  kic_algorithm: string
  kid_algorithm: string
  spi1: string
  spi2: string
  tar: string
  counter_management: string
  security_domain_aid: string
  applications: Application[]
  created_at: string
  updated_at: string
}

export interface Application {
  id: string
  profile_id: string
  name: string
  aid: string
  package_aid: string
  instance_aid: string
  security_domain_aid: string
  privileges: string
  install_params: string
  cap_file_id: string
  cap_file_name: string
  load_order: number
  created_at: string
  updated_at: string
}

// SIM card representation
export interface Card {
  id: string
  iccid: string
  imsi: string
  msisdn: string
  profile_id: string
  profile_name: string
  kic: string
  kid: string
  status: string
  tags: string[]
  metadata: Record<string, string>
  created_at: string
  updated_at: string
}

export interface CardCounter {
  id: string
  card_id: string
  tar: string
  counter_value: number
  last_response_counter: number
  updated_at: string
}

// Campaign management
export interface Campaign {
  id: string
  name: string
  description: string
  status: CampaignStatus
  type: CampaignType
  profile_id: string
  profile_name: string
  card_group_id: string
  card_group_name: string
  commands: CampaignCommand[]
  total_cards: number
  pending_cards: number
  in_progress_cards: number
  success_cards: number
  failed_cards: number
  progress_percent: number
  max_concurrent: number
  retry_count: number
  retry_delay_seconds: number
  scheduled_at: string
  started_at: string
  completed_at: string
  created_at: string
  updated_at: string
}

export type CampaignStatus = 'draft' | 'scheduled' | 'running' | 'paused' | 'completed' | 'aborted' | 'failed'

export type CampaignType = 'install_applet' | 'delete_applet' | 'update_applet' | 'send_script' | 'custom_apdu'

export interface CampaignCommand {
  id: string
  campaign_id: string
  sequence: number
  type: string
  apdu: string
  description: string
  expect_response: boolean
  timeout_seconds: number
}

export interface CampaignCard {
  id: string
  campaign_id: string
  card_id: string
  iccid: string
  msisdn: string
  status: string
  current_command: number
  total_commands: number
  error_message: string
  attempts: number
  started_at: string
  completed_at: string
  updated_at: string
}

// Message logging / monitoring
export interface MessageLog {
  id: string
  campaign_id: string
  campaign_card_id: string
  card_id: string
  msisdn: string
  direction: 'outbound' | 'inbound'
  message_type: string
  raw_payload: string
  decoded_payload: string
  status: string
  status_code: number
  error_message: string
  dlr_status: string
  dlr_timestamp: string
  response_payload: string
  response_status_word: string
  transport: string
  sent_at: string
  delivered_at: string
  responded_at: string
  created_at: string
}

// CAP file management
export interface CAPFile {
  id: string
  name: string
  filename: string
  package_aid: string
  applet_aid: string
  version: string
  size: number
  sha256: string
  component_count: number
  components: CAPComponent[]
  uploaded_at: string
  created_at: string
}

export interface CAPComponent {
  name: string
  size: number
  tag: string
}

// Script management
export interface Script {
  id: string
  name: string
  description: string
  language: string
  content: string
  parameters: ScriptParameter[]
  created_at: string
  updated_at: string
}

export interface ScriptParameter {
  name: string
  type: string
  required: boolean
  default_value: string
  description: string
}

// Card groups
export interface CardGroup {
  id: string
  name: string
  description: string
  query: string
  is_dynamic: boolean
  card_count: number
  created_at: string
  updated_at: string
}

// Dashboard
export interface DashboardKPIs {
  total_cards: number
  active_campaigns: number
  messages_today: number
  success_rate: number
  cards_by_status: Record<string, number>
  campaigns_by_status: Record<string, number>
}

export interface SMSThroughputPoint {
  timestamp: string
  sent: number   // avg TPS for the minute bucket
  delivered: number
  failed: number
}

export interface ActivityEvent {
  id: string
  type: string
  description: string
  entity_type: string
  entity_id: string
  user: string
  timestamp: string
  metadata: Record<string, any>
}

// System health / monitoring
export interface SystemHealth {
  status: string
  uptime_seconds: number
  version: string
  components: HealthComponent[]
}

export interface HealthComponent {
  name: string
  status: string
  details: Record<string, any>
  latency_ms: number
}

export interface ErrorSummary {
  total_errors_24h: number
  errors_by_type: Record<string, number>
  errors_by_hour: ErrorsByHour[]
  recent_errors: RecentError[]
}

export interface ErrorsByHour {
  hour: string
  count: number
}

export interface RecentError {
  id: string
  type: string
  message: string
  card_id: string
  campaign_id: string
  timestamp: string
  stack_trace: string
}

// Paginated response wrapper
export interface PaginatedResponse<T> {
  data: T[]
  total: number
  page: number
  page_size: number
  total_pages: number
}
