export interface TenderResult {
  procurement_id: string;
  title: string;
  buyer: string;
  region: string;
  cpv_main: string;
  estimated_value_eur?: number;
  publication_date: string;
  submission_deadline: string;
  status: string;
  procedure_type: string;
  eis_url: string;
  match_reason: string;
}

export interface InterpretedProfile {
  category: string;
  cpv_prefixes: string[];
  region: string;
  keywords: string[];
}

export interface SearchResponse {
  query: string;
  interpreted_profile: InterpretedProfile;
  filters: Record<string, unknown>;
  results: TenderResult[];
  total_count: number;
}

export interface ActiveFilter {
  key: string;
  label: string;
  value: string;
}

export interface AdvancedFilters {
  keywords: string;
  exactPhrase: boolean;
  noInflections: boolean;
  category: string;
  cpvGroup: string;
  cpvCode: string;
  titleKeywords: string;
  publicationFrom: string;
  publicationTo: string;
  deadlineFrom: string;
  deadlineTo: string;
  status: string;
  procedureType: string;
  legalBasis: string;
  subjectType: string;
  purchaser: string;
  purchaserKeyword: string;
  buyerName: string;
  procurementId: string;
  sortPrimary: string;
}

export const defaultAdvancedFilters: AdvancedFilters = {
  keywords: "",
  exactPhrase: false,
  noInflections: false,
  category: "",
  cpvGroup: "",
  cpvCode: "",
  titleKeywords: "",
  publicationFrom: "",
  publicationTo: "",
  deadlineFrom: "",
  deadlineTo: "",
  status: "",
  procedureType: "",
  legalBasis: "",
  subjectType: "",
  purchaser: "",
  purchaserKeyword: "",
  buyerName: "",
  procurementId: "",
  sortPrimary: "",
};

// --- Chat types ---

export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  quick_replies?: string[];
  filter_summary?: Record<string, string>;
  isTyping?: boolean;
  questionKey?: string;
}

export interface ExtractedFilters {
  keywords?: string[];
  category?: string;
  cpv_prefixes?: string[];
  cpv_code?: string;
  planning_region?: string;
  status?: string;
  procedure_type?: string;
  subject_type?: "works" | "supplies" | "services";
  value_min_eur?: number;
  value_max_eur?: number;
  deadline_days?: number;
  sort?: "relevance" | "date_desc" | "deadline" | "value_desc";
  similar_companies?: string[];
  similar_company_cpv_prefixes?: string[];
  buyer?: string;
  pub_date_from?: string;
  pub_date_to?: string;
}

export interface ChatState {
  messages: ChatMessage[];
  isWaiting: boolean;
  isSearching: boolean;
  extractedFilters: ExtractedFilters | null;
  sessionId: string | null;
  pendingCompanyResolve?: {
    remaining: string[];
    mergedFilters: ExtractedFilters;
  } | null;
}

export interface ChatRequest {
  messages: Array<{ role: "user" | "assistant"; content: string }>;
  my_company?: { name: string; cpv_prefixes: string[] };
}

export interface ChatResponseQuestion {
  type: "question";
  message: string;
  question_key?: string;
  quick_replies?: string[];
  filter_summary?: Record<string, string>;
  session_id?: string;
}

export interface ChatResponseSearchReady {
  type: "search_ready";
  message: string;
  filters: ExtractedFilters;
  session_id?: string;
}

export type ChatResponse = ChatResponseQuestion | ChatResponseSearchReady;

export interface SearchRequest {
  filters: ExtractedFilters;
}

// --- My Activity types ---

export interface ActivityItem {
  procurement_id: string;
  title: string;
  buyer: string;
  cpv_main: string;
  submission_deadline: string;
  estimated_value_eur?: number;
  contract_value_eur?: number;
  eis_url: string;
  status: string;
  signed_date?: string;
}

export interface ActivityStats {
  total_participations: number;
  total_wins: number;
  win_rate: number;
  total_won_value_eur: number;
}

export interface MyActivityResponse {
  company: string;
  participations: ActivityItem[];
  wins: ActivityItem[];
  stats: ActivityStats;
}

// --- Saved Filters types ---

export interface SavedFilter {
  id: string;
  name: string;
  filters: ExtractedFilters;
  created_at: string;
  last_seen_pub_date?: string;
}
