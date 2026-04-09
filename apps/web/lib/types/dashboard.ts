export interface DashboardFilters {
  years: number[];
  buyers: string[];
  planning_regions: string[];
  categories: string[];
}

export interface AppliedFilters {
  year: number | null;
  planning_region: string | null;
  multi_lot: boolean | null;
  buyer: string | null;
  category: string | null;
}

export interface RankedEntity {
  name: string;
  project_count: number;
  awarded_sum_eur: number;
}

export interface YearlyStat {
  year: number;
  projects: number;
  awarded_sum_eur: number;
  estimated_sum_eur: number;
  multi_lot_projects: number;
  multi_lot_share_pct: number | null;
}

export interface BidderEntry {
  name: string;
  applications: number;
  wins: number;
  losses: number;
  win_rate_pct: number | null;
  total_bid_value_eur: number;
  won_value_eur: number;
  buyers_count: number;
  top_category: string | null;
  close_losses_3pct: number;
  avg_loss_gap_pct: number | null;
}

export interface QualityItem {
  field: string;
  label: string;
  missing_count: number;
  coverage_pct: number | null;
}

export interface BuyerConcentration {
  name: string;
  project_count: number;
  awarded_sum_eur: number;
  top_winner: string;
  top_winner_projects: number;
  top_winner_share_pct: number | null;
  top_winner_amount_share_pct: number | null;
}

export interface DashboardData {
  filters: DashboardFilters;
  applied_filters: AppliedFilters;
  overview: {
    projects_in_scope: number;
    total_awarded_sum_eur: number;
    total_estimated_sum_eur: number;
    awarded_projects_count: number;
    winners_count: number;
    multi_lot_projects_count: number;
    multi_lot_share_pct: number | null;
    average_bidders: number | null;
    single_bidder_share_pct: number | null;
    buyers_count: number;
    regions_count: number;
  };
  market_concentration: {
    top5_awarded_share_pct: number | null;
    hhi: number | null;
    note: string;
  };
  yearly_series: YearlyStat[];
  top_winners: RankedEntity[];
  top_buyers: RankedEntity[];
  top_regions: RankedEntity[];
  categories: RankedEntity[];
  cpv_codes: RankedEntity[];
  category_summary: {
    design: { project_count: number; awarded_sum_eur: number };
    build: { project_count: number; awarded_sum_eur: number };
    design_build: { project_count: number; awarded_sum_eur: number };
  };
  bidder_leaderboard: BidderEntry[];
  biggest_winners: BidderEntry[];
  biggest_winners_by_value: BidderEntry[];
  biggest_losers: BidderEntry[];
  close_losses: BidderEntry[];
  buyer_concentration: BuyerConcentration[];
  multi_lot: {
    projects: number;
    share_pct: number | null;
    known_total_lots: number;
    average_lots_per_multi_lot_project: number | null;
  };
  data_quality: QualityItem[];
}

export interface CompanyData {
  filters: DashboardFilters;
  companies: string[];
  selected_company: string | null;
  selected_companies: string[];
  summary?: {
    applications: number;
    wins: number;
    losses: number;
    win_rate_pct: number | null;
    won_value_eur: number;
    avg_losing_gap_pct: number | null;
    avg_winning_margin_pct: number | null;
    close_losses_3pct: number;
    buyers_count: number;
    categories_count: number;
  };
  our_fit?: {
    buyers: Array<{ name: string; count: number }>;
    categories: Array<{ name: string; count: number }>;
    regions: Array<{ name: string; count: number }>;
    cpv_codes: Array<{ name: string; count: number }>;
    size_bands: Array<{ name: string; count: number }>;
    segments: Array<{ category: string; region: string; cpv: string; bids: number; wins: number; win_rate_pct: number | null; avg_competitors: number | null }>;
  };
  competitors?: {
    met_most: Array<{ name: string; meet_count: number; beat_us: number; we_beat: number; avg_gap_pct: number | null }>;
    beat_us: Array<{ name: string; meet_count: number; beat_us: number; we_beat: number; avg_gap_pct: number | null }>;
    we_beat: Array<{ name: string; meet_count: number; beat_us: number; we_beat: number; avg_gap_pct: number | null }>;
  };
  buyers?: {
    best: Array<{ name: string; bids: number; wins: number; win_rate_pct: number | null; won_value_eur: number; market_concentration_pct: number | null }>;
    targets: Array<{ name: string; bids: number; wins: number; win_rate_pct: number | null; won_value_eur: number; market_concentration_pct: number | null }>;
  };
}

export interface PurchaserData {
  filters: DashboardFilters;
  purchasers: string[];
  selected_purchaser: string | null;
  summary?: {
    projects: number;
    awarded_sum_eur: number;
    estimated_sum_eur: number;
    suppliers_count: number;
    avg_competitors: number | null;
    single_bidder_share_pct: number | null;
    top_supplier_share_pct: number | null;
    avg_decision_lag_days: number | null;
  };
  fit?: {
    categories: Array<{ name: string; count: number }>;
    regions: Array<{ name: string; count: number }>;
    cpv_codes: Array<{ name: string; count: number }>;
    years: Array<{ name: string; count: number }>;
    evaluation_methods: Array<{ name: string; count: number }>;
  };
  suppliers?: {
    top_winners: Array<{ name: string; project_count: number; awarded_sum_eur: number; win_share_pct: number | null }>;
    frequent_bidders: Array<{ name: string; project_count: number; awarded_sum_eur: number; win_count: number; win_rate_pct: number | null }>;
  };
  segments?: {
    biggest: Array<{ category: string; region: string; cpv: string; projects: number; awarded_sum_eur: number; avg_competitors: number | null; single_bidder_share_pct: number | null }>;
    open: Array<{ category: string; region: string; cpv: string; projects: number; awarded_sum_eur: number; avg_competitors: number | null; single_bidder_share_pct: number | null }>;
  };
  market_context?: {
    dominant_region: string | null;
    dominant_category: string | null;
    top_purchasers_region: Array<{ name: string; awarded_sum_eur: number; is_selected: boolean }>;
    top_purchasers_category: Array<{ name: string; awarded_sum_eur: number; is_selected: boolean }>;
    top_companies_region: Array<{ name: string; awarded_sum_eur: number; is_selected: boolean }>;
    top_companies_category: Array<{ name: string; awarded_sum_eur: number; is_selected: boolean }>;
  };
}

export interface RiskData {
  filters: DashboardFilters;
  summary: {
    projects: number;
    single_bidder_count: number;
    single_bidder_share_pct: number | null;
    low_competition_count: number;
    low_competition_share_pct: number | null;
    with_estimate_count: number;
    award_above_estimate_count: number;
    award_above_estimate_share_pct: number | null;
    award_above_estimate_10pct_count: number;
    award_above_estimate_10pct_share_pct: number | null;
    award_below_estimate_20pct_count: number;
    award_below_estimate_20pct_share_pct: number | null;
  };
  winners: Array<{
    name: string;
    project_count: number;
    risky_project_count: number;
    risky_project_share_pct: number | null;
    awarded_sum_eur: number;
    risky_awarded_sum_eur: number;
    single_bidder_wins: number;
    above_estimate_wins: number;
  }>;
  buyers: {
    single_bidder: Array<{ name: string; project_count: number; single_bidder_count: number; awarded_sum_eur: number; single_bidder_share_pct: number }>;
    concentration: Array<{ name: string; project_count: number; top_winner: string; top_winner_projects: number; awarded_sum_eur: number; top_winner_share_pct: number }>;
    risk_hotspots: Array<{ name: string; project_count: number; risky_project_count: number; risky_project_share_pct: number | null; single_bidder_count: number; above_estimate_count: number; awarded_sum_eur: number }>;
  };
  hotspots: {
    regions: Array<{ name: string; project_count: number; risky_project_count: number; risky_project_share_pct: number | null; single_bidder_count: number; above_estimate_count: number; awarded_sum_eur: number }>;
    categories: Array<{ name: string; project_count: number; risky_project_count: number; risky_project_share_pct: number | null; single_bidder_count: number; above_estimate_count: number; awarded_sum_eur: number }>;
  };
  pairs: Array<{ name: string; close_bid_count: number; meet_count: number; lowest_price_close_count: number; close_share_pct: number }>;
}

export interface ProjectsData {
  total: number;
  limit: number;
  offset: number;
  items: Array<{
    procurement_id: string;
    year: number;
    procurement_name: string;
    purchaser_name: string;
    planning_region: string;
    category: string;
    cpv_main: string | null;
    location_bucket: string;
    procurement_status: string;
    winner: string | null;
    awarded_sum_eur: number | null;
    estimated_sum_eur: number | null;
    participants_count: number | null;
    is_multi_lot: boolean;
    lot_count: number | null;
  }>;
}
