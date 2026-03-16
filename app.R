# ========================================================================
# UNIFIED PROCUREMENT ANALYSIS APP
# ========================================================================
# Combines:
#   - Economic Outcomes Analysis  (econ_app.R + econ_out_utils.R)
#   - Administrative Efficiency   (admin_app.R + admin_utils.R)
#
# Single CSV upload feeds both pipelines.
# Filters are independent per section.
# ========================================================================

# options(expressions = 10000)  # removed - can cause stack overflow
options(shiny.maxRequestSize = 1000 * 1024^2)
options(shiny.sanitize.errors = FALSE)
options(warn = 1)

library(shiny)
library(shinydashboard)
library(shinyWidgets)
library(DT)
library(ggplot2)
library(dplyr)
library(tidyr)
library(data.table)
library(scales)
library(officer)
library(flextable)
library(rmarkdown)
library(plotly)
library(patchwork)
library(corrr)
library(tidytext)
library(fixest)
library(ggeffects)
library(igraph)
library(ggraph)
library(purrr)
library(ggrepel)
library(giscoR)
library(eurostat)
library(sf)
library(kableExtra)
library(zip)

# ── Source utility files ─────────────────────────────────────────────────
source("utils_shared.R")

econ_utils_loaded <- tryCatch({ source("econ_out_utils.R"); TRUE },
                              error = function(e) { warning("econ_out_utils.R not found: ", e$message); FALSE })
integrity_utils_loaded <- tryCatch({ source("integrity_utils.R"); TRUE },
                                   error = function(e) { warning("integrity_utils.R not found: ", e$message); FALSE })
admin_utils_loaded <- tryCatch({ source("admin_utils.R"); TRUE },
                               error = function(e) { warning("admin_utils.R not found: ", e$message); FALSE })

if (!econ_utils_loaded) {
  run_economic_efficiency_pipeline <- function(...) stop("utils_econ.R not loaded.")
  build_cpv_lookup <- function(...) NULL
}
if (!admin_utils_loaded) {
  run_admin_efficiency_pipeline <- function(...) stop("utils_admin.R not loaded.")
  get_admin_thresholds <- function(...) list(subm_short_open=30, subm_short_restricted=30,
                                             subm_short_negotiated=30, subm_medium_open_min=NA,
                                             subm_medium_open_max=NA, long_decision_days=60)
}
if (!integrity_utils_loaded) {
  # run_integrity_pipeline_fast stub removed — use run_integrity_pipeline_fast_local directly
  create_pipeline_config        <- function(cc) list(country_code = cc)
  prepare_data                  <- function(df, ...) df
  analyze_missing_values        <- function(...) NULL
  analyze_interoperability      <- function(...) NULL
  analyze_competition           <- function(...) NULL
  analyze_markets               <- function(...) NULL
  analyze_prices                <- function(...) NULL
  safely_run_module             <- function(fn, ...) tryCatch(fn(...), error = function(e) NULL)
}

# ── Patch utils functions to use a self-contained recode ────────────────
# Both econ_out_utils.R and admin_utils.R define their own recode_procedure_type
# with different (wrong) mappings for bare "NEGOTIATED". Rather than relying on
# global-environment name-lookup ordering, we patch every function in those
# utils files that calls recode_procedure_type so they each carry the correct
# mapping in their own closure.  This is called immediately after source().

.CANONICAL_RECODE <- function(x) {
  LUT <- c(
    OPEN="Open Procedure", RESTRICTED="Restricted Procedure",
    NEGOTIATED_WITH_PUBLICATION="Negotiated with publications",
    NEGOTIATED_WITHOUT_PUBLICATION="Negotiated without publications",
    NEGOTIATED="Negotiated (unspecified)",
    COMPETITIVE_DIALOG="Competitive Dialogue",
    INNOVATION_PARTNERSHIP="Innovation Partnership",
    OUTRIGHT_AWARD="Direct Award", OTHER="Other",
    # pass-through already-recoded labels
    "Open Procedure"="Open Procedure",
    "Restricted Procedure"="Restricted Procedure",
    "Negotiated with publications"="Negotiated with publications",
    "Negotiated without publications"="Negotiated without publications",
    "Negotiated (unspecified)"="Negotiated (unspecified)",
    "Negotiated"="Negotiated (unspecified)",
    "Competitive Dialogue"="Competitive Dialogue",
    "Competitive Dialog"="Competitive Dialogue",
    "Innovation Partnership"="Innovation Partnership",
    "Direct Award"="Direct Award",
    "Other"="Other", "Other Procedures"="Other"
  )
  raw <- as.character(x)
  idx <- match(raw, names(LUT))
  out <- LUT[idx]
  out[is.na(idx)] <- "Other"
  out[is.na(x)]   <- NA_character_
  unname(out)
}

# Patch each utils function that calls recode_procedure_type so it uses
# .CANONICAL_RECODE from its own closure — immune to global env ordering.
if (exists("build_proc_share_data")) {
  .orig_bpsd <- body(build_proc_share_data)
  build_proc_share_data <- function(df) {
    df %>%
      dplyr::mutate(
        tender_proceduretype = .CANONICAL_RECODE(tender_proceduretype),
        tender_proceduretype = forcats::fct_explicit_na(
          as.factor(tender_proceduretype), na_level = "Missing value")) %>%
      dplyr::group_by(tender_proceduretype) %>%
      dplyr::summarise(
        total_value  = sum(bid_priceusd, na.rm = TRUE),
        n_contracts  = dplyr::n(), .groups = "drop") %>%
      dplyr::mutate(
        share_value     = total_value / sum(total_value),
        share_contracts = n_contracts / sum(n_contracts))
  }
}




# Global recode_procedure_type — delegates to .CANONICAL_RECODE once that
# is defined (after source() calls and the patch block).
# .CANONICAL_RECODE is defined in the patch block below; this wrapper
# simply calls it so all code paths use the same lookup table.
recode_procedure_type <- function(x) .CANONICAL_RECODE(x)

# ── Currency conversion constant ─────────────────────────────────────────
# BGN is pegged: 1 EUR = 1.95583 BGN. Using approximate 1 USD = 1.85 BGN.
# Update .BGN_PER_USD to reflect current rate if needed.
.BGN_PER_USD <- 1.85

# Shared value filter widget builder — used by econ, admin, integrity.
# Returns a tagList with a BGN/USD radio and a 0-to-max slider.
# currency_input_id: shiny input ID for the radio (e.g. "econ_value_currency")
# slider_input_id  : shiny input ID for the slider  (e.g. "econ_value_range")
# prices           : numeric vector of raw USD values from the data
# divisor_rv       : a function(div) to store the resolved divisor reactively
make_value_filter_widget <- function(prices, currency_input_id, slider_input_id,
                                     current_currency = "USD") {
  prices <- prices[!is.na(prices) & is.finite(prices) & prices > 0]
  if (length(prices) == 0) return(NULL)
  
  rate  <- if (current_currency == "BGN") .BGN_PER_USD else 1
  p_cur <- prices * rate
  max_p <- quantile(p_cur, 0.99, na.rm = TRUE)
  cur_sym <- if (current_currency == "BGN") "BGN" else "USD"
  
  # Slider in M for coarse dragging
  max_m  <- ceiling(max_p / 1e6)
  # Precise inputs in K
  max_k  <- ceiling(max_p / 1e3)
  
  tagList(
    radioButtons(currency_input_id, NULL,
                 choices = c("USD", "BGN"), selected = current_currency, inline = TRUE),
    # Coarse slider (millions)
    tags$div(style = "margin-bottom:4px;",
             tags$small(style = "color:#64748B;",
                        paste0("Drag to set rough range (", cur_sym, ", millions):")),
             sliderInput(paste0(slider_input_id, "_coarse"),
                         NULL,
                         min = 0, max = max(1, max_m), value = c(0, max(1, max_m)),
                         step = max(1, round(max_m / 50)),
                         ticks = FALSE, sep = ",",
                         pre = if (current_currency == "BGN") "" else "$",
                         post = "M")
    ),
    # Precise numeric inputs (thousands)
    tags$div(style = "margin-top:4px;",
             tags$small(style = "color:#64748B;",
                        paste0("Or enter exact values (", cur_sym, ", thousands):")),
             fluidRow(
               column(6,
                      numericInput(paste0(slider_input_id, "_min_k"),
                                   "From (K):", value = 0, min = 0, max = max_k, step = 1)),
               column(6,
                      numericInput(paste0(slider_input_id, "_max_k"),
                                   "To (K):",   value = max_k, min = 0, max = max_k, step = 1))
             )
    )
  )
}


# ── Load CPV codes ───────────────────────────────────────────────────────
cpv_lookup_global <- NULL
tryCatch({
  cpv_table <- fread("cpv_codes.csv", encoding = "UTF-8", stringsAsFactors = FALSE)
  if (econ_utils_loaded && "CODE" %in% names(cpv_table) && "EN" %in% names(cpv_table))
    cpv_lookup_global <- build_cpv_lookup(cpv_table, code_col = "CODE", label_col = "EN")
}, error = function(e) warning("CPV codes not loaded: ", e$message))



# ========================================================================
# CONSTANTS (Admin section)
# ========================================================================

PROC_TYPE_LABELS <- c(
  "Open Procedure",
  "Restricted Procedure",
  "Negotiated with publications",
  "Negotiated without publications",
  "Negotiated (unspecified)",
  "Competitive Dialogue",
  "Innovation Partnership",
  "Direct Award",
  "Other"
)

CUTOFF_CHOICES <- c(
  "Tukey fence (Q3 + 1.5×IQR)" = "iqr",
  "75th percentile (Q3)"       = "p75",
  "80th percentile"            = "p80",
  "85th percentile"            = "p85",
  "90th percentile"            = "p90",
  "95th percentile"            = "p95",
  "99th percentile"            = "p99",
  "Mean"                       = "mean",
  "Median"                     = "median"
)

ALL_PROC_TYPES <- list(
  list(id = "open",        label = "Open Procedure",                  default = 30,  medium = TRUE,  med_min = 30, med_max = 60),
  list(id = "restricted",  label = "Restricted Procedure",            default = 30,  medium = TRUE,  med_min = NA, med_max = NA),
  list(id = "neg_pub",     label = "Negotiated with publications",    default = 30,  medium = TRUE,  med_min = NA, med_max = NA),
  list(id = "neg_nopub",   label = "Negotiated without publications", default = NA,  medium = FALSE, med_min = NA, med_max = NA),
  list(id = "neg_unspec",  label = "Negotiated (unspecified)",        default = NA,  medium = FALSE, med_min = NA, med_max = NA),
  list(id = "competitive", label = "Competitive Dialogue",            default = NA,  medium = FALSE, med_min = NA, med_max = NA),
  list(id = "innov",       label = "Innovation Partnership",          default = NA,  medium = FALSE, med_min = NA, med_max = NA),
  list(id = "direct",      label = "Direct Award",                   default = NA,  medium = FALSE, med_min = NA, med_max = NA),
  list(id = "other",       label = "Other",                          default = NA,  medium = FALSE, med_min = NA, med_max = NA)
)

ALL_DEC_PROC_TYPES <- list(
  list(id = "dec_open",        label = "Open Procedure",                  default = 60),
  list(id = "dec_restricted",  label = "Restricted Procedure",            default = 60),
  list(id = "dec_neg_pub",     label = "Negotiated with publications",    default = 60),
  list(id = "dec_neg_nopub",   label = "Negotiated without publications", default = NA),
  list(id = "dec_neg_unspec",  label = "Negotiated (unspecified)",        default = NA),
  list(id = "dec_competitive", label = "Competitive Dialogue",            default = NA),
  list(id = "dec_innov",       label = "Innovation Partnership",          default = NA),
  list(id = "dec_direct",      label = "Direct Award",                   default = NA),
  list(id = "dec_other",       label = "Other",                          default = NA)
)

TUKEY_EXPLANATION <- div(
  class = "alert alert-info",
  style = "font-size:12px; padding:8px 12px; margin-top:6px;",
  icon("info-circle"),
  tags$strong(" What is the Tukey fence?"),
  " The Tukey fence (also called the 1.5×IQR rule) flags values as outliers when they fall above",
  tags$strong(" Q3 + 1.5 × IQR"), ", where Q3 is the 75th percentile and IQR = Q3 − Q1.",
  " It is a robust, data-driven method that adapts to the spread of each dataset",
  " without assuming a normal distribution."
)

# ========================================================================
# ADMIN HELPER FUNCTIONS
# ========================================================================

compute_outlier_cutoff <- function(x, method = "iqr") {
  x <- x[!is.na(x) & is.finite(x)]
  if (length(x) < 10) return(NA_real_)
  switch(method,
         iqr    = { q <- quantile(x, c(0.25, 0.75)); q[2] + 1.5 * (q[2] - q[1]) },
         p75 = quantile(x, 0.75), p80 = quantile(x, 0.80), p85 = quantile(x, 0.85),
         p90 = quantile(x, 0.90), p95 = quantile(x, 0.95), p99 = quantile(x, 0.99),
         mean = mean(x), median = median(x),
         quantile(x, 0.75))
}

has_any_price_threshold <- function(pt) {
  !is.null(pt) && any(sapply(pt, function(proc)
    any(sapply(proc, function(v) !is.null(v) && !is.na(v) && is.finite(v) && v > 0))))
}

classify_supply <- function(df) {
  if (!"tender_supplytype" %in% names(df)) return(rep("Goods", nrow(df)))
  dplyr::case_when(
    grepl("WORK",        toupper(as.character(df$tender_supplytype))) ~ "Works",
    grepl("SERV",        toupper(as.character(df$tender_supplytype))) ~ "Services",
    grepl("GOODS|SUPPL", toupper(as.character(df$tender_supplytype))) ~ "Goods",
    TRUE ~ "Goods"
  )
}

.PROC_LABEL_TO_KEY <- c(
  "Open Procedure"                  = "open",
  "Restricted Procedure"            = "restricted",
  "Negotiated with publications"    = "neg_pub",
  "Negotiated without publications" = "neg_nopub",
  "Negotiated (unspecified)"        = "neg_unspec",
  "Competitive Dialogue"            = "competitive",
  "Innovation Partnership"          = "innov",
  "Direct Award"                    = "direct"
)
proc_to_key <- function(proc_label) {
  key <- .PROC_LABEL_TO_KEY[as.character(proc_label)]
  key[is.na(key)] <- "other"
  unname(key)
}

# ── Column detection helpers ─────────────────────────────────────────────
.PRICE_COLS_PRIORITY  <- c("bid_priceusd","lot_estimatedpriceusd","tender_finalprice","lot_estimatedprice","bid_price")
.PRICE_COLS_ADMIN     <- c("bid_priceusd","bid_price")
.PRICE_COLS_SUPP      <- c("bid_priceusd","lot_estimatedpriceusd","lot_estimatedprice","bid_price")

detect_price_col <- function(df, candidates = .PRICE_COLS_PRIORITY) {
  candidates[candidates %in% names(df)][1L]  # first match or NA
}


# ========================================================================
# ECON FILTER DATA
# ========================================================================

econ_filter_data <- function(df, year_range = NULL, market = NULL, value_range = NULL,
                             buyer_type = NULL, procedure_type = NULL, value_divisor = 1,
                             buyer_mapping = NULL, procedure_mapping = NULL) {
  filtered <- df
  
  if (!is.null(year_range) && "tender_year" %in% names(df))
    filtered <- filtered %>% filter(tender_year >= year_range[1] & tender_year <= year_range[2])
  
  if (!is.null(market) && length(market) > 0 && "All" %ni% market && "cpv_cluster" %in% names(df))
    filtered <- filtered %>% filter(cpv_cluster %in% market)
  
  if (!is.null(value_range) && !is.null(value_divisor)) {
    # Use actual contract price (bid_priceusd) so filter matches Competition/Admin plots.
    # Falls back to bid_price, then estimated price if USD column absent.
    price_col_f <- if ("bid_priceusd"       %in% names(df)) "bid_priceusd"
    else if ("bid_price"       %in% names(df)) "bid_price"
    else if ("lot_estimatedprice" %in% names(df)) "lot_estimatedprice"
    else NULL
    if (!is.null(price_col_f)) {
      actual_min <- value_range[1] * value_divisor
      actual_max <- value_range[2] * value_divisor
      filtered <- filtered %>%
        filter(!is.na(.data[[price_col_f]])) %>%
        filter(.data[[price_col_f]] >= actual_min & .data[[price_col_f]] <= actual_max)
    }
  }
  
  if (!is.null(buyer_type) && length(buyer_type) > 0 && "All" %ni% buyer_type &&
      "buyer_buyertype" %in% names(df)) {
    if (!is.null(buyer_mapping)) {
      raw_values <- buyer_mapping[buyer_mapping$group %in% buyer_type, "raw"]
      filtered <- filtered %>% filter(buyer_buyertype %in% raw_values)
    } else {
      filtered <- filtered %>% filter(buyer_buyertype %in% buyer_type)
    }
  }
  
  if (!is.null(procedure_type) && length(procedure_type) > 0 && "All" %ni% procedure_type &&
      "tender_proceduretype" %in% names(df)) {
    if (!is.null(procedure_mapping)) {
      raw_values <- procedure_mapping[procedure_mapping$cleaned %in% procedure_type, "raw"]
      filtered <- filtered %>% filter(tender_proceduretype %in% raw_values)
    } else {
      filtered <- filtered %>% filter(tender_proceduretype %in% procedure_type)
    }
  }
  # Ensure single_bid stays numeric after any join/filter operations
  if ("single_bid" %in% names(filtered))
    filtered$single_bid <- as.numeric(as.character(filtered$single_bid))
  # Ensure price_bin stays ordered factor
  if ("price_bin" %in% names(filtered) && !is.ordered(filtered$price_bin))
    filtered$price_bin <- factor(filtered$price_bin, ordered = TRUE)
  return(filtered)
}

# ========================================================================
# ADMIN FILTER DATA
# ========================================================================

admin_filter_data <- function(df, year_range = NULL, market = NULL, value_range = NULL,
                              buyer_type = NULL, procedure_type = NULL, value_divisor = 1,
                              procedure_mapping = NULL) {
  filtered_df <- df
  
  if (!is.null(year_range)) {
    year_col <- if ("tender_year" %in% names(df)) "tender_year"
    else if ("year" %in% names(df)) "year"
    else if ("cal_year" %in% names(df)) "cal_year" else NULL
    if (!is.null(year_col))
      filtered_df <- filtered_df %>%
        filter(.data[[year_col]] >= year_range[1] & .data[[year_col]] <= year_range[2])
  }
  
  if (!is.null(market) && length(market) > 0 && "All" %ni% market && "lot_productcode" %in% names(df))
    filtered_df <- filtered_df %>%
      mutate(cpv_2dig = substr(lot_productcode, 1, 2)) %>%
      filter(cpv_2dig %in% market) %>%
      select(-cpv_2dig)
  
  if (!is.null(value_range) && !is.null(value_divisor)) {
    price_col <- if ("bid_priceusd" %in% names(df)) "bid_priceusd"
    else if ("bid_price" %in% names(df)) "bid_price" else NULL
    if (!is.null(price_col)) {
      actual_min <- value_range[1] * value_divisor
      actual_max <- value_range[2] * value_divisor
      filtered_df <- filtered_df %>%
        filter(!is.na(.data[[price_col]])) %>%
        filter(.data[[price_col]] >= actual_min & .data[[price_col]] <= actual_max)
    }
  }
  
  if (!is.null(buyer_type) && length(buyer_type) > 0 && "All" %ni% buyer_type &&
      "buyer_buyertype" %in% names(df))
    filtered_df <- filtered_df %>%
      mutate(buyer_group = add_buyer_group(buyer_buyertype)) %>%
      filter(as.character(buyer_group) %in% buyer_type) %>%
      select(-buyer_group)
  
  if (!is.null(procedure_type) && length(procedure_type) > 0 && "All" %ni% procedure_type &&
      "tender_proceduretype" %in% names(df)) {
    if (!is.null(procedure_mapping)) {
      raw_values <- procedure_mapping[procedure_mapping$cleaned %in% procedure_type, "raw"]
      filtered_df <- filtered_df %>% filter(tender_proceduretype %in% raw_values)
    } else {
      # fallback: match on recoded column directly
      filtered_df <- filtered_df %>%
        filter(recode_procedure_type(tender_proceduretype) %in% procedure_type)
    }
  }
  
  return(filtered_df)
}

# ========================================================================
# INTEGRITY FILTER DATA
# ========================================================================

integrity_filter_data <- function(df, year_range = NULL, market = NULL, value_range = NULL,
                                  buyer_type = NULL, procedure_type = NULL, value_divisor = 1) {
  filtered_df <- df
  if (!is.null(year_range)) {
    year_col <- if ("tender_year" %in% names(df)) "tender_year"
    else if ("year" %in% names(df)) "year"
    else if ("cal_year" %in% names(df)) "cal_year" else NULL
    if (!is.null(year_col))
      filtered_df <- filtered_df %>%
        dplyr::filter(.data[[year_col]] >= year_range[1] & .data[[year_col]] <= year_range[2])
  }
  if (!is.null(market) && length(market) > 0 && "All" %ni% market && "lot_productcode" %in% names(df))
    filtered_df <- filtered_df %>%
      dplyr::mutate(cpv_2dig = substr(lot_productcode, 1, 2)) %>%
      dplyr::filter(cpv_2dig %in% market) %>%
      dplyr::select(-cpv_2dig)
  if (!is.null(value_range) && !is.null(value_divisor)) {
    price_col <- if ("bid_priceusd" %in% names(df)) "bid_priceusd"
    else if ("bid_price" %in% names(df)) "bid_price" else NULL
    if (!is.null(price_col)) {
      filtered_df <- filtered_df %>%
        dplyr::filter(!is.na(.data[[price_col]])) %>%
        dplyr::filter(.data[[price_col]] >= value_range[1] * value_divisor &
                        .data[[price_col]] <= value_range[2] * value_divisor)
    }
  }
  if (!is.null(buyer_type) && length(buyer_type) > 0 && "All" %ni% buyer_type &&
      "buyer_buyertype" %in% names(df))
    filtered_df <- filtered_df %>%
      dplyr::mutate(buyer_group = add_buyer_group(buyer_buyertype)) %>%
      dplyr::filter(as.character(buyer_group) %in% buyer_type) %>%
      dplyr::select(-buyer_group)
  if (!is.null(procedure_type) && length(procedure_type) > 0 && "All" %ni% procedure_type &&
      "tender_proceduretype" %in% names(df))
    filtered_df <- filtered_df %>%
      dplyr::filter(recode_procedure_type(tender_proceduretype) %in% procedure_type)
  return(filtered_df)
}

get_filter_caption <- function(filters_list) {
  if (is.null(filters_list)) return("")
  desc <- get_filter_description(filters_list)
  if (desc == "No filters applied") return("")
  paste("Filters Applied:", desc)
}

# ========================================================================
# UI HELPER: PROC THRESHOLD BLOCK
# ========================================================================

proc_threshold_ui <- function(proc_id, proc_label, default_days,
                              show_medium = FALSE, med_min = NA, med_max = NA,
                              is_decision = FALSE) {
  field_id_no_thr  <- paste0("no_thr_",         proc_id)
  field_id_days    <- paste0("thr_days_",        proc_id)
  field_id_outlier <- paste0("outlier_method_",  proc_id)
  field_id_med_min <- paste0("thr_med_min_",     proc_id)
  field_id_med_max <- paste0("thr_med_max_",     proc_id)
  field_id_no_med  <- paste0("no_medium_",       proc_id)
  
  tagList(
    h5(style = "margin-top:10px; font-weight:bold; color:#2c3e50;", proc_label),
    checkboxInput(field_id_no_thr, "No legal threshold (derive statistically)", value = is.na(default_days)),
    conditionalPanel(
      condition = paste0("!input.", field_id_no_thr),
      numericInput(field_id_days,
                   if (is_decision) "Long threshold (days)" else "Short threshold (days)",
                   value = if (is.na(default_days)) 30 else default_days, min = 1, step = 1)
    ),
    conditionalPanel(
      condition = paste0("input.", field_id_no_thr),
      selectInput(field_id_outlier, "Derive cutoff using:", choices = CUTOFF_CHOICES, selected = "iqr")
    ),
    if (show_medium) tagList(
      hr(style = "margin: 6px 0;"),
      checkboxInput(field_id_no_med, "No medium band", value = TRUE),
      conditionalPanel(
        condition = paste0("!input.", field_id_no_med),
        fluidRow(
          column(6, numericInput(field_id_med_min, "Medium band min (days)",
                                 value = if (is.na(med_min)) 30 else med_min, min = 1, step = 1)),
          column(6, numericInput(field_id_med_max, "Medium band max (days)",
                                 value = if (is.na(med_max)) 60 else med_max, min = 1, step = 1))
        )
      )
    )
  )
}

# ========================================================================
# UI HELPER: FILTER BAR (namespaced with prefix)
# ========================================================================

# filter_bar_ui: section = "econ" or "admin", tab = short tab name e.g. "market", "proc"
# Generates IDs that match server registrations:
#   econ_year_filter_market, econ_apply_filters_market, etc.
filter_bar_ui <- function(section, tab) {
  p <- paste0(section, "_")    # prefix: "econ_" or "admin_"
  tagList(
    fluidRow(
      column(2, uiOutput(paste0(p, "year_filter_",           tab))),
      column(2, uiOutput(paste0(p, "market_filter_",         tab))),
      column(2, uiOutput(paste0(p, "value_filter_",          tab))),
      column(3, uiOutput(paste0(p, "buyer_type_filter_",     tab))),
      column(3, uiOutput(paste0(p, "procedure_type_filter_", tab)))
    ),
    fluidRow(
      column(12,
             actionButton(paste0(p, "apply_filters_", tab), "Apply Filters",
                          icon = icon("filter"), class = "btn-primary"),
             actionButton(paste0(p, "reset_filters_",  tab), "Reset Filters",
                          icon = icon("undo"),   class = "btn-warning"),
             textOutput(paste0(p, "filter_status_", tab), inline = TRUE)
      )
    )
  )
}

# ========================================================================
# ECON: REGENERATE PLOTS FOR EXPORT
# ========================================================================

econ_regenerate_plots <- function(filtered_data) {
  plots <- list()
  tryCatch({
    price_var <- detect_price_col(filtered_data)
    market_summary <- summarise_market_size(filtered_data, value_col = price_var)
    plots$market_size_n  <- plot_market_contract_counts(market_summary)
    plots$market_size_v  <- plot_market_total_value(market_summary)
    plots$market_size_av <- plot_market_bubble(market_summary)
    if (all(c("bidder_masterid","tender_year","cpv_cluster") %in% names(filtered_data))) {
      supplier_stats <- compute_supplier_entry(filtered_data)
      plots$supplier_stats     <- supplier_stats
      plots$suppliers_entrance <- plot_supplier_shares_heatmap(supplier_stats)
      plots$unique_supp        <- plot_unique_suppliers_heatmap(filtered_data)
    }
    if (all(c("bid_price","lot_estimatedprice") %in% names(filtered_data))) {
      rp <- filtered_data %>% add_relative_price()
      # rel_tot: use custom density logic so percentages are correct (not old util)
      plots$rel_tot <- tryCatch({
        rp_col <- "relative_price"
        if (rp_col %in% names(rp)) {
          v <- rp[[rp_col]]; v <- v[!is.na(v)&is.finite(v)&v>0]; n_total <- length(v)
          if (n_total > 5) {
            n_under <- sum(v<0.999); n_at <- sum(v>=0.999&v<=1.001); n_over <- sum(v>1.001)
            pu <- round(n_under/n_total*100,1); pa2 <- round(n_at/n_total*100,1)
            po <- round(n_over/n_total*100,1) + (100-(round(n_under/n_total*100,1)+round(n_at/n_total*100,1)+round(n_over/n_total*100,1)))
            med_rp <- median(v); xr <- quantile(v,c(0.005,0.995))
            dens <- density(v,from=max(0,xr[1]),to=xr[2],n=512)
            df_d <- data.frame(x=dens$x,y=dens$y)
            stxt <- paste0("Under budget: ",pu,"% | At budget: ",pa2,"% | Over budget: ",po,"%  (n=",scales::comma(n_total),")")
            plotly::plot_ly() %>%
              plotly::add_trace(data=df_d%>%dplyr::filter(x<=1),x=~x,y=~y,type="scatter",mode="none",fill="tozeroy",fillcolor="rgba(0,105,180,0.25)",name="Under budget",hoverinfo="skip") %>%
              plotly::add_trace(data=df_d%>%dplyr::filter(x>=1),x=~x,y=~y,type="scatter",mode="none",fill="tozeroy",fillcolor="rgba(180,0,0,0.20)",name="Over budget",hoverinfo="skip") %>%
              plotly::add_trace(data=df_d,x=~x,y=~y,type="scatter",mode="lines",line=list(color="#334155",width=2),name="Density",hoverinfo="text",text=stxt) %>%
              plotly::layout(xaxis=list(title="Relative price"),yaxis=list(title="Density"),
                             shapes=list(list(type="line",x0=1,x1=1,y0=0,y1=1,yref="paper",line=list(color="#888",width=1.5,dash="dash")),
                                         list(type="line",x0=med_rp,x1=med_rp,y0=0,y1=1,yref="paper",line=list(color="#D97706",width=1.5,dash="dot"))),
                             annotations=list(
                               list(x=(xr[1]+1)/2,y=0.5,yref="paper",xanchor="center",text=paste0("<b>",pu,"%</b><br>under budget"),showarrow=FALSE,font=list(size=11,color="#0069B4")),
                               list(x=1.02,y=0.5,yref="paper",xanchor="left",text=paste0("<b>",pa2,"%</b><br>at budget"),showarrow=FALSE,font=list(size=11,color="#475569")),
                               list(x=(1+xr[2])/2,y=0.5,yref="paper",xanchor="center",text=paste0("<b>",po,"%</b><br>over budget"),showarrow=FALSE,font=list(size=11,color="#B40000"))),
                             paper_bgcolor="#ffffff",plot_bgcolor="#ffffff",
                             legend=list(orientation="h",y=-0.15),margin=list(l=60,r=20,t=20,b=60))
          }
        }
      }, error=function(e) NULL)
      if ("tender_year" %in% names(rp)) plots$rel_year <- plot_relative_price_by_year(rp)
      if ("cpv_cluster" %in% names(rp) || "cpv_category" %in% names(rp)) {
        # Apply same get_cpv_label relabelling as the screen render
        if ("cpv_cluster" %in% names(rp))
          rp <- rp %>% dplyr::mutate(cpv_category = get_cpv_label(cpv_cluster))
        top_m <- top_markets_by_relative_price(rp, n=10)
        plots$rel_10 <- tryCatch(plot_top_markets_relative_price(rp, top_m), error=function(e) NULL)
      }
      if ("buyer_name" %in% names(rp)) {
        top_b <- top_buyers_by_relative_price(rp, min_contracts=10, n=20)
        if (nrow(top_b) > 0) plots$rel_buy <- plot_top_buyers_relative_price(top_b, label_max_chars=30)
      }
    }
    plots$single_bid_overall          <- tryCatch(plot_single_bid_overall(filtered_data),          error = function(e) NULL)
    plots$single_bid_by_procedure     <- tryCatch(plot_single_bid_by_procedure(filtered_data),     error = function(e) NULL)
    plots$single_bid_by_price         <- tryCatch(plot_single_bid_by_price(filtered_data),         error = function(e) NULL)
    plots$single_bid_by_buyer_group   <- tryCatch(plot_single_bid_by_buyer_group(filtered_data),   error = function(e) NULL)
    plots$single_bid_by_market        <- tryCatch(plot_single_bid_by_market(filtered_data),        error = function(e) NULL)
    plots$top_buyers_single_bid       <- tryCatch(plot_top_buyers_single_bid(filtered_data,
                                                                             buyer_id_col = "buyer_masterid",
                                                                             top_n = 20, min_tenders = 30),                error = function(e) NULL)
  }, error = function(e) message("Plot regen error: ", e$message))
  return(plots)
}

admin_build_word_plots <- function(filtered_data, thresholds, global_proc_filter, subm_cutoffs, dec_cutoffs, price_thresholds = list()) {
  # Build ggplot objects using the SAME logic as the renderPlotly blocks.
  # Arguments mirror the server-scope reactives so the caller can pass live values.
  plots <- list()
  
  tryCatch({
    # ── Helpers ────────────────────────────────────────────────────────
    apply_proc_filter <- function(df) {
      if (is.null(global_proc_filter) || length(global_proc_filter) == 0) return(df)
      pr <- recode_procedure_type(df$tender_proceduretype)
      df[!is.na(pr) & pr %in% global_proc_filter, , drop = FALSE]
    }
    
    # ── Procedure shares ───────────────────────────────────────────────
    plot_data <- tryCatch(build_proc_share_data(filtered_data), error = function(e) NULL)
    if (!is.null(plot_data)) {
      plots$sh <- tryCatch(
        ggplot2::ggplot(plot_data,
                        ggplot2::aes(x = stats::reorder(tender_proceduretype, share_value), y = share_value)) +
          ggplot2::geom_col(fill = "#3c8dbc", width = 0.6) +
          ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                                      expand = ggplot2::expansion(mult = c(0, 0.4))) +
          ggplot2::coord_flip() +
          ggplot2::labs( x = NULL, y = "Share of total value") +
          pa_theme(),
        error = function(e) NULL)
      plots$p_count <- tryCatch(
        ggplot2::ggplot(plot_data,
                        ggplot2::aes(x = stats::reorder(tender_proceduretype, share_value), y = share_contracts)) +
          ggplot2::geom_col(fill = "#3c8dbc", width = 0.6) +
          ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                                      expand = ggplot2::expansion(mult = c(0, 0.4))) +
          ggplot2::coord_flip() +
          ggplot2::labs( x = NULL, y = "Share of contracts") +
          pa_theme(),
        error = function(e) NULL)
    }
    
    # ── Submission period distribution ─────────────────────────────────
    tp_open <- tryCatch(
      compute_tender_days(filtered_data,
                          tender_publications_firstcallfortenderdate,
                          tender_biddeadline, tender_days_open),
      error = function(e) NULL)
    if (!is.null(tp_open)) {
      days_open <- tp_open$tender_days_open[!is.na(tp_open$tender_days_open) &
                                              tp_open$tender_days_open >= 0 &
                                              tp_open$tender_days_open <= 365]
      if (length(days_open) > 1) {
        q_open  <- quantile(days_open, probs = c(0.25, 0.5, 0.75), na.rm = TRUE)
        mu_open <- mean(days_open, na.rm = TRUE)
        plots$subm <- tryCatch(
          ggplot2::ggplot(data.frame(days = days_open), ggplot2::aes(x = days)) +
            ggplot2::geom_histogram(binwidth = 5, fill = PA_NORMAL, colour = "white") +
            ggplot2::geom_vline(xintercept = q_open,
                                colour = c(PA_Q_Q1,PA_Q_MEDIAN,PA_Q_Q1),
                                linetype = c("dashed","solid","dashed"), linewidth = 1) +
            ggplot2::geom_vline(xintercept = mu_open, colour = PA_Q_MEAN,
                                linetype = "dotted", linewidth = 1) +
            ggplot2::coord_cartesian(xlim = c(0, 365)) +
            ggplot2::labs(
              x = "Days from call opening to bid deadline",
              y = "Number of contracts") +
            pa_theme(),
          error = function(e) NULL)
      }
      
      # ── Submission by procedure type ─────────────────────────────────
      tp_open_proc <- apply_proc_filter(tp_open) %>%
        dplyr::mutate(tender_proceduretype = recode_procedure_type(tender_proceduretype)) %>%
        tidyr::drop_na(tender_proceduretype) %>%
        dplyr::filter(tender_days_open >= 0, tender_days_open <= 365)
      
      if (nrow(tp_open_proc) > 0) {
        ql <- tp_open_proc %>%
          dplyr::group_by(tender_proceduretype) %>%
          dplyr::filter(dplyr::n() >= 5) %>%
          dplyr::summarise(Q1 = quantile(tender_days_open, 0.25, na.rm = TRUE),
                           Median = quantile(tender_days_open, 0.50, na.rm = TRUE),
                           Q3 = quantile(tender_days_open, 0.75, na.rm = TRUE),
                           Mean = mean(tender_days_open, na.rm = TRUE), .groups = "drop") %>%
          tidyr::pivot_longer(c(Q1, Median, Q3, Mean), names_to = "stat", values_to = "xintercept")
        plots$subm_proc_facet_q <- tryCatch(
          ggplot2::ggplot(tp_open_proc, ggplot2::aes(x = tender_days_open)) +
            ggplot2::geom_histogram(binwidth = 5, fill = PA_NORMAL, colour = "white", linewidth = 0.3) +
            ggplot2::geom_vline(data = ql,
                                ggplot2::aes(xintercept = xintercept, colour = stat, linetype = stat),
                                linewidth = 0.9) +
            ggplot2::scale_colour_manual(values = c(Q1=PA_Q_Q1,Median=PA_Q_MEDIAN,Q3=PA_Q_Q1,Mean=PA_Q_MEAN),
                                         breaks = c("Q1","Median","Q3","Mean")) +
            ggplot2::scale_linetype_manual(values = c(Q1="dashed",Median="solid",Q3="dashed",Mean="dotted"),
                                           breaks = c("Q1","Median","Q3","Mean")) +
            ggplot2::facet_wrap(~ tender_proceduretype, scales = "free_y", ncol = 3) +
            ggplot2::coord_cartesian(xlim = c(0, 365)) +
            ggplot2::labs(
              x = "Days from call opening to bid deadline",
              y = "Contracts", colour = NULL, linetype = NULL) +
            pa_theme() +
            ggplot2::theme(legend.position = "top",
                           strip.text = ggplot2::element_text(face = "bold", size = 10),
                           panel.spacing=ggplot2::unit(0.4,"cm")),
          error = function(e) NULL)
        
        # ── Short submission deadlines ──────────────────────────────────
        if (!is.null(subm_cutoffs) && nrow(subm_cutoffs) > 0) {
          tp_flagged <- tp_open_proc %>%
            dplyr::left_join(subm_cutoffs, by = "tender_proceduretype") %>%
            dplyr::mutate(
              status = dplyr::case_when(
                tender_days_open < short_cut ~ "Short",
                !no_medium & !is.na(med_min) & !is.na(med_max) &
                  tender_days_open >= med_min & tender_days_open <= med_max ~ "Medium",
                TRUE ~ "Normal"),
              status = factor(status, levels = c("Short","Medium","Normal")))
          share_df <- tp_flagged %>%
            dplyr::group_by(tender_proceduretype, short_cut) %>%
            dplyr::summarise(share_short = mean(status == "Short", na.rm = TRUE), .groups = "drop") %>%
            dplyr::mutate(thr_str = dplyr::if_else(is.na(short_cut), "no threshold",
                                                   paste0("<", round(short_cut), " days")),
                          label = paste0(tender_proceduretype, "\nThreshold: ", thr_str,
                                         " | ", scales::percent(share_short, accuracy = 0.1), " short"))
          binned <- tp_flagged %>%
            dplyr::filter(tender_days_open >= 0, tender_days_open <= 60) %>%
            dplyr::mutate(day_bin = floor(tender_days_open)) %>%
            dplyr::count(tender_proceduretype, day_bin, status) %>%
            dplyr::left_join(share_df %>% dplyr::select(tender_proceduretype, label, short_cut),
                             by = "tender_proceduretype") %>%
            dplyr::mutate(label = factor(label))
          if (nrow(binned) > 0) {
            vline_df <- share_df %>% dplyr::select(label, short_cut) %>% dplyr::distinct() %>%
              dplyr::filter(!is.na(short_cut)) %>%
              dplyr::mutate(label = factor(label, levels = levels(binned$label)))
            plots$subm_r <- tryCatch(
              ggplot2::ggplot(binned, ggplot2::aes(x = day_bin, y = n, fill = status)) +
                ggplot2::geom_col(position = "stack", width = 1) +
                ggplot2::geom_vline(data = vline_df, ggplot2::aes(xintercept = short_cut),
                                    colour = PA_ROSE, linetype = "dashed", linewidth = 0.8) +
                ggplot2::scale_fill_manual(values = c(Short=PA_FLAG, Medium=PA_AMBER, Normal=PA_NORMAL)) +
                ggplot2::facet_wrap(~ label, scales = "free_y") +
                ggplot2::coord_cartesian(xlim = c(0, 60)) +
                ggplot2::labs(
                  x = "Days", y = "Contracts", fill = NULL) +
                pa_theme() +
                ggplot2::theme(legend.position = "top",
                               strip.text = ggplot2::element_text(size = 10, face = "bold"),
                               panel.spacing=ggplot2::unit(0.4,"cm")),
              error = function(e) NULL)
          }
          
          # ── Short by buyer group ──────────────────────────────────────
          tp_buyer <- tp_open_proc %>%
            dplyr::left_join(subm_cutoffs %>% dplyr::select(tender_proceduretype, short_cut),
                             by = "tender_proceduretype") %>%
            dplyr::mutate(short_deadline = tender_days_open < short_cut,
                          buyer_group    = add_buyer_group(buyer_buyertype))
          if (nrow(tp_buyer) > 0) {
            by_count <- tp_buyer %>%
              dplyr::group_by(buyer_group, tender_proceduretype) %>%
              dplyr::summarise(share_short = mean(short_deadline, na.rm = TRUE),
                               n_total = dplyr::n(), .groups = "drop") %>%
              dplyr::mutate(share_other = 1 - share_short, metric = "Count") %>%
              tidyr::pivot_longer(c(share_short, share_other),
                                  names_to = "type", values_to = "share") %>%
              dplyr::mutate(label = factor(dplyr::if_else(type == "share_short","Short","Normal"),
                                           levels = c("Normal","Short")))
            plots$buyer_short <- tryCatch(
              ggplot2::ggplot(by_count, ggplot2::aes(x = buyer_group, y = share, fill = label)) +
                ggplot2::geom_col(position = "stack", width = 0.7) +
                ggplot2::scale_fill_manual(values = c(Short=PA_FLAG, Normal=PA_NORMAL)) +
                ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                                            expand = ggplot2::expansion(mult = c(0, 0.02))) +
                ggplot2::facet_wrap(~ tender_proceduretype) +
                ggplot2::labs(
                  x = NULL, y = "Share", fill = NULL) +
                pa_theme() +
                ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 35, hjust = 1),
                               legend.position = "top",
                               strip.text = ggplot2::element_text(face = "bold", size = 10),
                               panel.spacing=ggplot2::unit(0.4,"cm")),
              error = function(e) NULL)
          }
        }
      }
    }
    
    # ── Decision period distribution ────────────────────────────────────
    df_dec <- filtered_data
    if (!"tender_contractsignaturedate" %in% names(df_dec))
      df_dec <- df_dec %>% dplyr::mutate(tender_contractsignaturedate = as.Date(NA))
    tp_dec <- tryCatch(
      df_dec %>%
        dplyr::mutate(decision_end_date = dplyr::coalesce(
          as.Date(tender_contractsignaturedate), as.Date(tender_awarddecisiondate))) %>%
        compute_tender_days(tender_biddeadline, decision_end_date, tender_days_dec),
      error = function(e) NULL)
    
    if (!is.null(tp_dec)) {
      days_dec <- tp_dec$tender_days_dec[!is.na(tp_dec$tender_days_dec) &
                                           tp_dec$tender_days_dec >= 0 &
                                           tp_dec$tender_days_dec <= 730]
      if (length(days_dec) > 1) {
        q_dec  <- quantile(days_dec, probs = c(0.25, 0.5, 0.75), na.rm = TRUE)
        mu_dec <- mean(days_dec, na.rm = TRUE)
        plots$decp <- tryCatch(
          ggplot2::ggplot(data.frame(days = days_dec), ggplot2::aes(x = days)) +
            ggplot2::geom_histogram(binwidth = 10, fill = PA_NORMAL, colour = "white") +
            ggplot2::geom_vline(xintercept = q_dec,
                                colour = c(PA_Q_Q1,PA_Q_MEDIAN,PA_Q_Q1),
                                linetype = c("dashed","solid","dashed"), linewidth = 1) +
            ggplot2::geom_vline(xintercept = mu_dec, colour = PA_Q_MEAN,
                                linetype = "dotted", linewidth = 1) +
            ggplot2::coord_cartesian(xlim = c(0, 730)) +
            ggplot2::labs(
              x = "Days from bid deadline to contract award",
              y = "Number of contracts") +
            pa_theme(),
          error = function(e) NULL)
      }
      
      # ── Decision by procedure type ──────────────────────────────────
      tp_dec_proc <- apply_proc_filter(tp_dec) %>%
        dplyr::mutate(tender_proceduretype = recode_procedure_type(tender_proceduretype)) %>%
        tidyr::drop_na(tender_proceduretype) %>%
        dplyr::filter(tender_days_dec >= 0, tender_days_dec <= 730)
      
      if (nrow(tp_dec_proc) > 0) {
        ql_dec <- tp_dec_proc %>%
          dplyr::group_by(tender_proceduretype) %>%
          dplyr::filter(dplyr::n() >= 5) %>%
          dplyr::summarise(Q1 = quantile(tender_days_dec, 0.25, na.rm = TRUE),
                           Median = quantile(tender_days_dec, 0.50, na.rm = TRUE),
                           Q3 = quantile(tender_days_dec, 0.75, na.rm = TRUE),
                           Mean = mean(tender_days_dec, na.rm = TRUE), .groups = "drop") %>%
          tidyr::pivot_longer(c(Q1, Median, Q3, Mean), names_to = "stat", values_to = "xintercept")
        plots$decp_proc_facet_q <- tryCatch(
          ggplot2::ggplot(tp_dec_proc, ggplot2::aes(x = tender_days_dec)) +
            ggplot2::geom_histogram(binwidth = 10, fill = PA_NORMAL, colour = "white", linewidth = 0.3) +
            ggplot2::geom_vline(data = ql_dec,
                                ggplot2::aes(xintercept = xintercept, colour = stat, linetype = stat),
                                linewidth = 0.9) +
            ggplot2::scale_colour_manual(values = c(Q1=PA_Q_Q1,Median=PA_Q_MEDIAN,Q3=PA_Q_Q1,Mean=PA_Q_MEAN),
                                         breaks = c("Q1","Median","Q3","Mean")) +
            ggplot2::scale_linetype_manual(values = c(Q1="dashed",Median="solid",Q3="dashed",Mean="dotted"),
                                           breaks = c("Q1","Median","Q3","Mean")) +
            ggplot2::facet_wrap(~ tender_proceduretype, scales = "free_y", ncol = 3) +
            ggplot2::coord_cartesian(xlim = c(0, 730)) +
            ggplot2::labs(
              x = "Days from bid deadline to contract award",
              y = "Contracts", colour = NULL, linetype = NULL) +
            pa_theme() +
            ggplot2::theme(legend.position = "top",
                           strip.text = ggplot2::element_text(face = "bold", size = 10),
                           panel.spacing=ggplot2::unit(0.4,"cm")),
          error = function(e) NULL)
        
        # ── Long decision flags ─────────────────────────────────────────
        if (!is.null(dec_cutoffs) && nrow(dec_cutoffs) > 0) {
          tp_long <- tp_dec_proc %>%
            dplyr::left_join(dec_cutoffs, by = "tender_proceduretype") %>%
            dplyr::mutate(long_decision = tender_days_dec >= long_cut)
          share_df_dec <- tp_long %>%
            dplyr::group_by(tender_proceduretype, long_cut) %>%
            dplyr::summarise(share_long = mean(long_decision, na.rm = TRUE), .groups = "drop") %>%
            dplyr::mutate(label = paste0(tender_proceduretype,
                                         "\nThreshold: \u2265", round(long_cut), " days | ",
                                         scales::percent(share_long, accuracy = 0.1), " long"))
          binned_dec <- tp_long %>%
            dplyr::filter(tender_days_dec >= 0, tender_days_dec <= 300) %>%
            dplyr::mutate(day_bin = floor(tender_days_dec / 4) * 4,
                          status  = factor(dplyr::if_else(long_decision, "Long", "Normal"),
                                           levels = c("Long","Normal"))) %>%
            dplyr::count(tender_proceduretype, day_bin, status) %>%
            dplyr::left_join(share_df_dec %>% dplyr::select(tender_proceduretype, label, long_cut),
                             by = "tender_proceduretype") %>%
            dplyr::mutate(label = factor(label))
          if (nrow(binned_dec) > 0) {
            vline_dec <- share_df_dec %>% dplyr::select(label, long_cut) %>% dplyr::distinct() %>%
              dplyr::mutate(label = factor(label, levels = levels(binned_dec$label)))
            plots$decp_r <- tryCatch(
              ggplot2::ggplot(binned_dec, ggplot2::aes(x = day_bin, y = n, fill = status)) +
                ggplot2::geom_col(position = "stack", width = 4) +
                ggplot2::geom_vline(data = vline_dec, ggplot2::aes(xintercept = long_cut),
                                    colour = PA_ROSE, linetype = "dashed", linewidth = 0.8) +
                ggplot2::scale_fill_manual(values = c(Long=PA_FLAG, Normal=PA_NORMAL)) +
                ggplot2::facet_wrap(~ label, scales = "free_y") +
                ggplot2::coord_cartesian(xlim = c(0, 300)) +
                ggplot2::labs(
                  x = "Days", y = "Contracts", fill = NULL) +
                pa_theme() +
                ggplot2::theme(legend.position = "top",
                               strip.text = ggplot2::element_text(size = 10, face = "bold"),
                               panel.spacing=ggplot2::unit(0.4,"cm")),
              error = function(e) NULL)
          }
          
          # ── Long by buyer group ─────────────────────────────────────
          tp_buyer_long <- tp_long %>%
            dplyr::mutate(buyer_group = add_buyer_group(buyer_buyertype))
          if (nrow(tp_buyer_long) > 0) {
            by_count_long <- tp_buyer_long %>%
              dplyr::group_by(buyer_group, tender_proceduretype) %>%
              dplyr::summarise(share_long = mean(long_decision, na.rm = TRUE),
                               n_total = dplyr::n(), .groups = "drop") %>%
              dplyr::mutate(share_other = 1 - share_long, metric = "Count") %>%
              tidyr::pivot_longer(c(share_long, share_other),
                                  names_to = "type", values_to = "share") %>%
              dplyr::mutate(label = factor(dplyr::if_else(type == "share_long","Long","Normal"),
                                           levels = c("Normal","Long")))
            plots$buyer_long <- tryCatch(
              ggplot2::ggplot(by_count_long, ggplot2::aes(x = buyer_group, y = share, fill = label)) +
                ggplot2::geom_col(position = "stack", width = 0.7) +
                ggplot2::scale_fill_manual(values = c(Long=PA_FLAG, Normal=PA_NORMAL)) +
                ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                                            expand = ggplot2::expansion(mult = c(0, 0.02))) +
                ggplot2::facet_wrap(~ tender_proceduretype) +
                ggplot2::labs(
                  x = NULL, y = "Share", fill = NULL) +
                pa_theme() +
                ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 35, hjust = 1),
                               legend.position = "top",
                               strip.text = ggplot2::element_text(face = "bold", size = 10),
                               panel.spacing=ggplot2::unit(0.4,"cm")),
              error = function(e) NULL)
          }
        }
      }
    }
    
    # ── Bunching (contract value distribution near thresholds) ───────────
    if (has_any_price_threshold(price_thresholds) && "bid_price" %in% names(filtered_data)) {
      message("[bunching] price_thresholds has ", length(price_thresholds), " proc groups; bid_price present")
      tryCatch({
        proc_label_map <- c(open="Open Procedure", restricted="Restricted Procedure",
                            neg_pub="Negotiated with publications", neg_nopub="Negotiated without publications",
                            neg="Negotiated Procedure", competitive="Competitive Dialogue",
                            innov="Innovation Partnership", direct="Direct Award", other="Other")
        supply_map <- c(goods="Goods", works="Works", services="Services")
        all_thr <- list()
        for (pk in names(price_thresholds)) for (sk in names(price_thresholds[[pk]])) {
          v <- price_thresholds[[pk]][[sk]]
          if (!is.null(v) && !is.na(v) && is.finite(v) && v > 0 &&
              pk %in% names(proc_label_map) && sk %in% names(supply_map)) {
            key <- paste0(sk, "_", round(v))
            if (is.null(all_thr[[key]]))
              all_thr[[key]] <- list(supply_label = supply_map[[sk]], threshold = v,
                                     log_thr = log10(v), proc_labels = proc_label_map[[pk]])
            else
              all_thr[[key]]$proc_labels <- paste0(all_thr[[key]]$proc_labels, ", ", proc_label_map[[pk]])
          }
        }
        if (length(all_thr) > 0) {
          df_b <- filtered_data %>%
            dplyr::mutate(supply_grp = classify_supply(.)) %>%
            dplyr::filter(!is.na(bid_price), bid_price > 1) %>%
            dplyr::mutate(log_val = log10(bid_price))
          bin_size <- 0.05; show_win <- 3.0
          panels <- unname(all_thr)
          panel_plots <- lapply(panels, function(pn) {
            d_win <- df_b %>% dplyr::filter(supply_grp == pn$supply_label,
                                            log_val >= pn$log_thr - show_win,
                                            log_val <= pn$log_thr + show_win)
            if (nrow(d_win) < 15) return(NULL)
            breaks <- seq(pn$log_thr - show_win, pn$log_thr + show_win + bin_size, by = bin_size)
            h      <- graphics::hist(d_win$log_val, breaks = breaks, plot = FALSE)
            df_h   <- data.frame(x = h$breaks[-length(h$breaks)] + bin_size/2, y = h$counts)
            excl   <- abs(df_h$x - pn$log_thr) <= (10 * bin_size)
            fit_df <- df_h[!excl, ]
            df_h$expected <- NA_real_
            if (nrow(fit_df) >= 8) {
              fit <- tryCatch(lm(y ~ poly(x, 4), data = fit_df), error = function(e) NULL)
              if (!is.null(fit)) df_h$expected <- pmax(predict(fit, newdata = df_h), 0)
            }
            df_h$below_win <- df_h$x < pn$log_thr & df_h$x >= (pn$log_thr - 10 * bin_size)
            df_h$is_bunch  <- df_h$below_win & !is.na(df_h$expected) & df_h$expected > 0 &
              df_h$y > df_h$expected * 1.5
            df_h$fill <- dplyr::case_when(df_h$is_bunch ~ "Bunching", df_h$below_win ~ "Near threshold", TRUE ~ "Normal")
            df_h$fill <- factor(df_h$fill, levels = c("Normal", "Near threshold", "Bunching"))
            tick_at  <- seq(ceiling((pn$log_thr - show_win) / 0.5) * 0.5,
                            floor((pn$log_thr + show_win) / 0.5) * 0.5, by = 0.5)
            tick_at  <- sort(unique(c(tick_at, pn$log_thr)))
            tick_lbl <- sapply(tick_at, function(v)
              if (abs(v - pn$log_thr) < 0.001) paste0(fmt_value(10^v), " ★") else fmt_value(10^v))
            p <- ggplot2::ggplot(df_h, ggplot2::aes(x = x, y = y, fill = fill)) +
              ggplot2::geom_col(width = bin_size * 0.9) +
              ggplot2::scale_fill_manual(values = c(Normal = "#5dade2",
                                                    "Near threshold" = "#f0b27a",
                                                    Bunching = "#e74c3c")) +
              ggplot2::geom_vline(xintercept = pn$log_thr, colour = "#922b21",
                                  linewidth = 1.2, linetype = "solid") +
              ggplot2::scale_x_continuous(breaks = tick_at, labels = tick_lbl) +
              ggplot2::labs(title = paste0(pn$supply_label, " — Threshold: ", fmt_value(pn$threshold)),
                            x = "Contract value (log scale)", y = "Contracts", fill = NULL) +
              pa_theme() +
              ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 30, hjust = 1, size = 8),
                             legend.position = "bottom", legend.text = ggplot2::element_text(size = 8))
            if (!all(is.na(df_h$expected))) {
              df_line <- df_h[!is.na(df_h$expected), ]
              p <- p + ggplot2::geom_line(ggplot2::aes(x = x, y = expected), colour = "#1c2833",
                                          linewidth = 0.9, linetype = "dashed", inherit.aes = FALSE,
                                          data = df_line)
            }
            p
          })
          panel_plots <- Filter(Negate(is.null), panel_plots)
          if (length(panel_plots) > 0) {
            message("[bunching] assigning ", length(panel_plots), " panel plot(s) to plots$bunching")
            plots$bunching <- panel_plots
          }
        }
      }, error = function(e) {
        message("[bunching] ggplot build ERROR: ", e$message)
      })
    }
    
    # Regressions — too slow to rerun; taken from stored filtered_analysis
    plots$plot_short_reg <- NULL
    plots$plot_long_reg  <- NULL
    
  }, error = function(e) message("admin_build_word_plots error: ", e$message))
  return(plots)
}


integ_regenerate_plots <- function(filtered_data, country_code) {
  tryCatch(
    run_integrity_pipeline_fast_local(filtered_data, country_code),
    error = function(e) { message("Integ plot regen error: ", e$message); list() }
  )
}


# ========================================================================
# UI
# ========================================================================

# ========================================================================
# WORD REPORT GENERATION FUNCTIONS
# ========================================================================
# Three separate functions: econ, admin, integrity — all same signature.

# ══════════════════════════════════════════════════════════════════════
# PROCUREMENT ANALYTICS — GLOBAL PLOT THEME & PALETTE
# ══════════════════════════════════════════════════════════════════════
PA_NAVY   <- "#0F1F3D"; PA_NAVY2  <- "#1A3160"
PA_TEAL   <- "#00897B"; PA_AMBER  <- "#D97706"
PA_ROSE   <- "#DC2626"; PA_SLATE  <- "#475569"
PA_SLATE2 <- "#94A3B8"; PA_GREY   <- "#E2E8F0"
PA_NORMAL <- "#5B8DB8"   # neutral bars
PA_FLAG   <- PA_AMBER    # short/long flagged bars
PA_SEVERE <- PA_ROSE     # severe / bunching
PA_Q_Q1     <- PA_NAVY2  # Q1/Q3 quantile lines
PA_Q_MEDIAN <- PA_ROSE   # median line
PA_Q_MEAN   <- PA_TEAL   # mean line

pa_theme <- function(base_size = 11) {
  ggplot2::theme_minimal(base_size = base_size) %+replace%
    ggplot2::theme(
      text             = ggplot2::element_text(colour = PA_SLATE),
      plot.title       = ggplot2::element_blank(),
      plot.subtitle    = ggplot2::element_blank(),
      plot.caption     = ggplot2::element_text(size = base_size - 1, colour = PA_SLATE2,
                                               hjust = 0, margin = ggplot2::margin(t = 6)),
      axis.title       = ggplot2::element_text(size = base_size, colour = PA_SLATE),
      axis.text        = ggplot2::element_text(size = base_size, colour = PA_SLATE),
      axis.line        = ggplot2::element_line(colour = PA_GREY, linewidth = 0.4),
      axis.ticks       = ggplot2::element_line(colour = PA_GREY, linewidth = 0.3),
      panel.grid.major = ggplot2::element_line(colour = "#F1F5F9", linewidth = 0.4),
      panel.grid.minor = ggplot2::element_blank(),
      panel.background = ggplot2::element_rect(fill = "white", colour = NA),
      plot.background  = ggplot2::element_rect(fill = "white", colour = NA),
      strip.text       = ggplot2::element_text(size = base_size, face = "bold",
                                               colour = PA_NAVY),
      strip.background = ggplot2::element_rect(fill = "#F8FAFC", colour = PA_GREY,
                                               linewidth = 0.4),
      legend.text      = ggplot2::element_text(size = base_size, colour = PA_SLATE),
      legend.title     = ggplot2::element_text(size = base_size, face = "bold",
                                               colour = PA_NAVY),
      legend.key.size  = ggplot2::unit(0.85, "lines"),
      legend.background = ggplot2::element_rect(fill = "white", colour = NA),
      plot.margin      = ggplot2::margin(6, 10, 6, 6)
    )
}

# ---------------------------------------------------------------------------
# Post-process a ggplotly object: override every baked-in font size so that
# integrity (and other) plots look correct regardless of when they were built.
# ---------------------------------------------------------------------------
post_process_plotly <- function(p,
                                tick_size   = 10,
                                title_size  = 11,
                                legend_size = 9) {
  # Apply font sizing to axes and legend
  p <- plotly::layout(p,
                      font   = list(size = tick_size),
                      xaxis  = list(tickfont  = list(size = tick_size),
                                    titlefont = list(size = title_size)),
                      yaxis  = list(tickfont  = list(size = tick_size),
                                    titlefont = list(size = title_size)),
                      legend = list(font = list(size = legend_size))
  )
  # Walk all axes (xaxis2, yaxis3 …) present in facets/subplots
  pb <- plotly::plotly_build(p)
  for (nm in names(pb$x$layout)) {
    if (grepl("^[xy]axis", nm)) {
      pb$x$layout[[nm]]$tickfont  <- list(size = tick_size)
      pb$x$layout[[nm]]$titlefont <- list(size = title_size)
    }
  }
  # Size the plot title; keep a small top margin for it
  if (!is.null(pb$x$layout$title$text) && nchar(pb$x$layout$title$text) > 0) {
    pb$x$layout$title$font <- list(size = title_size, color = "#222222")
    pb$x$layout$margin$t   <- max(pb$x$layout$margin$t %||% 30, 40)
  } else {
    # No title — minimise top margin so plot sits flush
    pb$x$layout$margin$t <- 20
  }
  pb
}


# Called as generate_econ_word_report(), generate_admin_word_report(),
# generate_integrity_word_report() to avoid naming collision.
# ========================================================================

generate_econ_word_report <- function(filtered_data, filtered_analysis, country_code,
                                      output_file, filters_text = "") {
  tryCatch({
    doc <- officer::read_docx()
    cw  <- 6.5  # usable page width in inches
    
    h1_  <- function(d, t) officer::body_add_par(d, t, style = "heading 1")
    h2_  <- function(d, t) officer::body_add_par(d, t, style = "heading 2")
    par_ <- function(d, t) officer::body_add_par(d, t, style = "Normal")
    br_  <- function(d)    officer::body_add_par(d, "",  style = "Normal")
    pg_  <- function(d)    officer::body_add_break(d)
    
    add_fig <- function(d, p, aspect = 0.60, label = NULL, max_h = 5.5) {
      if (is.null(p)) return(d)
      tmp <- tempfile(fileext = ".png")
      h   <- min(max_h, cw * aspect)
      tryCatch({
        ggplot2::ggsave(tmp, plot = p, width = cw, height = h, dpi = 180, bg = "white")
        if (!is.null(label)) d <- h2_(d, label)
        d <- officer::body_add_img(d, src = tmp, width = cw, height = h)
        d <- br_(d)
      }, error = function(e) message("add_fig error (", label, "): ", e$message))
      d
    }
    
    n_c <- format(nrow(filtered_data), big.mark = ",")
    n_b <- if ("buyer_masterid"  %in% names(filtered_data)) format(dplyr::n_distinct(filtered_data$buyer_masterid,  na.rm = TRUE), big.mark = ",") else "N/A"
    n_s <- if ("bidder_masterid" %in% names(filtered_data)) format(dplyr::n_distinct(filtered_data$bidder_masterid, na.rm = TRUE), big.mark = ",") else "N/A"
    yr  <- if ("tender_year" %in% names(filtered_data)) {
      paste(min(filtered_data$tender_year, na.rm = TRUE), "–", max(filtered_data$tender_year, na.rm = TRUE))
    } else "N/A"
    
    doc <- h1_(doc, "Economic Outcomes Analysis Report")
    doc <- par_(doc, paste("Country:", country_code))
    doc <- par_(doc, paste("Date:", format(Sys.Date(), "%B %d, %Y")))
    if (nchar(filters_text) > 0) { doc <- h2_(doc, "Applied Filters"); doc <- par_(doc, filters_text) }
    doc <- pg_(doc)
    
    doc <- h1_(doc, "Executive Summary")
    doc <- par_(doc, paste0("Economic outcomes analysis for ", country_code,
                            ": ", n_c, " contracts, ", n_b, " buyers, ",
                            n_s, " suppliers, years ", yr, "."))
    ov <- data.frame(Metric = c("Total Contracts","Unique Buyers","Unique Suppliers","Years Covered"),
                     Value  = c(n_c, n_b, n_s, yr), stringsAsFactors = FALSE)
    ft <- flextable::flextable(ov) %>% flextable::theme_booktabs() %>% flextable::autofit()
    doc <- flextable::body_add_flextable(doc, ft)
    doc <- pg_(doc)
    
    # ── 1. Market Size ─────────────────────────────────────────────────
    doc <- h1_(doc, "1. Market Size")
    doc <- add_fig(doc, filtered_analysis$market_size_n,  aspect = 0.55, label = "Number of Contracts per Market")
    doc <- add_fig(doc, filtered_analysis$market_size_v,  aspect = 0.55, label = "Total Contract Value per Market")
    doc <- add_fig(doc, filtered_analysis$market_size_av, aspect = 0.55, label = "Average Contract Value per Market")
    doc <- pg_(doc)
    
    # ── 2. Relative Prices ─────────────────────────────────────────────
    doc <- h1_(doc, "2. Relative Prices")
    doc <- add_fig(doc, filtered_analysis$rel_tot,  aspect = 0.55, label = "Relative Price — Overall")
    doc <- add_fig(doc, filtered_analysis$rel_year, aspect = 0.55, label = "Relative Price — Over Time")
    doc <- add_fig(doc, filtered_analysis$rel_10,   aspect = 0.55, label = "Relative Price — Top 10 Markets")
    doc <- add_fig(doc, filtered_analysis$rel_buy,  aspect = 0.55, label = "Relative Price — By Buyer")
    doc <- pg_(doc)
    
    # ── 3. Single Bidding ──────────────────────────────────────────────
    doc <- h1_(doc, "3. Single Bidding")
    doc <- add_fig(doc, filtered_analysis$single_bid_overall,          aspect = 0.55, label = "Single Bidding — Overall Rate")
    doc <- add_fig(doc, filtered_analysis$single_bid_by_procedure,     aspect = 0.55, label = "Single Bidding — By Procedure Type")
    doc <- add_fig(doc, filtered_analysis$single_bid_by_buyer_group,   aspect = 0.55, label = "Single Bidding — By Buyer Group")
    doc <- add_fig(doc, filtered_analysis$single_bid_by_market,        aspect = 0.65, label = "Single Bidding — By Market",        max_h = 6)
    doc <- add_fig(doc, filtered_analysis$top_buyers_single_bid,       aspect = 0.65, label = "Top Buyers by Single Bidding Rate", max_h = 6)
    
    doc <- par_(doc, paste0("Report generated on ", format(Sys.Date(), "%B %d, %Y"), "."))
    print(doc, target = output_file)
    TRUE
  }, error = function(e) { message("Econ Word error: ", e$message); FALSE })
}

generate_admin_word_report <- function(filtered_data, filtered_analysis, country_code,
                                       output_file, filters_text = "") {
  tryCatch({
    doc <- officer::read_docx()
    pg_w <- 8.5; pg_h <- 11; margin <- 1
    cw   <- pg_w - 2 * margin  # usable width = 6.5 in
    
    h1_  <- function(d, t) officer::body_add_par(d, t, style = "heading 1")
    h2_  <- function(d, t) officer::body_add_par(d, t, style = "heading 2")
    par_ <- function(d, t) officer::body_add_par(d, t, style = "Normal")
    br_  <- function(d)    officer::body_add_par(d, "",  style = "Normal")
    pg_  <- function(d)    officer::body_add_break(d)
    
    add_fig <- function(d, p, aspect = 0.55, label = NULL, max_h = 5.5) {
      if (is.null(p)) return(d)
      tmp <- tempfile(fileext = ".png")
      h   <- min(max_h, cw * aspect)
      tryCatch({
        ggplot2::ggsave(tmp, plot = p, width = cw, height = h, dpi = 180, bg = "white")
        if (!is.null(label)) d <- h2_(d, label)
        d <- officer::body_add_img(d, src = tmp, width = cw, height = h)
        d <- br_(d)
      }, error = function(e) message("add_fig error: ", e$message))
      d
    }
    
    n_c <- format(nrow(filtered_data), big.mark = ",")
    n_b <- if ("buyer_masterid" %in% names(filtered_data))
      format(dplyr::n_distinct(filtered_data$buyer_masterid, na.rm = TRUE), big.mark = ",")
    else "N/A"
    yr  <- if ("tender_year" %in% names(filtered_data))
      paste(min(filtered_data$tender_year, na.rm = TRUE), "–",
            max(filtered_data$tender_year, na.rm = TRUE))
    else "N/A"
    
    doc <- h1_(doc, "Administrative Efficiency Analysis Report")
    doc <- par_(doc, paste("Country:", country_code))
    doc <- par_(doc, paste("Date:", format(Sys.Date(), "%B %d, %Y")))
    if (nchar(filters_text) > 0) {
      doc <- h2_(doc, "Applied Filters"); doc <- par_(doc, filters_text)
    }
    doc <- pg_(doc)
    
    doc <- h1_(doc, "Executive Summary")
    doc <- par_(doc, paste0(
      "Administrative efficiency analysis for ", country_code, ". Dataset: ",
      n_c, " contracts, ", n_b, " unique buyers, years ", yr, ". ",
      "Analysis covers procedure type distribution, submission deadline compliance, ",
      "contract value bunching near thresholds, decision period lengths, and regression results."))
    
    ov  <- data.frame(Metric = c("Total Contracts", "Unique Buyers", "Years Covered"),
                      Value  = c(n_c, n_b, yr), stringsAsFactors = FALSE)
    ft  <- flextable::flextable(ov) %>% flextable::theme_booktabs() %>% flextable::autofit()
    doc <- flextable::body_add_flextable(doc, ft)
    doc <- pg_(doc)
    
    # ── Procedure Types ──────────────────────────────────────────────
    doc <- h1_(doc, "1. Procedure Types")
    doc <- par_(doc, "Distribution of procurement contracts by procedure type.")
    doc <- add_fig(doc, filtered_analysis$sh,      aspect = 0.50, label = "Share of Contract Value by Procedure Type")
    doc <- add_fig(doc, filtered_analysis$p_count, aspect = 0.50, label = "Share of Contract Count by Procedure Type")
    
    # ── Submission Periods ────────────────────────────────────────────
    doc <- pg_(doc)
    doc <- h1_(doc, "2. Submission Periods")
    doc <- par_(doc, paste0(
      "Days between call for tenders publication and bid deadline. ",
      "Short deadlines may reduce competition."))
    doc <- add_fig(doc, filtered_analysis$subm,              aspect = 0.55, label = "Overall Submission Period Distribution")
    doc <- add_fig(doc, filtered_analysis$subm_proc_facet_q, aspect = 0.70, label = "Submission Periods by Procedure Type",  max_h = 6)
    doc <- add_fig(doc, filtered_analysis$subm_r,            aspect = 0.70, label = "Short vs Normal Submission Deadlines",   max_h = 6)
    doc <- add_fig(doc, filtered_analysis$buyer_short,       aspect = 0.70, label = "Short Deadlines by Buyer Group",         max_h = 6)
    
    # ── Contract Value Bunching ───────────────────────────────────────
    bunching     <- filtered_analysis$bunching
    bunching_fig <- filtered_analysis$bunching_fig_fallback
    has_bunching <- (!is.null(bunching) && length(bunching) > 0) ||
      !is.null(bunching_fig)
    if (has_bunching) {
      doc <- pg_(doc)
      doc <- h1_(doc, "3. Contract Value Bunching Near Thresholds")
      doc <- par_(doc, paste0(
        "Histogram of contract values near procedure-type value thresholds. ",
        "Red bars indicate possible bunching (observed counts exceed counterfactual by ≥50%). ",
        "The dashed line shows the counterfactual expected distribution."))
      if (!is.null(bunching) && length(bunching) > 0) {
        # Preferred: ggplot panels (one per supply-type threshold)
        for (i in seq_along(bunching)) {
          doc <- add_fig(doc, bunching[[i]], aspect = 0.55,
                         label = if (i == 1) "Bunching Analysis" else NULL)
        }
      } else if (!is.null(bunching_fig)) {
        # Fallback: render the stored plotly figure via webshot2
        tryCatch({
          tmp_html <- tempfile(fileext = ".html")
          tmp_png  <- tempfile(fileext = ".png")
          htmlwidgets::saveWidget(bunching_fig, tmp_html, selfcontained = TRUE)
          webshot2::webshot(tmp_html, tmp_png, vwidth = 1400, vheight = 900, delay = 2, zoom = 2)
          if (file.exists(tmp_png) && file.size(tmp_png) > 1000) {
            doc <- h2_(doc, "Bunching Analysis")
            doc <- officer::body_add_img(doc, src = tmp_png, width = cw, height = cw * 0.6)
            doc <- br_(doc)
          }
          unlink(c(tmp_html, tmp_png))
        }, error = function(e) message("Bunching webshot fallback error: ", e$message))
      }
    }
    
    # ── Decision Periods ──────────────────────────────────────────────
    doc <- pg_(doc)
    doc <- h1_(doc, "4. Decision Periods")
    doc <- par_(doc, paste0(
      "Days from bid deadline to contract award. ",
      "Long decision periods may indicate administrative bottlenecks."))
    doc <- add_fig(doc, filtered_analysis$decp,               aspect = 0.55, label = "Overall Decision Period Distribution")
    doc <- add_fig(doc, filtered_analysis$decp_proc_facet_q,  aspect = 0.70, label = "Decision Periods by Procedure Type",   max_h = 6)
    doc <- add_fig(doc, filtered_analysis$decp_r,             aspect = 0.70, label = "Long vs Normal Decision Periods",       max_h = 6)
    doc <- add_fig(doc, filtered_analysis$buyer_long,         aspect = 0.70, label = "Long Decision Periods by Buyer Group",  max_h = 6)
    
    # ── Regression Analysis ───────────────────────────────────────────
    if (!is.null(filtered_analysis$plot_short_reg) || !is.null(filtered_analysis$plot_long_reg)) {
      doc <- pg_(doc)
      doc <- h1_(doc, "5. Regression Analysis")
      doc <- par_(doc, "Multivariate regression estimates for submission and decision period determinants.")
      doc <- add_fig(doc, filtered_analysis$plot_short_reg, aspect = 0.75, label = "Short Submission Regression Results", max_h = 6.5)
      doc <- add_fig(doc, filtered_analysis$plot_long_reg,  aspect = 0.75, label = "Long Decision Regression Results",    max_h = 6.5)
    }
    
    doc <- par_(doc, paste0("Report generated on ", format(Sys.Date(), "%B %d, %Y"), "."))
    print(doc, target = output_file)
    TRUE
  }, error = function(e) { message("Admin Word error: ", e$message); FALSE })
}


generate_integrity_word_report <- function(filtered_data, filtered_analysis, country_code,
                                           output_file, filters_text = "") {
  tryCatch({
    doc <- officer::read_docx()
    pg_w <- 8.5; margin <- 1
    cw   <- pg_w - 2 * margin  # 6.5 in
    
    h1_  <- function(d, t) officer::body_add_par(d, t, style = "heading 1")
    h2_  <- function(d, t) officer::body_add_par(d, t, style = "heading 2")
    par_ <- function(d, t) officer::body_add_par(d, t, style = "Normal")
    br_  <- function(d)    officer::body_add_par(d, "",  style = "Normal")
    pg_  <- function(d)    officer::body_add_break(d)
    
    add_fig <- function(d, p, aspect = 0.60, label = NULL, max_h = 5.5) {
      if (is.null(p)) return(d)
      tmp <- tempfile(fileext = ".png")
      h   <- min(max_h, cw * aspect)
      tryCatch({
        ggplot2::ggsave(tmp, plot = p, width = cw, height = h, dpi = 180, bg = "white")
        if (!is.null(label)) d <- h2_(d, label)
        d <- officer::body_add_img(d, src = tmp, width = cw, height = h)
        d <- br_(d)
      }, error = function(e) message("add_fig error (", label, "): ", e$message))
      d
    }
    
    n_c  <- format(nrow(filtered_data), big.mark = ",")
    n_b  <- if ("buyer_masterid"  %in% names(filtered_data)) format(dplyr::n_distinct(filtered_data$buyer_masterid,  na.rm = TRUE), big.mark = ",") else "N/A"
    n_s  <- if ("bidder_masterid" %in% names(filtered_data)) format(dplyr::n_distinct(filtered_data$bidder_masterid, na.rm = TRUE), big.mark = ",") else "N/A"
    yrs  <- if ("tender_year" %in% names(filtered_data)) {
      y <- sort(unique(filtered_data$tender_year[!is.na(filtered_data$tender_year)]))
      if (length(y) > 1) paste0(min(y), "–", max(y)) else as.character(y[1])
    } else "N/A"
    
    doc <- h1_(doc, "Procurement Integrity Analysis Report")
    doc <- par_(doc, paste("Country:", country_code))
    doc <- par_(doc, paste("Date:", format(Sys.Date(), "%B %d, %Y")))
    if (nchar(filters_text) > 0) { doc <- h2_(doc, "Applied Filters"); doc <- par_(doc, filters_text) }
    doc <- pg_(doc)
    
    # ── 1. Executive Summary ──────────────────────────────────────────
    doc <- h1_(doc, "1. Executive Summary")
    doc <- par_(doc, paste0(
      "Procurement integrity assessment for ", country_code, ". Covers ", n_c,
      " contracts, ", n_b, " buyers, ", n_s, " suppliers over years ", yrs, "."))
    overall_miss <- filtered_analysis$missing$overall_long
    high_miss <- if (!is.null(overall_miss)) sum(overall_miss$missing_share >= 0.20, na.rm = TRUE) else NA
    mod_miss  <- if (!is.null(overall_miss)) sum(overall_miss$missing_share >= 0.05 & overall_miss$missing_share < 0.20, na.rm = TRUE) else NA
    if (!is.na(high_miss))
      doc <- par_(doc, paste0("• Data Quality: ", high_miss, " variable(s) >20% missing; ", mod_miss, " in 5–20% range."))
    conc_data <- filtered_analysis$competition$concentration_yearly_data
    if (!is.null(conc_data) && nrow(conc_data) > 0) {
      max_conc <- max(conc_data$max_conc, na.rm = TRUE)
      doc <- par_(doc, paste0("• Concentration: highest buyer-year concentration ", scales::percent(max_conc, accuracy = 1), "."))
    }
    unusual_mat <- filtered_analysis$markets$unusual_matrix
    if (!is.null(unusual_mat) && nrow(unusual_mat) > 0)
      doc <- par_(doc, paste0("• Unusual Market Entries: ", nrow(unusual_mat), " cross-market routes detected."))
    doc <- pg_(doc)
    
    # ── 2. Data Overview ──────────────────────────────────────────────
    doc <- h1_(doc, "2. Data Overview")
    ov  <- data.frame(Indicator = c("Total contracts","Unique buyers","Unique suppliers","Years covered"),
                      Value     = c(n_c, n_b, n_s, yrs), stringsAsFactors = FALSE)
    ft_ov <- flextable::flextable(ov) %>% flextable::theme_booktabs() %>% flextable::autofit()
    doc   <- flextable::body_add_flextable(doc, ft_ov); doc <- br_(doc)
    
    if (!is.null(overall_miss) && nrow(overall_miss) > 0) {
      doc <- h2_(doc, "Variable Completeness")
      miss_tbl <- overall_miss %>%
        dplyr::arrange(dplyr::desc(missing_share)) %>%
        dplyr::mutate(Variable = variable,
                      `Missing Share` = scales::percent(missing_share, accuracy = 0.1),
                      Severity = dplyr::case_when(missing_share >= 0.20 ~ "High (>20%)",
                                                  missing_share >= 0.05 ~ "Moderate", TRUE ~ "Low (<5%)")) %>%
        dplyr::select(Variable, `Missing Share`, Severity)
      ft_miss <- flextable::flextable(as.data.frame(miss_tbl)) %>% flextable::theme_booktabs() %>% flextable::autofit()
      doc <- flextable::body_add_flextable(doc, ft_miss)
    }
    doc <- pg_(doc)
    
    # ── 3. Missing Values Analysis ────────────────────────────────────
    doc <- h1_(doc, "3. Missing Values Analysis")
    doc <- add_fig(doc, filtered_analysis$missing$overall_plot,    aspect = 0.70, label = "Overall Missing Values",           max_h = 6)
    doc <- add_fig(doc, filtered_analysis$missing$by_buyer_plot,   aspect = 0.65, label = "Missing Values by Buyer Type",     max_h = 5.5)
    doc <- add_fig(doc, filtered_analysis$missing$by_procedure_plot, aspect = 0.65, label = "Missing Values by Procedure Type", max_h = 5.5)
    doc <- add_fig(doc, filtered_analysis$missing$by_year_plot,    aspect = 0.55, label = "Missing Values Over Time",         max_h = 4.5)
    if (!is.null(filtered_analysis$missing$cooccurrence_plot))
      doc <- add_fig(doc, filtered_analysis$missing$cooccurrence_plot, aspect = 0.65, label = "Co-occurrence of Missing Values", max_h = 5.5)
    if (!is.null(filtered_analysis$missing$mar_plot))
      doc <- add_fig(doc, filtered_analysis$missing$mar_plot,      aspect = 0.65, label = "MAR Pattern Analysis",             max_h = 5.5)
    doc <- pg_(doc)
    
    # ── 4. Interoperability ───────────────────────────────────────────
    doc <- h1_(doc, "4. Interoperability")
    org_miss <- filtered_analysis$interoperability$org_missing
    if (!is.null(org_miss) && nrow(org_miss) > 0) {
      tbl <- org_miss %>%
        dplyr::mutate(`Missing Share` = scales::percent(missing_share, accuracy = 0.1)) %>%
        dplyr::select(`Organization Type` = organization_type, `ID Type` = id_type, `Missing Share`)
      ft_org <- flextable::flextable(as.data.frame(tbl)) %>% flextable::theme_booktabs() %>% flextable::autofit()
      doc <- flextable::body_add_flextable(doc, ft_org)
    } else {
      doc <- par_(doc, "Interoperability data not available.")
    }
    doc <- pg_(doc)
    
    # ── 5. Market Competition & Unusual Entry Analysis ────────────────
    doc <- h1_(doc, "5. Market Competition & Unusual Market Entry Analysis")
    
    # Supplier & market unusual plots (available after network analysis)
    if (!is.null(filtered_analysis$markets$supplier_unusual_plot))
      doc <- add_fig(doc, filtered_analysis$markets$supplier_unusual_plot, aspect = 0.60,
                     label = "Unusual Supplier Entries", max_h = 5)
    if (!is.null(filtered_analysis$markets$market_unusual_plot))
      doc <- add_fig(doc, filtered_analysis$markets$market_unusual_plot,   aspect = 0.60,
                     label = "Most-Affected Markets",    max_h = 5)
    
    # Flow matrix heatmap
    if (!is.null(unusual_mat) && nrow(unusual_mat) > 0) {
      tryCatch({
        edges <- unusual_mat %>%
          dplyr::rename(from = home_cpv_cluster, to = target_cpv_cluster) %>%
          dplyr::filter(n_bidders >= 4, from != to)
        top_clusters <- edges %>%
          tidyr::pivot_longer(c(from, to), values_to = "cluster") %>%
          dplyr::count(cluster, wt = n_bidders, sort = TRUE) %>%
          dplyr::slice_head(n = 20) %>% dplyr::pull(cluster)
        df_mat <- edges %>%
          dplyr::filter(from %in% top_clusters, to %in% top_clusters) %>%
          dplyr::mutate(from = factor(from, levels = rev(top_clusters)),
                        to   = factor(to,   levels = top_clusters))
        if (nrow(df_mat) > 0) {
          n_cl      <- length(top_clusters)
          txt_sz    <- max(3.2, min(5.5, 56 / max(n_cl, 1)))
          axis_sz   <- max(7,   min(11,  110 / max(n_cl, 1)))
          h_mat     <- min(6.5, max(3.5, n_cl * 0.28 + 1.2))
          p_mat <- ggplot2::ggplot(df_mat, ggplot2::aes(x = to, y = from, fill = n_bidders)) +
            ggplot2::geom_tile(colour = "white", linewidth = 0.5) +
            ggplot2::geom_text(ggplot2::aes(label = n_bidders), size = txt_sz, fontface = "bold") +
            ggplot2::scale_fill_gradientn(
              colours = c("#f0f7ff","#93c6e0","#2471a3","#1a5276"),
              na.value = "grey95", name = "Suppliers
crossing") +
            ggplot2::scale_x_discrete(position = "top") +
            ggplot2::labs(
              x = "↓ Target market", y = "Home market →") +
            pa_theme() +
            ggplot2::theme(
              axis.text.x = ggplot2::element_text(angle = 40, hjust = 0,
                                                  size = axis_sz, face = "bold"),
              axis.text.y = ggplot2::element_text(size = axis_sz, face = "bold"),
              panel.grid  = ggplot2::element_blank(),
              legend.position = "right",
              plot.title  = ggplot2::element_text(size = 11, face = "bold"))
          tmp_mat <- tempfile(fileext = ".png")
          ggplot2::ggsave(tmp_mat, plot = p_mat, width = cw, height = h_mat, dpi = 180, bg = "white")
          doc <- h2_(doc, "Supplier Flow Matrix (cross-market bidding)")
          doc <- officer::body_add_img(doc, src = tmp_mat, width = cw, height = h_mat)
          doc <- br_(doc)
        }
      }, error = function(e) message("flow matrix error: ", e$message))
      
      # Network graph — rendered via base graphics
      tryCatch({
        tmp_net <- tempfile(fileext = ".png")
        grDevices::png(tmp_net, width = cw, height = min(6, cw * 0.75),
                       units = "in", res = 180, bg = "white")
        set.seed(42)
        build_network_graph_from_matrix(
          unusual_matrix = unusual_mat, min_bidders = 4, top_n = 20,
          cl_filter = NULL, country = country_code %||% "")
        grDevices::dev.off()
        doc <- h2_(doc, "Supplier Network Graph")
        doc <- officer::body_add_img(doc, src = tmp_net, width = cw,
                                     height = min(6, cw * 0.75))
        doc <- br_(doc)
      }, error = function(e) message("network graph error: ", e$message))
    } else if (is.null(filtered_analysis$markets)) {
      doc <- par_(doc, "Network analysis not yet run. Click 'Run Network Analysis' in the app first.")
    }
    doc <- pg_(doc)
    
    # ── 6. Supplier Concentration Over Time ──────────────────────────
    conc_data <- filtered_analysis$competition$concentration_yearly_data
    if (!is.null(conc_data) && nrow(conc_data) > 0) {
      doc <- h1_(doc, "6. Top Buyers by Supplier Concentration Over Time")
      tryCatch({
        p_conc <- build_concentration_yearly_plot(
          yearly_data = conc_data, n_buyers = 10, min_contracts = 1,
          country = country_code %||% "")
        if (!is.null(p_conc)) {
          n_years <- dplyr::n_distinct(conc_data$tender_year)
          n_cols  <- min(max(n_years, 1), 3)
          n_rows  <- ceiling(n_years / n_cols)
          h_conc  <- min(7, max(3.5, n_rows * 2.8))
          tmp_conc <- tempfile(fileext = ".png")
          ggplot2::ggsave(tmp_conc, plot = p_conc, width = cw, height = h_conc, dpi = 180, bg = "white")
          doc <- officer::body_add_img(doc, src = tmp_conc, width = cw, height = h_conc)
          doc <- br_(doc)
        }
      }, error = function(e) message("concentration plot error: ", e$message))
      doc <- pg_(doc)
    }
    
    # ── 7. Effect on Prices and Competition (Regressions) ────────────
    # singleb_plot may live in $prices or $competition (observer stores both)
    singleb_plot  <- filtered_analysis$prices$singleb_plot %||%
      filtered_analysis$competition$singleb_plot
    relprice_plot <- filtered_analysis$prices$rel_price_plot
    has_singleb   <- !is.null(singleb_plot)
    has_relprice  <- !is.null(relprice_plot)
    if (has_singleb || has_relprice) {
      doc <- h1_(doc, "7. Effect on Prices and Competition")
      doc <- par_(doc, "Regression results showing the effect of missing data and market structure on competition and prices.")
      if (has_singleb)
        doc <- add_fig(doc, singleb_plot,   aspect = 0.65,
                       label = "Single-Bidding vs. Missing Data Share", max_h = 5.5)
      if (has_relprice)
        doc <- add_fig(doc, relprice_plot, aspect = 0.65,
                       label = "Relative Prices vs. Missing Data Share", max_h = 5.5)
    } else {
      doc <- h1_(doc, "7. Effect on Prices and Competition")
      doc <- par_(doc, "Regression analysis not yet run. Click 'Run / Re-run Regression Analysis' in the app first.")
    }
    doc <- pg_(doc)
    
    # ── 8. Conclusions ────────────────────────────────────────────────
    doc <- h1_(doc, "8. Conclusions")
    doc <- par_(doc, paste0(
      "Report generated on ", format(Sys.Date(), "%B %d, %Y"),
      ". All findings are statistical indicators and should be interpreted with domain knowledge."))
    
    print(doc, target = output_file)
    TRUE
  }, error = function(e) {
    message("Integrity Word report FAILED: ", e$message)
    message("  Call stack: ", paste(capture.output(traceback()), collapse=" | "))
    FALSE
  })
}



# Safe wrapper for create_pipeline_config — handles older integrity_utils that
# lack "rel_price" in get_year_range choices (produces match.arg error).
safe_pipeline_config <- function(country_code) {
  # First attempt: call create_pipeline_config normally
  cfg <- tryCatch(create_pipeline_config(country_code), error = function(e) e)
  if (!inherits(cfg, "error")) return(cfg)
  
  # Second attempt: create_pipeline_config failed (likely get_year_range match.arg
  # does not include "rel_price" in this version of integrity_utils).
  # Rebuild manually, calling get_year_range only with the safe "singleb"/"default" args.
  message("create_pipeline_config failed (", cfg$message, "); rebuilding config safely")
  yr_default  <- tryCatch(get_year_range(country_code, "default"),  error = function(e) list(min_year=-Inf, max_year=Inf))
  yr_singleb  <- tryCatch(get_year_range(country_code, "singleb"),  error = function(e) list(min_year=-Inf, max_year=Inf))
  yr_relprice <- tryCatch(get_year_range(country_code, "rel_price"),error = function(e) yr_singleb)
  list(
    country    = toupper(country_code),
    thresholds = list(
      min_buyer_contracts=100, min_suppliers_for_buyer_conc=3,
      min_buyer_years=3, cpv_digits=3, min_bidders_for_edge=4,
      top_n_buyers=30, top_n_suppliers=30, top_n_markets=30,
      top_n_vars=10, marginal_share_threshold=0.05,
      max_wins_atypical=3, min_history_threshold=4,
      max_relative_price=5, min_relative_price=0
    ),
    years          = yr_default,
    years_singleb  = yr_singleb,
    years_relprice = yr_relprice,
    models = list(
      p_max=0.10,
      fe_set=c("buyer","year","buyer+year","buyer#year"),
      cluster_set=c("none","buyer","year","buyer_year","buyer_buyertype"),
      controls_set=c("x_only","base","base_extra"),
      model_types_relprice=c("ols_level","ols_log","gamma_log")
    ),
    plots = list(width=10, height=6, width_large=12, height_large=12, dpi=300, base_size=14)
  )
}

# ========================================================================
# INTEGRITY PIPELINE RUNNER (global scope — called by run_analysis + apply_integ_filters)
# ========================================================================
run_integrity_pipeline_fast_local <- function(df, country_code, output_dir = tempdir()) {
  # Build config — guard against older integrity_utils that lack "rel_price" in get_year_range
  config <- safe_pipeline_config(country_code)
  tryCatch(ensure_output_directory(output_dir), error = function(e) NULL)
  df <- tryCatch(prepare_data(df), error = function(e) { message("prepare_data failed: ", e$message); df })
  list(
    config           = config,
    data             = df,
    data_quality     = tryCatch(check_data_quality(df, config),            error = function(e) NULL),
    summary_stats    = tryCatch(log_summary_stats(df, config, output_dir),  error = function(e) NULL),
    missing          = safely_run_module(analyze_missing_values,   df, config, output_dir, save_plots = FALSE),
    interoperability = safely_run_module(analyze_interoperability, df, config, output_dir),
    competition      = safely_run_module(analyze_competition,      df, config, output_dir, save_plots = FALSE),
    markets          = list(network_plot = NULL, flow_matrix_plot = NULL,
                            supplier_unusual_plot = NULL, market_unusual_plot = NULL),
    prices           = list(singleb_plot = NULL, rel_price_plot = NULL,
                            singleb_sensitivity = NULL, relprice_sensitivity = NULL)
  )
}

shared_css <- "
/* ═══════════════════════════════════════════════════════════
   PROCUREMENT ANALYTICS — DESIGN SYSTEM
   ═══════════════════════════════════════════════════════════ */
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
:root {
  --navy:#0F1F3D; --navy-mid:#1A3160; --navy-light:#2A4A8A;
  --teal:#00897B; --teal-light:#E0F2F1;
  --amber:#D97706; --amber-light:#FFFBEB;
  --rose:#DC2626; --rose-light:#FEF2F2;
  --slate-50:#F8FAFC; --slate-100:#F1F5F9; --slate-200:#E2E8F0;
  --slate-400:#94A3B8; --slate-600:#475569; --slate-800:#1E293B;
  --white:#FFFFFF;
  --font-main:'DM Sans',sans-serif; --font-mono:'DM Mono',monospace;
  --radius:6px;
  --shadow-sm:0 1px 3px rgba(15,31,61,.08),0 1px 2px rgba(15,31,61,.04);
  --shadow-md:0 4px 12px rgba(15,31,61,.10),0 2px 4px rgba(15,31,61,.06);
}
body,.content-wrapper,.right-side{font-family:var(--font-main)!important;font-size:13.5px;color:var(--slate-800);background-color:var(--slate-100)!important;}
h1,h2,h3,h4,h5,h6{font-family:var(--font-main);font-weight:600;color:var(--navy);}
.tab-content h2{font-size:22px;font-weight:700;color:var(--navy);padding-bottom:10px;border-bottom:3px solid var(--slate-200);margin-bottom:20px;letter-spacing:-0.3px;}
/* Sidebar */
.main-sidebar,.left-side{background:var(--navy)!important;font-family:var(--font-main)!important;}
.sidebar-menu>li>a{font-family:var(--font-main)!important;font-size:13px!important;font-weight:500!important;color:rgba(255,255,255,0.75)!important;padding:9px 15px 9px 20px!important;border-left:3px solid transparent!important;transition:all .15s ease!important;}
.sidebar-menu>li>a:hover,.sidebar-menu>li.active>a{color:#fff!important;background:rgba(255,255,255,0.08)!important;border-left-color:var(--teal)!important;}
.sidebar-menu>li>a .fa,.sidebar-menu>li>a .fas,.sidebar-menu>li>a .far{width:18px;margin-right:8px;opacity:.8;}
.section-header{background:transparent!important;color:rgba(255,255,255,0.40)!important;font-family:var(--font-main)!important;font-size:10px!important;font-weight:700!important;letter-spacing:1.5px!important;text-transform:uppercase!important;padding:14px 20px 5px!important;margin-top:4px!important;border-top:1px solid rgba(255,255,255,0.07)!important;}
.section-header:first-child{border-top:none!important;}
.export-sep-li{border-top:2px solid rgba(0,137,123,0.5)!important;margin:8px 12px!important;padding:0!important;height:0!important;}
.main-header .navbar,.main-header .logo{background-color:var(--navy-mid)!important;border-bottom:none!important;}
.main-header .logo{width:260px!important;display:flex!important;align-items:center!important;padding:0 16px!important;}
.main-header .navbar{margin-left:260px!important;}
.sidebar-toggle{height:50px!important;padding:15px 18px!important;}
/* ── App title in header ── */
.app-logo{display:flex;align-items:baseline;gap:5px;line-height:1;}
.app-logo-name{font-family:var(--font-main)!important;font-weight:800!important;font-size:20px!important;letter-spacing:-0.5px!important;color:#ffffff!important;}
.app-logo-sub{font-family:var(--font-main)!important;font-weight:300!important;font-size:11px!important;color:rgba(255,255,255,0.60)!important;text-transform:uppercase!important;letter-spacing:1.5px!important;}

/* ════════════════════════════════════════════════════
   RESPONSIVE SCALING — three breakpoints
   • < 1440px  = laptop / small screen  → tighter
   • 1440–1920 = standard monitor       → base sizes (as coded)
   • > 1920px  = large / hi-res monitor → scale up
   ════════════════════════════════════════════════════ */

/* ── Laptop (< 1440 px) ───────────────────────────── */
@media (max-width: 1440px) {
  body,.content-wrapper,.right-side{font-size:12px!important;}
  .sidebar-menu>li>a{font-size:12px!important;padding:7px 12px 7px 16px!important;}
  .section-header{font-size:9px!important;}
  .box-header{font-size:12px!important;padding:8px 12px!important;}
  .box-body{padding:10px!important;}
  .question-header{font-size:13px!important;padding:9px 12px!important;}
  .description-box{font-size:12px!important;padding:9px 12px!important;}
  .control-label{font-size:11.5px!important;}
  .selectize-input,.form-control{font-size:12px!important;}
  .btn{font-size:12px!important;}
  /* Value boxes */
  .small-box .inner h3{font-size:20px!important;}
  .small-box .inner p{font-size:11px!important;}
  .small-box{min-height:60px!important;}
  .small-box .inner{padding:10px 14px!important;}
  /* Header logo */
  .app-logo-name{font-size:17px!important;}
  .app-logo-sub{font-size:10px!important;}
  /* Tighten dashboard body margin */
  .content-wrapper{padding:8px!important;}
  /* Missing values plot fonts on small screens — plotly inherits global font */
  .js-plotly-plot .ytick text,.js-plotly-plot .xtick text{font-size:10px!important;}
  .tab-content h2{font-size:18px!important;}
  .export-card{padding:16px 14px!important;}
}

/* ── Large monitor (> 1920 px) ────────────────────── */
@media (min-width: 1921px) {
  body,.content-wrapper,.right-side{font-size:15px!important;}
  .sidebar-menu>li>a{font-size:14px!important;padding:10px 18px 10px 22px!important;}
  .section-header{font-size:11px!important;}
  .box-header{font-size:14.5px!important;padding:12px 16px!important;}
  .box-body{padding:18px!important;}
  .question-header{font-size:17px!important;padding:14px 18px!important;}
  .description-box{font-size:14.5px!important;padding:14px 18px!important;}
  .control-label{font-size:13.5px!important;}
  .selectize-input,.form-control{font-size:14px!important;}
  .btn{font-size:14px!important;}
  /* Value boxes */
  .small-box .inner h3{font-size:32px!important;}
  .small-box .inner p{font-size:15px!important;}
  .small-box{min-height:96px!important;}
  .small-box .inner{padding:16px 22px!important;}
  /* Header logo */
  .app-logo-name{font-size:24px!important;}
  .app-logo-sub{font-size:13px!important;}
  /* More breathing room */
  .content-wrapper{padding:16px!important;}
  /* Bigger plotly tick labels on large monitors */
  .js-plotly-plot .ytick text,.js-plotly-plot .xtick text{font-size:13px!important;}
  .tab-content h2{font-size:26px!important;}
  .export-card{padding:28px 24px!important;}
  /* Sidebar wider feels better on big screens */
  .main-header .logo{width:280px!important;}
  .main-header .navbar{margin-left:280px!important;}
}
/* Boxes */
.box{border-radius:var(--radius)!important;box-shadow:var(--shadow-sm)!important;border:1px solid var(--slate-200)!important;border-top:none!important;margin-bottom:18px!important;background:var(--white)!important;}
.box-header{font-family:var(--font-main)!important;font-weight:600!important;font-size:13px!important;padding:10px 14px!important;border-radius:var(--radius) var(--radius) 0 0!important;border-bottom:1px solid var(--slate-200)!important;}
.box-body{padding:14px!important;}
.box.box-solid.box-primary>.box-header{background:var(--navy)!important;color:#fff!important;border-color:var(--navy)!important;}
.box.box-solid.box-info>.box-header{background:#0369A1!important;color:#fff!important;border-color:#0369A1!important;}
.box.box-solid.box-warning>.box-header{background:var(--amber)!important;color:#fff!important;border-color:var(--amber)!important;}
.box.box-solid.box-danger>.box-header{background:var(--rose)!important;color:#fff!important;border-color:var(--rose)!important;}
.box.box-solid.box-success>.box-header{background:var(--teal)!important;color:#fff!important;border-color:var(--teal)!important;}
.box.box-primary{border-left:3px solid var(--navy)!important;}
.box.box-info{border-left:3px solid #0369A1!important;}
.box.box-warning{border-left:3px solid var(--amber)!important;}
.box.box-danger{border-left:3px solid var(--rose)!important;}
.box.box-success{border-left:3px solid var(--teal)!important;}
/* Content components */
.question-header{font-family:var(--font-main)!important;font-size:15px!important;font-weight:600!important;color:var(--navy)!important;background:var(--white)!important;border-left:4px solid var(--teal)!important;padding:12px 16px!important;margin:16px 0 10px!important;border-radius:0 var(--radius) var(--radius) 0!important;box-shadow:var(--shadow-sm)!important;}
.description-box{background:var(--slate-50)!important;border-left:3px solid var(--slate-200)!important;padding:12px 15px!important;margin:10px 0 14px!important;border-radius:0 var(--radius) var(--radius) 0!important;font-size:13px!important;line-height:1.65!important;color:var(--slate-600)!important;}
.description-box strong,.description-box b{color:var(--navy)!important;}
.description-box p{margin:4px 0!important;}
.info-box-custom{background:#FFFBEB!important;border-left:3px solid var(--amber)!important;padding:12px 15px!important;margin:10px 0!important;border-radius:0 var(--radius) var(--radius) 0!important;font-size:13px!important;line-height:1.65!important;}
/* Regression run box */
.reg-run-box{display:flex!important;align-items:center!important;gap:16px!important;padding:14px 16px!important;background:var(--amber-light)!important;border:1px solid #FDE68A!important;border-radius:var(--radius)!important;margin-bottom:14px!important;}
.reg-run-box .reg-status{flex:1!important;}
.reg-run-box .reg-btn-wrap{flex-shrink:0!important;}
.reg-run-btn{font-family:var(--font-main)!important;font-weight:600!important;font-size:13px!important;background:var(--amber)!important;border:none!important;color:#fff!important;padding:8px 20px!important;border-radius:var(--radius)!important;cursor:pointer!important;white-space:nowrap!important;transition:background .15s ease!important;}
.reg-run-btn:hover{background:#B45309!important;}
.reg-status-ok{color:var(--teal)!important;font-weight:600!important;font-size:13px!important;}
.reg-status-wait{color:var(--slate-400)!important;font-size:13px!important;}
/* Deferred box */
.deferred-box{background:var(--slate-50)!important;border:1.5px dashed var(--slate-200)!important;border-radius:var(--radius)!important;padding:28px 20px!important;text-align:center!important;color:var(--slate-400)!important;font-size:13px!important;margin-bottom:12px!important;}
/* Buttons */
.btn{font-family:var(--font-main)!important;font-weight:500!important;font-size:13px!important;border-radius:var(--radius)!important;transition:all .15s ease!important;}
.download-btn{margin:4px 4px 4px 0!important;min-width:160px!important;}
.btn-info{background:#0369A1!important;border-color:#0369A1!important;color:#fff!important;}
.btn-info:hover{background:#025885!important;border-color:#025885!important;}
.btn-success{background:var(--teal)!important;border-color:var(--teal)!important;color:#fff!important;}
.btn-success:hover{background:#00695C!important;border-color:#00695C!important;}
.btn-warning{background:var(--amber)!important;border-color:var(--amber)!important;color:#fff!important;}
.btn-warning:hover{background:#B45309!important;border-color:#B45309!important;}
.btn-primary{background:var(--navy)!important;border-color:var(--navy)!important;color:#fff!important;}
.btn-primary:hover{background:var(--navy-light)!important;border-color:var(--navy-light)!important;}
.btn-danger{background:var(--rose)!important;border-color:var(--rose)!important;color:#fff!important;}
.btn-wb-primary{background:var(--teal)!important;border-color:var(--teal)!important;color:#fff!important;}
.btn-wb-success{background:var(--teal)!important;border-color:var(--teal)!important;color:#fff!important;}
/* Export cards */
.export-card{background:var(--white)!important;border:1px solid var(--slate-200)!important;border-radius:8px!important;padding:24px 20px!important;box-shadow:var(--shadow-sm)!important;height:100%!important;}
.export-card-title{font-size:14px!important;font-weight:700!important;color:var(--navy)!important;margin-bottom:6px!important;display:flex!important;align-items:center!important;gap:8px!important;}
.export-card-desc{font-size:12.5px!important;color:var(--slate-600)!important;margin-bottom:16px!important;line-height:1.5!important;}
.export-card-btns{display:flex!important;flex-direction:column!important;gap:8px!important;}
.export-card-btns .btn{width:100%!important;text-align:left!important;}
.export-card-econ{border-top:3px solid var(--teal)!important;}
.export-card-admin{border-top:3px solid var(--amber)!important;}
.export-card-integ{border-top:3px solid var(--rose)!important;}
.export-status-bar{background:var(--slate-50)!important;border:1px solid var(--slate-200)!important;border-radius:var(--radius)!important;padding:12px 16px!important;font-size:13px!important;color:var(--slate-600)!important;min-height:44px!important;}
/* Misc */
.proc-section{border:1px solid var(--slate-200)!important;border-radius:var(--radius)!important;padding:12px 14px!important;margin-bottom:10px!important;background:var(--slate-50)!important;}
.alert{border-radius:var(--radius)!important;font-size:13px!important;font-family:var(--font-main)!important;}
.alert-success{border-left:3px solid var(--teal)!important;}
.alert-warning{border-left:3px solid var(--amber)!important;}
.alert-danger{border-left:3px solid var(--rose)!important;}
.alert-info{border-left:3px solid #0369A1!important;}
.small-box{border-radius:var(--radius)!important;box-shadow:var(--shadow-sm)!important;}
.small-box h3{font-family:var(--font-mono)!important;font-weight:500!important;}
.selectize-input,.form-control{font-family:var(--font-main)!important;font-size:13px!important;border-radius:var(--radius)!important;border-color:var(--slate-200)!important;}
.control-label{font-weight:600!important;font-size:12.5px!important;color:var(--slate-600)!important;}
::-webkit-scrollbar{width:6px;height:6px;}
::-webkit-scrollbar-track{background:var(--slate-100);}
::-webkit-scrollbar-thumb{background:var(--slate-200);border-radius:3px;}
::-webkit-scrollbar-thumb:hover{background:var(--slate-400);}
/* ── Value / Info boxes: compact & white text ─────────────── */
.small-box .inner h3 {
  font-family: var(--font-mono) !important;
  font-size: 26px !important;
  font-weight: 700 !important;
  color: #fff !important;
  line-height: 1.2 !important;
  word-break: break-word !important;
}
.small-box .inner p {
  font-size: 13px !important;
  font-weight: 600 !important;
  color: rgba(255,255,255,0.92) !important;
  margin-top: 3px !important;
  letter-spacing: 0.2px !important;
}
.small-box .icon { display: none !important; }
.small-box { min-height: 80px !important; padding-right: 10px !important; }
.small-box .inner { padding: 14px 18px !important; }

/* ── FIXES: white text on all solid headers ───────────────── */
.box.box-solid > .box-header,
.box.box-solid > .box-header .box-title,
.box.box-solid > .box-header .box-tools .btn {
  color: #fff !important;
}
/* Filter boxes (collapsible, status=info) — subtle slate style */
.box.box-solid.box-info > .box-header {
  background: #334155 !important;
  border-color: #334155 !important;
  color: #fff !important;
}
.box.box-info { border-left: 3px solid #334155 !important; }
/* Non-solid box titles inherit navy */
.box:not(.box-solid) > .box-header .box-title { color: var(--navy) !important; }

/* ── Plotly: force white background everywhere ────────────────
   The toImage camera button renders the SVG directly.
   We must white-out BOTH the HTML container AND the inner
   SVG rect.bg — AdminLTE blue skin sets both.               */
.js-plotly-plot,
.plot-container.plotly,
.plotly.html-widget,
.svg-container {
  background-color: #ffffff !important;
  background: #ffffff !important;
}
/* SVG paper background rect — three selectors for specificity */
.js-plotly-plot .bg,
.js-plotly-plot svg rect.bg,
.plot-container svg rect.bg {
  fill: #ffffff !important;
  stroke: none !important;
}
/* Kill AdminLTE skin colour leaking into plotly wrappers */
.content-wrapper .plotly.html-widget,
.box-body .plotly.html-widget {
  background-color: #ffffff !important;
}

"

ui <- dashboardPage(
  skin = "blue",
  
  dashboardHeader(
    title = tags$span(class = "app-logo",
                      tags$span("procuR",    class = "app-logo-name"),
                      tags$span("Analytics", class = "app-logo-sub")
    ),
    titleWidth = 260
  ),
  
  dashboardSidebar(
    width = 260,
    sidebarMenu(
      id = "sidebar",
      
      # ── Shared ──────────────────────────────────────────────────────
      menuItem("Setup",        tabName = "setup",        icon = icon("cog")),
      menuItem("Overview",     tabName = "overview",     icon = icon("home")),
      menuItem("Data Overview",tabName = "data_overview",icon = icon("table")),
      
      # ── Economic Outcomes ────────────────────────────────────────────
      tags$li(class = "section-header", "Economic Outcomes"),
      menuItem("Market Sizing",     tabName = "market_sizing",     icon = icon("chart-bar")),
      menuItem("Supplier Dynamics", tabName = "supplier_dynamics", icon = icon("users")),
      menuItem("Networks",          tabName = "networks",          icon = icon("project-diagram")),
      menuItem("Relative Prices",   tabName = "relative_prices",   icon = icon("dollar-sign")),
      menuItem("Competition",       tabName = "competition",       icon = icon("trophy")),
      
      # ── Administrative Efficiency ────────────────────────────────────
      tags$li(class = "section-header", "Administrative Efficiency"),
      menuItem("Configuration",      tabName = "configuration",  icon = icon("sliders")),
      menuItem("Procedure Types",    tabName = "procedures",     icon = icon("list-check")),
      menuItem("Submission Periods", tabName = "submission",     icon = icon("clock")),
      menuItem("Decision Periods",   tabName = "decision",       icon = icon("gavel")),
      menuItem("Regression Analysis",tabName = "regression",     icon = icon("chart-line")),
      
      # ── Procurement Integrity ────────────────────────────────────────
      tags$li(class = "section-header", "Procurement Integrity"),
      menuItem("Missing Values",       tabName = "integrity_missing",     icon = icon("exclamation-triangle")),
      menuItem("Interoperability",     tabName = "integrity_interop",     icon = icon("link")),
      menuItem("Risky Profiles",       tabName = "integrity_risky",       icon = icon("exclamation-circle")),
      menuItem("Prices & Competition", tabName = "integrity_prices",      icon = icon("dollar-sign")),
      
      # ── Export ───────────────────────────────────────────────────────
      tags$li(class = "export-sep-li"),
      menuItem("Export & Download", tabName = "export", icon = icon("download"))
    )
  ),
  
  dashboardBody(
    tags$head(tags$style(HTML(shared_css))),
    
    tabItems(
      
      # ==================================================================
      # SETUP
      # ==================================================================
      tabItem(tabName = "setup",
              h2("Setup & Data Upload"),
              fluidRow(
                box(title = "Upload Your Data", width = 8,
                    solidHeader = TRUE, status = "primary",
                    div(class = "info-box-custom",
                        icon("info-circle"),
                        tags$strong(" Upload a CSV file from "),
                        tags$a(href = "https://www.procurementintegrity.org/data",
                               target = "_blank", "ProAct",
                               style = "color:#009FDA; font-weight:bold;"),
                        " then click Run Analysis. Both the Economic Outcomes and",
                        " Administrative Efficiency sections will be populated."
                    ),
                    fluidRow(
                      column(6,
                             fileInput("datafile", label = tags$div(icon("file-csv"), tags$strong(" Choose CSV File")),
                                       accept = c("text/csv",".csv"), buttonLabel = "Browse..."),
                             textInput("country_code", "Country Code (2 letters — blank = auto-detect)", value = "",
                                       placeholder = "Auto-detecting from data…"),
                             hr(),
                             h4(icon("cog"), " Analysis Options"),
                             checkboxInput("skip_regressions",
                                           label = tags$span(icon("fast-forward"),
                                                             " Skip regression analysis (recommended — run on demand from the Regression tab)"),
                                           value = TRUE),
                             helpText(icon("info-circle"),
                                      " Regressions test whether short submission or long decision periods are linked to single-bidding.",
                                      " Skipped by default for speed. Run on demand from the Regression Analysis tab after upload."),
                             helpText(icon("project-diagram"),
                                      " Networks can be generated on-demand from the Networks tab after analysis completes.")
                      ),
                      column(6,
                             h4("Instructions:"),
                             tags$ol(
                               tags$li("Upload your procurement CSV file"),
                               tags$li("Enter the two-letter country code"),
                               tags$li("Configure analysis options as needed"),
                               tags$li("Click 'Run Analysis' — both pipelines run automatically"),
                               tags$li("Use Configuration tab to set admin thresholds"),
                               tags$li("Navigate tabs to explore results"),
                               tags$li("Use Export tab to download reports")
                             )
                      )
                    ),
                    br(),
                    actionButton("run_analysis",
                                 label = tags$span(icon("play-circle", class = "fa-lg"), " Run Analysis"),
                                 class = "btn-wb-success btn-lg",
                                 style = "width:100%; padding:15px; font-size:18px;"),
                    hr(),
                    verbatimTextOutput("analysis_status")
                ),
                box(title = "What This Tool Does", width = 4,
                    solidHeader = TRUE, status = "info", collapsible = TRUE,
                    h5(icon("chart-bar"), " Economic Outcomes"),
                    tags$ul(
                      tags$li("Market sizing across CPV sectors"),
                      tags$li("Supplier entry and dynamics"),
                      tags$li("Buyer–supplier network maps"),
                      tags$li("Relative price diagnostics"),
                      tags$li("Single-bid competition analysis")
                    ),
                    hr(),
                    h5(icon("gavel"), " Administrative Efficiency"),
                    tags$ul(
                      tags$li("Procedure type distribution"),
                      tags$li("Contract value bunching analysis"),
                      tags$li("Submission period diagnostics"),
                      tags$li("Decision period diagnostics"),
                      tags$li("Regression: admin efficiency → competition")
                    ),
                    hr(),
                    h5(icon("shield-alt"), " Procurement Integrity"),
                    tags$ul(
                      tags$li("Data quality & missing value patterns"),
                      tags$li("Interoperability of buyer/supplier IDs"),
                      tags$li("Risky supplier profiles & market entry"),
                      tags$li("Buyer-supplier concentration"),
                      tags$li("Transparency impact on prices & competition")
                    )
                )
              )
      ),
      
      # ==================================================================
      # OVERVIEW
      # ==================================================================
      tabItem(tabName = "overview",
              h2("Analysis Overview"),
              fluidRow(
                box(title = "Economic Outcomes", width = 4, status = "primary", solidHeader = TRUE,
                    p("Analyzes market structure, supplier dynamics, buyer-supplier networks,",
                      "pricing patterns, and competition levels across CPV procurement markets."),
                    tags$ul(
                      tags$li(tags$b("Market Sizing:"), " Distribution of contracts and values"),
                      tags$li(tags$b("Supplier Dynamics:"), " New vs repeat suppliers"),
                      tags$li(tags$b("Networks:"), " Buyer-supplier relationship structures"),
                      tags$li(tags$b("Relative Prices:"), " Contract prices vs estimates"),
                      tags$li(tags$b("Competition:"), " Single-bid incidence analysis")
                    )
                ),
                box(title = "Administrative Efficiency", width = 4, status = "warning", solidHeader = TRUE,
                    p("Examines administrative barriers in procurement, focusing on procedural",
                      "compliance, timing efficiency, and their link to competitive outcomes."),
                    tags$ul(
                      tags$li(tags$b("Procedure Mix:"), " Overuse of certain procedure types?"),
                      tags$li(tags$b("Submission Periods:"), " Are bid deadlines too short?"),
                      tags$li(tags$b("Decision Periods:"), " Are award decisions too slow?"),
                      tags$li(tags$b("Competition Impact:"), " Does admin efficiency affect bids?")
                    )
                ),
                box(title = "Procurement Integrity", width = 4, status = "primary", solidHeader = TRUE,
                    p("Assesses transparency and accountability in procurement systems,",
                      "examining data quality, interoperability, and corruption risk indicators."),
                    tags$ul(
                      tags$li(tags$b("Data Quality:"), " Patterns of underreporting in the data"),
                      tags$li(tags$b("Interoperability:"), " Can data be matched to other registers?"),
                      tags$li(tags$b("Risky Profiles:"), " Suppliers with unusual market activity"),
                      tags$li(tags$b("Concentration:"), " Suspicious buyer-supplier connections"),
                      tags$li(tags$b("Price Impact:"), " Transparency effect on prices & bids")
                    )
                )
              )
      ),
      
      # ==================================================================
      # DATA OVERVIEW (shared)
      # ==================================================================
      tabItem(tabName = "data_overview",
              h2("Data Overview"),
              fluidRow(
                box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                    tags$p(style = "color:#666; font-size:12px; margin-bottom:8px;",
                           icon("info-circle"),
                           " Filters here apply to the Data Overview tab only. Each analysis",
                           " section has its own independent filter controls."),
                    filter_bar_ui("econ", "overview")
                )
              ),
              fluidRow(
                valueBoxOutput("n_contracts",  width = 3),
                valueBoxOutput("n_buyers",     width = 3),
                valueBoxOutput("n_suppliers",  width = 3),
                valueBoxOutput("n_years",      width = 3)
              ),
              fluidRow(
                box(title = "Total Contract Number per Year", width = 6, solidHeader = TRUE, status = "primary",
                    style = "height:480px;",
                    plotlyOutput("contracts_year_plot", height = "400px"),
                    downloadButton("dl_contracts_year_econ", "Download Figure", class = "download-btn btn-sm")),
                box(title = "Total Contract Value by Year", width = 6, solidHeader = TRUE, status = "primary",
                    style = "height:480px;",
                    plotlyOutput("value_by_year_plot", height = "400px"),
                    downloadButton("dl_value_by_year", "Download Figure", class = "download-btn btn-sm"))
              ),
              fluidRow(
                box(title = "CPV Market Definitions", width = 12, solidHeader = TRUE, status = "primary",
                    DT::dataTableOutput("cpv_legend_table"))
              ),
              
      ),
      
      # ==================================================================
      # ECONOMIC OUTCOMES — MARKET SIZING
      # ==================================================================
      tabItem(tabName = "market_sizing",
              h2("Market Sizing Analysis"),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("econ", "market"))),
              div(class = "question-header", "What is the overall market composition?"),
              div(class = "description-box",
                  p("Market sizing examines how procurement spending is distributed across CPV markets.")),
              fluidRow(
                box(title = "Market Size by Number of Contracts", width = 12, solidHeader = TRUE, status = "primary",
                    plotlyOutput("market_size_n_plot", height = "450px"),
                    downloadButton("dl_market_size_n", "Download Figure", class = "download-btn btn-sm"))
              ),
              fluidRow(
                box(title = "Market Size by Total Value", width = 12, solidHeader = TRUE, status = "primary",
                    plotlyOutput("market_size_v_plot", height = "450px"),
                    downloadButton("dl_market_size_v", "Download Figure", class = "download-btn btn-sm"))
              ),
              fluidRow(
                box(title = "Market Size Bubble Plot", width = 12, solidHeader = TRUE, status = "primary",
                    plotlyOutput("market_size_av_plot", height = "500px"),
                    downloadButton("dl_market_size_av", "Download Figure", class = "download-btn btn-sm"))
              )
      ),
      
      # ==================================================================
      # ECONOMIC OUTCOMES — SUPPLIER DYNAMICS
      # ==================================================================
      tabItem(tabName = "supplier_dynamics",
              h2("Supplier Dynamics Analysis"),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("econ", "supplier"))),
              
              # ── Market size filter row ──────────────────────────────────────
              fluidRow(
                box(title = "Market Size Filters", width = 12, solidHeader = FALSE, status = "warning",
                    collapsible = TRUE, collapsed = FALSE,
                    p(style="color:#666;font-size:13px;margin-bottom:10px;",
                      "Narrow to markets of a specific size so small or very large markets don't dominate the charts."),
                    fluidRow(
                      column(6, uiOutput("market_contracts_range_slider")),
                      column(6, uiOutput("market_value_range_slider"))
                    ),
                    fluidRow(
                      column(12,
                             actionButton("apply_market_filters", "Apply",
                                          icon = icon("filter"), class = "btn-primary btn-sm"),
                             actionButton("reset_market_filters", "Reset",
                                          icon = icon("undo"), class = "btn-default btn-sm"),
                             span(style="margin-left:12px;font-size:12px;color:#666;",
                                  textOutput("market_filter_status", inline=TRUE))
                      )
                    )
                )
              ),
              
              # ── Plot 1: Combined bubble grid ────────────────────────────────
              div(class = "question-header", "Which markets are large, and how volatile is their supplier base?"),
              fluidRow(
                box(title = "Supplier Landscape: Size & Volatility", width = 12,
                    solidHeader = TRUE, status = "primary",
                    div(class = "description-box",
                        p("Each cell shows one CPV market in one year.",
                          strong(" Bubble size"), " = total unique suppliers (market depth).",
                          strong(" Colour"), " = % new suppliers that year: red = high churn (many first-time entrants),",
                          " blue = stable base (mostly repeat suppliers).",
                          " Large red cells are both competitive and volatile; small blue cells are shallow and captive.")),
                    fluidRow(
                      column(4,
                             sliderInput("econ_new_threshold", "Red above (% new suppliers):",
                                         min=0, max=100, value=50, step=5, post="%", width="100%")
                      ),
                      
                      column(4,
                             checkboxInput("supp_show_labels", "Show CPV labels on y-axis", value=TRUE),
                             checkboxInput("supp_show_counts", "Show supplier counts in cells", value=FALSE)
                      )
                    ),
                    uiOutput("supplier_bubble_plot_ui"),
                    downloadButton("dl_suppliers_entrance", "Download Figure", class="download-btn btn-sm")
                )
              ),
              
              # ── Plot 2: Market stability scatter ────────────────────────────
              div(class = "question-header", "Which markets are deep and stable vs shallow and churning?"),
              fluidRow(
                box(title = "Market Stability Overview (avg across years)", width = 12,
                    solidHeader = TRUE, status = "primary",
                    div(class = "description-box",
                        p("One dot per CPV market, each metric averaged across all observed years.",
                          strong(" X-axis:"), " average % new suppliers — higher means more first-time entrants each year (open / competitive market).",
                          strong(" Y-axis:"), " average number of unique suppliers per year (market depth).",
                          strong(" Bubble size:"), " average number of contracts per year.",
                          strong(" Colour:"), " blue = low entry rate (mostly repeat suppliers), red = high entry rate (many new entrants).",
                          br(),
                          "The dotted lines mark the median of each axis across all markets, splitting the chart into four quadrants:",
                          tags$ul(
                            tags$li(strong("Top-left:"), " deep + high entry — large competitive market with frequent newcomers."),
                            tags$li(strong("Top-right:"), " deep + stable — large market dominated by repeat suppliers (possible incumbency)."),
                            tags$li(strong("Bottom-left:"), " shallow + high entry — small market with high churn (fragile, few contracts)."),
                            tags$li(strong("Bottom-right:"), " shallow + stable — small captive market, same few suppliers repeat (red flag).")
                          ),
                          "Hover over any dot for the entry rate ", strong("volatility (SD)"), " — the year-to-year standard deviation of % new suppliers.",
                          " A market with SD = 20% swings between, say, 10% and 50% new suppliers in different years.",
                          " A market with SD = 2% is consistently predictable regardless of its average.")),
                    plotlyOutput("supplier_stability_plot", height="550px"),
                    downloadButton("dl_unique_supp", "Download Figure", class="download-btn btn-sm")
                )
              ),
              
              # ── Plot 3: Supplier trend for selected markets ──────────────────
              div(class = "question-header", "How has the supplier base evolved over time in key markets?"),
              fluidRow(
                box(title = "New vs Repeat Suppliers Over Time", width = 12,
                    solidHeader = TRUE, status = "info",
                    div(class = "description-box",
                        p("Stacked area chart showing how many suppliers were new (first appearance in that market)",
                          " vs repeat (seen in prior years) for each selected market.",
                          " A growing green area means the market is attracting new entrants.",
                          " A shrinking or flat area dominated by repeat suppliers may indicate barriers to entry.")),
                    fluidRow(
                      column(4, uiOutput("supp_trend_market_picker_ui")),
                      column(4, radioButtons("supp_trend_metric", "Show as:",
                                             choices = c("Count" = "count", "Share (%)" = "share"),
                                             selected = "count", inline = TRUE))
                    ),
                    uiOutput("supplier_trend_plot_ui"),
                    downloadButton("dl_supp_trend", "Download Figure", class="download-btn btn-sm")
                )
              ),
              
              # ── Plot 4: Top Suppliers ───────────────────────────────────────
              div(class = "question-header", "Who are the dominant suppliers?"),
              fluidRow(
                box(title = "Top Suppliers by Contracts Won", width = 12,
                    solidHeader = TRUE, status = "primary",
                    div(class = "description-box",
                        p("Ranks the top suppliers by number of contracts won in the filtered dataset.",
                          strong(" Bar length"), " = contracts won.",
                          strong(" Dot colour"), " = total contract value won (darker = higher value).",
                          strong(" Right label"), " = number of distinct CPV markets served.",
                          " Suppliers active across many markets may indicate broad dominance or",
                          " potential conflict-of-interest risk.")),
                    fluidRow(
                      column(4,
                             sliderInput("top_supp_n", "Number of suppliers to show:",
                                         min = 5, max = 50, value = 20, step = 5, width = "100%")),
                      column(4,
                             selectInput("top_supp_metric", "Rank by:",
                                         choices = c("Contracts won" = "n_contracts",
                                                     "Total value won" = "total_value",
                                                     "Markets served"  = "n_markets"),
                                         selected = "n_contracts", width = "100%"))
                    ),
                    uiOutput("top_suppliers_plot_ui"),
                    downloadButton("dl_top_suppliers", "Download Figure", class = "download-btn btn-sm")
                )
              )
      ),
      
      # ==================================================================
      # ECONOMIC OUTCOMES — NETWORKS
      # ==================================================================
      tabItem(tabName = "networks",
              h2("Buyer-Supplier Networks"),
              fluidRow(
                box(title = "Network Generation", width = 12, status = "warning", solidHeader = TRUE, collapsible = TRUE,
                    div(class = "description-box",
                        p(icon("info-circle"), tags$strong(" About networks:"),
                          " These diagrams visualize buyer-supplier relationships in selected CPV markets.",
                          " Dense, well-connected networks suggest diverse sourcing, while star-shaped networks",
                          " around a single supplier may indicate limited competition.",
                          " Networks are memory-intensive — generate only the markets you need.")),
                    fluidRow(
                      column(6,
                             uiOutput("network_cpv_picker_ui"),
                             numericInput("network_top_buyers", "Max buyers per network:", value = 15, min = 5, max = 50, step = 5)
                      ),
                      column(6,
                             uiOutput("network_status_box"),
                             br(),
                             actionButton("run_networks_now",
                                          label = tags$span(icon("play-circle"), " Generate / Re-generate Networks"),
                                          class = "btn-success btn-lg",
                                          style = "width: 100%; margin-top: 10px;")
                      )
                    )
                )
              ),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("econ", "network"))),
              div(class = "question-header", "Are buyers able to choose from a variety of market offerings?"),
              fluidRow(
                box(title = "Network Visualizations", width = 12, solidHeader = TRUE, status = "primary",
                    div(class = "description-box",
                        p("Each diagram shows which buyers (squares) connect to which suppliers (circles) in a CPV market.",
                          " Use the controls above to select markets and click Generate.")),
                    uiOutput("network_plots_ui"))
              )
      ),
      
      # ==================================================================
      # ECONOMIC OUTCOMES — RELATIVE PRICES
      # ==================================================================
      tabItem(tabName = "relative_prices",
              h2("Relative Price Analysis"),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("econ", "price"))),
              div(class = "question-header", "Are there price savings or price overruns prevailing?"),
              fluidRow(
                box(title = "Overall: Under vs Over Budget", width = 12, solidHeader = TRUE, status = "primary",
                    div(class = "description-box", p("Distribution of relative prices (contract ÷ estimate).",
                                                     " Blue zone = came in under budget, red zone = over budget.",
                                                     " The dashed line marks the estimate (1.0); the amber line marks the median.")),
                    plotlyOutput("rel_tot_plot", height = "380px"),
                    downloadButton("dl_rel_tot", "Download Figure", class = "download-btn btn-sm"))
              ),
              fluidRow(
                box(title = "Trend Over Time", width = 12, solidHeader = TRUE, status = "primary",
                    div(class = "description-box", p("Violin shape shows the full distribution each year.",
                                                     " The dot marks the annual median, colour-coded: teal = under budget, amber = slightly over, red = over.",
                                                     " Grey band = \u00b120% of estimate.")),
                    plotlyOutput("rel_year_plot", height = "350px"),
                    downloadButton("dl_rel_year", "Download Figure", class = "download-btn btn-sm"))
              ),
              fluidRow(
                box(title = "By Market (CPV Sector)", width = 12, solidHeader = TRUE, status = "primary",
                    div(class = "description-box", p("All CPV markets ranked by median relative price.",
                                                     " Dot = median, bar = IQR (25th\u201375th percentile), dot size proportional to contract count.",
                                                     " Colour: teal = under budget, amber = up to 20% over, red = more than 20% over.")),
                    uiOutput("rel_10_plot_ui"),
                    downloadButton("dl_rel_10", "Download Figure", class = "download-btn btn-sm"))
              ),
              fluidRow(
                box(title = "By Buyer", width = 12, solidHeader = TRUE, status = "primary",
                    div(class = "description-box", p("Buyers ranked by mean relative price (contract \u00f7 estimate).",
                                                     " Stick starts at 1.0 (budget). Dot size = contract volume.",
                                                     " Dot colour = price level (teal = near budget, red = far over).",
                                                     " Over budget defined as rel. price > 1.0.")),
                    fluidRow(
                      column(6, sliderInput("rel_buy_top_n",
                                            "Number of buyers to show:",
                                            min = 5, max = 50, value = 20, step = 5)),
                      column(6, sliderInput("rel_buy_min_contracts",
                                            "Minimum contracts per buyer:",
                                            min = 1, max = 100, value = 10, step = 1))
                    ),
                    uiOutput("rel_buy_plot_ui"),
                    downloadButton("dl_rel_buy", "Download Figure", class = "download-btn btn-sm"))
              ),
              div(class = "question-header", "Do small or large contracts suffer more from price overruns?"),
              fluidRow(
                box(title = "Relative Price by Contract Size", width = 12,
                    solidHeader = TRUE, status = "primary",
                    div(class = "description-box",
                        p("Each box shows the distribution of relative prices within a contract value band.",
                          " Bands are in ", strong("USD"), " and match those in the Competition Analysis tab:",
                          " < $5k \u2022 $5k\u2013$10k \u2022 $10k\u2013$50k \u2022 $50k\u2013$100k \u2022 $100k\u2013$500k \u2022 $500k\u2013$1M \u2022 > $1M.",
                          strong(" Box"), " = 25th\u201375th percentile,",
                          strong(" line"), " = median,",
                          strong(" whiskers"), " = 5th\u201395th percentile.",
                          " The dashed line at 1.0 is the budget. Boxes coloured",
                          strong(" red"), " when median is above budget,",
                          strong(" teal"), " when below.",
                          " The % label above each box = share of contracts in that band that are over budget.")),
                    plotlyOutput("rel_size_plot", height = "420px"),
                    downloadButton("dl_rel_size", "Download Figure", class = "download-btn btn-sm"))
              )
      ),
      
      # ==================================================================
      # ECONOMIC OUTCOMES — COMPETITION
      # ==================================================================
      tabItem(tabName = "competition",
              h2("Competition Analysis"),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("econ", "competition"))),
              div(class = "question-header", "Is there high competition?"),
              # Row 1: Trend over time + By Contract Value
              fluidRow(
                box(title = "Single-Bid Rate Over Time", width = 6, solidHeader = TRUE, status = "primary",
                    div(class = "description-box",
                        p("Share of contracts receiving only one bid, by year.",
                          " A single bid means no competitive pressure on price.",
                          " Red dots = years above the overall average.")),
                    plotlyOutput("single_bid_overall_plot", height = "420px"),
                    downloadButton("dl_single_bid_overall", "Download", class = "download-btn btn-sm")),
                box(title = "By Contract Value", width = 6, solidHeader = TRUE, status = "primary",
                    div(class = "description-box",
                        p("Single-bid rate across contract value bands.",
                          " Very low-value and very high-value contracts often attract fewer bidders.")),
                    plotlyOutput("single_bid_price_plot", height = "420px"),
                    downloadButton("dl_single_bid_price", "Download", class = "download-btn btn-sm"))
              ),
              # Row 2: Procedure + Buyer Group
              fluidRow(
                box(title = "By Procedure Type", width = 6, solidHeader = TRUE, status = "primary",
                    div(class = "description-box",
                        p("Single-bid rate per procedure type with 95% confidence intervals.",
                          " Dot size reflects contract volume. Direct awards typically show higher rates by design.")),
                    plotlyOutput("single_bid_procedure_plot", height = "420px"),
                    downloadButton("dl_single_bid_procedure", "Download", class = "download-btn btn-sm")),
                box(title = "By Buyer Group", width = 6, solidHeader = TRUE, status = "primary",
                    div(class = "description-box",
                        p("Single-bid rate by buyer institution type.",
                          " Highlights which types of buyers face the least competition.")),
                    plotlyOutput("single_bid_buyer_group_plot", height = "420px"),
                    downloadButton("dl_single_bid_buyer_group", "Download", class = "download-btn btn-sm"))
              ),
              # Row 3: CPV Sector (full width)
              fluidRow(
                box(title = "By CPV Sector", width = 12, solidHeader = TRUE, status = "primary",
                    div(class = "description-box",
                        p("Single-bid rate by procurement market (CPV sector).",
                          " Some sectors are structurally less competitive than others.")),
                    uiOutput("single_bid_market_plot_ui"),
                    downloadButton("dl_single_bid_market", "Download", class = "download-btn btn-sm"))
              ),
              # Row 4: Top buyers (full width)
              fluidRow(
                box(title = "Top Buyers by Single-Bid Rate", width = 12, solidHeader = TRUE, status = "primary",
                    div(class = "description-box",
                        p("Buyers with the highest share of single-bid contracts (minimum contract threshold applies).",
                          " Label shows buyer name with total contract count in brackets.",
                          " Dot size reflects contract volume. Colour: teal = near average, red = well above average.")),
                    fluidRow(
                      column(6, sliderInput("sb_buy_top_n",
                                            "Number of buyers to show:",
                                            min = 5, max = 50, value = 20, step = 5)),
                      column(6, sliderInput("sb_buy_min_tenders",
                                            "Minimum contracts per buyer:",
                                            min = 0, max = 500, value = 50, step = 10))
                    ),
                    uiOutput("top_buyers_single_bid_plot_ui"),
                    downloadButton("dl_top_buyers_single_bid", "Download", class = "download-btn btn-sm"))
              )
      ),
      
      # ==================================================================
      # ADMIN — CONFIGURATION
      # ==================================================================
      tabItem(tabName = "configuration",
              h2("Thresholds & Configuration"),
              p(style = "color:#666; margin-bottom:20px;",
                "Configure global settings and value thresholds.",
                " Submission and decision period thresholds have moved to their respective tabs."),
              
              h3(style = "margin-top:0;", "A. Procurement Value Thresholds (local currency — bid_price column)"),
              p(style = "color:#666;",
                "Legal contract-value boundaries that determine which procedure type is required.",
                " Bunching just below a threshold may indicate contract splitting.",
                " These thresholds power the ", strong("Bunching Analysis"),
                " chart in the Procedure Types tab."),
              fluidRow(
                box(title = "Value Thresholds by Supply Type",
                    width = 12, solidHeader = TRUE, status = "primary",
                    p(class = "description-box",
                      "Enter the contract value above which", strong(" Open Procedure"),
                      " is legally required, separately for each supply type.",
                      " Contracts kept just", em("below"), " these values to avoid the",
                      " more demanding procedure will show up as bunching in the analysis.",
                      " Leave blank if no threshold applies."),
                    fluidRow(
                      column(3), column(3, strong(icon("box"), " Goods")),
                      column(3, strong(icon("hard-hat"), " Works")),
                      column(3, strong(icon("briefcase"), " Services"))
                    ),
                    hr(style = "margin:6px 0;"),
                    fluidRow(
                      column(3, p(strong("Open Procedure threshold:"))),
                      column(3, numericInput("price_open_goods",    NULL, value = NA, min = 0, step = 1000)),
                      column(3, numericInput("price_open_works",    NULL, value = NA, min = 0, step = 1000)),
                      column(3, numericInput("price_open_services", NULL, value = NA, min = 0, step = 1000))
                    ),
                    tags$div(style = "display:none;",
                             numericInput("price_rest_goods", NULL, value = NA),
                             numericInput("price_rest_works", NULL, value = NA),
                             numericInput("price_rest_services", NULL, value = NA),
                             numericInput("price_neg_pub_goods", NULL, value = NA),
                             numericInput("price_neg_pub_works", NULL, value = NA),
                             numericInput("price_neg_pub_services", NULL, value = NA),
                             numericInput("price_neg_nopub_goods", NULL, value = NA),
                             numericInput("price_neg_nopub_works", NULL, value = NA),
                             numericInput("price_neg_nopub_services", NULL, value = NA),
                             numericInput("price_competitive_goods", NULL, value = NA),
                             numericInput("price_competitive_works", NULL, value = NA),
                             numericInput("price_competitive_services", NULL, value = NA),
                             numericInput("price_innov_goods", NULL, value = NA),
                             numericInput("price_innov_works", NULL, value = NA),
                             numericInput("price_innov_services", NULL, value = NA),
                             numericInput("price_direct_goods", NULL, value = NA),
                             numericInput("price_direct_works", NULL, value = NA),
                             numericInput("price_direct_services", NULL, value = NA),
                             numericInput("price_other_goods", NULL, value = NA),
                             numericInput("price_other_works", NULL, value = NA),
                             numericInput("price_other_services", NULL, value = NA)
                    )
                )
              ),
              
              h3("B. Procedure Types to Include in All Outputs"),
              p(style = "color:#555; margin-bottom:10px;",
                "Select which procedure types to include across all plots and analyses.",
                " Deselecting a type removes it from every chart in the app."),
              fluidRow(
                box(title = "Global Procedure Type Filter", width = 12, solidHeader = TRUE, status = "info",
                    checkboxGroupInput("global_proc_filter", label = NULL,
                                       choices = PROC_TYPE_LABELS, selected = PROC_TYPE_LABELS, inline = TRUE),
                    fluidRow(column(6,
                                    actionButton("select_all_procs",   "Select All",   class = "btn-xs btn-default"),
                                    actionButton("deselect_all_procs", "Deselect All", class = "btn-xs btn-default")
                    ))
                )
              ),
              
              fluidRow(
                column(12,
                       actionButton("apply_thresholds", "Apply Thresholds",
                                    icon = icon("check"), class = "btn-success btn-lg"),
                       span(style = "margin-left:15px; color:#27ae60; font-weight:bold;",
                            textOutput("threshold_status", inline = TRUE))
                )
              ),
              br(),
              fluidRow(
                box(title = "Current Active Thresholds", width = 12, status = "info",
                    verbatimTextOutput("threshold_summary"))
              )
      ),
      
      # ==================================================================
      # ADMIN — PROCEDURE TYPES
      # ==================================================================
      tabItem(tabName = "procedures",
              h2("Procedure Type Analysis"),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("admin", "proc"))),
              div(class = "question-header", "Is there an overuse of some procedure types?"),
              fluidRow(
                box(title = "Procedure Type Share by Contract Value", width = 6, solidHeader = TRUE, status = "primary",
                    div(class = "description-box", p("Share of total contract value awarded through each procedure type.")),
                    plotlyOutput("procedure_share_value_plot", height = "420px"),
                    downloadButton("dl_proc_share_value", "Download Figure", class = "download-btn btn-sm")),
                box(title = "Procedure Type Share by Contract Count", width = 6, solidHeader = TRUE, status = "primary",
                    div(class = "description-box", p("Share of contract numbers for each procedure type.")),
                    plotlyOutput("procedure_share_count_plot", height = "420px"),
                    downloadButton("dl_proc_share_count", "Download Figure", class = "download-btn btn-sm"))
              ),
              div(class = "question-header", "Is there bunching of contract values near procedure-type thresholds?"),
              fluidRow(
                box(title = "Contract Value Distribution by Supply Type",
                    width = 12, solidHeader = TRUE, status = "primary",
                    div(class = "description-box",
                        p("Log-scale histogram of contract values, with separate panels for",
                          strong(" Goods"), ",", strong(" Works"), ", and", strong(" Services"), ".",
                          " Each coloured series represents one procedure type.",
                          " Use this chart to understand the overall shape and spread of procurement spending.")),
                    fluidRow(
                      column(3,
                             checkboxGroupInput("proc_value_dist_procs", "Show procedure types:",
                                                choices  = PROC_TYPE_LABELS,
                                                selected = c("Open Procedure","Restricted Procedure","Negotiated with publications"),
                                                inline   = FALSE)
                      ),
                      column(9, plotlyOutput("proc_value_dist_plot", height = "420px"))
                    ),
                    downloadButton("dl_proc_value_dist", "Download Figure", class = "download-btn btn-sm")
                )
              ),
              fluidRow(
                box(title = "Bunching Analysis: Are contracts clustered just below legal thresholds?",
                    width = 12, solidHeader = TRUE, status = "warning",
                    div(class = "description-box",
                        p("Each panel shows the", strong("overall distribution of all contracts"),
                          " (across all procedure types) for one supply category,",
                          " zoomed in around one legal value threshold.",
                          " The logic is: a threshold like 30K means Open Procedure is",
                          strong(" required"), " above that value.",
                          " Buyers who want to avoid that requirement may split contracts",
                          " to keep them just below the threshold.",
                          " A spike in", strong(" all contracts"), " just below the threshold",
                          " — regardless of which procedure they formally used —",
                          " is the warning sign.", br(), br(),
                          " The", strong(" dotted line"), " is the statistical counterfactual:",
                          " what the distribution", em("would"), " look like with no manipulation,",
                          " estimated by fitting a smooth curve to the parts of the distribution",
                          " away from the threshold.", br(),
                          strong("Excess mass %"), " = how many more contracts sit just below",
                          " the threshold than the model expects under normal procurement behaviour.",
                          " Red bars and a large positive % are the key warning indicators.")),
                    uiOutput("bunching_status_ui"),
                    fluidRow(
                      column(4,
                             sliderInput("n_search_bins",
                                         "Search zone width (bins below threshold):",
                                         min = 1, max = 25, value = 10, step = 1),
                             helpText(icon("info-circle"),
                                      " Each bin = 0.05 log₁₀ units ≈ 12% of the threshold value.",
                                      " 10 bins covers roughly the 50% of the threshold value",
                                      strong(" below"), " it (e.g. threshold 100K → zone covers ~50K–99K).", br(),
                                      " The polynomial is fitted excluding this zone on both sides",
                                      " of the threshold, then extrapolated through it.", br(),
                                      strong(" Wider zone = more conservative counterfactual"),
                                      " (more data excluded from the fit, curve must extrapolate further).")
                      ),
                      column(4,
                             sliderInput("spike_sensitivity",
                                         "Highlight red: bins exceeding expected by at least:",
                                         min = 0, max = 200, value = 50, step = 10, post = "%"),
                             helpText(icon("info-circle"),
                                      " Controls only which bins turn red inside the search zone.",
                                      " Set to 0% to highlight every bin that exceeds the model.",
                                      " The total excess mass % in the panel titles never changes with this —",
                                      " it always sums the full search zone.")
                      ),
                      column(4,
                             div(style = "font-size:12px;",
                                 div(style = "background:#eaf4fb; border-left:3px solid #3498db; padding:10px; margin-bottom:8px;",
                                     strong(icon("info-circle"), " How to read this chart"), br(),
                                     "The dotted line shows the", strong(" predicted"),
                                     " contract frequency — estimated from the distribution on both sides of the threshold,",
                                     " excluding the zone immediately around it.",
                                     " It represents the baseline: what the distribution would look like absent any strategic behaviour.", br(), br(),
                                     strong("Excess mass"), " is the percentage gap between observed and predicted contracts just below the threshold.",
                                     " A large positive value indicates an unusual concentration of contracts just under the legal limit."
                                 ),
                                 tags$details(
                                   tags$summary(style = "cursor:pointer; color:#2980b9; font-size:11px;",
                                                icon("cog"), " Technical detail"),
                                   div(style = "background:#f8f9fa; border-left:3px solid #aaa; padding:8px; margin-top:4px;",
                                       "A degree-4 polynomial is fitted by OLS to all bins ",
                                       strong("outside"), " the search zone.",
                                       " The zone is excluded symmetrically on both sides of the threshold",
                                       " so the fit is not distorted by bunching below or a gap above.", br(), br(),
                                       "The fitted curve is extrapolated through the excluded zone.",
                                       " Excess mass = Σ(observed − predicted) / Σpredicted,",
                                       " summed only over bins ", strong("below"), " the threshold.", br(), br(),
                                       "Wider search zone → more bins excluded → longer extrapolation → dotted line changes shape."
                                   )
                                 )
                             )
                      )
                    ),
                    uiOutput("bunching_analysis_plot_ui"),
                    div(style = "margin-top:8px;",
                        downloadButton("dl_bunching", "Download Chart",
                                       class = "btn-sm btn-default", icon = icon("download")))
                )
              )
      ),
      
      # ==================================================================
      # ADMIN — SUBMISSION PERIODS
      # ==================================================================
      tabItem(tabName = "submission",
              h2("Submission Period Analysis"),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("admin", "subm"))),
              
              # ── 1. Overall distribution (no procedure filter) ─────────────────
              div(class = "question-header", "How are submission periods distributed overall?"),
              fluidRow(
                box(title = "Overall Submission Period Distribution", width = 12,
                    solidHeader = TRUE, status = "primary",
                    div(class = "description-box", p("Distribution of submission periods (days from publication of call for tender to bid deadline) across all procedure types. Vertical lines mark Q1 (25th), median, Q3 (75th) percentiles and the mean.")),
                    plotlyOutput("submission_dist_plot", height = "400px"),
                    downloadButton("dl_subm_dist", "Download Figure", class = "download-btn btn-sm"))
              ),
              
              # ── 2. Procedure selector + distribution by procedure ─────────────
              div(class = "question-header", "How do submission periods vary by procedure type?"),
              fluidRow(
                box(title = "Procedure Type Selection", width = 12, solidHeader = TRUE, status = "info",
                    p(style = "color:#555; margin-bottom:8px;",
                      "Select which procedure types to display in the distribution and compliance charts below."),
                    checkboxGroupInput("subm_proc_filter", label = NULL,
                                       choices  = PROC_TYPE_LABELS,
                                       selected = "Open Procedure",
                                       inline   = TRUE),
                    fluidRow(column(6,
                                    actionButton("subm_proc_select_all",   "Select All",   class = "btn-xs btn-default"),
                                    actionButton("subm_proc_deselect_all", "Deselect All", class = "btn-xs btn-default")
                    ))
                )
              ),
              fluidRow(
                box(title = "Submission Periods by Procedure Type", width = 12,
                    solidHeader = TRUE, status = "primary",
                    div(class = "description-box", p("Faceted histograms (2-day bins) per procedure type, sorted by median. Each panel shows the full distribution with Q1/Q3 (dashed), median (solid) and mean (dotted) reference lines. Only procedure types selected above are shown.")),
                    plotlyOutput("submission_proc_plot", height = "420px"),
                    downloadButton("dl_subm_proc", "Download Figure", class = "download-btn btn-sm"))
              ),
              
              # ── 3. Threshold configuration (inline, collapsible) ──────────────
              div(class = "question-header", "Configure short-deadline thresholds"),
              fluidRow(
                box(title = "Submission Short-Deadline Thresholds", width = 12,
                    solidHeader = TRUE, status = "warning", collapsible = TRUE, collapsed = FALSE,
                    p(style = "color:#555; margin-bottom:8px;",
                      "Set the legal minimum days for bid submission by procedure type.",
                      " Values below this threshold are flagged as 'short'.",
                      " Tick 'No legal threshold' to derive the cutoff statistically.",
                      " You can also enable an optional ", strong("medium band"),
                      " to flag contracts between two day-counts as 'medium'."),
                    TUKEY_EXPLANATION,
                    fluidRow(
                      column(3, div(class="proc-section", proc_threshold_ui("open",        "Open Procedure",                  30, show_medium=TRUE, med_min=30, med_max=60))),
                      column(3, div(class="proc-section", proc_threshold_ui("restricted",  "Restricted Procedure",            30, show_medium=TRUE))),
                      column(3, div(class="proc-section", proc_threshold_ui("neg_pub",     "Negotiated with publications",    30, show_medium=TRUE))),
                      column(3, div(class="proc-section", proc_threshold_ui("neg_nopub",   "Negotiated without publications", NA, show_medium=TRUE)))
                    ),
                    fluidRow(
                      column(3, div(class="proc-section", proc_threshold_ui("neg_unspec",  "Negotiated (unspecified)",        NA, show_medium=TRUE))),
                      column(3, div(class="proc-section", proc_threshold_ui("competitive", "Competitive Dialogue",            NA, show_medium=TRUE))),
                      column(3, div(class="proc-section", proc_threshold_ui("innov",       "Innovation Partnership",          NA, show_medium=TRUE))),
                      column(3, div(class="proc-section", proc_threshold_ui("direct",      "Direct Award",                   NA, show_medium=TRUE)))
                    ),
                    fluidRow(
                      column(3, div(class="proc-section", proc_threshold_ui("other", "Other", NA, show_medium=TRUE)))
                    ),
                    br(),
                    fluidRow(column(12,
                                    actionButton("apply_thresholds_subm", "Apply Submission Thresholds",
                                                 icon = icon("check"), class = "btn-success"),
                                    span(style = "margin-left:12px; color:#27ae60; font-weight:bold;",
                                         textOutput("threshold_status_subm", inline = TRUE))
                    ))
                )
              ),
              
              # ── 4. Threshold compliance summary ───────────────────────────────
              div(class = "question-header", "What share of contracts have too-short submission periods?"),
              fluidRow(
                box(title = "Short vs Normal Submission Periods — Flagged Shares", width = 12,
                    solidHeader = TRUE, status = "warning",
                    div(class = "description-box",
                        p("Top chart: share of contracts flagged as short (amber), medium (orange, when enabled), or normal (blue) per procedure type, sorted by share short.",
                          " Bottom chart: per-procedure histograms with 1–2 day bins; the dashed red line marks the short-deadline threshold and the shaded zone highlights the flagged region.",
                          " Threshold values and medium bands are configured in the panel above.")),
                    plotlyOutput("subm_share_chart",  height = "280px"),
                    downloadButton("dl_subm_share", "Download Share Chart", class = "download-btn btn-sm"),
                    hr(),
                    plotlyOutput("submission_short_plot", height = "500px"),
                    downloadButton("dl_subm_short", "Download Distribution Figure", class = "download-btn btn-sm"))
              ),
              
              # ── 5. Buyer types breakdown ──────────────────────────────────────
              div(class = "question-header", "Which buyer types set the shortest submission periods?"),
              fluidRow(
                box(title = "Short Submission Periods by Buyer Group", width = 12,
                    solidHeader = TRUE, status = "primary",
                    div(class = "description-box", p("Share of contracts with short submission deadlines by buyer group, grouped by procedure type. Toggle between contract count share (default), contract value share, or both panels stacked. Colors distinguish procedure types.")),
                    fluidRow(
                      column(4,
                             radioButtons("subm_buyer_view", label = "Show metric:",
                                          choices  = c("By contract count" = "count",
                                                       "By contract value" = "value",
                                                       "Both"              = "both"),
                                          selected = "count", inline = TRUE)
                      )
                    ),
                    plotlyOutput("buyer_short_plot", height = "480px"),
                    downloadButton("dl_buyer_short", "Download Figure", class = "download-btn btn-sm"))
              )
      ),
      
      # ==================================================================
      # ADMIN — DECISION PERIODS
      # ==================================================================
      tabItem(tabName = "decision",
              h2("Decision Period Analysis"),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("admin", "dec"))),
              
              # ── 1. Overall distribution ───────────────────────────────────────
              div(class = "question-header", "How are decision periods distributed overall?"),
              fluidRow(
                box(title = "Overall Decision Period Distribution", width = 12,
                    solidHeader = TRUE, status = "primary",
                    div(class = "description-box", p("Distribution of decision periods (days from bid deadline to contract award or signature) across all procedure types. Vertical lines mark Q1 (25th), median, Q3 (75th) percentiles and the mean.")),
                    plotlyOutput("decision_dist_plot", height = "400px"),
                    downloadButton("dl_dec_dist", "Download Figure", class = "download-btn btn-sm"))
              ),
              
              # ── 2. Procedure selector + distribution by procedure ─────────────
              div(class = "question-header", "How do decision periods vary by procedure type?"),
              fluidRow(
                box(title = "Procedure Type Selection", width = 12, solidHeader = TRUE, status = "info",
                    p(style = "color:#555; margin-bottom:8px;",
                      "Select which procedure types to display in the distribution and compliance charts below."),
                    checkboxGroupInput("dec_proc_filter", label = NULL,
                                       choices  = PROC_TYPE_LABELS,
                                       selected = "Open Procedure",
                                       inline   = TRUE),
                    fluidRow(column(6,
                                    actionButton("dec_proc_select_all",   "Select All",   class = "btn-xs btn-default"),
                                    actionButton("dec_proc_deselect_all", "Deselect All", class = "btn-xs btn-default")
                    ))
                )
              ),
              fluidRow(
                box(title = "Decision Periods by Procedure Type", width = 12,
                    solidHeader = TRUE, status = "primary",
                    div(class = "description-box", p("Faceted histograms (2–3 day bins) per procedure type, sorted by median. Each panel shows the distribution with Q1/Q3 (dashed), median (solid) and mean (dotted) reference lines. Only procedure types selected above are shown.")),
                    plotlyOutput("decision_proc_plot", height = "420px"),
                    downloadButton("dl_dec_proc", "Download Figure", class = "download-btn btn-sm"))
              ),
              
              # ── 3. Threshold configuration (inline, collapsible) ──────────────
              div(class = "question-header", "Configure long-decision thresholds"),
              fluidRow(
                box(title = "Decision Long-Period Thresholds", width = 12,
                    solidHeader = TRUE, status = "warning", collapsible = TRUE, collapsed = FALSE,
                    p(style = "color:#555; margin-bottom:8px;",
                      "Maximum acceptable days between bid deadline and contract award per procedure type.",
                      " Decisions above this threshold are flagged as 'long'."),
                    fluidRow(
                      column(3, div(class="proc-section", proc_threshold_ui("dec_open",        "Open Procedure",                  60, is_decision=TRUE))),
                      column(3, div(class="proc-section", proc_threshold_ui("dec_restricted",  "Restricted Procedure",            60, is_decision=TRUE))),
                      column(3, div(class="proc-section", proc_threshold_ui("dec_neg_pub",     "Negotiated with publications",    60, is_decision=TRUE))),
                      column(3, div(class="proc-section", proc_threshold_ui("dec_neg_nopub",   "Negotiated without publications", NA, is_decision=TRUE)))
                    ),
                    fluidRow(
                      column(3, div(class="proc-section", proc_threshold_ui("dec_neg_unspec",  "Negotiated (unspecified)",        NA, is_decision=TRUE))),
                      column(3, div(class="proc-section", proc_threshold_ui("dec_competitive", "Competitive Dialogue",            NA, is_decision=TRUE))),
                      column(3, div(class="proc-section", proc_threshold_ui("dec_innov",       "Innovation Partnership",          NA, is_decision=TRUE))),
                      column(3, div(class="proc-section", proc_threshold_ui("dec_direct",      "Direct Award",                   NA, is_decision=TRUE)))
                    ),
                    fluidRow(
                      column(3, div(class="proc-section", proc_threshold_ui("dec_other", "Other", NA, is_decision=TRUE)))
                    ),
                    br(),
                    fluidRow(column(12,
                                    actionButton("apply_thresholds_dec", "Apply Decision Thresholds",
                                                 icon = icon("check"), class = "btn-success"),
                                    span(style = "margin-left:12px; color:#27ae60; font-weight:bold;",
                                         textOutput("threshold_status_dec", inline = TRUE))
                    ))
                )
              ),
              
              # ── 4. Threshold compliance summary ───────────────────────────────
              div(class = "question-header", "What share of contracts have too-long decision periods?"),
              fluidRow(
                box(title = "Long vs Normal Decision Periods — Flagged Shares", width = 12,
                    solidHeader = TRUE, status = "warning",
                    div(class = "description-box",
                        p("Top chart: share of contracts flagged as long (amber) or normal (blue) per procedure type, sorted by share long.",
                          " Bottom chart: per-procedure histograms with 2–3 day bins; the dashed red line marks the long-decision threshold and the shaded zone highlights the flagged region.",
                          " Threshold values are configured in the panel above.")),
                    plotlyOutput("dec_share_chart",    height = "280px"),
                    downloadButton("dl_dec_share", "Download Share Chart", class = "download-btn btn-sm"),
                    hr(),
                    plotlyOutput("decision_long_plot", height = "500px"),
                    downloadButton("dl_dec_long", "Download Distribution Figure", class = "download-btn btn-sm"))
              ),
              
              # ── 5. Buyer types breakdown ──────────────────────────────────────
              div(class = "question-header", "Which buyer types have the longest decision periods?"),
              fluidRow(
                box(title = "Long Decision Periods by Buyer Group", width = 12,
                    solidHeader = TRUE, status = "primary",
                    div(class = "description-box", p("Share of contracts with long decision periods by buyer group, grouped by procedure type. Toggle between contract count share (default), contract value share, or both panels stacked. Colors distinguish procedure types.")),
                    fluidRow(
                      column(4,
                             radioButtons("dec_buyer_view", label = "Show metric:",
                                          choices  = c("By contract count" = "count",
                                                       "By contract value" = "value",
                                                       "Both"              = "both"),
                                          selected = "count", inline = TRUE)
                      )
                    ),
                    plotlyOutput("buyer_long_plot", height = "480px"),
                    downloadButton("dl_buyer_long", "Download Figure", class = "download-btn btn-sm"))
              )
      ),
      
      # ==================================================================
      # ADMIN — REGRESSION
      # ==================================================================
      tabItem(tabName = "regression",
              h2("Regression Analysis: Administrative Efficiency & Competition"),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("admin", "reg"))),
              div(class = "question-header",
                  icon("chart-line", style = "margin-right:8px;color:var(--amber);"),
                  "Are short submission and long decision periods linked to reduced competition?"),
              div(class = "description-box",
                  p(icon("info-circle"), tags$strong(" About these regressions: "),
                    "Fractional logit models with year fixed effects test whether short submission periods and long decision periods are associated with higher single-bidding rates. ",
                    "Each model is tested across ", tags$strong("~40 specifications"),
                    " (varying fixed effects, clustering, and controls) to assess robustness.")),
              fluidRow(
                box(width = 12, status = "warning",
                    div(class = "reg-run-box",
                        div(class = "reg-status",  uiOutput("regression_status_box")),
                        div(class = "reg-btn-wrap",
                            actionButton("run_regressions_now",
                                         label = tagList(icon("play-circle"), " Run / Re-run Regressions"),
                                         class = "btn-warning reg-run-btn")))
                )
              ),
              fluidRow(box(title = "Effect of Short Submission Periods on Single Bidding", width = 12,
                           solidHeader = TRUE, status = "info",
                           uiOutput("short_reg_plot_ui"), uiOutput("dl_short_reg_ui"),
                           uiOutput("short_reg_formula_ui"))),
              fluidRow(box(title = "Sensitivity Analysis: Short Submission Period Model", width = 12,
                           solidHeader = TRUE, status = "info", collapsible = TRUE, collapsed = TRUE,
                           uiOutput("sensitivity_short_ui"))),
              fluidRow(box(title = "Effect of Long Decision Periods on Single Bidding", width = 12,
                           solidHeader = TRUE, status = "info",
                           uiOutput("long_reg_plot_ui"), uiOutput("dl_long_reg_ui"),
                           uiOutput("long_reg_formula_ui"))),
              fluidRow(box(title = "Sensitivity Analysis: Long Decision Period Model", width = 12,
                           solidHeader = TRUE, status = "info", collapsible = TRUE, collapsed = TRUE,
                           uiOutput("sensitivity_long_ui")))
      ),
      
      # ==================================================================
      # EXPORT
      # ==================================================================
      # ==================================================================
      # INTEGRITY — OVERVIEW
      # ==================================================================
      # ==================================================================
      # INTEGRITY — MISSING VALUES
      # ==================================================================
      tabItem(tabName = "integrity_missing",
              h2("Missing Values Analysis"),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("integ", "missing"))),
              div(class = "question-header", "Are there observable patterns of underreporting information in the data?"),
              div(class = "description-box",
                  p("Assessment of data completeness by examining missing values across all variables. ",
                    "Each variable contributes to one of the ProAct indicators.")),
              fluidRow(
                box(title = "Advanced Missingness Tests", width = 12, solidHeader = TRUE, status = "warning",
                    div(class = "description-box",
                        tags$b("Little's MCAR test"), " checks whether values are Missing Completely At Random. ",
                        tags$b("MAR predictability"), " fits a logistic regression per variable to see if missingness is ",
                        "predictable from year, buyer type, procedure, and contract value. ",
                        tags$b("Co-occurrence"), " shows which variable pairs tend to go missing on the same row."),
                    actionButton("integ_run_missing_advanced", "Run Advanced Missingness Tests",
                                 icon = icon("flask"), class = "btn-warning"),
                    br(), br(),
                    uiOutput("integ_mcar_summary_card"))
              ),
              fluidRow(
                box(title = "Overall Missing Values by Variable", width = 12, solidHeader = TRUE, status = "warning",
                    div(class = "description-box",
                        "Share of missing values for each key variable, sorted by severity. ",
                        "Colour zones: ", tags$b("green < 5%"), " (low), ",
                        tags$b("amber 5-20%"), " (notable), ", tags$b("red >= 20%"), " (high-risk)."),
                    plotlyOutput("integ_missing_overall_plot", height = "auto"),
                    uiOutput("integ_missing_overall_height_spacer"),
                    downloadButton("integ_dl_missing_overall", "Download Figure", class = "download-btn btn-sm"))
              ),
              fluidRow(
                box(title = "Missingness by Buyer Type", width = 12, solidHeader = TRUE, status = "warning",
                    div(class = "description-box", "Heatmap of missing share per variable by buyer type. Hover for exact values."),
                    uiOutput("integ_missing_buyer_slider_ui"),
                    plotlyOutput("integ_missing_buyer_plot", height = "auto"),
                    uiOutput("integ_missing_buyer_plot_height"),
                    downloadButton("integ_dl_missing_buyer", "Download Figure", class = "download-btn btn-sm"))
              ),
              fluidRow(
                box(title = "Missingness by Procedure Type", width = 12, solidHeader = TRUE, status = "warning",
                    div(class = "description-box", "Same heatmap broken down by procurement procedure type. Hover for exact values."),
                    uiOutput("integ_missing_procedure_slider_ui"),
                    plotlyOutput("integ_missing_procedure_plot", height = "auto"),
                    uiOutput("integ_missing_procedure_plot_height"),
                    downloadButton("integ_dl_missing_procedure", "Download Figure", class = "download-btn btn-sm"))
              ),
              fluidRow(
                box(title = "Trends in Missing Shares Over Time", width = 12, solidHeader = TRUE, status = "warning",
                    div(class = "description-box",
                        "Year x variable heatmap. A block of red in a specific year signals a reporting regime change or data-collection failure."),
                    uiOutput("integ_missing_time_slider_ui"),
                    plotlyOutput("integ_missing_time_plot", height = "auto"),
                    uiOutput("integ_missing_time_height_spacer"),
                    downloadButton("integ_dl_missing_time", "Download Figure", class = "download-btn btn-sm"))
              ),
              fluidRow(
                box(title = "Variable Pairs Missing Together", width = 12, solidHeader = TRUE, status = "warning",
                    div(class = "description-box",
                        "Jaccard co-occurrence: how often do two variables ", tags$em("both"),
                        " go missing on the same row? Pairs coloured red represent a single root cause."),
                    uiOutput("integ_missing_cooccurrence_ui"),
                    uiOutput("integ_missing_cooccurrence_download_ui"))
              ),
              fluidRow(
                box(title = "Is Missing Data Random? Pattern Analysis per Variable", width = 12, solidHeader = TRUE, status = "warning",
                    div(class = "description-box",
                        tags$p("For each variable, we test: can we predict which records will have this field missing?"),
                        tags$p(tags$b("Pattern score near 0%"), " means missing data appears random. ",
                               tags$b("Score above 10%"), " means certain contracts systematically skip this field.")),
                    uiOutput("integ_missing_mar_ui"),
                    uiOutput("integ_missing_mar_download_ui"))
              )
      ),
      
      # ==================================================================
      # INTEGRITY — INTEROPERABILITY
      # ==================================================================
      tabItem(tabName = "integrity_interop",
              h2("Interoperability Analysis"),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("integ", "interop"))),
              div(class = "question-header",
                  "Can this data be matched to other registers in the country to ensure higher quality monitoring?"),
              div(class = "description-box",
                  p("The ability to match public procurement data with other registers significantly enhances analytical power and auditing capabilities.")),
              fluidRow(
                box(title = "Interoperability Potential at Organization Level",
                    width = 12, solidHeader = TRUE, status = "info",
                    DT::dataTableOutput("integ_interoperability_table"))
              )
      ),
      
      # ==================================================================
      # INTEGRITY — RISKY PROFILES
      # ==================================================================
      tabItem(tabName = "integrity_risky",
              h2("Companies with Risky Profiles"),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("integ", "risky"))),
              div(class = "question-header", "Do companies winning contracts have risky profiles?"),
              div(class = "description-box",
                  p("This analysis seeks to identify potentially suspicious patterns in company behavior, including movements between markets.")),
              fluidRow(
                box(title = "Unusual Market Entry Analysis", width = 12, solidHeader = TRUE, status = "warning",
                    div(class = "description-box",
                        h4(icon("info-circle"), " Methodology", style = "margin-top:0; color:#c0392b;"),
                        p(tags$b("What is being detected:"), " Suppliers who systematically win contracts in markets unrelated to their core specialisation."),
                        p(tags$b("Supplier home market:"), " Each supplier's 'home' CPV cluster is the 3-digit product code group where they have the most contract wins."),
                        p(tags$b("Atypicality flag:"), " A supplier-cluster combination is flagged as atypical if the supplier has >= 4 total wins, <= 3 wins in this market, and this market < 5% of their total portfolio."),
                        p(tags$b("Surprise score:"), " Computed as -log[(n_ic+1)/(n_c+n_suppliers_c)], z-scored within each cluster.")
                    ),
                    uiOutput("integ_network_status_ui"),
                    actionButton("integ_run_network_analysis", "Run Network Analysis",
                                 icon = icon("project-diagram"), class = "btn-warning btn-lg run-btn"),
                    br(), br(),
                    conditionalPanel(
                      condition = "output.integ_network_done_flag",
                      fluidRow(
                        column(4, uiOutput("integ_net_min_bidders_ui")),
                        column(4, uiOutput("integ_net_top_clusters_ui")),
                        column(4, uiOutput("integ_net_cluster_filter_ui"))
                      )
                    ),
                    tabsetPanel(id = "integ_network_tabs", type = "tabs",
                                tabPanel("Flow Matrix",
                                         br(),
                                         div(class = "description-box",
                                             tags$b(icon("map"), " How to read:"),
                                             tags$ul(
                                               tags$li("Row = home market, Column = target market where suppliers entered unusually"),
                                               tags$li("Number = how many suppliers crossed that route"),
                                               tags$li("Darker blue = more suppliers — higher concern")
                                             )),
                                         uiOutput("integ_flow_matrix_plot_ui"),
                                         uiOutput("integ_download_network_ui")
                                ),
                                tabPanel("Network Graph",
                                         br(),
                                         div(class = "description-box",
                                             tags$b(icon("project-diagram"), " How to read:"),
                                             tags$ul(
                                               tags$li("Node = CPV market cluster. Arrow A->B = suppliers normally in A winning unusually in B."),
                                               tags$li("Arrow thickness = supplier count. Colour: grey = average, orange = moderate, red = high surprise.")
                                             )),
                                         uiOutput("integ_network_plot_ui"),
                                         uiOutput("integ_download_network_graph_ui")
                                )
                    )
                )
              ),
              fluidRow(
                box(title = "Suppliers with Unusually Diversified Market Entries",
                    width = 12, solidHeader = TRUE, status = "warning",
                    div(class = "description-box",
                        p("Suppliers ranked by how consistently unusual their out-of-portfolio wins are. Hover for details.")),
                    uiOutput("integ_supplier_unusual_plot_ui"),
                    uiOutput("integ_download_supplier_unusual_ui"))
              ),
              fluidRow(
                box(title = "Markets Attracting Unusual Supplier Entries",
                    width = 12, solidHeader = TRUE, status = "warning",
                    div(class = "description-box",
                        p("Each bubble is a CPV market ranked by a risk index combining surprise intensity and number of unusual entrants.")),
                    uiOutput("integ_market_unusual_plot_ui"),
                    uiOutput("integ_download_market_unusual_ui"))
              ),
              div(class = "question-header", "Are there suspicious connections between buyers and suppliers?"),
              fluidRow(
                box(title = "Top Buyers by Supplier Concentration Over Time",
                    width = 12, solidHeader = TRUE, status = "warning",
                    div(class = "description-box",
                        "Each panel shows the top buyers by maximum supplier concentration in that year. ",
                        tags$b("Red bars"), " flag buyers appearing across multiple years — persistent concentration risk."),
                    fluidRow(
                      column(6, uiOutput("integ_conc_n_buyers_slider_ui")),
                      column(6, uiOutput("integ_conc_min_contracts_slider_ui"))
                    ),
                    plotlyOutput("integ_concentration_plot", height = "auto"),
                    uiOutput("integ_concentration_plot_height"),
                    downloadButton("integ_dl_concentration", "Download Figure", class = "download-btn btn-sm"))
              )
      ),
      
      # ==================================================================
      # INTEGRITY — PRICES & COMPETITION
      # ==================================================================
      tabItem(tabName = "integrity_prices",
              h2("Regression Analysis: Prices & Competition"),
              fluidRow(box(title = "Filters", width = 12, collapsible = TRUE, status = "info",
                           filter_bar_ui("integ", "prices"))),
              div(class = "question-header",
                  icon("chart-line", style = "margin-right:8px;color:var(--rose);"),
                  "Does data incompleteness correlate with reduced competition and higher prices?"),
              div(class = "description-box",
                  p(icon("info-circle"), tags$strong(" About these regressions: "),
                    "Fractional logit and OLS models explore correlations between data missingness, single-bidding rates, and relative prices. ",
                    "These are observational models — they do not establish causality.")),
              fluidRow(
                box(width = 12, status = "warning",
                    div(class = "reg-run-box",
                        div(class = "reg-status",  uiOutput("integ_regression_status_box")),
                        div(class = "reg-btn-wrap",
                            actionButton("integ_run_regressions_now",
                                         label = tagList(icon("play-circle"), " Run / Re-run Regressions"),
                                         class = "btn-warning reg-run-btn")))
                )
              ),
              fluidRow(
                box(title = "Predicted Single-Bidding by Missing Share", width = 12, solidHeader = TRUE, status = "info",
                    div(class = "description-box",
                        p("Estimated relationship between data missingness and single-bid tender prevalence. ",
                          "A fractional logit model with year fixed effects regresses the buyer-year single-bidding share on cumulative missingness.")),
                    uiOutput("integ_singleb_plot_ui"),
                    uiOutput("integ_download_singleb_ui"))
              ),
              fluidRow(
                box(title = "Single-Bidding Sensitivity Analysis", width = 12, solidHeader = TRUE, status = "info",
                    div(class = "description-box", "Robustness of the single-bidding model across different specifications."),
                    uiOutput("integ_singleb_sensitivity_table"))
              ),
              fluidRow(
                box(title = "Predicted Relative Price by Missing Share", width = 12, solidHeader = TRUE, status = "info",
                    div(class = "description-box",
                        p("How relative prices correlate with overall missingness after controlling for key factors. ",
                          "A linear fixed-effects model of relative_price on total_missing_share.")),
                    uiOutput("integ_relprice_plot_ui"),
                    uiOutput("integ_download_relprice_ui"))
              ),
              fluidRow(
                box(title = "Relative Price Sensitivity Analysis", width = 12, solidHeader = TRUE, status = "info",
                    div(class = "description-box", "Robustness of the relative price model across different specifications."),
                    uiOutput("integ_relprice_sensitivity_table"))
              )
      ),
      
      # ==================================================================
      # EXPORT (existing)
      # ==================================================================
      tabItem(tabName = "export",
              h2("Export & Download"),
              tags$p(style = "color:var(--slate-600);font-size:13.5px;margin:-8px 0 20px;",
                     "Download analysis reports (Word) or individual figures (ZIP) for any module. ",
                     "Reports include all charts, tables, and narrative generated from your current filters."),
              fluidRow(
                column(4,
                       div(class = "export-card export-card-econ",
                           div(class = "export-card-title",
                               icon("chart-line", style = "color:var(--teal);"),
                               "Economic Outcomes"),
                           div(class = "export-card-desc",
                               "Market sizing, relative prices, single-bidding rates and supplier dynamics."),
                           div(class = "export-card-btns",
                               downloadButton("dl_econ_word", tagList(icon("file-word"), " Word Report"),
                                              class = "btn btn-info"),
                               downloadButton("dl_econ_zip",  tagList(icon("file-archive"), " All Figures (ZIP)"),
                                              class = "btn btn-success")
                           )
                       )
                ),
                column(4,
                       div(class = "export-card export-card-admin",
                           div(class = "export-card-title",
                               icon("clock", style = "color:var(--amber);"),
                               "Administrative Efficiency"),
                           div(class = "export-card-desc",
                               "Procedure types, submission and decision periods, bunching analysis and regressions."),
                           div(class = "export-card-btns",
                               downloadButton("dl_admin_word", tagList(icon("file-word"), " Word Report"),
                                              class = "btn btn-info"),
                               downloadButton("dl_admin_zip",  tagList(icon("file-archive"), " All Figures (ZIP)"),
                                              class = "btn btn-success")
                           )
                       )
                ),
                column(4,
                       div(class = "export-card export-card-integ",
                           div(class = "export-card-title",
                               icon("shield-alt", style = "color:var(--rose);"),
                               "Procurement Integrity"),
                           div(class = "export-card-desc",
                               "Missing values, interoperability, market concentration, network analysis and regressions."),
                           div(class = "export-card-btns",
                               downloadButton("integ_dl_word_report", tagList(icon("file-word"), " Word Report"),
                                              class = "btn btn-info"),
                               downloadButton("integ_dl_all_figures", tagList(icon("file-archive"), " All Figures (ZIP)"),
                                              class = "btn btn-success")
                           )
                       )
                )
              ),
              br(),
              div(class = "export-status-bar",
                  icon("info-circle", style = "color:var(--slate-400);margin-right:6px;"),
                  textOutput("export_status", inline = TRUE))
      )
    )
  )
)

# ==============================================================================
# SERVER
# ==============================================================================

server <- function(input, output, session) {
  
  # ============================================================
  # REACTIVE VALUES
  # ============================================================
  
  econ <- reactiveValues(
    data              = NULL,
    analysis          = NULL,
    filtered_data     = NULL,
    filtered_analysis = NULL,
    country_code      = NULL,
    cpv_lookup        = NULL,
    value_divisor     = 1,
    slider_trigger    = 0,
    # stored plotly figs — always match what's currently displayed
    fig_supp_bubble        = NULL,
    fig_supp_stability     = NULL,
    fig_supp_trend         = NULL,
    fig_top_suppliers      = NULL,
    fig_rel_buy            = NULL,
    fig_rel_size           = NULL,
    value_max_k            = NULL,
    fig_contracts_year_econ= NULL,
    fig_value_by_year      = NULL
  )
  
  admin <- reactiveValues(
    data              = NULL,
    analysis          = NULL,
    filtered_data     = NULL,
    filtered_analysis = NULL,
    country_code      = NULL,
    value_divisor     = 1,
    thresholds        = NULL,
    price_thresholds  = list(),
    global_proc_filter = PROC_TYPE_LABELS,
    bunching_fig              = NULL,
    fig_contracts_year        = NULL,
    fig_proc_share_value      = NULL,
    fig_proc_share_count      = NULL,
    fig_proc_value_dist       = NULL,
    fig_subm_dist             = NULL,
    fig_subm_proc             = NULL,
    fig_subm_short            = NULL,
    fig_buyer_short           = NULL,
    fig_dec_dist              = NULL,
    fig_dec_proc              = NULL,
    fig_dec_long              = NULL,
    fig_buyer_long            = NULL,
    fig_subm_share            = NULL,
    fig_dec_share             = NULL,
    gg_proc_share_value       = NULL,
    gg_proc_share_count       = NULL,
    gg_subm_dist              = NULL,
    gg_subm_proc              = NULL,
    gg_subm_short             = NULL,
    gg_buyer_short            = NULL,
    gg_dec_dist               = NULL,
    gg_dec_proc               = NULL,
    gg_dec_long               = NULL,
    gg_buyer_long             = NULL,
    value_max_k               = NULL
  )
  
  econ_filters <- reactiveValues(
    active      = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    overview    = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    market      = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    supplier    = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    network     = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    price       = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    competition = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL)
  )
  
  admin_filters <- reactiveValues(
    active   = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    overview = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    proc     = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    subm     = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    dec      = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    reg      = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL)
  )
  
  # Buyer/procedure type mappings (for econ section)
  econ_buyer_mapping       <- reactiveVal(NULL)
  econ_procedure_mapping   <- reactiveVal(NULL)
  admin_procedure_mapping  <- reactiveVal(NULL)
  
  # ── Integrity reactive state ──────────────────────────────────────────
  integ <- reactiveValues(
    data              = NULL,
    analysis          = NULL,
    filtered_data     = NULL,
    filtered_analysis = NULL,
    country_code      = NULL,
    value_divisor     = 1,
    network_done      = FALSE,
    regression_done   = FALSE,
    missing_advanced_done = FALSE,
    # stored plotly figs — always match what's currently displayed
    fig_contracts_year = NULL,
    fig_miss_overall   = NULL,
    fig_miss_buyer     = NULL,
    fig_miss_proc      = NULL,
    fig_miss_time      = NULL,
    fig_miss_cooc      = NULL,
    fig_miss_mar       = NULL,
    fig_supp_unusual   = NULL,
    fig_mkt_unusual    = NULL,
    fig_concentration  = NULL,
    fig_singleb        = NULL,
    fig_relprice       = NULL,
    value_max_k        = NULL
  )
  
  integ_filters <- reactiveValues(
    active   = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    data     = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    missing  = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    interop  = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    risky    = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL),
    prices   = list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL)
  )
  
  integ_filtered_data <- reactive({
    req(integ$data)
    integrity_filter_data(
      df             = integ$data,
      year_range     = input$integ_year_range,
      market         = input$integ_market_filter,
      value_range    = input$integ_value_range,
      buyer_type     = input$integ_buyer_type_filter,
      procedure_type = input$integ_procedure_type_filter,
      value_divisor  = isolate(integ$value_divisor)
    )
  })
  
  # ============================================================
  # ADMIN HELPER FUNCTIONS
  # ============================================================
  
  get_thr_val <- function(proc_id, is_decision = FALSE) {
    no_thr_id <- paste0("no_thr_", proc_id)
    days_id   <- paste0("thr_days_", proc_id)
    if (isTRUE(input[[no_thr_id]])) NA_real_
    else { v <- input[[days_id]]; if (is.null(v) || is.na(v)) NA_real_ else as.numeric(v) }
  }
  
  get_outlier_method <- function(proc_id) {
    if (isTRUE(input[[paste0("no_thr_", proc_id)]])) input[[paste0("outlier_method_", proc_id)]] else NULL
  }
  
  get_medium_band <- function(proc_id) {
    no_med_val <- input[[paste0("no_medium_",   proc_id)]]
    min_val    <- input[[paste0("thr_med_min_", proc_id)]]
    max_val    <- input[[paste0("thr_med_max_", proc_id)]]
    if (is.null(no_med_val) && is.null(min_val))
      return(list(min = NA_real_, max = NA_real_))
    if (isTRUE(no_med_val)) {
      list(min = NA_real_, max = NA_real_)
    } else {
      list(
        min = if (!is.null(min_val)) as.numeric(min_val) else NA_real_,
        max = if (!is.null(max_val)) as.numeric(max_val) else NA_real_
      )
    }
  }
  
  apply_global_proc_filter <- function(df) {
    gf <- admin$global_proc_filter
    if (is.null(gf) || length(gf) == 0 || "tender_proceduretype" %ni% names(df)) return(df)
    proc_recoded <- recode_procedure_type(df$tender_proceduretype)
    df[!is.na(proc_recoded) & proc_recoded %in% gf, , drop = FALSE]
  }
  
  build_thresholds_list <- function(thr_loaded) {
    mk_subm <- function(days, med_min = NA_real_, med_max = NA_real_, no_med = NULL) {
      # no_med defaults to TRUE (no medium band) unless both min and max are
      # non-NA AND caller explicitly passes no_med=FALSE.
      if (is.null(no_med)) no_med <- is.na(med_min) || is.na(med_max)
      list(days = days, outlier_method = "iqr",
           medium = list(min = med_min, max = med_max), no_medium = no_med)
    }
    mk_dec <- function(days) list(days = days, outlier_method = "iqr")
    list(
      subm = list(
        open        = mk_subm(thr_loaded$subm_short_open,       thr_loaded$subm_medium_open_min, thr_loaded$subm_medium_open_max),
        restricted  = mk_subm(thr_loaded$subm_short_restricted, NA_real_, NA_real_),
        neg_pub     = mk_subm(thr_loaded$subm_short_negotiated, NA_real_, NA_real_),
        neg_nopub   = mk_subm(NA_real_),
        neg_unspec  = mk_subm(NA_real_),
        competitive = mk_subm(NA_real_),
        innov       = mk_subm(NA_real_),
        direct      = mk_subm(NA_real_),
        other       = mk_subm(NA_real_)
      ),
      dec = list(
        open        = mk_dec(thr_loaded$long_decision_days),
        restricted  = mk_dec(thr_loaded$long_decision_days),
        neg_pub     = mk_dec(thr_loaded$long_decision_days),
        neg_nopub   = mk_dec(NA_real_),
        neg_unspec  = mk_dec(NA_real_),
        competitive = mk_dec(NA_real_),
        innov       = mk_dec(NA_real_),
        direct      = mk_dec(NA_real_),
        other       = mk_dec(NA_real_)
      )
    )
  }
  
  pre_populate_config <- function(session, thr_loaded) {
    set_subm <- function(pid, days_val) {
      if (!is.na(days_val)) {
        updateCheckboxInput(session, paste0("no_thr_", pid),   value = FALSE)
        updateNumericInput(session,  paste0("thr_days_", pid), value = days_val)
      } else updateCheckboxInput(session, paste0("no_thr_", pid), value = TRUE)
    }
    set_subm("open",       thr_loaded$subm_short_open)
    set_subm("restricted", thr_loaded$subm_short_restricted)
    set_subm("neg_pub",    if (!is.null(thr_loaded$subm_short_negotiated)) thr_loaded$subm_short_negotiated else NA)
    if (!is.na(thr_loaded$subm_medium_open_min))
      updateNumericInput(session, "thr_med_min_open", value = thr_loaded$subm_medium_open_min)
    if (!is.na(thr_loaded$subm_medium_open_max))
      updateNumericInput(session, "thr_med_max_open", value = thr_loaded$subm_medium_open_max)
    if (!is.na(thr_loaded$long_decision_days)) {
      for (pid in c("dec_open", "dec_restricted", "dec_neg_pub")) {
        updateCheckboxInput(session, paste0("no_thr_", pid),   value = FALSE)
        updateNumericInput(session,  paste0("thr_days_", pid), value = thr_loaded$long_decision_days)
      }
    }
    # Country-specific price thresholds
    # BG (Bulgaria): EU procurement thresholds in BGN
    #   Goods/Services: 300,000 BGN  |  Works: 10,000,000 BGN
    if (!is.null(thr_loaded$country_code) && toupper(thr_loaded$country_code) == "BG") {
      price_procs <- c("open","rest","neg_pub","neg_nopub","competitive","innov","direct","other")
      for (proc in price_procs) {
        updateNumericInput(session, paste0("price_", proc, "_goods"),    value = 300000)
        updateNumericInput(session, paste0("price_", proc, "_works"),    value = 10000000)
        updateNumericInput(session, paste0("price_", proc, "_services"), value = 300000)
      }
      showNotification(
        "ℹ️ BG price thresholds pre-filled: 300,000 BGN (Goods/Services), 10,000,000 BGN (Works).",
        type = "message", duration = 5
      )
    }
  }
  
  # ============================================================
  # SINGLE DATA UPLOAD — runs both pipelines
  # ============================================================
  
  observeEvent(input$run_analysis, {
    req(input$datafile)
    
    withProgress(message = "Loading data...", value = 0, {
      incProgress(0.05, detail = "Reading file...")
      
      tryCatch({
        df <- fread(
          input$datafile$datapath,
          keepLeadingZeros = TRUE, encoding = "UTF-8",
          stringsAsFactors = FALSE, showProgress = FALSE,
          na.strings = c("", "-", "NA")
        )
        dup_cols <- duplicated(names(df))
        if (any(dup_cols)) df <- df[, !dup_cols, with = FALSE]
        df <- as.data.frame(df)
        
        # --- Resolve country code ---
        # Auto-detect if user left blank, typed placeholder, or typed generic code
        user_cc  <- toupper(trimws(input$country_code %||% ""))
        use_auto <- nchar(user_cc) == 0 || user_cc %in% c("XX", "GEN", "NA")
        
        if (!use_auto) {
          country_code <- user_cc
        } else {
          auto_code <- NULL
          # Scan columns that often carry a single-valued country identifier
          for (col in c("tender_country", "buyer_country", "country_code", "country",
                        "tender_countryofpublication", "buyer_mainactivities")) {
            if (col %in% names(df)) {
              vals <- unique(df[[col]][!is.na(df[[col]])])
              vals <- vals[nchar(as.character(vals)) == 2]   # keep only 2-letter codes
              if (length(vals) == 1) {
                auto_code <- toupper(as.character(vals[1]))
                cat("Auto-detected country code from column '", col, "':", auto_code, "\n")
                showNotification(paste0("Auto-detected country: ", auto_code,
                                        " (from column '", col, "')"),
                                 type = "message", duration = 4)
                break
              }
            }
          }
          # Try majority-vote across multi-value columns as fallback
          if (is.null(auto_code)) {
            for (col in c("tender_country", "buyer_country", "country_code", "country")) {
              if (col %in% names(df)) {
                vals <- df[[col]][!is.na(df[[col]])]
                vals <- vals[nchar(as.character(vals)) == 2]
                if (length(vals) > 0) {
                  top <- names(sort(table(vals), decreasing = TRUE))[1]
                  if (!is.null(top) && nchar(top) == 2) {
                    auto_code <- toupper(top)
                    cat("Country code via majority vote from '", col, "':", auto_code, "\n")
                    showNotification(paste0("Country inferred: ", auto_code,
                                            " (majority in '", col, "')"),
                                     type = "message", duration = 4)
                    break
                  }
                }
              }
            }
          }
          country_code <- if (!is.null(auto_code) && nchar(auto_code) == 2) auto_code else "GEN"
        }
        
        # --- Econ pipeline (networks skipped here, generated on-demand in Networks tab) ---
        incProgress(0.15, detail = "Running economic outcomes analysis...")
        
        econ_results <- run_economic_efficiency_pipeline(
          df                   = df,
          country_code         = country_code,
          output_dir           = tempdir(),
          save_outputs         = FALSE,
          cpv_lookup           = cpv_lookup_global,
          network_cpv_clusters = character(0)   # always skip at load time
        )
        
        df_econ <- df
        if (!"tender_year" %in% names(df_econ)) df_econ <- add_tender_year(df_econ)
        if (!"cpv_cluster" %in% names(df_econ) && "lot_productcode" %in% names(df_econ))
          df_econ <- df_econ %>% mutate(cpv_cluster = substr(as.character(lot_productcode), 1, 2))
        
        econ$data              <- df_econ
        econ$analysis          <- econ_results
        econ$filtered_data     <- econ_results$df
        econ$filtered_analysis <- econ_results
        econ$country_code      <- country_code
        econ$cpv_lookup        <- cpv_lookup_global
        
        incProgress(0.5, detail = "Running administrative efficiency analysis...")
        
        # --- Admin pipeline ---
        # The admin pipeline calls ggsave() unconditionally.
        # Override it with a no-op so it does not open a graphics device
        # (which can crash R in headless/Shiny environments).
        local_ggsave <- ggplot2::ggsave
        suppressMessages(
          assignInNamespace("ggsave", function(...) invisible(NULL), ns = "ggplot2")
        )
        on.exit(
          suppressMessages(
            assignInNamespace("ggsave", local_ggsave, ns = "ggplot2")
          ),
          add = TRUE
        )
        
        df_admin <- as.data.frame(df) %>% add_tender_year()
        
        admin_results <- run_admin_efficiency_pipeline(
          df              = df_admin,
          country_code    = country_code,
          output_dir      = tempdir(),
          run_regressions = !isTRUE(input$skip_regressions),
          thresholds      = NULL
        )
        
        admin$data              <- df_admin
        admin$analysis          <- admin_results
        admin$filtered_data     <- df_admin
        admin$filtered_analysis <- admin_results
        admin$country_code      <- country_code
        
        thr_loaded         <- get_admin_thresholds(country_code)
        thr_loaded$country_code <- country_code   # pass through for country-specific pre-fills
        admin$thresholds   <- build_thresholds_list(thr_loaded)
        
        # Build country-specific price thresholds directly (not waiting for user to click Apply).
        # If no prefill exists for this country, price_thresholds stays empty and bunching
        # simply won't render — no error, just the "configure thresholds" prompt.
        country_price_thresholds <- local({
          cc <- toupper(country_code %||% "")
          if (cc == "BG") {
            bg_row <- function() list(goods=300000, works=10000000, services=300000)
            list(open=bg_row(), restricted=bg_row(), neg_pub=bg_row(),
                 neg_nopub=bg_row(), neg_unspec=bg_row(), competitive=bg_row(),
                 innov=bg_row(), direct=bg_row(), other=bg_row())
          } else {
            list()   # no prefill — bunching stays inactive, no error
          }
        })
        admin$price_thresholds <- country_price_thresholds
        
        pre_populate_config(session, thr_loaded)
        
        incProgress(0.95, detail = "Finalizing...")
        
        output$analysis_status <- renderText({
          paste0(
            "\u2713 Both analyses complete!\n",
            "Country: ", country_code, "\n",
            "Rows: ", formatC(nrow(df), format = "d", big.mark = ","), "\n",
            "Columns: ", ncol(df)
          )
        })
        
        # --- Integrity pipeline ---
        incProgress(0.90, detail = "Running procurement integrity analysis...")
        tryCatch({
          integ_df <- as.data.frame(df)
          integ_results <- run_integrity_pipeline_fast_local(
            df           = integ_df,
            country_code = country_code,
            output_dir   = tempdir()
          )
          integ$data              <- integ_results$data
          integ$analysis          <- integ_results
          integ$filtered_data     <- integ_results$data
          integ$filtered_analysis <- integ_results
          integ$country_code      <- country_code
          integ$network_done      <- FALSE
          integ$regression_done   <- FALSE
          integ$missing_advanced_done <- FALSE
        }, error = function(e_integ) {
          warning("Integrity pipeline error: ", e_integ$message)
          showNotification(paste("Integrity pipeline warning:", e_integ$message),
                           type = "warning", duration = 8)
        })
        
        showNotification(
          "\u2713 Economic + Administrative + Integrity analyses complete! Navigate tabs to explore results.",
          type = "message", duration = 6
        )
        
      }, error = function(e) {
        output$analysis_status <- renderText(paste("Error:", e$message))
        showNotification(paste("Error:", e$message), type = "error", duration = NULL)
      })
    })
  })
  
  # ============================================================
  # ADMIN — APPLY THRESHOLDS
  # ============================================================
  
  observeEvent(input$apply_thresholds, {
    req(admin$data)
    subm_keys <- c("open","restricted","neg_pub","neg_nopub","neg_unspec","competitive","innov","direct","other")
    thr_new   <- list(subm = list(), dec = list())
    
    for (k in subm_keys) {
      med         <- get_medium_band(k)
      no_med_val  <- input[[paste0("no_medium_", k)]]
      no_med_flag <- if (is.null(no_med_val)) TRUE else isTRUE(no_med_val)
      thr_new$subm[[k]] <- list(days = get_thr_val(k), outlier_method = get_outlier_method(k),
                                medium = med, no_medium = no_med_flag)
    }
    for (k in subm_keys) {
      dk <- paste0("dec_", k)
      thr_new$dec[[k]] <- list(days = get_thr_val(dk, is_decision = TRUE), outlier_method = get_outlier_method(dk))
    }
    admin$thresholds <- thr_new
    
    read_price_row <- function(prefix) {
      list(
        goods    = as.numeric(input[[paste0("price_", prefix, "_goods")]]),
        works    = as.numeric(input[[paste0("price_", prefix, "_works")]]),
        services = as.numeric(input[[paste0("price_", prefix, "_services")]])
      )
    }
    admin$price_thresholds <- list(
      open        = read_price_row("open"),
      restricted  = read_price_row("rest"),
      neg_pub     = read_price_row("neg_pub"),
      neg_nopub   = read_price_row("neg_nopub"),
      neg_unspec  = read_price_row("neg_nopub"),  # shares threshold with neg_nopub by default
      competitive = read_price_row("competitive"),
      innov       = read_price_row("innov"),
      direct      = read_price_row("direct"),
      other       = read_price_row("other")
    )
    
    gf <- input$global_proc_filter
    admin$global_proc_filter <- if (is.null(gf) || length(gf) == 0) PROC_TYPE_LABELS else gf
    
    showNotification("\u2713 Thresholds applied — all admin plots updated.", type = "message", duration = 4)
  })
  
  observeEvent(input$select_all_procs,   { updateCheckboxGroupInput(session, "global_proc_filter", selected = PROC_TYPE_LABELS) })
  observeEvent(input$deselect_all_procs, { updateCheckboxGroupInput(session, "global_proc_filter", selected = character(0)) })
  
  # ── Per-tab procedure filter select-all/deselect-all ──────────────────
  observeEvent(input$subm_proc_select_all,   { updateCheckboxGroupInput(session, "subm_proc_filter", selected = PROC_TYPE_LABELS) })
  observeEvent(input$subm_proc_deselect_all, { updateCheckboxGroupInput(session, "subm_proc_filter", selected = character(0)) })
  observeEvent(input$dec_proc_select_all,    { updateCheckboxGroupInput(session, "dec_proc_filter",  selected = PROC_TYPE_LABELS) })
  observeEvent(input$dec_proc_deselect_all,  { updateCheckboxGroupInput(session, "dec_proc_filter",  selected = character(0)) })
  
  # ── Apply submission thresholds (tab-local button) ─────────────────────
  observeEvent(input$apply_thresholds_subm, {
    req(admin$data)
    subm_keys <- c("open","restricted","neg_pub","neg_nopub","neg_unspec","competitive","innov","direct","other")
    thr_cur   <- if (is.null(admin$thresholds)) list(subm = list(), dec = list()) else admin$thresholds
    for (k in subm_keys) {
      med         <- get_medium_band(k)
      no_med_val  <- input[[paste0("no_medium_", k)]]
      no_med_flag <- if (is.null(no_med_val)) TRUE else isTRUE(no_med_val)
      thr_cur$subm[[k]] <- list(days = get_thr_val(k), outlier_method = get_outlier_method(k),
                                medium = med, no_medium = no_med_flag)
    }
    admin$thresholds <- thr_cur
    showNotification("\u2713 Submission thresholds applied.", type = "message", duration = 3)
  })
  
  # ── Apply decision thresholds (tab-local button) ───────────────────────
  observeEvent(input$apply_thresholds_dec, {
    req(admin$data)
    subm_keys <- c("open","restricted","neg_pub","neg_nopub","neg_unspec","competitive","innov","direct","other")
    thr_cur   <- if (is.null(admin$thresholds)) list(subm = list(), dec = list()) else admin$thresholds
    for (k in subm_keys) {
      dk <- paste0("dec_", k)
      thr_cur$dec[[k]] <- list(days = get_thr_val(dk, is_decision = TRUE), outlier_method = get_outlier_method(dk))
    }
    admin$thresholds <- thr_cur
    showNotification("\u2713 Decision thresholds applied.", type = "message", duration = 3)
  })
  
  output$threshold_status_subm <- renderText({ if (is.null(admin$thresholds)) "" else "Thresholds active \u2713" })
  output$threshold_status_dec  <- renderText({ if (is.null(admin$thresholds)) "" else "Thresholds active \u2713" })
  output$threshold_status  <- renderText({ if (is.null(admin$thresholds)) "" else "Thresholds active \u2713" })
  output$threshold_summary <- renderPrint({
    if (is.null(admin$thresholds)) { cat("No thresholds loaded yet.\n"); return(invisible(NULL)) }
    thr <- admin$thresholds
    cat("=== Submission Short Thresholds ===\n")
    for (k in names(thr$subm)) {
      v       <- thr$subm[[k]]
      val_str <- if (is.na(v$days)) paste0("stat (", v$outlier_method, ")") else paste0(v$days, " days")
      med_str <- if (isTRUE(v$no_medium)) "no medium band" else if (is.na(v$medium$min)) "medium: not set" else paste0("medium: ", v$medium$min, "\u2013", v$medium$max, " days")
      cat(sprintf("  %-12s: %-20s | %s\n", k, val_str, med_str))
    }
    cat("\n=== Decision Long Thresholds ===\n")
    for (k in names(thr$dec)) {
      v <- thr$dec[[k]]
      val_str <- if (is.na(v$days)) paste0("stat (", v$outlier_method, ")") else paste0(v$days, " days")
      cat(sprintf("  %-12s: %s\n", k, val_str))
    }
    pt <- admin$price_thresholds
    if (!is.null(pt) && length(pt) > 0) {
      cat("\n=== Price Thresholds (local currency) ===\n")
      any_set <- FALSE
      for (proc in names(pt)) for (stype in names(pt[[proc]])) {
        v <- pt[[proc]][[stype]]
        if (!is.null(v) && !is.na(v) && v > 0) {
          cat(sprintf("  %-12s / %-8s: %s\n", proc, stype, format(v, big.mark = ",")))
          any_set <- TRUE
        }
      }
      if (!any_set) cat("  (none set)\n")
    }
  })
  
  # ============================================================
  # ADMIN — RE-RUN REGRESSIONS ON DEMAND
  # ============================================================
  
  output$regression_status_box <- renderUI({
    if (!is.null(admin$filtered_analysis$plot_short_reg) || !is.null(admin$filtered_analysis$plot_long_reg))
      div(class = "reg-status-ok", icon("check-circle"), " Regression results available and up to date.")
    else
      div(class = "reg-status-wait", icon("clock"), " No results yet. Set your filters, then click Run.")
  })
  
  observeEvent(input$run_regressions_now, {
    req(admin$filtered_data, admin$country_code)
    withProgress(message = "Running regression analysis...", value = 0, {
      incProgress(0.1, detail = "Preparing data...")
      tryCatch({
        reg_results <- run_admin_efficiency_pipeline(
          df              = admin$filtered_data,
          country_code    = admin$country_code,
          output_dir      = tempdir(),
          run_regressions = TRUE,
          thresholds      = admin$thresholds
        )
        for (nm in c("plot_short_reg","plot_long_reg","sensitivity_short","sensitivity_long",
                     "specs_short","specs_long","model_short_glm","model_long_glm"))
          admin$filtered_analysis[[nm]] <- reg_results[[nm]]
        incProgress(1, detail = "Done.")
        showNotification("Regression analysis complete!", type = "message", duration = 5)
      }, error = function(e) showNotification(paste("Error:", e$message), type = "error", duration = 10))
    })
  })
  
  # ============================================================
  # FILTER UI GENERATION — ECON SECTION
  # ============================================================
  
  econ_tabs <- c("overview","market","supplier","network","price","competition")
  
  # ── Shared filter widget reactives (computed once; each tab's uiOutput
  #    container renders the same widget — all tabs share the same input IDs) ──
  .econ_year_widget <- reactive({
    req(econ$filtered_data)
    years <- econ$filtered_data$tender_year; years <- years[!is.na(years)]
    if ("tender_year" %in% names(econ$filtered_data) && length(years) > 0)
      sliderInput("econ_year_range", "Year Range:",
                  min=min(years), max=max(years), value=c(min(years),max(years)), step=1, sep="")
  })
  .econ_market_widget <- reactive({
    req(econ$filtered_data)
    if (!"cpv_cluster" %in% names(econ$filtered_data)) return(NULL)
    cpv_codes <- sort(unique(econ$filtered_data$cpv_cluster))
    cpv_codes  <- cpv_codes[!is.na(cpv_codes) & cpv_codes != ""]
    if (length(cpv_codes) == 0) return(NULL)
    cpv_choices <- setNames(cpv_codes, sapply(cpv_codes, get_cpv_label))
    pickerInput("econ_market_filter", "Market (CPV):",
                choices=c("All"="All", cpv_choices), selected="All", multiple=TRUE,
                options=list(`actions-box`=TRUE, `live-search`=TRUE))
  })
  .econ_value_widget <- reactive({
    # Use full unfiltered data so widget range never changes when other filters applied
    src <- if (!is.null(econ$data)) econ$data else econ$filtered_data
    req(!is.null(src))
    price_col <- if ("bid_priceusd"          %in% names(src)) "bid_priceusd"
    else if ("bid_price"           %in% names(src)) "bid_price"
    else if ("lot_estimatedprice"  %in% names(src)) "lot_estimatedprice" else NULL
    if (is.null(price_col)) return(NULL)
    prices <- src[[price_col]]; prices <- prices[!is.na(prices) & prices > 0]
    if (length(prices) == 0) return(NULL)
    cur  <- input$econ_value_currency %||% "USD"
    rate <- if (cur == "BGN") .BGN_PER_USD else 1
    div  <- 1e3
    econ$value_divisor  <- div / rate
    econ$value_max_k    <- ceiling(max(prices) * rate / div)
    make_value_filter_widget(prices, "econ_value_currency", "econ_value_range", cur)
  })
  .econ_buyer_widget <- reactive({
    req(econ$filtered_data)
    if (!"buyer_buyertype" %in% names(econ$filtered_data)) return(NULL)
    raw_types <- unique(econ$filtered_data$buyer_buyertype); raw_types <- raw_types[!is.na(raw_types)]
    if (length(raw_types) == 0) return(NULL)
    df_map <- data.frame(raw=raw_types, group=as.character(add_buyer_group(raw_types)), stringsAsFactors=FALSE)
    econ_buyer_mapping(df_map)
    pickerInput("econ_buyer_type_filter", "Buyer Type:",
                choices=c("All", sort(unique(df_map$group))), selected="All",
                multiple=TRUE, options=list(`actions-box`=TRUE))
  })
  .econ_proc_widget <- reactive({
    req(econ$filtered_data)
    if (!"tender_proceduretype" %in% names(econ$filtered_data)) return(NULL)
    raw_types <- unique(econ$filtered_data$tender_proceduretype); raw_types <- raw_types[!is.na(raw_types)]
    if (length(raw_types) == 0) return(NULL)
    df_map <- data.frame(raw=raw_types, cleaned=recode_procedure_type(raw_types), stringsAsFactors=FALSE)
    econ_procedure_mapping(df_map)
    pickerInput("econ_procedure_type_filter", "Procedure Type:",
                choices=c("All", sort(unique(df_map$cleaned))), selected="All",
                multiple=TRUE, options=list(`actions-box`=TRUE, `live-search`=TRUE))
  })
  
  for (tab in econ_tabs) {
    local({
      t <- tab
      output[[paste0("econ_year_filter_",           t)]] <- renderUI(.econ_year_widget())
      output[[paste0("econ_market_filter_",         t)]] <- renderUI(.econ_market_widget())
      output[[paste0("econ_value_filter_",          t)]] <- renderUI(.econ_value_widget())
      output[[paste0("econ_buyer_type_filter_",     t)]] <- renderUI(.econ_buyer_widget())
      output[[paste0("econ_procedure_type_filter_", t)]] <- renderUI(.econ_proc_widget())
      output[[paste0("econ_filter_status_",         t)]] <- renderText({
        econ$slider_trigger
        paste("  📋", get_filter_description(econ_filters$active))
      })
    })
  }
  
  # ============================================================
  # FILTER APPLICATION — ECON
  # ============================================================
  
  apply_econ_filters <- function(tab_name) {
    req(econ$data, econ$analysis)
    tryCatch({
      current_filters <- list(
        year           = input$econ_year_range,
        market         = input$econ_market_filter,
        value = {
          mn_k <- input$econ_value_range_min_k
          mx_k <- input$econ_value_range_max_k
          # Blank = use data min (0) or max
          mn <- if (is.null(mn_k) || is.na(mn_k)) 0 else mn_k
          mx <- if (is.null(mx_k) || is.na(mx_k)) (econ$value_max_k %||% 1e9) else mx_k
          c(mn, mx)
        },
        buyer_type     = input$econ_buyer_type_filter,
        procedure_type = input$econ_procedure_type_filter
      )
      econ_filters$active        <- current_filters
      econ_filters[[tab_name]]   <- current_filters
      
      filtered <- econ_filter_data(
        df             = econ$analysis$df,
        year_range     = current_filters$year,
        market         = current_filters$market,
        value_range    = current_filters$value,
        buyer_type     = current_filters$buyer_type,
        procedure_type = current_filters$procedure_type,
        value_divisor  = econ$value_divisor,
        buyer_mapping  = econ_buyer_mapping(),
        procedure_mapping = econ_procedure_mapping()
      )
      econ$filtered_data      <- filtered
      econ$slider_trigger     <- econ$slider_trigger + 1
      
      # Recompute market sizing plots from filtered data (fast, no networks/regressions)
      tryCatch({
        price_var <- detect_price_col(filtered)
        ms <- summarise_market_size(filtered, value_col = price_var)
        econ$filtered_analysis$market_size_n  <- plot_market_contract_counts(ms)
        econ$filtered_analysis$market_size_v  <- plot_market_total_value(ms)
        econ$filtered_analysis$market_size_av <- plot_market_bubble(ms)
        if (all(c("bidder_masterid","tender_year","cpv_cluster") %in% names(filtered))) {
          ss <- compute_supplier_entry(filtered)
          econ$filtered_analysis$supplier_stats <- ss
        }
      }, error = function(e) message("Market sizing update: ", e$message))
      
      showNotification(paste("Econ filters applied:", scales::comma(nrow(filtered)), "contracts"),
                       type = "message", duration = 2)
    }, error = function(e) {
      showNotification(paste("Filter error:", e$message), type = "error", duration = 8)
    })
  }
  
  reset_econ_filters <- function(tab_name) {
    empty <- list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL)
    econ_filters$active      <- empty
    econ_filters[[tab_name]] <- empty
    econ$filtered_data       <- econ$analysis$df
    econ$filtered_analysis   <- econ$analysis   # restore full pre-computed plots
    econ$slider_trigger      <- econ$slider_trigger + 1
    showNotification("Econ filters reset", type = "message", duration = 2)
  }
  
  
  
  for (tn in econ_tabs) {
    local({
      t <- tn
      observeEvent(input[[paste0("econ_apply_filters_", t)]],  { apply_econ_filters(t) })
      observeEvent(input[[paste0("econ_reset_filters_",  t)]], { reset_econ_filters(t) })
    })
  }
  
  # ============================================================
  # FILTER APPLICATION — ADMIN
  # ============================================================
  
  apply_admin_filters <- function(tab_name) {
    req(admin$data)
    tryCatch({
      current_filters <- list(
        year           = input$admin_year_range,
        market         = input$admin_market_filter,
        value = {
          mn_k <- input$admin_value_range_min_k
          mx_k <- input$admin_value_range_max_k
          # Blank = use data min (0) or max
          mn <- if (is.null(mn_k) || is.na(mn_k)) 0 else mn_k
          mx <- if (is.null(mx_k) || is.na(mx_k)) (admin$value_max_k %||% 1e9) else mx_k
          c(mn, mx)
        },
        buyer_type     = input$admin_buyer_type_filter,
        procedure_type = input$admin_procedure_type_filter
      )
      admin_filters$active      <- current_filters
      admin_filters[[tab_name]] <- current_filters
      
      filtered <- admin_filter_data(
        df                 = admin$data,
        year_range         = current_filters$year,
        market             = current_filters$market,
        value_range        = current_filters$value,
        buyer_type         = current_filters$buyer_type,
        procedure_type     = current_filters$procedure_type,
        value_divisor      = admin$value_divisor,
        procedure_mapping  = admin_procedure_mapping()
      )
      if (!"tender_year" %in% names(filtered)) filtered <- add_tender_year(filtered)
      admin$filtered_data <- filtered
      
      showNotification(paste("Admin filters applied:", formatC(nrow(filtered), format = "d", big.mark = ",")),
                       type = "message", duration = 3)
    }, error = function(e) {
      showNotification(paste("Filter error:", e$message), type = "error", duration = 8)
    })
  }
  
  reset_admin_filters <- function(tab_name) {
    empty <- list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL)
    admin_filters$active      <- empty
    admin_filters[[tab_name]] <- empty
    admin$filtered_data       <- admin$data
    admin$filtered_analysis   <- admin$analysis
    showNotification("Admin filters reset", type = "message", duration = 2)
  }
  
  # ============================================================
  # FILTER UI GENERATION — ADMIN SECTION
  # ============================================================
  
  admin_tabs <- c("overview","proc","subm","dec","reg")
  
  for (tab in admin_tabs) {
    local({
      t <- tab
      
      output[[paste0("admin_year_filter_", t)]] <- renderUI({
        req(admin$data)
        year_col <- if ("tender_year" %in% names(admin$data)) "tender_year"
        else if ("year" %in% names(admin$data)) "year"
        else if ("cal_year" %in% names(admin$data)) "cal_year" else NULL
        if (!is.null(year_col)) {
          years <- admin$data[[year_col]]; years <- years[!is.na(years)]
          if (length(years) > 0)
            sliderInput("admin_year_range", "Year Range:",
                        min = min(years), max = max(years),
                        value = c(min(years), max(years)), step = 1, sep = "")
        }
      })
      
      output[[paste0("admin_market_filter_", t)]] <- renderUI({
        req(econ$filtered_data)
        if ("cpv_cluster" %in% names(econ$filtered_data)) {
          cpv_codes <- sort(unique(econ$filtered_data$cpv_cluster))
          cpv_codes <- cpv_codes[!is.na(cpv_codes) & cpv_codes != ""]
          if (length(cpv_codes) > 0) {
            cpv_choices <- setNames(cpv_codes, sapply(cpv_codes, get_cpv_label))
            pickerInput("admin_market_filter", "Market (CPV):",
                        choices = c("All" = "All", cpv_choices),
                        selected = "All", multiple = TRUE,
                        options = list(`actions-box` = TRUE, `live-search` = TRUE))
          }
        }
      })
      
      output[[paste0("admin_value_filter_", t)]] <- renderUI({
        req(admin$data)
        price_col <- if ("bid_priceusd" %in% names(admin$data)) "bid_priceusd"
        else if ("bid_price" %in% names(admin$data)) "bid_price" else NULL
        if (!is.null(price_col)) {
          prices <- admin$data[[price_col]]; prices <- prices[!is.na(prices) & prices > 0]
          if (length(prices) > 0) {
            cur  <- input$admin_value_currency %||% "USD"
            rate <- if (cur == "BGN") .BGN_PER_USD else 1
            div  <- 1e3
            admin$value_divisor <- div / rate
            admin$value_max_k   <- ceiling(max(prices) * rate / div)
            make_value_filter_widget(prices, "admin_value_currency", "admin_value_range", cur)
          }
        }
      })
      
      output[[paste0("admin_buyer_type_filter_", t)]] <- renderUI({
        req(admin$data)
        if ("buyer_buyertype" %in% names(admin$data)) {
          buyer_groups <- admin$data %>%
            mutate(buyer_group = add_buyer_group(buyer_buyertype)) %>%
            pull(buyer_group) %>% as.character() %>% unique() %>% sort()
          buyer_groups <- buyer_groups[!is.na(buyer_groups)]
          if (length(buyer_groups) > 0)
            pickerInput("admin_buyer_type_filter", "Buyer Type:",
                        choices = c("All", buyer_groups), selected = "All",
                        multiple = TRUE, options = list(`actions-box` = TRUE))
        }
      })
      
      output[[paste0("admin_procedure_type_filter_", t)]] <- renderUI({
        req(admin$data)
        if ("tender_proceduretype" %in% names(admin$data)) {
          raw_types <- unique(admin$data$tender_proceduretype)
          raw_types <- raw_types[!is.na(raw_types)]
          if (length(raw_types) > 0) {
            df_map <- data.frame(raw     = raw_types,
                                 cleaned = recode_procedure_type(raw_types),
                                 stringsAsFactors = FALSE)
            admin_procedure_mapping(df_map)
            types <- sort(unique(df_map$cleaned[!is.na(df_map$cleaned)]))
            pickerInput("admin_procedure_type_filter", "Procedure Type:",
                        choices = c("All", types), selected = "All",
                        multiple = TRUE, options = list(`actions-box` = TRUE, `live-search` = TRUE))
          }
        }
      })
      
      output[[paste0("admin_filter_status_", t)]] <- renderText({
        paste("  \U0001f4cb", get_filter_description(admin_filters$active))
      })
    })
  }
  
  for (tn in admin_tabs) {
    local({
      t <- tn
      observeEvent(input[[paste0("admin_apply_filters_", t)]],  { apply_admin_filters(t) })
      observeEvent(input[[paste0("admin_reset_filters_",  t)]], { reset_admin_filters(t) })
    })
  }
  
  # ============================================================
  # ============================================================
  # VALUE FILTER SYNC: coarse M slider → precise K inputs
  # ============================================================
  make_slider_sync <- function(coarse_id, min_k_id, max_k_id, rate_fn) {
    observeEvent(input[[coarse_id]], {
      v <- input[[coarse_id]]; req(!is.null(v))
      rate <- rate_fn()
      updateNumericInput(session, min_k_id, value = floor(v[1] * 1e3))
      updateNumericInput(session, max_k_id, value = ceiling(v[2] * 1e3))
    }, ignoreInit = TRUE)
  }
  make_slider_sync("econ_value_range_coarse",  "econ_value_range_min_k",
                   "econ_value_range_max_k",
                   function() if ((input$econ_value_currency  %||% "USD") == "BGN") .BGN_PER_USD else 1)
  make_slider_sync("admin_value_range_coarse", "admin_value_range_min_k",
                   "admin_value_range_max_k",
                   function() if ((input$admin_value_currency %||% "USD") == "BGN") .BGN_PER_USD else 1)
  make_slider_sync("integ_value_range_coarse", "integ_value_range_min_k",
                   "integ_value_range_max_k",
                   function() if ((input$integ_value_currency %||% "USD") == "BGN") .BGN_PER_USD else 1)
  
  # SHARED PLOTLY HELPERS
  # ============================================================
  
  # Adds autoscale button + high-res PNG export to every chart.
  # Forces pure white backgrounds so the camera-button PNG is clean (no blue tint).
  pa_config <- function(fig) {
    fig %>%
      plotly::layout(
        paper_bgcolor = "#ffffff",
        plot_bgcolor  = "#ffffff"
      ) %>%
      plotly::config(
        displayModeBar       = TRUE,
        modeBarButtonsToAdd  = list("autoScale2d"),
        toImageButtonOptions = list(format          = "png",
                                    scale           = 2,
                                    filename        = "chart_export",
                                    # Force white background in camera-button export.
                                    # Without this Plotly inherits the page background
                                    # colour (AdminLTE blue-grey) into the PNG.
                                    setBackground   = "#ffffff"),
        responsive           = TRUE
      )
  }
  
  # Export a plotly figure to PNG.
  .save_fig_png <- function(fig, file, vw = 1200, vh = 700) {
    fig_dl <- fig %>%
      plotly::layout(paper_bgcolor = "#ffffff", plot_bgcolor = "#ffffff") %>%
      plotly::config(displayModeBar = FALSE)
    
    # webshot2 (Chrome) — the method that was confirmed working
    ok <- tryCatch({
      tmp <- tempfile(fileext = ".html")
      htmlwidgets::saveWidget(fig_dl, tmp, selfcontained = TRUE)
      webshot2::webshot(tmp, file = file, vwidth = vw, vheight = vh,
                        delay = 2.5, zoom = 2)
      unlink(tmp)
      file.exists(file) && file.size(file) > 1000
    }, error = function(e) { message("webshot2 error: ", e$message); FALSE })
    if (isTRUE(ok)) return(invisible(NULL))
    
    showNotification(
      "Download failed. Check the R console for the error. Make sure webshot2 is installed: install.packages('webshot2')",
      type = "error", duration = 15
    )
    req(FALSE)
  }
  
  # Helper: show a warning toast and cancel download when chart not yet rendered
  .require_fig <- function(fig, plot_label = "this chart") {
    if (is.null(fig)) {
      showNotification(
        paste0("Please view ", plot_label, " first, then download."),
        type = "warning", duration = 5
      )
      req(FALSE)
    }
    fig
  }
  
  # Download helper used by all admin plotly figures
  dl_plotly_fig <- function(fig_expr, fname, vw = 1200, vh = 700) {
    downloadHandler(
      filename = function() {
        cc <- tryCatch(admin$country_code %||% "export", error = function(e) "export")
        paste0(fname, "_", cc, "_", format(Sys.Date(), "%Y%m%d"), ".png")
      },
      content = function(file) {
        fig <- tryCatch(fig_expr(), error = function(e) NULL)
        .require_fig(fig, fname)
        .save_fig_png(fig, file, vw, vh)
      }
    )
  }
  
  # ============================================================
  # DATA OVERVIEW OUTPUTS (shared, uses econ filtered_data)
  # ============================================================
  
  output$n_contracts <- renderValueBox({
    req(econ$filtered_data)
    valueBox(formatC(nrow(econ$filtered_data), format="d", big.mark=","),
             "Contracts", icon=icon("file-contract"), color="navy")
  })
  output$n_buyers <- renderValueBox({
    req(econ$filtered_data); df <- econ$filtered_data
    n <- if ("buyer_masterid" %in% names(df)) length(unique(df$buyer_masterid)) else "N/A"
    valueBox(formatC(n, format="d", big.mark=","), "Buyers", icon=icon("building"), color="teal")
  })
  output$n_suppliers <- renderValueBox({
    req(econ$filtered_data); df <- econ$filtered_data
    n <- if ("bidder_masterid" %in% names(df)) length(unique(df$bidder_masterid)) else "N/A"
    valueBox(formatC(n, format="d", big.mark=","), "Suppliers", icon=icon("truck"), color="olive")
  })
  output$n_years <- renderValueBox({
    req(econ$filtered_data); df <- econ$filtered_data
    years <- if ("tender_year" %in% names(df)) unique(df$tender_year[!is.na(df$tender_year)]) else NA
    yr    <- if (length(years) > 0) paste(min(years), "-", max(years)) else "N/A"
    valueBox(yr, "Period", icon=icon("calendar"), color="navy")
  })
  
  output$contracts_year_plot <- renderPlotly({
    req(econ$filtered_data)
    df <- econ$filtered_data
    if (!"tender_year" %in% names(df)) return(NULL)
    year_counts <- df %>% group_by(tender_year) %>% summarise(n = n(), .groups="drop")
    year_counts <- year_counts %>%
      mutate(label = formatC(n, format="d", big.mark=","))
    p <- ggplot(year_counts, aes(x=tender_year, y=n,
                                 text=paste0("Year: ", tender_year, "<br>Contracts: ", label))) +
      geom_col(fill=PA_NORMAL) +
      labs(x="Year", y="Number of Contracts") +
      pa_theme() + scale_y_continuous(labels=scales::comma)
    ggplotly(p, tooltip="text") %>%
      layout(hoverlabel=list(bgcolor="white", font=list(size=13)),
             hovermode="x unified") %>%
      pa_config() -> .stored_fig
    econ$fig_contracts_year_econ <- .stored_fig
    .stored_fig
  })
  
  output$value_by_year_plot <- renderPlotly({
    req(econ$filtered_data)
    df <- econ$filtered_data
    price_var <- NULL
    for (col in c("bid_priceusd","lot_estimatedpriceusd","tender_finalprice","lot_estimatedprice"))
      if (col %in% names(df)) { price_var <- col; break }
    if (!"tender_year" %in% names(df) || is.null(price_var)) return(NULL)
    year_values <- df %>%
      filter(!is.na(.data[[price_var]]), .data[[price_var]] > 0) %>%
      group_by(tender_year) %>%
      summarise(total_value=sum(.data[[price_var]], na.rm=TRUE),
                avg_value=mean(.data[[price_var]], na.rm=TRUE),
                n_contracts=n(), .groups="drop")
    max_val <- max(year_values$total_value, na.rm=TRUE)
    if (max_val > 1e9) { sdiv <- 1e9; ylbl <- "Total Value (Billions)"; sn <- "B"
    } else if (max_val > 1e6) { sdiv <- 1e6; ylbl <- "Total Value (Millions)"; sn <- "M"
    } else if (max_val > 1e3) { sdiv <- 1e3; ylbl <- "Total Value (Thousands)"; sn <- "K"
    } else { sdiv <- 1; ylbl <- "Total Value"; sn <- "" }
    year_values <- year_values %>% mutate(tv_disp = total_value / sdiv)
    p <- ggplot(year_values, aes(x=tender_year, y=tv_disp,
                                 text=paste0("Year: ", tender_year, "<br>Contracts: ",
                                             format(n_contracts, big.mark=","), "<br>Total: ",
                                             round(tv_disp, 2), sn))) +
      geom_col(fill="#00a65a") +
      labs(x="Year", y=ylbl) +
      pa_theme() + scale_y_continuous(labels=scales::comma)
    ggplotly(p, tooltip="text") %>% layout(hoverlabel=list(bgcolor="white"), hovermode="x unified") %>%
      pa_config() -> .stored_fig
    econ$fig_value_by_year <- .stored_fig
    .stored_fig
  })
  
  output$cpv_legend_table <- DT::renderDataTable({
    lkp <- econ$cpv_lookup
    if (!is.null(lkp) && is.list(lkp) && !is.null(lkp$cpv_2d)) {
      tbl <- lkp$cpv_2d
      if ("cpv_cluster" %in% names(tbl) && "cpv_category" %in% names(tbl))
        tbl <- tbl %>% rename(`CPV Code`=cpv_cluster, `Category`=cpv_category)
      datatable(tbl, options=list(pageLength=10, scrollY="350px"), rownames=FALSE,
                caption="CPV 2-digit Market Definitions")
    } else if (!is.null(econ$data) && "lot_productcode" %in% names(econ$data)) {
      tbl <- econ$data %>%
        mutate(cpv_2dig=substr(as.character(lot_productcode),1,2)) %>%
        filter(!is.na(cpv_2dig), nchar(cpv_2dig)==2) %>%
        group_by(cpv_2dig) %>%
        summarise(Category=paste0("CPV ", cpv_2dig), Contracts=n(), .groups="drop") %>%
        rename(`CPV Code`=cpv_2dig)
      datatable(tbl, options=list(pageLength=10, scrollY="350px"), rownames=FALSE)
    } else {
      datatable(data.frame(Message="No CPV data available"), options=list(dom="t"), rownames=FALSE)
    }
  })
  
  # ============================================================
  # MARKET SIZING OUTPUTS
  # ============================================================
  
  output$market_size_n_plot <- renderPlotly({
    req(econ$filtered_data)
    ms <- tryCatch({
      df <- econ$filtered_data
      if (!"cpv_cluster" %in% names(df)) return(NULL)
      df %>%
        filter(!is.na(cpv_cluster)) %>%
        mutate(cpv_label = get_cpv_label(cpv_cluster)) %>%
        group_by(cpv_label) %>%
        summarise(n_contracts = n(), .groups = "drop") %>%
        arrange(desc(n_contracts)) %>% slice_head(n = 30) %>%
        mutate(
          label_short = ifelse(nchar(cpv_label) > 35,
                               paste0(substr(cpv_label, 1, 33), "…"), cpv_label),
          tooltip = paste0("<b>", cpv_label, "</b><br>Contracts: ",
                           formatC(n_contracts, format="d", big.mark=",")),
          label_short = factor(label_short, levels = label_short[order(n_contracts)])
        )
    }, error = function(e) NULL)
    if (is.null(ms) || nrow(ms) == 0) {
      req(econ$filtered_analysis$market_size_n)
      return(ggplotly(econ$filtered_analysis$market_size_n, tooltip=c("x","y")) %>%
               layout(font=list(size=11), hoverlabel=list(bgcolor="white",font=list(size=11))))
    }
    p <- ggplot(ms, aes(x = label_short, y = n_contracts, text = tooltip)) +
      geom_col(fill = PA_NORMAL) +
      coord_flip() +
      scale_y_continuous(labels = scales::comma) +
      labs(x = NULL, y = "Number of contracts") +
      pa_theme()
    ggplotly(p, tooltip = "text") %>%
      layout(hoverlabel = list(bgcolor = "white", font = list(size = 13)),
             hovermode = "closest", margin = list(l = 10)) %>%
      pa_config()
  })
  
  output$market_size_v_plot <- renderPlotly({
    req(econ$filtered_data)
    ms <- tryCatch({
      df <- econ$filtered_data
      price_var <- detect_price_col(df)
      if (is.null(price_var) || !"cpv_cluster" %in% names(df)) return(NULL)
      df %>%
        filter(!is.na(cpv_cluster), !is.na(.data[[price_var]]),
               .data[[price_var]] > 0) %>%
        mutate(cpv_label = get_cpv_label(cpv_cluster)) %>%
        group_by(cpv_label) %>%
        summarise(total_value = sum(.data[[price_var]], na.rm = TRUE), .groups = "drop") %>%
        arrange(desc(total_value)) %>% slice_head(n = 30) %>%
        mutate(
          label_short = ifelse(nchar(cpv_label) > 35,
                               paste0(substr(cpv_label, 1, 33), "…"), cpv_label),
          tooltip = paste0("<b>", cpv_label, "</b><br>Total value: ",
                           scales::dollar(total_value, accuracy = 1, prefix = "$")),
          label_short = factor(label_short, levels = label_short[order(total_value)])
        )
    }, error = function(e) NULL)
    if (is.null(ms) || nrow(ms) == 0) {
      req(econ$filtered_analysis$market_size_v)
      return(ggplotly(econ$filtered_analysis$market_size_v, tooltip=c("x","y")) %>%
               layout(font=list(size=11), hoverlabel=list(bgcolor="white",font=list(size=11))))
    }
    p <- ggplot(ms, aes(x = label_short, y = total_value, text = tooltip)) +
      geom_col(fill = PA_TEAL) +
      coord_flip() +
      scale_y_continuous(labels = scales::dollar_format(scale = 1e-6, suffix = "M",
                                                        accuracy = 0.1)) +
      labs(x = NULL, y = "Total contract value") +
      pa_theme()
    ggplotly(p, tooltip = "text") %>%
      layout(hoverlabel = list(bgcolor = "white", font = list(size = 13)),
             hovermode = "closest", margin = list(l = 10)) %>%
      pa_config()
  })
  
  output$market_size_av_plot <- renderPlotly({
    req(econ$filtered_data)
    ms <- tryCatch({
      df  <- econ$filtered_data
      price_var <- detect_price_col(df)
      if (is.null(price_var)) stop("no price column")
      df %>%
        filter(!is.na(cpv_cluster)) %>%
        mutate(cpv_label = get_cpv_label(cpv_cluster)) %>%
        group_by(cpv_cluster, cpv_label) %>%
        summarise(
          n_contracts  = n(),
          total_value  = sum(.data[[price_var]], na.rm=TRUE),
          avg_value    = mean(.data[[price_var]], na.rm=TRUE),
          .groups="drop"
        ) %>%
        filter(n_contracts > 0, avg_value > 0, total_value > 0)
    }, error = function(e) NULL)
    
    if (is.null(ms) || nrow(ms) == 0) return(
      plotly::plot_ly(type = "scatter", mode = "markers") %>%
        plotly::add_annotations(text="No price data available for bubble plot",
                                x=0.5, y=0.5, xref="paper", yref="paper", showarrow=FALSE,
                                font=list(size=11, color="#888"))
    )
    
    # Attach derived columns to ms so plotly can reference them via ~
    ms <- ms %>%
      mutate(
        bubble_size  = scales::rescale(sqrt(total_value), to = c(8, 50)),
        log_avg      = log10(avg_value + 1),
        hover_text   = paste0(
          "<b>", cpv_label, "</b><br>",
          "Contracts: <b>", scales::comma(n_contracts), "</b><br>",
          "Avg contract value: <b>",
          scales::dollar(avg_value, scale=1e-3, suffix="K", accuracy=1), "</b><br>",
          "Total market value: <b>",
          scales::dollar(total_value, scale=1e-6, suffix="M", accuracy=0.1), "</b>"
        )
      )
    
    # Build clean decade tick labels for both log axes
    make_log_ticks <- function(vals, prefix = "") {
      lo <- floor(log10(min(vals, na.rm=TRUE)))
      hi <- ceiling(log10(max(vals, na.rm=TRUE)))
      pows <- seq(lo, hi)
      tv   <- 10^pows
      fmt  <- function(v) {
        dplyr::case_when(
          v >= 1e9  ~ paste0(prefix, scales::comma(v/1e9), "B"),
          v >= 1e6  ~ paste0(prefix, scales::comma(v/1e6), "M"),
          v >= 1e3  ~ paste0(prefix, scales::comma(v/1e3), "K"),
          TRUE      ~ paste0(prefix, scales::comma(v))
        )
      }
      list(tickvals = tv, ticktext = fmt(tv))
    }
    xt <- make_log_ticks(ms$n_contracts)
    yt <- make_log_ticks(ms$avg_value,   prefix = "$")
    
    plot_ly(ms,
            x         = ~n_contracts,
            y         = ~avg_value,
            text      = ~hover_text,
            hoverinfo = "text",
            type      = "scatter",
            mode      = "markers",
            marker    = list(
              size      = ~bubble_size,
              sizemode  = "diameter",
              color     = ~log_avg,
              colorscale = list(c(0,"#c6dbef"), c(0.5,"#4292c6"), c(1,"#08306b")),
              showscale = TRUE,
              colorbar  = list(title = "Avg value<br>(log₁₀ USD)", tickformat = ".1f"),
              opacity   = 0.85,
              line      = list(color = "white", width = 1)
            )
    ) %>%
      layout(
        xaxis  = list(
          title     = "Number of contracts (log scale)",
          type      = "log",
          tickvals  = xt$tickvals,
          ticktext  = xt$ticktext,
          tickangle = -35,
          zeroline  = FALSE,
          gridcolor = "#eeeeee"
        ),
        yaxis  = list(
          title     = "Average contract value (log scale)",
          type      = "log",
          tickvals  = yt$tickvals,
          ticktext  = yt$ticktext,
          zeroline  = FALSE,
          gridcolor = "#eeeeee"
        ),
        margin     = list(l=90, r=60, t=60, b=90),
        hoverlabel = list(bgcolor="white", font=list(size=12)),
        hovermode  = "closest",
        showlegend = FALSE
      ) %>%
      pa_config()
  })
  
  # ============================================================
  # SUPPLIER DYNAMICS — DYNAMIC SLIDERS
  # ============================================================
  
  output$market_contracts_range_slider <- renderUI({
    req(econ$filtered_data)
    trigger <- econ$slider_trigger
    df <- econ$filtered_data
    mkt <- df %>% filter(!is.na(cpv_cluster), !is.na(tender_year)) %>%
      group_by(cpv_cluster, tender_year) %>% summarise(n=n(), .groups="drop") %>%
      group_by(cpv_cluster) %>% summarise(avg=mean(n), .groups="drop")
    max_c <- max(mkt$avg, na.rm=TRUE); med_c <- median(mkt$avg, na.rm=TRUE)
    if (is.na(max_c) || max_c < 10) { max_c <- 1000; med_c <- 500 }
    max_val <- ceiling(max_c / 100) * 100
    tagList(
      sliderInput("econ_market_contracts_range", "Average contracts per market-year:",
                  min=0, max=max_val, value=c(0, max_val), step=max(10, round(max_val/100)), width="100%"),
      tags$div(style="margin-top:-15px;margin-bottom:10px;font-size:11px;color:#666;",
               HTML(paste0("\U0001f4ca Median: <span style='color:#d32f2f;font-weight:bold;'>",
                           round(med_c), "</span> contracts")))
    )
  })
  
  output$market_value_range_slider <- renderUI({
    req(econ$filtered_data)
    trigger <- econ$slider_trigger
    df <- econ$filtered_data
    price_col <- detect_price_col(df, .PRICE_COLS_SUPP)
    if (is.null(price_col)) return(p("Value data not available"))
    mkt <- df %>% filter(!is.na(cpv_cluster), !is.na(tender_year), !is.na(.data[[price_col]])) %>%
      group_by(cpv_cluster, tender_year) %>%
      summarise(tv=sum(.data[[price_col]], na.rm=TRUE)/1e6, .groups="drop") %>%
      group_by(cpv_cluster) %>% summarise(avg=mean(tv), .groups="drop")
    max_v <- max(mkt$avg, na.rm=TRUE); med_v <- median(mkt$avg, na.rm=TRUE)
    if (is.na(max_v) || max_v < 1) { max_v <- 1000; med_v <- 500 }
    max_val <- ceiling(max_v / 100) * 100
    tagList(
      sliderInput("econ_market_value_range", "Avg contract value per market-year (millions):",
                  min=0, max=max_val, value=c(0, max_val), step=max(10, round(max_val/100)), width="100%"),
      tags$div(style="margin-top:-15px;margin-bottom:10px;font-size:11px;color:#666;",
               HTML(paste0("\U0001f4ca Median: <span style='color:#d32f2f;font-weight:bold;'>$",
                           round(med_v,1), "M</span>")))
    )
  })
  
  output$market_filter_status <- renderText({
    cr <- input$econ_market_contracts_range; vr <- input$econ_market_value_range
    parts <- c()
    if (!is.null(cr) && cr[1] > 0) parts <- c(parts, paste0("Avg contracts: ", cr[1], "-", cr[2]))
    if (!is.null(vr) && vr[1] > 0) parts <- c(parts, paste0("Value: $", vr[1], "M-$", vr[2], "M"))
    if (length(parts) > 0) paste("Active filters:", paste(parts, collapse=" | "))
    else "No market size filters active (showing all markets)"
  })
  
  observeEvent(input$reset_market_filters, {
    if (!is.null(input$econ_market_contracts_range))
      updateSliderInput(session, "econ_market_contracts_range", value=c(0, input$econ_market_contracts_range[2]))
    if (!is.null(input$econ_market_value_range))
      updateSliderInput(session, "econ_market_value_range", value=c(0, input$econ_market_value_range[2]))
    showNotification("Market size filters reset", type="message", duration=2)
  })
  
  # Apply button: plots already react live to sliders; this just gives visual confirmation
  observeEvent(input$apply_market_filters, {
    n <- if (!is.null(input$econ_market_contracts_range) || !is.null(input$econ_market_value_range))
      "Market size filters applied — plots updated" else "No market size filters active"
    showNotification(n, type="message", duration=2)
  })
  
  # ── Helper: get filtered market CPVs from size sliders ──────────────
  get_market_filtered_cpvs <- function(df) {
    cr <- input$econ_market_contracts_range; vr <- input$econ_market_value_range
    if (is.null(cr) && is.null(vr)) return(NULL)
    price_col <- detect_price_col(df, .PRICE_COLS_SUPP)
    # Single-pass per-market aggregation
    base <- df %>%
      dplyr::filter(!is.na(cpv_cluster), !is.na(tender_year)) %>%
      dplyr::group_by(cpv_cluster, tender_year)
    if (!is.null(price_col)) {
      base <- base %>%
        dplyr::summarise(n=dplyr::n(),
                         tv=sum(.data[[price_col]], na.rm=TRUE)/1e6, .groups="drop") %>%
        dplyr::group_by(cpv_cluster) %>%
        dplyr::summarise(avg_contracts=mean(n), avg_value=mean(tv), .groups="drop")
    } else {
      base <- base %>%
        dplyr::summarise(n=dplyr::n(), .groups="drop") %>%
        dplyr::group_by(cpv_cluster) %>%
        dplyr::summarise(avg_contracts=mean(n), avg_value=0, .groups="drop")
    }
    if (!is.null(cr)) base <- base %>% dplyr::filter(avg_contracts >= cr[1], avg_contracts <= cr[2])
    if (!is.null(vr)) base <- base %>% dplyr::filter(avg_value    >= vr[1], avg_value    <= vr[2])
    base$cpv_cluster
  }
  
  # ── Plot 1: Bubble grid — size = unique suppliers, colour = % new entry ─
  output$supplier_bubble_plot_ui <- renderUI({
    df <- econ$filtered_data
    n_mkts <- if (!is.null(df) && "cpv_cluster" %in% names(df))
      dplyr::n_distinct(df$cpv_cluster, na.rm=TRUE) else 20
    # reasonable: ~24px per market row, capped at 750, min 420
    h <- max(420, min(750, n_mkts * 24 + 100))
    plotlyOutput("supplier_bubble_plot", height=paste0(h,"px"))
  })
  
  output$supplier_bubble_plot <- renderPlotly({
    req(econ$filtered_analysis$supplier_stats, econ$filtered_data)
    thr_new  <- input$econ_new_threshold %||% 50
    show_lab <- isTRUE(input$supp_show_labels)
    show_cnt <- isTRUE(input$supp_show_counts)
    
    ss   <- econ$filtered_analysis$supplier_stats
    keep <- get_market_filtered_cpvs(econ$filtered_data)
    if (!is.null(keep)) ss <- ss %>% filter(cpv_cluster %in% keep)
    if (nrow(ss) == 0) return(plotly::plot_ly(type = "scatter", mode = "markers") %>%
                                plotly::add_annotations(text="No data after market filters", x=0.5, y=0.5,
                                                        xref="paper", yref="paper", showarrow=FALSE, font=list(size=14, color="#888")))
    
    ss <- ss %>%
      mutate(
        cpv_label   = get_cpv_label(cpv_cluster),
        pct_new_lab = scales::percent(share_new, accuracy=1),
        flag_new    = share_new * 100 >= thr_new,
        tooltip_text = paste0(
          "<b>", cpv_label, "</b>  (", tender_year, ")<br>",
          "Unique suppliers: <b>", n_suppliers, "</b><br>",
          "% New this year: <b>", pct_new_lab, "</b>",
          ifelse(flag_new, " ❗ above threshold", ""), "<br>",
          "Repeat suppliers: ", scales::percent(share_repeat, accuracy=1)
        )
      )
    
    y_var <- if (show_lab) "cpv_label" else "cpv_cluster"
    
    p <- ggplot(ss, aes(
      x    = factor(tender_year),
      y    = reorder(.data[[y_var]], cpv_cluster),
      size = n_suppliers,
      fill = share_new,
      text = tooltip_text
    )) +
      geom_point(shape=21, colour="white", stroke=0.3, alpha=0.9) +
      scale_fill_gradient2(
        low="#1565c0", mid="#fffde7", high="#c62828",
        midpoint = thr_new / 100,
        limits   = c(0, 1),
        labels   = scales::percent,
        name     = "% New
suppliers"
      ) +
      scale_size_continuous(range=c(3, 20), name="Unique
suppliers", labels=scales::comma) +
      labs(x="Year", y="CPV Market",
           title="Supplier landscape: depth (size) × entry rate (colour)") +
      pa_theme() +
      theme(
        axis.text.y   = element_text(size=if(show_lab) 10 else 11),
        axis.text.x   = element_text(angle=45, hjust=1),
        panel.grid.major = element_line(colour="#f0f0f0"),
        panel.grid.minor = element_blank(),
        legend.position  = "right"
      )
    
    if (show_cnt)
      p <- p + geom_text(aes(label=n_suppliers), size=2.5, colour="#333", fontface="bold")
    
    n_mkts <- dplyr::n_distinct(ss$cpv_cluster)
    dyn_h  <- max(420, min(750, n_mkts * 24 + 100))
    econ$fig_supp_bubble <- ggplotly(p, tooltip="text", height=dyn_h) %>%
      layout(font=list(size=11), hoverlabel=list(bgcolor="white", font=list(size=11)),
             hovermode="closest", legend=list(orientation="v"),
             yaxis=list(tickfont=list(size=11))) %>%
      pa_config()
    econ$fig_supp_bubble
  })
  # ── Plot 2: Market stability scatter — native plotly (no ggrepel/ggplotly issues) ─
  output$supplier_stability_plot <- renderPlotly({
    req(econ$filtered_analysis$supplier_stats, econ$filtered_data)
    ss   <- econ$filtered_analysis$supplier_stats
    keep <- get_market_filtered_cpvs(econ$filtered_data)
    if (!is.null(keep)) ss <- ss %>% filter(cpv_cluster %in% keep)
    if (nrow(ss) == 0) return(plotly::plot_ly(type = "scatter", mode = "markers") %>%
                                plotly::add_annotations(text="No data after market filters", x=0.5, y=0.5,
                                                        xref="paper", yref="paper", showarrow=FALSE, font=list(size=14, color="#888")))
    
    df <- econ$filtered_data
    mkt_size <- df %>%
      filter(!is.na(cpv_cluster), !is.na(tender_year)) %>%
      group_by(cpv_cluster, tender_year) %>% summarise(n_contracts=n(), .groups="drop") %>%
      group_by(cpv_cluster) %>% summarise(avg_contracts=mean(n_contracts), .groups="drop")
    
    # Volatility = year-on-year standard deviation of % new suppliers.
    # A market where % new jumps between 10% and 80% across years has high SD = volatile.
    # A market stuck at ~30% every year has low SD = predictable/stable.
    s <- ss %>%
      group_by(cpv_cluster) %>%
      summarise(
        avg_pct_new = mean(share_new, na.rm=TRUE),
        avg_n_supp  = mean(n_suppliers, na.rm=TRUE),
        volatility  = sd(share_new, na.rm=TRUE),   # SD of % new across years
        n_years     = n(),
        .groups="drop"
      ) %>%
      left_join(mkt_size, by="cpv_cluster") %>%
      mutate(
        cpv_label    = get_cpv_label(cpv_cluster),
        avg_contracts = replace_na(avg_contracts, 1),
        volatility    = replace_na(volatility, 0),
        # colour on entry rate
        col_val = avg_pct_new,
        tooltip = paste0(
          "<b>", cpv_label, "</b><br>",
          "Avg unique suppliers/yr: <b>", round(avg_n_supp, 1), "</b><br>",
          "Avg % new suppliers: <b>", scales::percent(avg_pct_new, accuracy=1), "</b><br>",
          "Entry rate volatility (SD): ", scales::percent(volatility, accuracy=1),
          "<br><i>SD of % new across years &mdash; higher = more year-to-year swings</i><br>",
          "Avg contracts/yr: ", round(avg_contracts), "<br>",
          "Years observed: ", n_years
        )
      )
    
    med_x <- median(s$avg_pct_new, na.rm=TRUE)
    med_y <- median(s$avg_n_supp,  na.rm=TRUE)
    
    # Colour scale: blue (stable/low entry) → red (high churn)
    cols <- scales::col_numeric(
      palette = c("#1565c0","#fffde7","#c62828"),
      domain  = c(0, 1)
    )(s$col_val)
    
    # Bubble size scaled to avg_contracts
    sz <- scales::rescale(sqrt(s$avg_contracts), to=c(8, 40))
    
    fig <- plot_ly(s,
                   x         = ~avg_pct_new,
                   y         = ~avg_n_supp,
                   text      = ~tooltip,
                   hoverinfo = "text",
                   type      = "scatter",
                   mode      = "markers",
                   marker    = list(
                     size    = sz,
                     color   = cols,
                     opacity = 0.85,
                     line    = list(color="white", width=1.5)
                   )
    ) %>%
      # Median reference lines as shapes (survive without ggplot conversion loss)
      layout(font=list(size=11),
             xaxis  = list(title="Average % new suppliers per year (entry rate)",
                           tickformat=".0%",
                           range=c(-0.02, 1.08),   # pad left+right so edge bubbles aren't clipped
                           zeroline=FALSE),
             yaxis  = list(title="Average unique suppliers per year (market depth)",
                           zeroline=FALSE),
             # generous margins: extra bottom for "median entry" label, extra right for edge bubbles
             margin = list(l=90, r=60, t=44, b=110),
             shapes = list(
               list(type="line", x0=med_x, x1=med_x, y0=0, y1=1, yref="paper",
                    line=list(color="#bbb", width=1, dash="dot")),
               list(type="line", x0=0, x1=1, xref="paper", y0=med_y, y1=med_y,
                    line=list(color="#bbb", width=1, dash="dot"))
             ),
             annotations = list(
               # "median depth": just below the horizontal line at the very left edge
               list(x=0, y=med_y, xref="paper", yref="y",
                    xanchor="left", yanchor="top",
                    text="  median depth", showarrow=FALSE,
                    font=list(size=9, color="#aaa")),
               # "median entry": placed in the bottom margin BELOW the x-axis (negative paper y),
               # centred on the vertical line — never overlaps plot content
               list(x=med_x, y=-0.05, xref="x", yref="paper",
                    xanchor="center", yanchor="top",
                    text="median entry", showarrow=FALSE,
                    font=list(size=9, color="#aaa"))
             ),
             hoverlabel = list(bgcolor="white", font=list(size=12)),
             hovermode  = "closest",
             showlegend = FALSE
      )
    econ$fig_supp_stability <- fig %>%
      plotly::add_annotations(
        text      = "Market Stability Overview (avg across years)",
        x         = 0.5, y = 1.04, xref = "paper", yref = "paper",
        xanchor   = "center", yanchor = "bottom", showarrow = FALSE,
        font      = list(size = 11, color = "#444444")
      ) %>%
      pa_config()
    econ$fig_supp_stability
  })
  
  # ── Plot 3: New vs repeat trend — native plotly stacked area per market ─
  output$supp_trend_market_picker_ui <- renderUI({
    req(econ$filtered_analysis$supplier_stats)
    ss   <- econ$filtered_analysis$supplier_stats
    cpvs <- sort(unique(ss$cpv_cluster))
    choices <- setNames(cpvs, sapply(cpvs, get_cpv_label))
    pickerInput("supp_trend_markets", "Select markets to show:",
                choices  = choices,
                selected = head(cpvs, min(5, length(cpvs))),
                multiple = TRUE,
                options  = list(`actions-box`=TRUE, `live-search`=TRUE,
                                `selected-text-format`="count > 4",
                                `count-selected-text`="{0} markets"))
  })
  
  output$supplier_trend_plot_ui <- renderUI({
    req(econ$filtered_analysis$supplier_stats)
    # When nothing is selected (Deselect All), show a prompt instead of blank
    if (is.null(input$supp_trend_markets) || length(input$supp_trend_markets) == 0)
      return(div(class="deferred-box", icon("hand-pointer"),
                 " Select at least one market above to display the chart."))
    n_mkts <- length(input$supp_trend_markets)
    nrows  <- ceiling(n_mkts / 2L)
    h      <- max(400L, nrows * 280L)   # 280px per row, minimum 400px
    plotlyOutput("supplier_trend_plot", height=paste0(h, "px"))
  })
  
  output$supplier_trend_plot <- renderPlotly({
    req(econ$filtered_analysis$supplier_stats, input$supp_trend_markets)
    metric    <- input$supp_trend_metric %||% "count"
    use_share <- identical(metric, "share")
    
    ss <- econ$filtered_analysis$supplier_stats %>%
      filter(cpv_cluster %in% input$supp_trend_markets) %>%
      mutate(
        cpv_label = get_cpv_label(cpv_cluster),
        # Normalise to n_suppliers so shares always sum to 100%
        # n_new + n_repeat + n_other = n_suppliers by construction
        n_other   = pmax(0L, n_suppliers - n_new_suppliers - n_repeat_suppliers),
        pct_repeat = round(n_repeat_suppliers / n_suppliers * 100, 1),
        pct_new    = round(n_new_suppliers    / n_suppliers * 100, 1),
        pct_other  = round(n_other            / n_suppliers * 100, 1)
      ) %>%
      arrange(cpv_label, tender_year)
    
    if (nrow(ss) == 0) return(plotly::plot_ly(type = "scatter", mode = "markers") %>%
                                plotly::add_annotations(text="No data for selected markets", x=0.5, y=0.5,
                                                        xref="paper", yref="paper", showarrow=FALSE,
                                                        font=list(size=14, color="#888")))
    
    markets <- unique(ss$cpv_label)
    n_mkts  <- length(markets)
    ncols   <- 2L
    nrows   <- ceiling(n_mkts / ncols)
    
    ytitle  <- if (use_share) "Share of suppliers (%)" else "Number of suppliers"
    ysuffix <- if (use_share) "%" else ""
    
    plot_list <- lapply(seq_along(markets), function(i) {
      mkt      <- markets[[i]]
      d        <- ss %>% filter(cpv_label == mkt) %>%
        mutate(
          cum1 = if (use_share) pct_repeat             else n_repeat_suppliers,
          cum2 = if (use_share) pct_repeat + pct_new   else n_repeat_suppliers + n_new_suppliers,
          tip  = if (use_share)
            paste0("<b>", mkt, "</b>  (", tender_year, ")<br>",
                   "Repeat: <b>", pct_repeat, "%</b><br>",
                   "New: <b>", pct_new, "%</b><br>",
                   "Total: ", n_suppliers, " suppliers")
          else
            paste0("<b>", mkt, "</b>  (", tender_year, ")<br>",
                   "Repeat: <b>", n_repeat_suppliers, "</b><br>",
                   "New: <b>", n_new_suppliers, "</b><br>",
                   "Total: ", n_suppliers, " suppliers")
        )
      show_leg <- (i == 1L)
      
      # No per-panel title annotations here — they are injected at subplot level
      # so plotly does not reserve extra top-margin space per panel (which causes
      # the first row to appear taller than the rest).
      plot_ly(d, x=~tender_year) %>%
        layout(
          xaxis = list(title="Year", dtick=1, tickformat="d", tickangle=-45,
                       tickfont=list(size=11), titlefont=list(size=11)),
          yaxis = list(title=ytitle, ticksuffix=ysuffix,
                       tickfont=list(size=11), titlefont=list(size=11))
        ) %>%
        add_trace(y=~cum1, name="Repeat", legendgroup="Repeat", showlegend=show_leg,
                  type="scatter", mode="none", fill="tozeroy",
                  fillcolor="rgba(217,119,6,0.75)", hoverinfo="skip") %>%
        add_trace(y=~cum2, name="New", legendgroup="New", showlegend=show_leg,
                  type="scatter", mode="none", fill="tonexty",
                  fillcolor="rgba(0,137,123,0.75)",
                  hoverinfo="text", text=~tip)
    })
    
    # Build panel title annotations at the subplot level.
    # x positions: col centres (0.25 for left, 0.75 for right in a 2-col layout)
    # y positions: just above the top of each row's plot area.
    col_centres <- if (ncols == 2) c(0.22, 0.78) else 0.5
    row_height  <- 1 / nrows
    panel_anns  <- lapply(seq_along(markets), function(i) {
      col_idx <- ((i - 1L) %% ncols) + 1L
      row_idx <- ceiling(i / ncols)
      # y = top edge of the row's plot area (1 = very top, rows go downward)
      y_top <- 1 - (row_idx - 1L) * row_height
      list(
        text      = markets[[i]],
        x         = col_centres[[col_idx]],
        y         = y_top - 0.01,
        xref      = "paper", yref = "paper",
        xanchor   = "center", yanchor = "bottom",
        showarrow = FALSE,
        font      = list(size = 12, color = "#222")
      )
    })
    
    row_heights <- rep(row_height, nrows)
    fig_out <- subplot(plot_list, nrows=nrows, shareX=FALSE, shareY=FALSE,
                       titleX=FALSE, titleY=FALSE,
                       heights=row_heights,
                       margin=c(0.06, 0.06, 0.08, 0.06))
    fig_out <- plotly::layout(fig_out,
                              annotations   = panel_anns,
                              hoverlabel    = list(bgcolor="white", font=list(size=12)),
                              hovermode     = "x unified",
                              font          = list(size=12),
                              margin        = list(l=70, r=20, t=30, b=70),
                              paper_bgcolor = "#ffffff",
                              plot_bgcolor  = "#ffffff",
                              legend        = list(orientation="h", y=-0.06, x=0.5, xanchor="center",
                                                   font=list(size=12))
    )
    
    econ$fig_supp_trend <- fig_out
    fig_out
  })
  
  # ============================================================
  # TOP SUPPLIERS PLOT
  # ============================================================
  
  output$top_suppliers_plot_ui <- renderUI({
    req(econ$filtered_data, "bidder_masterid" %in% names(econ$filtered_data))
    top_n <- as.integer(input$top_supp_n %||% 20)
    h     <- paste0(max(350, min(900, top_n * 32 + 80)), "px")
    plotlyOutput("top_suppliers_plot", height = h)
  })
  
  output$top_suppliers_plot <- renderPlotly({
    req(econ$filtered_data)
    df <- econ$filtered_data
    
    # Detect supplier ID and name columns
    supp_id_col <- if ("bidder_masterid"       %in% names(df)) "bidder_masterid"
    else if ("bidder_id"          %in% names(df)) "bidder_id"
    else NULL
    supp_nm_col <- if ("bidder_name"            %in% names(df)) "bidder_name"
    else if ("bidder_normalized_name" %in% names(df)) "bidder_normalized_name"
    else if ("winner_name"        %in% names(df)) "winner_name"
    else if ("bidder_normalizedname" %in% names(df)) "bidder_normalizedname"
    else NULL
    if (is.null(supp_id_col) && is.null(supp_nm_col))
      return(.empty_plotly("No supplier ID or name column found in the data."))
    # Use name if available, fall back to ID
    supp_col <- if (!is.null(supp_nm_col)) supp_nm_col else supp_id_col
    
    # Detect buyer name column
    buy_nm_col <- if ("buyer_name"             %in% names(df)) "buyer_name"
    else if ("buyer_normalized_name"  %in% names(df)) "buyer_normalized_name"
    else if ("buyer_normalizedname"   %in% names(df)) "buyer_normalizedname"
    else if ("contracting_authority"  %in% names(df)) "contracting_authority"
    else if ("buyer_masterid"         %in% names(df)) "buyer_masterid"
    else NULL
    
    top_n     <- as.integer(input$top_supp_n  %||% 20)
    metric    <- input$top_supp_metric %||% "n_contracts"
    price_col <- detect_price_col(df, .PRICE_COLS_SUPP)
    
    # Per-supplier summary
    df_filt <- df %>% dplyr::filter(!is.na(.data[[supp_col]]),
                                    as.character(.data[[supp_col]]) != "")
    ss <- df_filt %>%
      dplyr::group_by(supplier = .data[[supp_col]]) %>%
      dplyr::summarise(
        n_contracts = dplyr::n(),
        total_value = if (!is.null(price_col))
          sum(.data[[price_col]], na.rm = TRUE) else NA_real_,
        n_markets   = if ("cpv_cluster" %in% names(df))
          dplyr::n_distinct(cpv_cluster, na.rm = TRUE) else NA_integer_,
        n_years     = if ("tender_year" %in% names(df))
          dplyr::n_distinct(tender_year, na.rm = TRUE) else NA_integer_,
        .groups = "drop"
      )
    
    # Find top buyer per supplier (by number of contracts together)
    if (!is.null(buy_nm_col)) {
      top_buyer <- df_filt %>%
        dplyr::filter(!is.na(.data[[buy_nm_col]]),
                      as.character(.data[[buy_nm_col]]) != "") %>%
        dplyr::group_by(supplier = .data[[supp_col]],
                        buyer    = .data[[buy_nm_col]]) %>%
        dplyr::summarise(n = dplyr::n(), .groups = "drop") %>%
        dplyr::group_by(supplier) %>%
        dplyr::slice_max(n, n = 1, with_ties = FALSE) %>%
        dplyr::ungroup() %>%
        dplyr::mutate(
          buyer_short = {
            b <- as.character(buyer)
            ifelse(nchar(b) > 40, paste0(substr(b, 1, 38), "…"), b)
          }
        ) %>%
        dplyr::select(supplier, top_buyer = buyer_short, top_buyer_n = n)
      ss <- dplyr::left_join(ss, top_buyer, by = "supplier")
    } else {
      ss$top_buyer   <- NA_character_
      ss$top_buyer_n <- NA_integer_
    }
    
    # Sort and trim
    sort_col <- if (metric == "total_value" && all(is.na(ss$total_value))) "n_contracts"
    else if (metric == "n_markets" && all(is.na(ss$n_markets))) "n_contracts"
    else metric
    
    ss <- ss %>%
      dplyr::arrange(dplyr::desc(.data[[sort_col]])) %>%
      dplyr::slice_head(n = top_n) %>%
      dplyr::arrange(.data[[sort_col]]) %>%   # ascending: highest ends up at top via categoryarray
      dplyr::mutate(
        label = {
          s <- as.character(supplier)
          ifelse(nchar(s) > 45, paste0(substr(s, 1, 43), "…"), s)
        },
        color_val = if (!all(is.na(total_value))) total_value else n_contracts
      )
    # Category order for plot_ly — must be set explicitly (unlike ggplot factors)
    cat_order <- ss$label   # ascending order → highest at top after axis reversal
    
    has_value   <- !all(is.na(ss$total_value))
    has_markets <- !all(is.na(ss$n_markets))
    has_years   <- !all(is.na(ss$n_years))
    has_buyer   <- !all(is.na(ss$top_buyer))
    
    # Join supplier ID from original data if it differs from display name
    has_id <- !is.null(supp_id_col) && supp_id_col != supp_col
    if (has_id) {
      id_lkp <- df %>%
        dplyr::filter(!is.na(.data[[supp_col]]), !is.na(.data[[supp_id_col]])) %>%
        dplyr::distinct(supplier = .data[[supp_col]],
                        supp_id_val = as.character(.data[[supp_id_col]]))
      ss <- dplyr::left_join(ss, id_lkp, by = "supplier")
    } else {
      ss$supp_id_val <- NA_character_
    }
    
    # Auto-scale total_value for display (B / M / K depending on magnitude)
    if (has_value) {
      max_tv <- max(ss$total_value, na.rm = TRUE)
      if      (max_tv >= 1e9) { tv_scale <- 1e9; tv_suffix <- "B" }
      else if (max_tv >= 1e6) { tv_scale <- 1e6; tv_suffix <- "M" }
      else if (max_tv >= 1e3) { tv_scale <- 1e3; tv_suffix <- "K" }
      else                    { tv_scale <- 1;   tv_suffix <- ""  }
    } else {
      tv_scale <- 1; tv_suffix <- ""
    }
    
    fmt_value <- function(v) scales::dollar(v, scale = 1/tv_scale,
                                            suffix = tv_suffix, accuracy = 0.1)
    
    # Build tooltip — all outside mutate to avoid .data pronoun issues
    ss$tooltip <- paste0(
      "<b>", ss$label, "</b><br>",
      if (has_id)      paste0("ID: ", ss$supp_id_val, "<br>") else "",
      "Contracts won: <b>", scales::comma(ss$n_contracts), "</b><br>",
      if (has_value)   paste0("Total value: <b>", fmt_value(ss$total_value), "</b><br>") else "",
      if (has_markets) paste0("Markets served: <b>", ss$n_markets, "</b><br>") else "",
      if (has_years)   paste0("Active years: <b>", ss$n_years, "</b><br>") else "",
      if (has_buyer)   paste0("Top buyer: <b>", ss$top_buyer, "</b> (", scales::comma(ss$top_buyer_n), " contracts)") else ""
    )
    
    x_val <- ss[[sort_col]]
    x_lab <- switch(sort_col,
                    n_contracts = "Number of contracts won",
                    total_value = paste0("Total contract value (", tv_suffix, " USD)"),
                    n_markets   = "Number of markets served")
    
    # Colour scale
    col_vals  <- ss$color_val
    col_range <- range(col_vals, na.rm = TRUE)
    col_norm  <- if (diff(col_range) > 0) (col_vals - col_range[1]) / diff(col_range) else rep(0.5, nrow(ss))
    hex_cols  <- scales::col_numeric(c("#93C5FD", "#1E3A8A"), domain = c(0, 1))(col_norm)
    
    # x-axis tick format — use auto-scaled values
    x_tickformat <- if (sort_col == "total_value") "$,.1f" else ","
    x_ticksuffix <- if (sort_col == "total_value") tv_suffix else ""
    x_tickscale  <- if (sort_col == "total_value") (1 / tv_scale) else 1
    
    dyn_h <- max(350, min(900, top_n * 32 + 80))
    
    # Native plot_ly — no ggplotly conversion overhead → instant metric switch
    plot_ly(height = dyn_h) %>%
      # Stems
      add_segments(
        data = ss,
        x = 0, xend = ~x_val * x_tickscale,
        y = ~label, yend = ~label,
        line = list(color = "#CBD5E1", width = 1.5),
        hoverinfo = "skip", showlegend = FALSE
      ) %>%
      # Dots
      add_markers(
        data = ss,
        x = ~x_val * x_tickscale, y = ~label,
        marker = list(
          size   = 11,
          color  = hex_cols,
          line   = list(color = "white", width = 1.5)
        ),
        text      = ~tooltip,
        hoverinfo = "text",
        showlegend = FALSE
      ) %>%
      layout(
        xaxis = list(
          title      = x_lab,
          tickformat = x_tickformat,
          ticksuffix = x_ticksuffix,
          zeroline   = FALSE,
          gridcolor  = "#F1F5F9",
          tickfont   = list(size = 11)
        ),
        yaxis = list(
          title         = "",
          tickfont      = list(size = 11),
          categoryorder = "array",
          categoryarray = cat_order       # ascending data + array order = highest at top
        ),
        hoverlabel  = list(bgcolor = "white", font = list(size = 11)),
        hovermode   = "closest",
        margin      = list(l = 10, r = 30, t = 30, b = 50),
        paper_bgcolor = "#ffffff",
        plot_bgcolor  = "#ffffff"
      ) %>%
      pa_config() -> fig
    econ$fig_top_suppliers <- fig
    fig
  })
  
  # ============================================================
  # NETWORK OUTPUTS
  # ============================================================
  
  # Network status box
  # CPV market picker — populated from live data after upload
  output$network_cpv_picker_ui <- renderUI({
    if (is.null(econ$filtered_data)) {
      return(div(class="alert alert-info", style="padding:8px;",
                 icon("info-circle"), " Load data first to see available CPV markets."))
    }
    df <- econ$filtered_data
    if (!"cpv_cluster" %in% names(df) && "lot_productcode" %in% names(df))
      df <- df %>% dplyr::mutate(cpv_cluster = substr(as.character(lot_productcode), 1, 2))
    
    if (!"cpv_cluster" %in% names(df)) {
      return(div(class="alert alert-warning", "No CPV cluster column found in data."))
    }
    
    cpv_codes <- sort(unique(df$cpv_cluster[!is.na(df$cpv_cluster)]))
    # Build labelled choices: "45 - Construction work" => value "45"
    cpv_labels <- sapply(cpv_codes, get_cpv_label)
    choices <- setNames(cpv_codes, cpv_labels)
    
    # Count contracts per market for the subtitle
    counts <- table(df$cpv_cluster)
    
    pickerInput(
      "network_cpv_selected",
      label = tags$div(icon("project-diagram"), tags$strong(" Select CPV Markets for Networks")),
      choices = choices,
      selected = NULL,
      multiple = TRUE,
      options = list(
        `actions-box`        = TRUE,
        `live-search`        = TRUE,
        `live-search-placeholder` = "Search markets...",
        `selected-text-format` = "count > 3",
        `count-selected-text`  = "{0} markets selected",
        `none-selected-text`   = "-- Select one or more markets --",
        size = 10
      )
    )
  })
  
  output$network_status_box <- renderUI({
    plots <- econ$filtered_analysis$network_plots
    if (is.null(plots)) {
      div(class="alert alert-info", style="margin-bottom:0;",
          icon("info-circle"), " No networks generated yet.",
          " Enter CPV codes above and click Generate.")
    } else if (length(plots) == 0) {
      div(class="alert alert-warning", style="margin-bottom:0;",
          icon("exclamation-triangle"), " Networks generated but no matching data found.",
          " Try different CPV codes.")
    } else {
      div(class="alert alert-success", style="margin-bottom:0;",
          icon("check-circle"), tags$strong(paste0(" ", length(plots), " network(s) ready.")),
          " Scroll down to view. Click Generate to regenerate with new settings.")
    }
  })
  
  # On-demand network generation
  # Calls plot_buyer_supplier_networks() directly - never re-runs the full pipeline.
  # The stress layout in ggraph can segfault on large graphs; we guard with row limits
  # and process one CPV at a time so a single failure does not kill everything.
  observeEvent(input$run_networks_now, {
    req(econ$filtered_data)
    cpv_list <- input$network_cpv_selected
    if (is.null(cpv_list) || length(cpv_list) == 0) {
      showNotification("Select at least one CPV market from the list before generating networks.",
                       type = "warning", duration = 5)
      return()
    }
    top_n    <- as.integer(input$network_top_buyers %||% 15)
    
    df_net <- econ$filtered_data
    
    # Determine correct buyer column
    buyer_col <- if ("buyer_masterid" %in% names(df_net)) "buyer_masterid" else
      if ("buyer_id"       %in% names(df_net)) "buyer_id"       else NULL
    if (is.null(buyer_col)) {
      showNotification("Cannot generate networks: no buyer ID column (buyer_masterid / buyer_id).",
                       type = "error", duration = 8)
      return()
    }
    
    # Ensure cpv_cluster column exists
    if (!"cpv_cluster" %in% names(df_net) && "lot_productcode" %in% names(df_net))
      df_net <- df_net %>% dplyr::mutate(cpv_cluster = substr(as.character(lot_productcode), 1, 2))
    
    # Hard cap on total rows to prevent ggraph stress layout from segfaulting
    MAX_TOTAL <- 150000L
    if (nrow(df_net) > MAX_TOTAL) {
      showNotification(
        paste0("Sampling ", formatC(MAX_TOTAL, format="d", big.mark=","),
               " rows from ", formatC(nrow(df_net), format="d", big.mark=","),
               " for network generation (memory safety)."),
        type = "warning", duration = 6)
      df_net <- df_net[sample.int(nrow(df_net), MAX_TOTAL), ]
    }
    
    withProgress(message = "Generating networks...", value = 0, {
      net_plots <- list()
      
      for (i in seq_along(cpv_list)) {
        cpv <- cpv_list[[i]]
        incProgress(i / length(cpv_list),
                    detail = paste0("CPV ", cpv, "  (", i, "/", length(cpv_list), ")"))
        
        # Skip if not enough data for this CPV
        n_rows <- sum(!is.na(df_net$cpv_cluster) & df_net$cpv_cluster == cpv)
        if (n_rows < 5) {
          message("Skipping CPV ", cpv, ": only ", n_rows, " rows in filtered data")
          next
        }
        
        # Per-CPV row cap - large markets crash the stress layout
        df_cpv <- df_net[!is.na(df_net$cpv_cluster) & df_net$cpv_cluster == cpv, ]
        MAX_CPV <- 30000L
        if (nrow(df_cpv) > MAX_CPV) {
          message("CPV ", cpv, ": sampling ", MAX_CPV, " from ", nrow(df_cpv), " rows")
          df_cpv <- df_cpv[sample.int(nrow(df_cpv), MAX_CPV), ]
        }
        
        p <- tryCatch(
          suppressWarnings(
            plot_buyer_supplier_networks(
              df_cpv,
              cpv_focus    = cpv,
              n_top_buyers = top_n,
              ncol         = 2,
              buyer_id_col = buyer_col,
              country_code = econ$country_code %||% "GEN"
            )
          ),
          error = function(e) {
            message("Network CPV ", cpv, " error: ", e$message)
            NULL
          }
        )
        
        if (!is.null(p)) {
          net_plots[[paste0("CPV ", cpv)]] <- p
        }
      }
      
      econ$filtered_analysis$network_plots <- net_plots
      n_ok <- length(net_plots)
      
      if (n_ok > 0) {
        showNotification(
          paste0("✓ ", n_ok, " network(s) ready: ", paste(names(net_plots), collapse=", ")),
          type = "message", duration = 4)
      } else {
        showNotification(
          paste0("No networks generated. Verify CPV codes [",
                 paste(cpv_list, collapse=", "), "] exist in the cpv_cluster column."),
          type = "warning", duration = 8)
      }
    })
  })
  
  output$network_plots_ui <- renderUI({
    plots <- econ$filtered_analysis$network_plots
    if (is.null(plots) || length(plots) == 0) {
      return(div(class="alert alert-info", style="margin:20px;",
                 icon("info-circle"),
                 " Use the panel above to select CPV markets and generate network diagrams."))
    }
    plot_boxes <- lapply(seq_along(plots), function(i) {
      pname <- paste0("econ_network_plot_", i)
      dname <- paste0("dl_network_", i)
      p     <- plots[[i]]
      output[[pname]] <- renderPlot({ p }, height = 800)
      # Register download handler here so it's always in sync with the displayed plot
      output[[dname]] <- downloadHandler(
        filename = function() paste0("network_cpv", i, "_", econ$country_code %||% "export",
                                     "_", format(Sys.Date(), "%Y%m%d"), ".png"),
        content  = function(file) {
          req(!is.null(p))
          ggplot2::ggsave(file, p, width = 14, height = 11, dpi = 300, bg = "white")
        }
      )
      box(title = names(plots)[i], width = 12, solidHeader = TRUE, status = "primary",
          plotOutput(pname, height = "800px"),
          downloadButton(dname, "Download Figure",
                         class = "download-btn btn-sm"))
    })
    do.call(tagList, plot_boxes)
  })
  
  # ============================================================
  # RELATIVE PRICE OUTPUTS
  # ============================================================
  
  # Computed once per filtered_data change; all four rel plots share it
  rel_price_data <- reactive({
    req(econ$filtered_data)
    tryCatch(add_relative_price(econ$filtered_data), error = function(e) NULL)
  })
  
  .empty_plotly <- function(msg, color = "#888")
    plotly::plot_ly(type = "scatter", mode = "markers") %>% plotly::add_annotations(
      text = msg, x = 0.5, y = 0.5, xref = "paper", yref = "paper",
      showarrow = FALSE, font = list(size = 13, color = color))
  
  output$rel_tot_plot <- renderPlotly({
    df_rel <- rel_price_data(); req(!is.null(df_rel))
    tryCatch({
      # Identify relative_price column (bid / estimate)
      rp_col <- if ("relative_price" %in% names(df_rel)) "relative_price" else NULL
      if (is.null(rp_col))
        return(ggplotly(plot_relative_price_density(df_rel), tooltip="text") %>%
                 layout(font=list(size=11), hoverlabel=list(bgcolor="white")) %>% pa_config())
      
      rp <- df_rel[[rp_col]]
      rp <- rp[!is.na(rp) & is.finite(rp) & rp > 0]
      n_total <- length(rp)
      req(n_total > 0)
      
      # Strict 3-way partition — every contract counted exactly once
      n_under <- sum(rp <  0.999)
      n_at    <- sum(rp >= 0.999 & rp <= 1.001)
      n_over  <- sum(rp >  1.001)
      # sanity: n_under + n_at + n_over == n_total by construction
      pct_under <- round(n_under / n_total * 100, 1)
      pct_at    <- round(n_at    / n_total * 100, 1)
      pct_over  <- round(n_over  / n_total * 100, 1)
      # Adjust rounding so they always sum to exactly 100
      diff <- 100 - (pct_under + pct_at + pct_over)
      pct_over <- pct_over + diff   # absorb rounding remainder into largest group
      
      med_rp  <- median(rp)
      x_range <- quantile(rp, c(0.005, 0.995))
      
      # Density for the plot
      dens <- density(rp, from=max(0, x_range[1]), to=x_range[2], n=512)
      df_dens <- data.frame(x=dens$x, y=dens$y)
      
      summary_txt <- paste0(
        "<b>Under budget</b> (< 1.0): <b>", pct_under, "%</b> (", formatC(n_under, big.mark=",", format="d"), " contracts)<br>",
        "<b>At budget</b> (≈1.0): <b>", pct_at, "%</b> (", formatC(n_at, big.mark=",", format="d"), ")<br>",
        "<b>Over budget</b> (> 1.0): <b>", pct_over, "%</b> (", formatC(n_over, big.mark=",", format="d"), ")<br>",
        "Total: ", formatC(n_total, big.mark=",", format="d"), " contracts | Median: ", round(med_rp, 3)
      )
      
      plot_ly() %>%
        # Under-budget fill (blue)
        add_trace(data=df_dens %>% filter(x <= 1),
                  x=~x, y=~y, type="scatter", mode="none",
                  fill="tozeroy", fillcolor="rgba(0,105,180,0.25)",
                  name="Under budget", hoverinfo="skip") %>%
        # Over-budget fill (red)
        add_trace(data=df_dens %>% filter(x >= 1),
                  x=~x, y=~y, type="scatter", mode="none",
                  fill="tozeroy", fillcolor="rgba(180,0,0,0.20)",
                  name="Over budget", hoverinfo="skip") %>%
        # Full density line
        add_trace(data=df_dens, x=~x, y=~y,
                  type="scatter", mode="lines",
                  line=list(color="#334155", width=2),
                  name="Density",
                  hoverinfo="text",
                  text=summary_txt) %>%
        layout(
          font       = list(size=11),
          hoverlabel = list(bgcolor="white", font=list(size=11)),
          hovermode  = "x unified",
          xaxis = list(title="Relative price (contract ÷ estimate)",
                       zeroline=FALSE, tickfont=list(size=11)),
          yaxis = list(title="Density", tickfont=list(size=11), zeroline=FALSE),
          shapes = list(
            # Budget line at 1.0
            list(type="line", x0=1, x1=1, y0=0, y1=1, yref="paper",
                 line=list(color="#888", width=1.5, dash="dash")),
            # Median line
            list(type="line", x0=med_rp, x1=med_rp, y0=0, y1=1, yref="paper",
                 line=list(color="#D97706", width=1.5, dash="dot"))
          ),
          annotations = list(
            list(x=med_rp, y=0.88, yref="paper", xanchor="left", yanchor="top",
                 text=paste0(" median (", round(med_rp,3), ")"), showarrow=FALSE,
                 font=list(size=10, color="#D97706")),
            list(x=(x_range[1]+1)/2, y=0.5, yref="paper", xanchor="center",
                 text=paste0("<b>", pct_under, "%</b><br>under budget"),
                 showarrow=FALSE, font=list(size=11, color="#0069B4")),
            list(x=1.02, y=0.5, yref="paper", xanchor="left", yanchor="center",
                 text=paste0("<b>", pct_at, "%</b><br>at budget"),
                 showarrow=FALSE, font=list(size=11, color="#475569")),
            list(x=(1+x_range[2])/2, y=0.5, yref="paper", xanchor="center",
                 text=paste0("<b>", pct_over, "%</b><br>over budget"),
                 showarrow=FALSE, font=list(size=11, color="#B40000"))
          ),
          legend = list(orientation="h", y=-0.15, font=list(size=10)),
          margin = list(l=60, r=20, t=20, b=60),
          paper_bgcolor="#ffffff", plot_bgcolor="#ffffff"
        ) %>%
        pa_config()
    }, error = function(e) .empty_plotly(paste("Not available:", e$message)))
  })
  
  output$rel_year_plot <- renderPlotly({
    df_rel <- rel_price_data(); req(!is.null(df_rel))
    tryCatch(
      ggplotly(plot_relative_price_by_year(df_rel), tooltip = "text") %>%
        layout(font=list(size=11), hoverlabel = list(bgcolor = "white"), hovermode = "closest",
               legend = list(orientation = "h", y = -0.12, font=list(size=10))) %>%
        pa_config(),
      error = function(e) .empty_plotly(paste("Not available:", e$message)))
  })
  
  # markets plot: dynamic height based on number of CPV categories
  output$rel_10_plot_ui <- renderUI({
    df_rel <- rel_price_data()
    n_mkts <- if (!is.null(df_rel) && "cpv_category" %in% names(df_rel))
      dplyr::n_distinct(df_rel$cpv_category, na.rm = TRUE) else 10
    h <- paste0(max(380, min(750, n_mkts * 30 + 100)), "px")
    plotlyOutput("rel_10_plot", height = h)
  })
  
  output$rel_10_plot <- renderPlotly({
    df_rel <- rel_price_data(); req(!is.null(df_rel))
    tryCatch({
      # Replace cpv_category with the full "XX — Name" label used in the filters
      # so the y-axis matches what the user sees in the Market dropdown.
      if ("cpv_cluster" %in% names(df_rel))
        df_rel <- df_rel %>%
          dplyr::mutate(cpv_category = get_cpv_label(cpv_cluster))
      top_mkts <- top_markets_by_relative_price(df_rel)
      ggplotly(plot_top_markets_relative_price(df_rel, top_mkts),
               tooltip = "text") %>%
        layout(font=list(size=11), hoverlabel = list(bgcolor = "white"), hovermode = "closest",
               margin = list(l=220, r=20, t=70, b=40),
               legend = list(orientation = "h", y = 1.06, x = 0.5, xanchor = "center", yanchor = "bottom",
                             font = list(size = 11))) %>%
        pa_config()
    }, error = function(e) .empty_plotly(paste("Not available:", e$message)))
  })
  
  output$rel_buy_plot_ui <- renderUI({
    top_n <- as.integer(input$rel_buy_top_n %||% 20)
    h     <- paste0(max(300, min(500, top_n * 18 + 80)), "px")
    plotlyOutput("rel_buy_plot", height = h)
  })
  
  output$rel_buy_plot <- renderPlotly({
    df_rel <- rel_price_data(); req(!is.null(df_rel))
    top_n         <- as.integer(input$rel_buy_top_n        %||% 20)
    min_contracts <- as.integer(input$rel_buy_min_contracts %||% 10)
    tryCatch({
      top_buy <- top_buyers_by_relative_price(df_rel, min_contracts = min_contracts, n = top_n)
      if (nrow(top_buy) == 0)
        return(.empty_plotly(paste0("No buyers found with \u2265 ", min_contracts, " contracts")))
      p_buy <- ggplotly(plot_top_buyers_relative_price(top_buy, label_max_chars = 30),
                        tooltip = "text") %>%
        layout(font=list(size=11), hoverlabel = list(bgcolor = "white"), hovermode = "closest",
               legend = list(orientation = "v", x = 1.02, y = 0.5, font = list(size = 10)))
      # Hide any vline/dashed-line traces from the legend
      dash_traces <- which(sapply(p_buy$x$data, function(t)
        !is.null(t$line) && isTRUE(t$line$dash %in% c("dash","dashdot","dot"))))
      if (length(dash_traces) > 0) p_buy <- plotly::style(p_buy, showlegend = FALSE, traces = dash_traces)
      econ$fig_rel_buy <- p_buy %>% pa_config(); econ$fig_rel_buy
    }, error = function(e) .empty_plotly(paste("Error:", e$message), "red"))
  })
  
  # ============================================================
  # COMPETITION OUTPUTS
  # ============================================================
  
  .comp_plotly <- function(p) {
    ggplotly(p, tooltip = "text") %>%
      layout(hoverlabel = list(bgcolor = "white"), hovermode = "closest",
             autosize = TRUE)
  }
  
  output$single_bid_overall_plot <- renderPlotly({
    req(econ$filtered_data)
    tryCatch(.comp_plotly(plot_single_bid_overall(econ$filtered_data)),
             error = function(e) .empty_plotly(paste("Not available:", e$message))) %>%
      pa_config()
  })
  
  output$single_bid_procedure_plot <- renderPlotly({
    req(econ$filtered_data)
    tryCatch(.comp_plotly(plot_single_bid_by_procedure(econ$filtered_data)),
             error = function(e) .empty_plotly(paste("Not available:", e$message))) %>%
      pa_config()
  })
  
  output$single_bid_price_plot <- renderPlotly({
    req(econ$filtered_data)
    tryCatch(.comp_plotly(plot_single_bid_by_price(econ$filtered_data)),
             error = function(e) .empty_plotly(paste("Not available:", e$message))) %>%
      pa_config()
  })
  
  output$single_bid_buyer_group_plot <- renderPlotly({
    req(econ$filtered_data)
    tryCatch(.comp_plotly(plot_single_bid_by_buyer_group(econ$filtered_data)),
             error = function(e) .empty_plotly(paste("Not available:", e$message))) %>%
      pa_config()
  })
  
  output$single_bid_market_plot_ui <- renderUI({
    df <- econ$filtered_data
    n_mkts <- if (!is.null(df)) {
      lbl <- if ("cpv_category" %in% names(df) && !all(is.na(df$cpv_category)))
        "cpv_category" else "cpv_cluster"
      dplyr::n_distinct(df[[lbl]], na.rm = TRUE)
    } else 10
    plotlyOutput("single_bid_market_plot", height = paste0(max(300, min(700, n_mkts * 24 + 100)), "px"))
  })
  
  output$single_bid_market_plot <- renderPlotly({
    req(econ$filtered_data)
    tryCatch(
      ggplotly(plot_single_bid_by_market(econ$filtered_data), tooltip = "text") %>%
        layout(hoverlabel = list(bgcolor = "white"), hovermode = "closest",
               font = list(size = 11),
               margin = list(l = 10, r = 20, t = 20, b = 40),
               autosize = TRUE) %>%
        pa_config(),
      error = function(e) .empty_plotly(paste("Not available:", e$message)))
  })
  
  output$top_buyers_single_bid_plot_ui <- renderUI({
    top_n <- as.integer(input$sb_buy_top_n %||% 20)
    plotlyOutput("top_buyers_single_bid_plot", height = paste0(max(400, top_n * 28), "px"))
  })
  
  output$top_buyers_single_bid_plot <- renderPlotly({
    req(econ$filtered_data)
    top_n       <- as.integer(input$sb_buy_top_n       %||% 20)
    min_tenders <- as.integer(input$sb_buy_min_tenders %||% 30)
    tryCatch(
      .comp_plotly(plot_top_buyers_single_bid(econ$filtered_data,
                                              buyer_id_col = "buyer_masterid",
                                              top_n        = top_n,
                                              min_tenders  = min_tenders)) %>%
        pa_config(),
      error = function(e) .empty_plotly(paste("Not available:", e$message)))
  })
  
  # ============================================================
  # ECON FIGURE DOWNLOAD HANDLERS
  # All downloads now render fresh from current filtered data
  # (same as what's displayed) via webshot2.
  # ============================================================
  
  # Helper: render a fresh plotly from filtered econ data and save via webshot2
  dl_econ_plotly <- function(make_fig, fname, vw = 1200, vh = 700) {
    downloadHandler(
      filename = function() paste0(fname, "_", econ$country_code %||% "export",
                                   "_", format(Sys.Date(), "%Y%m%d"), ".png"),
      content  = function(file) {
        fig <- tryCatch(make_fig(), error = function(e) NULL)
        .require_fig(fig, fname)
        .save_fig_png(fig, file)
      }
    )
  }
  
  # Overview charts — use stored figs (always match what's displayed)
  output$dl_contracts_year_econ <- downloadHandler(
    filename = function() paste0("contracts_per_year_", econ$country_code %||% "export",
                                 "_", format(Sys.Date(), "%Y%m%d"), ".png"),
    content  = function(file) {
      fig <- tryCatch(econ$fig_contracts_year_econ, error = function(e) NULL)
      .require_fig(fig, "Contracts per Year")
      .save_fig_png(fig, file)
    }
  )
  output$dl_value_by_year <- downloadHandler(
    filename = function() paste0("contract_value_by_year_", econ$country_code %||% "export",
                                 "_", format(Sys.Date(), "%Y%m%d"), ".png"),
    content  = function(file) {
      fig <- tryCatch(econ$fig_value_by_year, error = function(e) NULL)
      .require_fig(fig, "Contract Value by Year")
      .save_fig_png(fig, file)
    }
  )
  
  # Market sizing — re-computed live from filtered data
  output$dl_market_size_n <- dl_econ_plotly(function() {
    df <- econ$filtered_data; req(!is.null(df))
    if (!"cpv_cluster" %in% names(df)) req(FALSE)
    df %>% dplyr::filter(!is.na(cpv_cluster)) %>%
      dplyr::mutate(cpv_label = get_cpv_label(cpv_cluster)) %>%
      dplyr::group_by(cpv_label) %>%
      dplyr::summarise(n_contracts = dplyr::n(), .groups = "drop") %>%
      dplyr::arrange(dplyr::desc(n_contracts)) %>% dplyr::slice_head(n = 30) %>%
      dplyr::mutate(
        label_short = ifelse(nchar(cpv_label) > 35, paste0(substr(cpv_label,1,33),"\u2026"), cpv_label),
        label_short = factor(label_short, levels = label_short[order(n_contracts)])
      ) %>%
      { ggplotly(
        ggplot2::ggplot(., ggplot2::aes(x=label_short, y=n_contracts,
                                        text=paste0("<b>",cpv_label,"</b><br>Contracts: ",
                                                    formatC(n_contracts,format="d",big.mark=",")))) +
          ggplot2::geom_col(fill=PA_NORMAL) + ggplot2::coord_flip() +
          ggplot2::scale_y_continuous(labels=scales::comma) +
          ggplot2::labs(x=NULL, y="Number of contracts") + pa_theme(),
        tooltip="text")
      } %>% pa_config()
  }, "market_size_n", 1200, 800)
  
  output$dl_market_size_v <- dl_econ_plotly(function() {
    df <- econ$filtered_data; req(!is.null(df))
    price_var <- detect_price_col(df); req(!is.null(price_var))
    df %>% dplyr::filter(!is.na(cpv_cluster), !is.na(.data[[price_var]]), .data[[price_var]]>0) %>%
      dplyr::mutate(cpv_label = get_cpv_label(cpv_cluster)) %>%
      dplyr::group_by(cpv_label) %>%
      dplyr::summarise(total_value=sum(.data[[price_var]],na.rm=TRUE), .groups="drop") %>%
      dplyr::arrange(dplyr::desc(total_value)) %>% dplyr::slice_head(n=30) %>%
      dplyr::mutate(
        label_short = ifelse(nchar(cpv_label)>35, paste0(substr(cpv_label,1,33),"\u2026"), cpv_label),
        label_short = factor(label_short, levels=label_short[order(total_value)])
      ) %>%
      { ggplotly(
        ggplot2::ggplot(., ggplot2::aes(x=label_short, y=total_value,
                                        text=paste0("<b>",cpv_label,"</b>"))) +
          ggplot2::geom_col(fill=PA_TEAL) + ggplot2::coord_flip() +
          ggplot2::scale_y_continuous(labels=scales::dollar_format(scale=1e-6,suffix="M",accuracy=0.1)) +
          ggplot2::labs(x=NULL, y="Total contract value") + pa_theme(),
        tooltip="text")
      } %>% pa_config()
  }, "market_size_v", 1200, 800)
  
  output$dl_market_size_av <- dl_econ_plotly(function() {
    req(!is.null(econ$filtered_data))
    df <- econ$filtered_data
    price_var <- detect_price_col(df); req(!is.null(price_var))
    ms <- df %>%
      dplyr::filter(!is.na(cpv_cluster)) %>%
      dplyr::mutate(cpv_label = get_cpv_label(cpv_cluster)) %>%
      dplyr::group_by(cpv_cluster, cpv_label) %>%
      dplyr::summarise(n_contracts=dplyr::n(),
                       total_value=sum(.data[[price_var]],na.rm=TRUE),
                       avg_value=mean(.data[[price_var]],na.rm=TRUE), .groups="drop") %>%
      dplyr::filter(n_contracts>0, avg_value>0, total_value>0) %>%
      dplyr::mutate(
        bubble_size = scales::rescale(sqrt(total_value), to=c(8,50)),
        log_avg     = log10(avg_value+1),
        hover_text  = paste0("<b>",cpv_label,"</b><br>",
                             "Contracts: <b>",scales::comma(n_contracts),"</b><br>",
                             "Avg contract value: <b>",scales::dollar(avg_value,scale=1e-3,suffix="K",accuracy=1),"</b><br>",
                             "Total market value: <b>",scales::dollar(total_value,scale=1e-6,suffix="M",accuracy=0.1),"</b>")
      )
    req(nrow(ms) > 0)
    make_log_ticks <- function(vals, prefix="") {
      pows <- seq(floor(log10(min(vals,na.rm=TRUE))), ceiling(log10(max(vals,na.rm=TRUE))))
      tv <- 10^pows
      fmt <- function(v) dplyr::case_when(
        v>=1e9~paste0(prefix,scales::comma(v/1e9),"B"), v>=1e6~paste0(prefix,scales::comma(v/1e6),"M"),
        v>=1e3~paste0(prefix,scales::comma(v/1e3),"K"), TRUE~paste0(prefix,scales::comma(v)))
      list(tickvals=tv, ticktext=fmt(tv))
    }
    xt <- make_log_ticks(ms$n_contracts); yt <- make_log_ticks(ms$avg_value, prefix="$")
    plotly::plot_ly(ms, x=~n_contracts, y=~avg_value, text=~hover_text, hoverinfo="text",
                    type="scatter", mode="markers",
                    marker=list(size=~bubble_size, sizemode="diameter",
                                color=~log_avg,
                                colorscale=list(c(0,"#c6dbef"),c(0.5,"#4292c6"),c(1,"#08306b")),
                                showscale=TRUE,
                                colorbar=list(title="Avg value<br>(log₁₀ USD)", tickformat=".1f"),
                                opacity=0.85, line=list(color="white",width=1))) %>%
      plotly::layout(
        xaxis=list(title="Number of contracts (log scale)",type="log",
                   tickvals=xt$tickvals, ticktext=xt$ticktext, tickangle=-35, zeroline=FALSE, gridcolor="#eeeeee"),
        yaxis=list(title="Average contract value (log scale)",type="log",
                   tickvals=yt$tickvals, ticktext=yt$ticktext, zeroline=FALSE, gridcolor="#eeeeee"),
        margin=list(l=90,r=60,t=60,b=90),
        hoverlabel=list(bgcolor="white",font=list(size=12)), hovermode="closest", showlegend=FALSE) %>%
      pa_config()
  }, "market_size_av", 1200, 700)
  
  # Relative price plots — re-built from live rel_price_data()
  rel_price_dl_fig <- function(plot_fn, ...) {
    df_rel <- tryCatch(rel_price_data(), error=function(e) NULL)
    req(!is.null(df_rel))
    ggplotly(plot_fn(df_rel, ...), tooltip="text") %>%
      plotly::layout(hoverlabel=list(bgcolor="white")) %>% pa_config()
  }
  output$dl_rel_tot <- dl_econ_plotly(function() {
    # Reproduce the custom density plot that's shown on screen (not the util function)
    df_rel <- tryCatch(rel_price_data(), error=function(e) NULL); req(!is.null(df_rel))
    rp_col <- if ("relative_price" %in% names(df_rel)) "relative_price" else NULL
    req(!is.null(rp_col))
    rp      <- df_rel[[rp_col]]; rp <- rp[!is.na(rp) & is.finite(rp) & rp > 0]
    n_total <- length(rp); req(n_total > 0)
    n_under <- sum(rp <  0.999); n_at <- sum(rp >= 0.999 & rp <= 1.001); n_over <- sum(rp > 1.001)
    pct_under <- round(n_under/n_total*100,1); pct_at <- round(n_at/n_total*100,1)
    pct_over  <- round(n_over/n_total*100,1) + (100-(round(n_under/n_total*100,1)+round(n_at/n_total*100,1)+round(n_over/n_total*100,1)))
    med_rp  <- median(rp); x_range <- quantile(rp, c(0.005, 0.995))
    dens    <- density(rp, from=max(0,x_range[1]), to=x_range[2], n=512)
    df_dens <- data.frame(x=dens$x, y=dens$y)
    summary_txt <- paste0(
      "<b>Under budget</b> (< 1.0): <b>",pct_under,"%</b> (",formatC(n_under,big.mark=",",format="d")," contracts)<br>",
      "<b>At budget</b> (≈1.0): <b>",pct_at,"%</b> (",formatC(n_at,big.mark=",",format="d"),")<br>",
      "<b>Over budget</b> (> 1.0): <b>",pct_over,"%</b> (",formatC(n_over,big.mark=",",format="d"),")<br>",
      "Total: ",formatC(n_total,big.mark=",",format="d")," contracts | Median: ",round(med_rp,3))
    plot_ly() %>%
      add_trace(data=df_dens%>%filter(x<=1),x=~x,y=~y,type="scatter",mode="none",fill="tozeroy",fillcolor="rgba(0,105,180,0.25)",name="Under budget",hoverinfo="skip") %>%
      add_trace(data=df_dens%>%filter(x>=1),x=~x,y=~y,type="scatter",mode="none",fill="tozeroy",fillcolor="rgba(180,0,0,0.20)",name="Over budget",hoverinfo="skip") %>%
      add_trace(data=df_dens,x=~x,y=~y,type="scatter",mode="lines",line=list(color="#334155",width=2),name="Density",hoverinfo="text",text=summary_txt) %>%
      plotly::layout(
        font=list(size=11), hoverlabel=list(bgcolor="white",font=list(size=11)), hovermode="x unified",
        xaxis=list(title="Relative price (contract ÷ estimate)",zeroline=FALSE,tickfont=list(size=11)),
        yaxis=list(title="Density",tickfont=list(size=11),zeroline=FALSE),
        shapes=list(
          list(type="line",x0=1,x1=1,y0=0,y1=1,yref="paper",line=list(color="#888",width=1.5,dash="dash")),
          list(type="line",x0=med_rp,x1=med_rp,y0=0,y1=1,yref="paper",line=list(color="#D97706",width=1.5,dash="dot"))),
        annotations=list(
          list(x=med_rp,y=0.88,yref="paper",xanchor="left",yanchor="top",text=paste0(" median (",round(med_rp,3),")"),showarrow=FALSE,font=list(size=10,color="#D97706")),
          list(x=(x_range[1]+1)/2,y=0.5,yref="paper",xanchor="center",text=paste0("<b>",pct_under,"%</b><br>under budget"),showarrow=FALSE,font=list(size=11,color="#0069B4")),
          list(x=1.02,y=0.5,yref="paper",xanchor="left",yanchor="center",text=paste0("<b>",pct_at,"%</b><br>at budget"),showarrow=FALSE,font=list(size=11,color="#475569")),
          list(x=(1+x_range[2])/2,y=0.5,yref="paper",xanchor="center",text=paste0("<b>",pct_over,"%</b><br>over budget"),showarrow=FALSE,font=list(size=11,color="#B40000"))),
        legend=list(orientation="h",y=-0.15,font=list(size=10)),
        margin=list(l=60,r=20,t=20,b=60), paper_bgcolor="#ffffff", plot_bgcolor="#ffffff") %>%
      pa_config()
  }, "rel_tot", 1200, 700)
  output$dl_rel_year <- dl_econ_plotly(function() rel_price_dl_fig(plot_relative_price_by_year),  "rel_year", 1200, 700)
  output$dl_rel_10   <- dl_econ_plotly(function() {
    df_rel <- tryCatch(rel_price_data(), error=function(e) NULL); req(!is.null(df_rel))
    if ("cpv_cluster" %in% names(df_rel))
      df_rel <- df_rel %>% dplyr::mutate(cpv_category = get_cpv_label(cpv_cluster))
    top_mkts <- top_markets_by_relative_price(df_rel)
    ggplotly(plot_top_markets_relative_price(df_rel, top_mkts), tooltip="text") %>%
      plotly::layout(hoverlabel=list(bgcolor="white"), margin=list(l=220,r=20,t=70,b=40)) %>%
      pa_config()
  }, "rel_10", 1200, 800)
  output$dl_rel_buy  <- dl_econ_plotly(function() {
    df_rel <- tryCatch(rel_price_data(), error=function(e) NULL); req(!is.null(df_rel))
    top_n         <- as.integer(input$rel_buy_top_n %||% 20)
    min_contracts <- as.integer(input$rel_buy_min_contracts %||% 10)
    top_buy <- top_buyers_by_relative_price(df_rel, min_contracts=min_contracts, n=top_n)
    req(nrow(top_buy) > 0)
    ggplotly(plot_top_buyers_relative_price(top_buy, label_max_chars=30), tooltip="text") %>%
      plotly::layout(hoverlabel=list(bgcolor="white")) %>% pa_config()
  }, "rel_buy", 1200, 800)
  
  output$rel_size_plot <- renderPlotly({
    df_rel <- rel_price_data(); req(!is.null(df_rel))
    tryCatch({
      rp_col    <- if ("relative_price" %in% names(df_rel)) "relative_price" else NULL
      price_col <- detect_price_col(df_rel, .PRICE_COLS_SUPP)
      req(!is.null(rp_col), !is.null(price_col))
      
      d <- df_rel %>%
        dplyr::filter(!is.na(.data[[rp_col]]), is.finite(.data[[rp_col]]),
                      .data[[rp_col]] > 0, .data[[rp_col]] < 10,
                      !is.na(.data[[price_col]]), .data[[price_col]] > 0) %>%
        dplyr::mutate(rp = .data[[rp_col]], val = .data[[price_col]])
      
      req(nrow(d) >= 20)
      
      # Use price_bin from the econ pipeline if available — same bins as Competition tab
      if ("price_bin" %in% names(d) && dplyr::n_distinct(d$price_bin, na.rm=TRUE) > 1) {
        d <- d %>% dplyr::filter(!is.na(price_bin))
        d$band_lbl <- d$price_bin
      } else {
        # Fallback: 8 equal-frequency quantile bands
        breaks <- unique(quantile(d$val, probs = seq(0, 1, length.out = 9), na.rm = TRUE))
        req(length(breaks) >= 3)
        med_val <- median(d$val, na.rm = TRUE)
        if      (med_val >= 1e6) { scale <- 1e6; suffix <- "M" }
        else if (med_val >= 1e3) { scale <- 1e3; suffix <- "K" }
        else                     { scale <- 1;   suffix <- ""  }
        d$band     <- cut(d$val, breaks = breaks, include.lowest = TRUE, dig.lab = 10)
        fmt_band   <- function(lbl) {
          clean <- gsub("[^0-9,.-]", "", as.character(lbl))
          parts <- strsplit(clean, ",")[[1]]
          if (length(parts) < 2) return(as.character(lbl))
          lo <- suppressWarnings(as.numeric(trimws(parts[1])))
          hi <- suppressWarnings(as.numeric(trimws(parts[2])))
          if (is.na(lo) || is.na(hi)) return(as.character(lbl))
          paste0("$", formatC(lo/scale, format="fg", digits=3), suffix,
                 "–$", formatC(hi/scale, format="fg", digits=3), suffix)
        }
        band_levels <- levels(d$band)
        band_labels <- sapply(band_levels, fmt_band)
        d$band_lbl  <- factor(band_labels[as.integer(d$band)], levels = band_labels)
      }
      
      req(!all(is.na(d$band_lbl)))
      
      # Summary per band
      summ <- d %>%
        dplyr::group_by(band_lbl) %>%
        dplyr::summarise(
          n         = dplyr::n(),
          pct_over  = round(mean(rp > 1.001) * 100, 1),
          med_rp    = median(rp),
          q25       = quantile(rp, 0.25),
          q75       = quantile(rp, 0.75),
          p05       = quantile(rp, 0.05),
          p95       = quantile(rp, 0.95),
          .groups   = "drop"
        ) %>%
        dplyr::mutate(
          col = ifelse(med_rp > 1.001, "#DC2626", "#00897B"),
          tip = paste0("<b>", band_lbl, "</b><br>",
                       "Contracts: <b>", scales::comma(n), "</b><br>",
                       "Median relative price: <b>", round(med_rp, 3), "</b><br>",
                       "Over budget: <b>", pct_over, "%</b><br>",
                       "IQR: [", round(q25,3), " – ", round(q75,3), "]")
        )
      
      fig <- plot_ly(summ, x = ~band_lbl, hoverinfo = "text") %>%
        add_segments(x = ~band_lbl, xend = ~band_lbl, y = ~p05, yend = ~p95,
                     line = list(color = "#94A3B8", width = 1.5),
                     hoverinfo = "skip", showlegend = FALSE) %>%
        add_segments(x = ~band_lbl, xend = ~band_lbl, y = ~q25, yend = ~q75,
                     line = list(color = ~col, width = 10),
                     hoverinfo = "skip", showlegend = FALSE) %>%
        add_markers(x = ~band_lbl, y = ~med_rp,
                    marker = list(color = ~col, size = 8,
                                  line = list(color = "white", width = 1.5)),
                    text = ~tip, hoverinfo = "text", showlegend = FALSE) %>%
        add_text(x = ~band_lbl, y = ~p95,
                 text = ~paste0(pct_over, "%"),
                 textposition = "top center",
                 textfont = list(size = 10, color = ~col),
                 hoverinfo = "skip", showlegend = FALSE) %>%
        plotly::layout(
          xaxis  = list(title = "Contract value (USD)",
                        tickangle = -35, tickfont = list(size = 10)),
          yaxis  = list(title = "Relative price (contract ÷ estimate)",
                        zeroline = FALSE, tickfont = list(size = 11)),
          shapes = list(list(type = "line", x0 = 0, x1 = 1, xref = "paper",
                             y0 = 1, y1 = 1,
                             line = list(color = "#888", width = 1.5, dash = "dash"))),
          hoverlabel    = list(bgcolor = "white", font = list(size = 11)),
          hovermode     = "closest",
          margin        = list(l = 60, r = 20, t = 40, b = 100),
          paper_bgcolor = "#ffffff", plot_bgcolor  = "#ffffff"
        ) %>%
        pa_config()
      econ$fig_rel_size <- fig
      fig
    }, error = function(e) .empty_plotly(paste("Not available:", e$message)))
  })
  
  output$dl_rel_size <- dl_econ_plotly(function() {
    df_rel <- tryCatch(rel_price_data(), error=function(e) NULL); req(!is.null(df_rel))
    tryCatch(econ$fig_rel_size, error=function(e) NULL) %||%
      .empty_plotly("View the chart first, then download.")
  }, "rel_size", 1200, 600)
  
  # Single-bid plots — re-built from live econ$filtered_data
  output$dl_single_bid_overall <- dl_econ_plotly(function() {
    req(econ$filtered_data)
    .comp_plotly(plot_single_bid_overall(econ$filtered_data)) %>% pa_config()
  }, "single_bid_overall", 900, 600)
  output$dl_single_bid_procedure <- dl_econ_plotly(function() {
    req(econ$filtered_data)
    .comp_plotly(plot_single_bid_by_procedure(econ$filtered_data)) %>% pa_config()
  }, "single_bid_procedure", 900, 600)
  output$dl_single_bid_price <- dl_econ_plotly(function() {
    req(econ$filtered_data)
    .comp_plotly(plot_single_bid_by_price(econ$filtered_data)) %>% pa_config()
  }, "single_bid_price", 900, 600)
  output$dl_single_bid_buyer_group <- dl_econ_plotly(function() {
    req(econ$filtered_data)
    .comp_plotly(plot_single_bid_by_buyer_group(econ$filtered_data)) %>% pa_config()
  }, "single_bid_buyer_group", 900, 600)
  output$dl_single_bid_market <- dl_econ_plotly(function() {
    req(econ$filtered_data)
    ggplotly(plot_single_bid_by_market(econ$filtered_data), tooltip="text") %>%
      plotly::layout(hoverlabel=list(bgcolor="white"), autosize=TRUE) %>% pa_config()
  }, "single_bid_market", 900, 700)
  output$dl_top_buyers_single_bid <- dl_econ_plotly(function() {
    req(econ$filtered_data)
    top_n       <- as.integer(input$sb_buy_top_n %||% 20)
    min_tenders <- as.integer(input$sb_buy_min_tenders %||% 30)
    .comp_plotly(plot_top_buyers_single_bid(econ$filtered_data,
                                            buyer_id_col="buyer_masterid",
                                            top_n=top_n, min_tenders=min_tenders)) %>% pa_config()
  }, "top_buyers_single_bid", 900, 700)
  
  # Downloads the SUPPLIER ENTRY RATE BUBBLE CHART — same as displayed
  output$dl_top_suppliers <- downloadHandler(
    filename = function() paste0("top_suppliers_", econ$country_code %||% "export",
                                 "_", format(Sys.Date(), "%Y%m%d"), ".png"),
    content  = function(file) {
      fig <- tryCatch(econ$fig_top_suppliers, error = function(e) NULL)
      .require_fig(fig, "Top Suppliers")
      .save_fig_png(fig, file, 1200, 800)
    }
  )
  output$dl_supp_trend <- downloadHandler(
    filename = function() paste0("supplier_trend_", econ$country_code %||% "export",
                                 "_", format(Sys.Date(), "%Y%m%d"), ".png"),
    content  = function(file) {
      fig <- tryCatch(econ$fig_supp_trend, error = function(e) NULL)
      .require_fig(fig, "New vs Repeat Suppliers Trend")
      .save_fig_png(fig, file, 1400, 700)
    }
  )
  output$dl_suppliers_entrance <- downloadHandler(
    filename = function() paste0("supplier_entry_bubble_", econ$country_code %||% "export",
                                 "_", format(Sys.Date(), "%Y%m%d"), ".png"),
    content  = function(file) {
      fig <- tryCatch(econ$fig_supp_bubble, error = function(e) NULL)
      .require_fig(fig, "Supplier Entry Rate Bubble Chart")
      .save_fig_png(fig, file)
    }
  )
  
  # Downloads the MARKET STABILITY SCATTER — same as displayed
  output$dl_unique_supp <- downloadHandler(
    filename = function() paste0("market_stability_scatter_", econ$country_code %||% "export",
                                 "_", format(Sys.Date(), "%Y%m%d"), ".png"),
    content  = function(file) {
      fig <- tryCatch(econ$fig_supp_stability, error = function(e) NULL)
      .require_fig(fig, "Market Stability Scatter")
      .save_fig_png(fig, file)
    }
  )
  
  # ============================================================
  # ECON REPORT DOWNLOADS
  # ============================================================
  
  econ_get_export_data <- function() {
    econ_filter_data(
      df             = econ$analysis$df,
      year_range     = econ_filters$active$year,
      market         = econ_filters$active$market,
      value_range    = econ_filters$active$value,
      buyer_type     = econ_filters$active$buyer_type,
      procedure_type = econ_filters$active$procedure_type,
      value_divisor  = econ$value_divisor,
      buyer_mapping  = econ_buyer_mapping(),
      procedure_mapping = econ_procedure_mapping()
    )
  }
  
  admin_get_export_data <- function() {
    admin_filter_data(
      df                = admin$data,
      year_range        = admin_filters$active$year,
      market            = admin_filters$active$market,
      value_range       = admin_filters$active$value,
      buyer_type        = admin_filters$active$buyer_type,
      procedure_type    = admin_filters$active$procedure_type,
      value_divisor     = admin$value_divisor,
      procedure_mapping = admin_procedure_mapping()
    )
  }
  
  integ_get_export_data <- function() {
    integrity_filter_data(
      df             = integ$data,
      year_range     = integ_filters$active$year,
      market         = integ_filters$active$market,
      value_range    = integ_filters$active$value,
      buyer_type     = integ_filters$active$buyer_type,
      procedure_type = integ_filters$active$procedure_type,
      value_divisor  = integ$value_divisor
    )
  }
  
  
  output$dl_econ_word <- downloadHandler(
    filename = function() paste0("econ_outcomes_", econ$country_code, "_", Sys.Date(), ".docx"),
    content  = function(file) {
      req(econ$data, econ$analysis, econ$country_code)
      withProgress(message="Generating economic outcomes Word report...", value=0, {
        incProgress(0.2, detail="Filtering data...")
        exp_data <- econ_get_export_data()
        incProgress(0.4, detail="Regenerating plots...")
        regen    <- econ_regenerate_plots(exp_data)
        incProgress(0.6, detail="Creating report...")
        filter_desc  <- get_filter_description(econ_filters$active)
        filters_text <- if (filter_desc == "No filters applied") "" else paste0("Applied Filters: ", filter_desc)
        ok <- generate_econ_word_report(
          filtered_data     = exp_data,
          filtered_analysis = regen,
          country_code      = econ$country_code,
          output_file       = file,
          filters_text      = filters_text
        )
        output$export_status <- renderText(if (ok) "Economic Word report generated!" else "Error generating Word report.")
      })
    }
  )
  
  output$dl_econ_zip <- downloadHandler(
    filename = function() paste0("econ_figures_", econ$country_code, "_", format(Sys.Date(), "%Y%m%d"), ".zip"),
    content  = function(file) {
      req(econ$data, econ$analysis, econ$country_code)
      withProgress(message="Creating economic figures ZIP...", value=0, {
        incProgress(0.1, detail="Filtering data...")
        exp_data  <- econ_get_export_data()
        incProgress(0.2, detail="Regenerating plots...")
        regen     <- econ_regenerate_plots(exp_data)
        temp_dir  <- tempfile(); dir.create(temp_dir)
        cc        <- econ$country_code
        plot_list <- list(
          list(regen$market_size_n,                        "market_size_n",        10, 7),
          list(regen$market_size_v,                        "market_size_v",        10, 7),
          list(regen$market_size_av,                       "market_size_av",       10, 7),
          list(regen$suppliers_entrance,                   "suppliers_entrance",   12, 10),
          list(regen$unique_supp,                          "unique_supp",          12, 10),
          list(regen$rel_tot,                              "rel_tot",              10, 7),
          list(regen$rel_year,                             "rel_year",             10, 7),
          list(regen$rel_10,                               "rel_10",               10, 7),
          list(regen$rel_buy,                              "rel_buy",              10, 7),
          list(regen$single_bid_overall,            "single_bid_overall",   10, 6),
          list(regen$single_bid_by_procedure,       "single_bid_procedure", 10, 7),
          list(regen$single_bid_by_price,           "single_bid_price",     10, 7),
          list(regen$single_bid_by_buyer_group,     "single_bid_buyer_grp", 10, 7),
          list(regen$single_bid_by_market,          "single_bid_market",    10, 9),
          list(regen$top_buyers_single_bid,         "top_buyers_single_bid",10, 7)
        )
        saved <- 0
        for (i in seq_along(plot_list)) {
          incProgress(0.2 + i/length(plot_list)*0.7, detail=paste("Saving figure", i))
          pl <- plot_list[[i]]
          if (!is.null(pl[[1]])) tryCatch({
            ggsave(file.path(temp_dir, paste0(pl[[2]], "_", cc, ".png")),
                   pl[[1]], width=pl[[3]], height=pl[[4]], dpi=300)
            saved <- saved + 1
          }, error=function(e) NULL)
        }
        # Also include network plots — use filtered_analysis (what's displayed), not original pipeline
        net_plots <- econ$filtered_analysis$network_plots
        if (!is.null(net_plots) && length(net_plots) > 0) {
          for (j in seq_along(net_plots)) {
            np <- net_plots[[j]]
            if (!is.null(np)) tryCatch({
              nm <- names(net_plots)[j] %||% paste0("network_", j)
              ggsave(file.path(temp_dir, paste0(nm, "_", cc, ".png")), np, width=12, height=12, dpi=300)
              saved <- saved + 1
            }, error=function(e) NULL)
          }
        }
        if (saved > 0) {
          zip::zip(zipfile=file, files=list.files(temp_dir, full.names=TRUE), mode="cherry-pick")
          output$export_status <- renderText(paste0(saved, " economic figures saved to ZIP."))
        } else {
          showNotification("No figures were saved. View the charts first, then download the ZIP.", type="warning", duration=8)
          output$export_status <- renderText("No figures saved. View charts first, then download.")
        }
        unlink(temp_dir, recursive=TRUE)
      })
    }
  )
  
  
  # ============================================================
  # ADMIN DATA OVERVIEW OUTPUTS
  # ============================================================
  
  output$admin_n_contracts <- renderValueBox({
    req(admin$filtered_data)
    valueBox(formatC(nrow(admin$filtered_data), format="d", big.mark=","),
             "Contracts", icon=icon("file-contract"), color="navy")
  })
  output$admin_n_buyers <- renderValueBox({
    req(admin$filtered_data); df <- admin$filtered_data
    n <- if ("buyer_masterid" %in% names(df)) length(unique(df$buyer_masterid)) else "N/A"
    valueBox(formatC(n, format="d", big.mark=","), "Buyers", icon=icon("building"), color="teal")
  })
  output$admin_n_suppliers <- renderValueBox({
    req(admin$filtered_data); df <- admin$filtered_data
    n <- if ("bidder_masterid" %in% names(df)) length(unique(df$bidder_masterid)) else "N/A"
    valueBox(formatC(n, format="d", big.mark=","), "Suppliers", icon=icon("truck"), color="olive")
  })
  output$admin_n_years <- renderValueBox({
    req(admin$filtered_data); df <- admin$filtered_data
    years <- if ("tender_year" %in% names(df)) unique(df$tender_year[!is.na(df$tender_year)]) else NA
    yr    <- if (length(years) > 0) paste(min(years), "-", max(years)) else "N/A"
    valueBox(yr, "Period", icon=icon("calendar"), color="navy")
  })
  
  output$admin_contracts_year_plot <- renderPlotly({
    req(admin$filtered_data)
    df <- admin$filtered_data
    req("tender_year" %in% names(df))
    year_counts <- df %>% dplyr::count(tender_year, name="n_observations")
    p <- ggplot(year_counts, aes(x=tender_year, y=n_observations)) +
      geom_col(fill="#3c8dbc") +
      # bar labels removed — use hover
      
      labs(x="Year", y="Number of Contracts", title="Contracts per Year") +
      pa_theme() + scale_y_continuous(labels=scales::comma)
    p_out <- ggplotly(p, tooltip=c("x","y")) %>% layout(hoverlabel=list(bgcolor="white"), hovermode="closest")
    admin$fig_contracts_year <- p_out %>% pa_config(); admin$fig_contracts_year
  })
  
  output$admin_summary_table <- DT::renderDataTable({
    req(admin$filtered_data); df <- admin$filtered_data
    rows <- list(data.frame(Metric="Total Contracts", Value=format(nrow(df), big.mark=",")))
    if ("buyer_masterid"  %in% names(df)) rows[[length(rows)+1]] <- data.frame(Metric="Unique Buyers",  Value=format(dplyr::n_distinct(df$buyer_masterid),  big.mark=","))
    if ("bidder_masterid" %in% names(df)) rows[[length(rows)+1]] <- data.frame(Metric="Unique Bidders", Value=format(dplyr::n_distinct(df$bidder_masterid), big.mark=","))
    if ("tender_year"     %in% names(df)) {
      years <- sort(unique(df$tender_year[!is.na(df$tender_year)]))
      if (length(years) > 0) {
        rows[[length(rows)+1]] <- data.frame(Metric="Year Range",  Value=paste(min(years), "-", max(years)))
        rows[[length(rows)+1]] <- data.frame(Metric="Total Years", Value=as.character(length(years)))
      }
    }
    datatable(do.call(rbind, rows), options=list(pageLength=20, dom="t"), rownames=FALSE)
  })
  
  # ============================================================
  # ADMIN PROCEDURE TYPES OUTPUTS
  # ============================================================
  
  # Cached — called once, used by both value and count plots
  admin_proc_share <- reactive({
    req(admin$filtered_data)
    tryCatch(build_proc_share_data(admin$filtered_data), error = function(e) NULL)
  })
  
  output$procedure_share_value_plot <- renderPlotly({
    plot_data <- admin_proc_share(); req(!is.null(plot_data))
    tryCatch({
      p <- ggplot2::ggplot(plot_data,
                           ggplot2::aes(x=stats::reorder(tender_proceduretype, share_value), y=share_value,
                                        text=paste0(tender_proceduretype,"<br>",
                                                    scales::percent(share_value,accuracy=0.1)," (",
                                                    scales::dollar(total_value,scale=1e-6,suffix="M"),")"))) +
        ggplot2::geom_col(fill="#3c8dbc", width=0.6) +
        ggplot2::scale_y_continuous(labels=scales::percent_format(accuracy=1), expand=ggplot2::expansion(mult=c(0,0.4))) +
        ggplot2::coord_flip() +
        ggplot2::labs( x=NULL, y="Share of total value") +
        pa_theme()
      admin$gg_proc_share_value <- p
      p_out <- ggplotly(p, tooltip="text") %>% layout(font=list(size=11), showlegend=FALSE)
      admin$fig_proc_share_value <- p_out %>% pa_config(); admin$fig_proc_share_value
    }, error = function(e) .empty_plotly(paste("Not available:", e$message)))
  })
  
  output$procedure_share_count_plot <- renderPlotly({
    plot_data <- admin_proc_share(); req(!is.null(plot_data))
    tryCatch({
      p <- ggplot2::ggplot(plot_data,
                           ggplot2::aes(x=stats::reorder(tender_proceduretype, share_value), y=share_contracts,
                                        text=paste0(tender_proceduretype,"<br>",
                                                    scales::percent(share_contracts,accuracy=0.1)," (",n_contracts," contracts)"))) +
        ggplot2::geom_col(fill="#3c8dbc", width=0.6) +
        ggplot2::scale_y_continuous(labels=scales::percent_format(accuracy=1), expand=ggplot2::expansion(mult=c(0,0.4))) +
        ggplot2::coord_flip() +
        ggplot2::labs( x=NULL, y="Share of contracts") +
        pa_theme()
      admin$gg_proc_share_count <- p
      p_out <- ggplotly(p, tooltip="text") %>% layout(font=list(size=11), showlegend=FALSE)
      admin$fig_proc_share_count <- p_out %>% pa_config(); admin$fig_proc_share_count
    }, error = function(e) .empty_plotly(paste("Not available:", e$message)))
  })
  
  output$proc_value_dist_plot <- renderPlotly({
    req(admin$filtered_data)
    use_local <- has_any_price_threshold(admin$price_thresholds) && "bid_price" %in% names(admin$filtered_data)
    price_col    <- if (use_local) "bid_price" else "bid_priceusd"
    currency_lbl <- if (use_local) "local currency" else "USD"
    df <- admin$filtered_data %>%
      dplyr::mutate(proc_label=recode_procedure_type(tender_proceduretype), supply_grp=classify_supply(.)) %>%
      tidyr::drop_na(proc_label) %>%
      dplyr::filter(!is.na(.data[[price_col]]), .data[[price_col]] > 1)
    selected_procs <- input$proc_value_dist_procs
    if (!is.null(selected_procs) && length(selected_procs) > 0)
      df <- df %>% dplyr::filter(proc_label %in% selected_procs)
    req(nrow(df) > 0)
    df       <- df %>% dplyr::mutate(log_val=log10(.data[[price_col]]))
    x_min    <- floor(min(df$log_val,   na.rm=TRUE))
    x_max    <- ceiling(max(df$log_val, na.rm=TRUE))
    bin_size <- 0.1
    proc_colors <- c("Open Procedure"="#2980b9","Restricted Procedure"="#e67e22",
                     "Negotiated with publications"="#8e44ad","Negotiated without publications"="#c0392b",
                     "Negotiated (unspecified)"="#d35400",
                     "Competitive Dialogue"="#16a085","Innovation Partnership"="#27ae60",
                     "Direct Award"="#7f8c8d","Other"="#bdc3c7")
    tick_vals <- seq(x_min, x_max)
    tick_text <- sapply(tick_vals, function(v) fmt_value(10^v))
    supply_order <- c("Goods","Works","Services")
    supply_types <- supply_order[supply_order %in% unique(df$supply_grp)]
    sub_figs <- lapply(seq_along(supply_types), function(i) {
      st <- supply_types[[i]]; d_st <- df %>% dplyr::filter(supply_grp==st)
      fig <- plot_ly()
      for (proc in intersect(names(proc_colors), unique(d_st$proc_label))) {
        d_proc <- d_st %>% dplyr::filter(proc_label==proc) %>% dplyr::pull(log_val)
        if (length(d_proc) < 3) next
        h   <- graphics::hist(d_proc, breaks=seq(x_min, x_max+bin_size, by=bin_size), plot=FALSE)
        bl  <- h$breaks[-length(h$breaks)]
        tip <- paste0("<b>",proc,"</b><br>~",sapply(bl, function(v) fmt_value(10^v)),"<br>",h$counts," contracts")
        fig <- fig %>% add_bars(x=bl, y=h$counts, name=proc, legendgroup=proc, showlegend=(i==1),
                                marker=list(color=proc_colors[[proc]], line=list(color="white",width=0.2)),
                                opacity=0.72, hovertext=tip, hoverinfo="text", width=bin_size*0.9)
      }
      fig %>% layout(barmode="overlay",
                     annotations=list(list(text=paste0("<b>",st,"</b>"),x=0.5,xref="paper",y=1.08,yref="paper",
                                           xanchor="center",yanchor="bottom",showarrow=FALSE,font=list(size=13))),
                     xaxis=list(title="",tickvals=tick_vals,ticktext=tick_text,showgrid=TRUE,gridcolor="#ecf0f1"),
                     yaxis=list(title=if(i==1)"Number of contracts"else""))
    })
    p_out <- subplot(sub_figs, nrows=1, shareY=FALSE, titleX=FALSE, titleY=TRUE, margin=0.06) %>%
      layout(barmode="overlay",
             xaxis=list(title=paste0("Contract value (",currency_lbl,", log scale)")),
             legend=list(orientation="h",x=0,y=-0.28),
             hovermode="closest", hoverlabel=list(bgcolor="white",font=list(size=12)))
    admin$fig_proc_value_dist <- p_out %>% pa_config(); admin$fig_proc_value_dist
  })
  
  # ── Bunching analysis ────────────────────────────────────────────────
  output$bunching_status_ui <- renderUI({
    if (!has_any_price_threshold(admin$price_thresholds))
      div(class="alert alert-info", style="margin-bottom:12px;", icon("info-circle"),
          " No contract value thresholds are active. For BG data they are pre-filled automatically.",
          " For other countries, enter thresholds in ",
          strong("Configuration \u2192 Section C"), " and click ", strong("Apply Thresholds"), ".")
  })
  
  output$bunching_analysis_plot <- renderPlotly({
    req(admin$filtered_data)
    pt <- admin$price_thresholds
    req(has_any_price_threshold(pt))
    req("bid_price" %in% names(admin$filtered_data))
    proc_label_map <- c(open="Open Procedure",restricted="Restricted Procedure",
                        neg_pub="Negotiated with publications",neg_nopub="Negotiated without publications",
                        neg="Negotiated Procedure",competitive="Competitive Dialogue",
                        innov="Innovation Partnership",direct="Direct Award",other="Other")
    supply_map <- c(goods="Goods",works="Works",services="Services")
    all_thr <- list()
    for (pk in names(pt)) for (sk in names(pt[[pk]])) {
      v <- pt[[pk]][[sk]]
      if (!is.null(v) && !is.na(v) && is.finite(v) && v > 0 && pk %in% names(proc_label_map) && sk %in% names(supply_map)) {
        key <- paste0(sk,"_",round(v))
        if (is.null(all_thr[[key]])) all_thr[[key]] <- list(supply_label=supply_map[[sk]],threshold=v,log_thr=log10(v),proc_labels=proc_label_map[[pk]])
        else all_thr[[key]]$proc_labels <- paste0(all_thr[[key]]$proc_labels,", ",proc_label_map[[pk]])
      }
    }
    panels <- unname(all_thr); req(length(panels) > 0)
    df_all <- admin$filtered_data %>%
      dplyr::mutate(supply_grp=classify_supply(.)) %>%
      dplyr::filter(!is.na(bid_price), bid_price > 1) %>%
      dplyr::mutate(log_val=log10(bid_price))
    sensitivity <- (input$spike_sensitivity %||% 50) / 100
    bin_size    <- 0.05; n_bins <- input$n_search_bins %||% 10
    excl_win    <- n_bins * bin_size; show_win <- max(2.0, excl_win + 1.0)
    sub_figs <- lapply(seq_along(panels), function(i) {
      pn      <- panels[[i]]
      d_win   <- df_all %>% dplyr::filter(supply_grp==pn$supply_label, log_val>=pn$log_thr-show_win, log_val<=pn$log_thr+show_win)
      if (nrow(d_win) < 15) return(plot_ly() %>% layout(xaxis=list(visible=FALSE),yaxis=list(visible=FALSE),
                                                        annotations=list(list(text=paste0("<b>",pn$supply_label,"</b> \u2014 Insufficient data"),x=0.5,y=0.5,xref="paper",yref="paper",showarrow=FALSE,font=list(size=10)))))
      breaks  <- seq(pn$log_thr-show_win, pn$log_thr+show_win+bin_size, by=bin_size)
      h       <- graphics::hist(d_win$log_val, breaks=breaks, plot=FALSE)
      bin_lo  <- h$breaks[-length(h$breaks)]; bin_mid <- bin_lo + bin_size/2; counts <- h$counts
      excl    <- abs(bin_mid - pn$log_thr) <= excl_win
      fit_df  <- data.frame(x=bin_mid[!excl], y=counts[!excl])
      pred    <- rep(NA_real_, length(bin_mid))
      if (nrow(fit_df) >= 8) {
        fit <- tryCatch(lm(y ~ poly(x,4), data=fit_df), error=function(e) NULL)
        if (!is.null(fit)) {
          pred <- pmax(as.numeric(predict(fit, newdata=data.frame(x=bin_mid))), 0)
          pred[bin_mid < min(fit_df$x) | bin_mid > max(fit_df$x)] <- NA_real_
        }
      }
      below_win  <- bin_mid < pn$log_thr & bin_mid >= (pn$log_thr - excl_win)
      is_bunch   <- below_win & !is.na(pred) & pred > 0 & counts > pred * (1 + sensitivity)
      bar_colors <- dplyr::case_when(is_bunch~"#e74c3c", below_win~"#f0b27a", TRUE~"#5dade2")
      has_data   <- counts > 0
      pred_disp  <- pred
      if (any(has_data)) pred_disp[bin_lo < min(bin_lo[has_data]) | bin_lo > max(bin_lo[has_data])] <- NA_real_
      tick_seq <- seq(ceiling((pn$log_thr-show_win)/0.5)*0.5, floor((pn$log_thr+show_win)/0.5)*0.5, by=0.5)
      tick_seq <- sort(c(tick_seq[abs(tick_seq-pn$log_thr)>=0.20], pn$log_thr))
      tick_pos <- tick_seq[tick_seq>=pn$log_thr-show_win & tick_seq<=pn$log_thr+show_win]
      tick_lbl <- sapply(tick_pos, function(v) if(abs(v-pn$log_thr)<0.001) paste0(fmt_value(10^v)," \u2605") else fmt_value(10^v))
      hover_tip <- paste0(sapply(bin_lo,fmt_value_log),": <b>",counts," contracts</b>",
                          ifelse(!is.na(pred),paste0("<br>Expected: ",round(pred)," | Diff: ",ifelse(counts-round(pred)>=0,"+",""),counts-round(pred)),""),
                          ifelse(is_bunch,paste0("<br><b>\u26a0 Exceeds expected by \u2265",round(sensitivity*100),"%</b>"),""))
      fig <- plot_ly()
      if (any(!is.na(pred_disp)))
        fig <- fig %>% add_lines(x=bin_lo, y=pred_disp, name="Expected (counterfactual)", legendgroup="cf",
                                 showlegend=(i==1), line=list(color="#1c2833",width=2,dash="dot"),
                                 connectgaps=FALSE, hoverinfo="skip", inherit=FALSE)
      fig <- fig %>% add_bars(x=bin_lo, y=counts, name="All contracts", legendgroup="bars", showlegend=(i==1),
                              marker=list(color=bar_colors, line=list(color="white",width=0.3)),
                              opacity=0.85, hovertext=hover_tip, hoverinfo="text", width=bin_size*0.92)
      fig %>% layout(barmode="overlay",
                     shapes=list(list(type="rect",xref="x",yref="paper",x0=pn$log_thr-excl_win,x1=pn$log_thr,y0=0,y1=1,fillcolor="#e74c3c",opacity=0.05,line=list(width=0)),
                                 list(type="line",xref="x",yref="paper",x0=pn$log_thr,x1=pn$log_thr,y0=0,y1=1,line=list(color="#922b21",width=2.5,dash="solid"))),
                     annotations=list(list(text=paste0("<b>",pn$supply_label,"</b> | Threshold: <b>",fmt_value(pn$threshold),"</b>"),
                                           x=pn$log_thr+0.04,y=0.98,xref="x",yref="paper",xanchor="left",yanchor="top",showarrow=FALSE,
                                           font=list(size=10,color="#922b21"),bgcolor="rgba(255,255,255,0.85)",borderpad=2)),
                     xaxis=list(tickvals=tick_pos,ticktext=tick_lbl,tickfont=list(size=9),showgrid=TRUE,gridcolor="#ecf0f1",range=c(pn$log_thr-show_win,pn$log_thr+show_win)),
                     yaxis=list(tickfont=list(size=9)))
    })
    n_panels <- length(sub_figs); n_cols <- min(3,n_panels); n_rows <- ceiling(n_panels/n_cols)
    if (n_rows * n_cols > n_panels) {
      blank   <- plot_ly() %>% layout(xaxis=list(visible=FALSE),yaxis=list(visible=FALSE),paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
      sub_figs <- c(sub_figs, rep(list(blank), n_rows*n_cols-n_panels))
    }
    p_out <- subplot(sub_figs, nrows=n_rows, shareY=FALSE, titleX=TRUE, titleY=FALSE,
                     widths=rep(1/n_cols,n_cols), margin=c(0.06,0.06,0.12,0.04)) %>%
      layout(barmode="overlay", hovermode="closest", hoverlabel=list(bgcolor="white",font=list(size=12)),
             legend=list(orientation="h",x=0.5,xanchor="center",y=-0.08,yanchor="top",itemsizing="constant",
                         bgcolor="rgba(255,255,255,0.9)",bordercolor="#cccccc",borderwidth=1),
             margin=list(t=30,b=20,l=70,r=20))
    admin$bunching_fig <- p_out %>% pa_config(); admin$bunching_fig
  })
  
  output$bunching_analysis_plot_ui <- renderUI({
    pt <- admin$price_thresholds
    if (!has_any_price_threshold(pt)) return(plotlyOutput("bunching_analysis_plot", height="80px"))
    supply_map <- c(goods="Goods",works="Works",services="Services")
    seen <- character(0)
    for (pk in names(pt)) for (sk in names(pt[[pk]])) {
      v <- pt[[pk]][[sk]]
      if (!is.null(v) && !is.na(v) && is.finite(v) && v > 0 && sk %in% names(supply_map))
        seen <- union(seen, paste0(sk,"_",round(v)))
    }
    n_panels <- length(seen); n_cols <- min(3, max(n_panels,1)); n_rows <- ceiling(max(n_panels,1)/n_cols)
    plotlyOutput("bunching_analysis_plot", height=paste0(max(320, n_rows*320),"px"))
  })
  
  output$dl_bunching <- downloadHandler(
    filename = function() paste0("bunching_analysis_", admin$country_code, "_", format(Sys.Date(),"%Y%m%d"), ".png"),
    content  = function(file) {
      req(admin$bunching_fig)
      # Match the display height: n_rows * 320px, same logic as bunching_analysis_plot_ui
      pt <- isolate(admin$price_thresholds)
      if (has_any_price_threshold(pt)) {
        supply_map <- c(goods="Goods", works="Works", services="Services")
        seen <- character(0)
        for (pk in names(pt)) for (sk in names(pt[[pk]])) {
          v <- pt[[pk]][[sk]]
          if (!is.null(v) && !is.na(v) && is.finite(v) && v > 0 && sk %in% names(supply_map))
            seen <- union(seen, paste0(sk, "_", round(v)))
        }
        n_panels <- length(seen); n_cols <- min(3, max(n_panels, 1)); n_rows <- ceiling(max(n_panels,1) / n_cols)
        vh <- max(500, n_rows * 450)   # slightly taller than display for better proportions
      } else {
        vh <- 500
      }
      .save_fig_png(admin$bunching_fig, file, vw = 1400, vh = vh)
    }
  )
  
  # ============================================================
  # ADMIN SUBMISSION PERIODS OUTPUTS
  # ============================================================
  
  output$submission_dist_plot <- renderPlotly({
    req(admin$filtered_data)
    tp   <- compute_tender_days(admin$filtered_data, tender_publications_firstcallfortenderdate, tender_biddeadline, tender_days_open)
    days <- tp$tender_days_open[!is.na(tp$tender_days_open) & tp$tender_days_open>=0 & tp$tender_days_open<=365]
    q    <- quantile(days,probs=c(0.25,0.5,0.75),na.rm=TRUE); mu <- mean(days,na.rm=TRUE)
    p    <- plot_ly() %>%
      add_histogram(x=~days, xbins=list(start=0,end=365,size=5),
                    marker=list(color=PA_NORMAL,line=list(color="white",width=0.5)),
                    name="Contracts", hovertemplate="%{x} days: %{y} contracts<extra></extra>")
    y_max <- max(graphics::hist(days, breaks=seq(0,370,5), plot=FALSE)$counts)*1.05
    q_labels <- c("Q1 (25th)","Median (50th)","Q3 (75th)")
    q_colors <- c(PA_Q_Q1,PA_Q_MEDIAN,PA_Q_Q1); q_dash <- c("dash","solid","dash")
    for (i in seq_along(q))
      p <- p %>% add_segments(x=q[i],xend=q[i],y=0,yend=y_max,
                              line=list(color=q_colors[i],width=2,dash=q_dash[i]),name=q_labels[i],showlegend=TRUE,
                              hovertemplate=paste0("<b>",q_labels[i],"</b><br>",round(q[i],1)," days<extra></extra>"))
    p <- p %>% add_segments(x=mu,xend=mu,y=0,yend=y_max,
                            line=list(color=PA_Q_MEAN,width=2,dash="dot"),name="Mean",
                            hovertemplate=paste0("<b>Mean</b><br>",round(mu,1)," days<extra></extra>"))
    p_out <- p %>% layout(
      xaxis=list(title="Days from call opening to bid deadline",range=c(0,365)),
      yaxis=list(title="Number of contracts"),hovermode="x unified",
      legend=list(orientation="h",y=-0.15),bargap=0.05)
    admin$fig_subm_dist <- p_out %>% pa_config()
    admin$gg_subm_dist <- tryCatch({
      df_hist <- data.frame(days=days)
      ggplot2::ggplot(df_hist, ggplot2::aes(x=days)) +
        ggplot2::geom_histogram(binwidth=5, fill=PA_NORMAL, colour="white") +
        ggplot2::geom_vline(xintercept=q, colour=c(PA_Q_Q1,PA_Q_MEDIAN,PA_Q_Q1),
                            linetype=c("dashed","solid","dashed"), linewidth=1) +
        ggplot2::geom_vline(xintercept=mu, colour=PA_Q_MEAN, linetype="dotted", linewidth=1) +
        ggplot2::coord_cartesian(xlim=c(0,365)) +
        ggplot2::labs(
          x="Days from call opening to bid deadline", y="Number of contracts") +
        pa_theme()
    }, error=function(e) NULL)
    admin$fig_subm_dist
  })
  
  output$submission_proc_plot <- renderPlotly({
    req(admin$filtered_data)
    sel_procs <- if (length(input$subm_proc_filter) == 0) "Open Procedure" else input$subm_proc_filter
    tp <- compute_tender_days(admin$filtered_data,
                              tender_publications_firstcallfortenderdate, tender_biddeadline, tender_days_open) %>%
      dplyr::mutate(tender_proceduretype=recode_procedure_type(tender_proceduretype)) %>%
      tidyr::drop_na(tender_proceduretype) %>%
      dplyr::filter(tender_days_open >= 0, tender_days_open <= 365,
                    tender_proceduretype %in% sel_procs)
    req(nrow(tp) > 0)
    procs <- tp %>%
      dplyr::group_by(tender_proceduretype) %>% dplyr::filter(dplyr::n() >= 5) %>%
      dplyr::summarise(med = median(tender_days_open, na.rm=TRUE), .groups="drop") %>%
      dplyr::arrange(med) %>% dplyr::pull(tender_proceduretype)
    req(length(procs) > 0)
    tp <- tp %>% dplyr::filter(tender_proceduretype %in% procs)
    
    n_proc <- length(procs)
    ncols  <- min(3L, n_proc)
    nrows  <- ceiling(n_proc / ncols)
    subplot_figs <- lapply(procs, function(proc) {
      d    <- tp %>% dplyr::filter(tender_proceduretype == proc) %>% dplyr::pull(tender_days_open)
      q    <- quantile(d, c(0.25, 0.5, 0.75), na.rm=TRUE)
      mu   <- mean(d, na.rm=TRUE)
      x_max <- min(365, quantile(d, 0.99, na.rm=TRUE) * 1.1)
      x_max <- max(x_max, 15)
      # tight bins: 2 days for submission
      binw  <- max(1, min(2, round(x_max / 60)))
      bks   <- seq(0, x_max + binw, by=binw)
      cnts  <- tryCatch(graphics::hist(d[d <= x_max], breaks=bks, plot=FALSE)$counts, error=function(e) c(1))
      y_top <- max(cnts, 1) * 1.15
      plotly::plot_ly() %>%
        plotly::add_histogram(
          x = ~d[d <= x_max],
          xbins = list(start=0, end=x_max + binw, size=binw),
          marker = list(color=PA_NORMAL, line=list(color="white", width=0.4)),
          hovertemplate = "%{x} days: %{y} contracts<extra></extra>",
          showlegend = FALSE
        ) %>%
        plotly::add_segments(x=q[1],xend=q[1],y=0,yend=y_top,
                             line=list(color=PA_Q_Q1,width=1.8,dash="dash"),
                             hovertemplate=paste0("Q1: ",round(q[1],1)," days<extra></extra>"),showlegend=FALSE) %>%
        plotly::add_segments(x=q[2],xend=q[2],y=0,yend=y_top,
                             line=list(color=PA_Q_MEDIAN,width=2.2,dash="solid"),
                             hovertemplate=paste0("Median: ",round(q[2],1)," days<extra></extra>"),showlegend=FALSE) %>%
        plotly::add_segments(x=q[3],xend=q[3],y=0,yend=y_top,
                             line=list(color=PA_Q_Q1,width=1.8,dash="dash"),
                             hovertemplate=paste0("Q3: ",round(q[3],1)," days<extra></extra>"),showlegend=FALSE) %>%
        plotly::add_segments(x=mu,xend=mu,y=0,yend=y_top,
                             line=list(color=PA_Q_MEAN,width=1.5,dash="dot"),
                             hovertemplate=paste0("Mean: ",round(mu,1)," days<extra></extra>"),showlegend=FALSE) %>%
        plotly::layout(
          annotations=list(list(text=paste0("<b>",proc,"</b>"),
                                x=0.5,y=1.05,xref="paper",yref="paper",
                                xanchor="center",yanchor="bottom",showarrow=FALSE,
                                font=list(size=12,color="#222"))),
          xaxis=list(range=c(0,x_max+binw),tickfont=list(size=11),title=NULL,gridcolor="#eeeeee"),
          yaxis=list(tickfont=list(size=11),title=NULL,rangemode="nonnegative")
        )
    })
    fig <- plotly::subplot(subplot_figs, nrows=nrows, shareX=FALSE, shareY=FALSE,
                           titleX=FALSE, titleY=FALSE, margin=c(0.04,0.04,0.10,0.06))
    fig <- fig %>% plotly::layout(
      font        = list(size=12),
      hoverlabel  = list(bgcolor="white", font=list(size=12)),
      hovermode   = "closest",
      margin      = list(l=40, r=20, t=30, b=50),
      plot_bgcolor  = "#ffffff",
      paper_bgcolor = "#ffffff",
      annotations = list(list(
        text = paste0(
          "<span style='color:",PA_Q_Q1,";'>── Q1/Q3</span>  ",
          "<span style='color:",PA_Q_MEDIAN,";'>—— Median</span>  ",
          "<span style='color:",PA_Q_MEAN,";'>·· Mean</span>"
        ),
        x=0.5, y=-0.06, xref="paper", yref="paper",
        xanchor="center", yanchor="top", showarrow=FALSE,
        font=list(size=12)
      ))
    )
    admin$fig_subm_proc <- fig %>% pa_config(); admin$fig_subm_proc
  })
  
  # ── Submission share summary chart ────────────────────────────────────
  output$subm_share_chart <- renderPlotly({
    req(admin$filtered_data, admin$thresholds)
    sel_procs <- if (length(input$subm_proc_filter) == 0) "Open Procedure" else input$subm_proc_filter
    cutoffs   <- admin_subm_cutoffs()
    tp_base   <- admin_subm_open_data() %>%
      dplyr::filter(tender_proceduretype %in% sel_procs)
    req(nrow(tp_base) > 0)
    tp_flagged <- tp_base %>%
      dplyr::left_join(cutoffs, by="tender_proceduretype") %>%
      dplyr::mutate(status = dplyr::case_when(
        tender_days_open < short_cut ~ "Short",
        !no_medium & !is.na(med_min) & !is.na(med_max) &
          tender_days_open >= med_min & tender_days_open <= med_max ~ "Medium",
        TRUE ~ "Normal"
      ), status = factor(status, levels=c("Short","Medium","Normal")))
    share_df <- tp_flagged %>%
      dplyr::group_by(tender_proceduretype) %>%
      dplyr::summarise(
        n_total  = dplyr::n(),
        n_short  = sum(status == "Short",  na.rm=TRUE),
        n_medium = sum(status == "Medium", na.rm=TRUE),
        n_normal = sum(status == "Normal", na.rm=TRUE),
        .groups  = "drop"
      ) %>%
      dplyr::mutate(
        pct_short  = n_short  / n_total,
        pct_medium = n_medium / n_total,
        pct_normal = n_normal / n_total,
        proc_label = paste0(tender_proceduretype, "  (n = ", scales::comma(n_total), ")")
      ) %>%
      dplyr::arrange(dplyr::desc(pct_short))
    
    # Show Medium bar only if at least one procedure actually has medium band active
    has_medium <- any(!cutoffs$no_medium[cutoffs$tender_proceduretype %in% sel_procs], na.rm=TRUE) &&
      any(tp_flagged$status == "Medium", na.rm=TRUE)
    
    n_procs     <- nrow(share_df)
    chart_h_px  <- max(120, n_procs * 38 + 60)  # dynamic height hint (unused in px but guides margin)
    
    fig <- plotly::plot_ly(data = share_df, y = ~proc_label, orientation = "h") %>%
      plotly::add_bars(x = ~pct_short,  name = "Short",
                       marker = list(color = PA_FLAG),
                       hovertemplate = paste0("<b>%{y}</b><br>Short: %{x:.1%}<extra></extra>"))
    if (has_medium)
      fig <- fig %>% plotly::add_bars(x = ~pct_medium, name = "Medium",
                                      marker = list(color = PA_AMBER),
                                      hovertemplate = paste0("<b>%{y}</b><br>Medium: %{x:.1%}<extra></extra>"))
    fig <- fig %>%
      plotly::add_bars(x = ~pct_normal, name = "Normal",
                       marker = list(color = PA_NORMAL),
                       hovertemplate = paste0("<b>%{y}</b><br>Normal: %{x:.1%}<extra></extra>")) %>%
      plotly::layout(
        barmode     = "stack",
        xaxis       = list(title = list(text = "Share of contracts", font = list(size = 13)),
                           tickformat = ".0%", range = c(0, 1), gridcolor = "#eeeeee",
                           tickfont = list(size = 13)),
        yaxis       = list(title = "", automargin = TRUE, tickfont = list(size = 13)),
        legend      = list(orientation = "h", yanchor = "bottom", y = 1.02,
                           xanchor = "center", x = 0.5, font = list(size = 13)),
        hovermode   = "y unified",
        margin      = list(l = 10, r = 20, t = 50, b = 40),
        font        = list(size = 13),
        plot_bgcolor  = "#ffffff",
        paper_bgcolor = "#ffffff"
      )
    admin$fig_subm_share <- fig %>% pa_config()
    admin$fig_subm_share
  })
  
  output$submission_short_plot <- renderPlotly({
    req(admin$filtered_data, admin$thresholds)
    cutoffs  <- admin_subm_cutoffs()
    sel_procs <- if (length(input$subm_proc_filter) == 0) "Open Procedure" else input$subm_proc_filter
    tp_base  <- admin_subm_open_data() %>%
      dplyr::filter(tender_proceduretype %in% sel_procs)
    req(nrow(tp_base) > 0)
    tp_flagged <- tp_base %>%
      dplyr::left_join(cutoffs, by="tender_proceduretype") %>%
      dplyr::mutate(status = dplyr::case_when(
        tender_days_open < short_cut ~ "Short",
        !no_medium & !is.na(med_min) & !is.na(med_max) &
          tender_days_open >= med_min & tender_days_open <= med_max ~ "Medium",
        TRUE ~ "Normal"
      ), status = factor(status, levels=c("Short","Medium","Normal")))
    
    procs_s <- sort(unique(tp_flagged$tender_proceduretype))
    req(length(procs_s) > 0)
    n_ps    <- length(procs_s)
    ncols_s <- min(3L, n_ps)
    nrows_s <- ceiling(n_ps / ncols_s)
    
    col_map <- c(Short=PA_FLAG, Medium=PA_AMBER, Normal=PA_NORMAL)
    
    # Per-procedure histogram with red shaded zone for the "short" region.
    # x-axis auto-ranges per procedure — works even when days cluster at 3-30.
    sub_figs_s <- lapply(procs_s, function(proc) {
      d_proc  <- tp_flagged %>% dplyr::filter(tender_proceduretype == proc)
      thr_val <- dplyr::first(na.omit(dplyr::pull(
        dplyr::filter(cutoffs, tender_proceduretype == proc), short_cut)))
      if (length(thr_val) == 0) thr_val <- NA_real_
      pct_s   <- mean(d_proc$status == "Short", na.rm=TRUE)
      n_tot   <- nrow(d_proc)
      x_max   <- max(d_proc$tender_days_open, na.rm=TRUE)
      x_max   <- max(x_max, if (!is.na(thr_val)) thr_val * 3 else 30, 15)
      # Hard cap at 1-2 day bins for submission periods
      binw    <- min(2, max(1, round(x_max / 60)))
      
      traces <- plotly::plot_ly()
      
      for (st in c("Normal","Medium","Short")) {
        d_st <- d_proc %>% dplyr::filter(status == st) %>% dplyr::pull(tender_days_open)
        if (length(d_st) == 0) next
        traces <- traces %>% plotly::add_histogram(
          x = d_st,
          xbins = list(start=0, end=x_max + binw, size=binw),
          name  = st, legendgroup = st, showlegend = FALSE,
          marker = list(color=col_map[[st]], line=list(color="white", width=0.3)),
          hovertemplate = paste0("<b>", st, "</b>: %{y} contracts<extra></extra>")
        )
      }
      
      shape_list <- list()
      if (!is.na(thr_val)) {
        shape_list <- list(list(
          type="line", x0=thr_val, x1=thr_val, y0=0, y1=1,
          xref="x", yref="paper",
          line=list(color="#cc0000", width=1.8, dash="dash")
        ), list(
          type="rect", x0=0, x1=thr_val, y0=0, y1=1,
          xref="x", yref="paper",
          fillcolor="rgba(220,50,50,0.07)", line=list(width=0)
        ))
      }
      
      sub_txt <- paste0(
        if (!is.na(thr_val)) paste0("< ", round(thr_val), " days threshold  \u2502  ") else "",
        scales::percent(pct_s, accuracy=0.1), " short",
        "  (n = ", scales::comma(n_tot), ")"
      )
      
      traces %>% plotly::layout(
        barmode = "stack",
        shapes  = shape_list,
        xaxis = list(
          title      = list(text = sub_txt, font = list(size = 11, color = "#555")),
          tickfont   = list(size = 11),
          gridcolor  = "#eeeeee",
          rangemode  = "nonnegative",
          automargin = TRUE
        ),
        yaxis = list(tickfont=list(size=11), title=NULL, rangemode="nonnegative")
      )
    })
    
    row_gap_s <- if (nrows_s == 1) 0.06 else 0.14
    fig <- plotly::subplot(sub_figs_s, nrows=nrows_s, shareX=FALSE, shareY=FALSE,
                           titleX=TRUE, titleY=FALSE,
                           margin=c(0.04, 0.04, row_gap_s, 0.06))
    
    # Per-panel procedure-name titles via domain arithmetic
    col_w_s <- (1 - 0.04 * (ncols_s - 1)) / ncols_s
    row_h_s <- (1 - row_gap_s * (nrows_s - 1)) / nrows_s
    panel_ann_s <- lapply(seq_along(procs_s), function(i) {
      col_i <- ((i - 1) %% ncols_s)
      row_i <- ((i - 1) %/% ncols_s)
      x_mid <- col_i * (col_w_s + 0.04) + col_w_s / 2
      y_top <- 1 - row_i * (row_h_s + row_gap_s)
      list(text=paste0("<b>", procs_s[i], "</b>"),
           x=x_mid, y=y_top + 0.015,
           xref="paper", yref="paper",
           xanchor="center", yanchor="bottom", showarrow=FALSE,
           font=list(size=12, color="#2c3e50"))
    })
    
    has_medium_s <- any(!cutoffs$no_medium[cutoffs$tender_proceduretype %in% procs_s], na.rm=TRUE) &&
      any(tp_flagged$status == "Medium", na.rm=TRUE)
    legend_txt_s <- paste0(
      "<span style='color:", PA_FLAG,   ";'>▪ Short</span>   ",
      if (has_medium_s) paste0("<span style='color:", PA_AMBER,  ";'>▪ Medium</span>   ") else "",
      "<span style='color:", PA_NORMAL, ";'>▪ Normal</span>   ",
      "<span style='color:#cc0000;'>‒ ‒ threshold</span>"
    )
    legend_ann_s <- list(
      text = legend_txt_s,
      x=0.5, y=-0.03, xref="paper", yref="paper",
      xanchor="center", yanchor="top", showarrow=FALSE,
      font=list(size=12)
    )
    
    fig <- fig %>% plotly::layout(
      font        = list(size=12),
      hoverlabel  = list(bgcolor="white", font=list(size=12)),
      hovermode   = "closest",
      margin      = list(l=45, r=20, t=35, b=55),
      plot_bgcolor  = "#ffffff",
      paper_bgcolor = "#ffffff",
      annotations = c(panel_ann_s, list(legend_ann_s))
    )
    admin$fig_subm_short <- fig %>% pa_config(); admin$fig_subm_short
  })
  
  output$buyer_short_plot <- renderPlotly({
    req(admin$filtered_data, admin$thresholds)
    cutoffs <- admin_subm_cutoffs() %>% dplyr::select(tender_proceduretype, short_cut)
    tp_base <- apply_global_proc_filter(admin_subm_open_data())
    req(nrow(tp_base) > 0)
    tp_buyer <- tp_base %>% dplyr::left_join(cutoffs,by="tender_proceduretype") %>%
      dplyr::mutate(short_deadline=tender_days_open<short_cut, buyer_group=add_buyer_group(buyer_buyertype))
    req(nrow(tp_buyer) > 0)
    by_count <- tp_buyer %>%
      dplyr::group_by(buyer_group,tender_proceduretype) %>%
      dplyr::summarise(n_short=sum(short_deadline,na.rm=TRUE),n_total=dplyr::n(),share_short=mean(short_deadline,na.rm=TRUE),.groups="drop") %>%
      dplyr::mutate(share_other=1-share_short,
                    tip_s=paste0(buyer_group," | ",tender_proceduretype,"<br><b>Short: ",scales::percent(share_short,accuracy=0.1),"</b> (",n_short," of ",n_total," contracts)"),
                    tip_n=paste0(buyer_group," | ",tender_proceduretype,"<br>Normal: ",scales::percent(1-share_short,accuracy=0.1)),metric="Count")
    by_value <- tp_buyer %>%
      dplyr::group_by(buyer_group,tender_proceduretype) %>%
      dplyr::summarise(total_value=sum(bid_priceusd,na.rm=TRUE),short_value=sum(bid_priceusd[short_deadline %in% TRUE],na.rm=TRUE),.groups="drop") %>%
      dplyr::mutate(share_short=dplyr::if_else(total_value>0,short_value/total_value,0),share_other=1-share_short,
                    tip_s=paste0(buyer_group," | ",tender_proceduretype,"<br><b>Short: ",scales::percent(share_short,accuracy=0.1),"</b><br>",
                                 scales::dollar(short_value,scale=1e-6,suffix="M",accuracy=0.1)," of ",scales::dollar(total_value,scale=1e-6,suffix="M",accuracy=0.1)),
                    tip_n=paste0(buyer_group," | ",tender_proceduretype,"<br>Normal: ",scales::percent(1-share_short,accuracy=0.1)),metric="Contract Value")
    
    # Perceptually distinct palette — cycles through hue, avoids grey/navy clash
    PA_PROC_PAL <- c(
      "#1A6FAF",  # steel blue     (Open)
      "#2CA02C",  # green          (Restricted)
      "#D62728",  # red            (Neg w/ pub)
      "#FF7F0E",  # orange         (Neg w/o pub)
      "#9467BD",  # purple         (Neg unspec)
      "#17BECF",  # cyan           (Comp. Dialogue)
      "#E377C2",  # pink           (Innovation)
      "#8C564B",  # brown          (Direct Award)
      "#7F7F7F"   # mid-grey       (Other)
    )
    all_procs_s <- sort(unique(by_count$tender_proceduretype))
    proc_pal_s  <- setNames(PA_PROC_PAL[seq_along(all_procs_s)], all_procs_s)
    buyers_ord_s <- sort(unique(by_count$buyer_group))
    
    make_buyer_bar_short <- function(df, share_col, n_col, hover_label, show_legend = TRUE) {
      df <- df %>% dplyr::mutate(
        hover_txt = paste0(
          "<b>", buyer_group, " | ", tender_proceduretype, "</b><br>",
          hover_label, ": ", scales::percent(.data[[share_col]], accuracy=0.1), "<br>",
          "N: ", scales::comma(.data[[n_col]])
        )
      )
      fig <- plotly::plot_ly()
      procs_s2 <- sort(unique(df$tender_proceduretype))
      for (proc in procs_s2) {
        d_p <- df %>% dplyr::filter(tender_proceduretype == proc) %>%
          dplyr::arrange(match(buyer_group, buyers_ord_s))
        fig <- fig %>% plotly::add_bars(
          x           = ~buyer_group, y = ~.data[[share_col]],
          data        = d_p,
          name        = proc, legendgroup = proc,
          showlegend  = show_legend,
          marker      = list(color=proc_pal_s[[proc]], line=list(color="white",width=0.5)),
          hovertext   = ~hover_txt, hoverinfo="text"
        )
      }
      fig %>% plotly::layout(
        barmode = "group",
        xaxis   = list(title=NULL, tickfont=list(size=13), automargin=TRUE),
        yaxis   = list(title = list(text=hover_label, font=list(size=13)),
                       tickformat=".0%", range=c(0,1),
                       tickfont=list(size=13), gridcolor="#eeeeee"),
        plot_bgcolor  = "#ffffff",
        paper_bgcolor = "#ffffff",
        margin        = list(l=65, r=10, t=10, b=10)
      )
    }
    
    p_count_s <- make_buyer_bar_short(
      by_count %>% dplyr::rename(n_col=n_total),
      "share_short", "n_col", "% short by count", show_legend = TRUE)
    p_value_s <- make_buyer_bar_short(
      by_value %>% dplyr::mutate(n_col=round(total_value/1e6)),
      "share_short", "n_col", "% short by value", show_legend = FALSE)
    
    view_sel <- input$subm_buyer_view %||% "count"
    p_out <- if (view_sel == "count") {
      p_count_s %>% plotly::layout(
        hoverlabel = list(bgcolor="white", font=list(size=13)),
        font = list(size=13),
        legend = list(orientation="h", yanchor="bottom", y=1.02,
                      xanchor="center", x=0.5, font=list(size=14)),
        margin = list(l=65, r=20, t=60, b=60))
    } else if (view_sel == "value") {
      p_value_s %>% plotly::layout(
        hoverlabel = list(bgcolor="white", font=list(size=13)),
        font = list(size=13),
        legend = list(orientation="h", yanchor="bottom", y=1.02,
                      xanchor="center", x=0.5, font=list(size=14)),
        margin = list(l=65, r=20, t=60, b=60))
    } else {
      plotly::subplot(p_count_s, p_value_s, nrows=2,
                      shareX=TRUE, shareY=FALSE,
                      titleX=FALSE, titleY=TRUE, margin=0.06) %>%
        plotly::layout(
          hoverlabel = list(bgcolor="white", font=list(size=13)),
          font = list(size=13),
          legend = list(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="center", x=0.5, font=list(size=14)),
          margin = list(l=65, r=20, t=60, b=80))
    }
    admin$fig_buyer_short <- p_out %>% pa_config(); admin$fig_buyer_short
  })
  
  # ============================================================
  # ADMIN DECISION PERIODS OUTPUTS
  # ============================================================
  
  admin_decision_data <- reactive({
    req(admin$filtered_data)
    df <- admin$filtered_data
    if (!"tender_contractsignaturedate" %in% names(df))
      df <- df %>% dplyr::mutate(tender_contractsignaturedate=as.Date(NA))
    df %>%
      dplyr::mutate(decision_end_date=dplyr::coalesce(as.Date(tender_contractsignaturedate), as.Date(tender_awarddecisiondate))) %>%
      compute_tender_days(tender_biddeadline, decision_end_date, tender_days_dec)
  })
  
  admin_subm_open_data <- reactive({
    req(admin$filtered_data)
    compute_tender_days(admin$filtered_data, tender_publications_firstcallfortenderdate, tender_biddeadline, tender_days_open) %>%
      dplyr::mutate(tender_proceduretype=recode_procedure_type(tender_proceduretype)) %>%
      tidyr::drop_na(tender_proceduretype)
  })
  
  # Submission short cutoffs — shared between submission_short_plot and buyer_short_plot
  admin_subm_cutoffs <- reactive({
    req(admin$thresholds)
    tp   <- apply_global_proc_filter(admin_subm_open_data())
    thr  <- admin$thresholds
    purrr::map_dfr(sort(unique(tp$tender_proceduretype)), function(proc) {
      key       <- as.character(proc_to_key(proc)[1])
      d         <- tp %>% dplyr::filter(tender_proceduretype==proc) %>% dplyr::pull(tender_days_open)
      thr_entry <- thr$subm[[key]]
      sc <- if (!is.null(thr_entry) && !is.na(thr_entry$days)) thr_entry$days
      else compute_outlier_cutoff(d, thr_entry$outlier_method %||% "iqr")
      nm  <- if (!is.null(thr_entry)) isTRUE(thr_entry$no_medium) else TRUE
      mm  <- if (!nm && !is.null(thr_entry$medium)) thr_entry$medium$min else NA_real_
      mx  <- if (!nm && !is.null(thr_entry$medium)) thr_entry$medium$max else NA_real_
      data.frame(tender_proceduretype=proc, short_cut=sc, no_medium=nm, med_min=mm, med_max=mx,
                 stringsAsFactors=FALSE)
    })
  })
  
  # Decision long cutoffs — shared between decision_long_plot and buyer_long_plot
  admin_dec_cutoffs <- reactive({
    req(admin$thresholds)
    tp  <- admin_subm_open_data() %>%
      dplyr::filter(tender_proceduretype %in% admin$global_proc_filter)
    thr <- admin$thresholds
    purrr::map_dfr(sort(unique(tp$tender_proceduretype)), function(proc) {
      key       <- as.character(proc_to_key(proc)[1])
      d         <- tp %>% dplyr::filter(tender_proceduretype==proc) %>% dplyr::pull(tender_days_open)
      thr_entry <- thr$dec[[key]]
      lc <- if (!is.null(thr_entry) && !is.na(thr_entry$days)) thr_entry$days
      else compute_outlier_cutoff(d, thr_entry$outlier_method %||% "iqr")
      data.frame(tender_proceduretype=proc, long_cut=lc, stringsAsFactors=FALSE)
    })
  })
  
  output$decision_dist_plot <- renderPlotly({
    tp   <- admin_decision_data(); req(nrow(tp)>0)
    days <- tp$tender_days_dec[!is.na(tp$tender_days_dec) & tp$tender_days_dec>=0 & tp$tender_days_dec<=730]
    req(length(days)>0)
    q  <- quantile(days,probs=c(0.25,0.5,0.75),na.rm=TRUE); mu <- mean(days,na.rm=TRUE)
    p  <- plot_ly() %>%
      add_histogram(x=~days,xbins=list(start=0,end=730,size=10),marker=list(color=PA_NORMAL,line=list(color="white",width=0.5)),
                    name="Contracts",hovertemplate="%{x} days: %{y} contracts<extra></extra>")
    y_max <- max(graphics::hist(days,breaks=seq(0,740,10),plot=FALSE)$counts)*1.05
    q_labels <- c("Q1 (25th)","Median (50th)","Q3 (75th)")
    q_colors <- c(PA_Q_Q1,PA_Q_MEDIAN,PA_Q_Q1); q_dash <- c("dash","solid","dash")
    for (i in seq_along(q))
      p <- p %>% add_segments(x=q[i],xend=q[i],y=0,yend=y_max,line=list(color=q_colors[i],width=2,dash=q_dash[i]),name=q_labels[i],showlegend=TRUE,
                              hovertemplate=paste0("<b>",q_labels[i],"</b><br>",round(q[i],1)," days<extra></extra>"))
    p <- p %>% add_segments(x=mu,xend=mu,y=0,yend=y_max,line=list(color=PA_Q_MEAN,width=2,dash="dot"),name="Mean",
                            hovertemplate=paste0("<b>Mean</b><br>",round(mu,1)," days<extra></extra>"))
    p_out <- p %>% layout(
      xaxis=list(title="Days from bid deadline to contract award",range=c(0,730)),
      yaxis=list(title="Number of contracts"),hovermode="x unified",legend=list(orientation="h",y=-0.15),bargap=0.05)
    admin$fig_dec_dist <- p_out %>% pa_config()
    admin$gg_dec_dist <- tryCatch({
      df_hist <- data.frame(days=days)
      ggplot2::ggplot(df_hist, ggplot2::aes(x=days)) +
        ggplot2::geom_histogram(binwidth=10, fill=PA_NORMAL, colour="white") +
        ggplot2::geom_vline(xintercept=q, colour=c(PA_Q_Q1,PA_Q_MEDIAN,PA_Q_Q1),
                            linetype=c("dashed","solid","dashed"), linewidth=1) +
        ggplot2::geom_vline(xintercept=mu, colour=PA_Q_MEAN, linetype="dotted", linewidth=1) +
        ggplot2::coord_cartesian(xlim=c(0,730)) +
        ggplot2::labs(
          x="Days from bid deadline to contract award", y="Number of contracts") +
        pa_theme()
    }, error=function(e) NULL)
    admin$fig_dec_dist
  })
  
  output$decision_proc_plot <- renderPlotly({
    sel_procs <- if (length(input$dec_proc_filter) == 0) "Open Procedure" else input$dec_proc_filter
    tp <- admin_decision_data() %>%
      dplyr::filter(recode_procedure_type(tender_proceduretype) %in% sel_procs) %>%
      dplyr::mutate(tender_proceduretype=recode_procedure_type(tender_proceduretype)) %>%
      tidyr::drop_na(tender_proceduretype) %>%
      dplyr::filter(tender_days_dec >= 0, tender_days_dec <= 730)
    req(nrow(tp) > 0)
    procs <- tp %>%
      dplyr::group_by(tender_proceduretype) %>% dplyr::filter(dplyr::n() >= 5) %>%
      dplyr::summarise(med = median(tender_days_dec, na.rm=TRUE), .groups="drop") %>%
      dplyr::arrange(med) %>% dplyr::pull(tender_proceduretype)
    req(length(procs) > 0)
    tp <- tp %>% dplyr::filter(tender_proceduretype %in% procs)
    
    n_proc <- length(procs)
    ncols  <- min(3L, n_proc)
    nrows  <- ceiling(n_proc / ncols)
    subplot_figs <- lapply(procs, function(proc) {
      d   <- tp %>% dplyr::filter(tender_proceduretype == proc) %>% dplyr::pull(tender_days_dec)
      q   <- quantile(d, c(0.25, 0.5, 0.75), na.rm=TRUE)
      mu  <- mean(d, na.rm=TRUE)
      bks <- seq(0, 730, by=10)
      cnts <- graphics::hist(d, breaks=bks, plot=FALSE)$counts
      y_top <- max(cnts, 1) * 1.15
      plotly::plot_ly() %>%
        plotly::add_histogram(
          x = ~d, xbins = list(start=0, end=730, size=10),
          marker = list(color=PA_NORMAL, line=list(color="white", width=0.4)),
          hovertemplate = "%{x} days: %{y} contracts<extra></extra>",
          showlegend = FALSE
        ) %>%
        plotly::add_segments(x=q[1],xend=q[1],y=0,yend=y_top,
                             line=list(color=PA_Q_Q1,width=1.8,dash="dash"),
                             hovertemplate=paste0("Q1: ",round(q[1],1)," days<extra></extra>"),showlegend=FALSE) %>%
        plotly::add_segments(x=q[2],xend=q[2],y=0,yend=y_top,
                             line=list(color=PA_Q_MEDIAN,width=2.2,dash="solid"),
                             hovertemplate=paste0("Median: ",round(q[2],1)," days<extra></extra>"),showlegend=FALSE) %>%
        plotly::add_segments(x=q[3],xend=q[3],y=0,yend=y_top,
                             line=list(color=PA_Q_Q1,width=1.8,dash="dash"),
                             hovertemplate=paste0("Q3: ",round(q[3],1)," days<extra></extra>"),showlegend=FALSE) %>%
        plotly::add_segments(x=mu,xend=mu,y=0,yend=y_top,
                             line=list(color=PA_Q_MEAN,width=1.5,dash="dot"),
                             hovertemplate=paste0("Mean: ",round(mu,1)," days<extra></extra>"),showlegend=FALSE) %>%
        plotly::layout(
          annotations=list(list(text=paste0("<b>",proc,"</b>"),
                                x=0.5,y=1.05,xref="paper",yref="paper",
                                xanchor="center",yanchor="bottom",showarrow=FALSE,
                                font=list(size=11,color="#222"))),
          xaxis=list(range=c(0,730),tickfont=list(size=10),title=NULL,gridcolor="#eeeeee"),
          yaxis=list(tickfont=list(size=10),title=NULL,rangemode="nonnegative")
        )
    })
    fig <- plotly::subplot(subplot_figs, nrows=nrows, shareX=FALSE, shareY=FALSE,
                           titleX=FALSE, titleY=FALSE, margin=c(0.04,0.04,0.10,0.06))
    fig <- fig %>% plotly::layout(
      font        = list(size=11),
      hoverlabel  = list(bgcolor="white", font=list(size=12)),
      hovermode   = "closest",
      margin      = list(l=40, r=20, t=30, b=50),
      plot_bgcolor  = "#ffffff",
      paper_bgcolor = "#ffffff",
      annotations = list(list(
        text = paste0(
          "<span style='color:",PA_Q_Q1,";'>── Q1/Q3</span>  ",
          "<span style='color:",PA_Q_MEDIAN,";'>—— Median</span>  ",
          "<span style='color:",PA_Q_MEAN,";'>·· Mean</span>"
        ),
        x=0.5, y=-0.06, xref="paper", yref="paper",
        xanchor="center", yanchor="top", showarrow=FALSE,
        font=list(size=11)
      ))
    )
    admin$fig_dec_proc <- fig %>% pa_config(); admin$fig_dec_proc
  })
  
  # ── Decision share summary chart ──────────────────────────────────────
  output$dec_share_chart <- renderPlotly({
    req(admin$thresholds)
    sel_procs   <- if (length(input$dec_proc_filter) == 0) "Open Procedure" else input$dec_proc_filter
    cutoffs_dec <- admin_dec_cutoffs()
    tp_base     <- admin_decision_data() %>%
      dplyr::mutate(tender_proceduretype = recode_procedure_type(tender_proceduretype)) %>%
      dplyr::filter(tender_proceduretype %in% sel_procs, tender_days_dec >= 0)
    req(nrow(tp_base) > 0)
    tp_flagged <- tp_base %>%
      dplyr::left_join(cutoffs_dec, by = "tender_proceduretype") %>%
      dplyr::mutate(status = dplyr::if_else(tender_days_dec >= long_cut, "Long", "Normal"))
    share_df <- tp_flagged %>%
      dplyr::group_by(tender_proceduretype) %>%
      dplyr::summarise(
        n_total = dplyr::n(),
        n_long  = sum(status == "Long",   na.rm = TRUE),
        n_norm  = sum(status == "Normal", na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::mutate(
        pct_long   = n_long  / n_total,
        pct_normal = n_norm  / n_total,
        proc_label = paste0(tender_proceduretype, " (n=", scales::comma(n_total), ")")
      ) %>%
      dplyr::arrange(dplyr::desc(pct_long))
    
    fig <- plotly::plot_ly(data = share_df, y = ~proc_label, orientation = "h") %>%
      plotly::add_bars(x = ~pct_long,   name = "Long",
                       marker = list(color = PA_FLAG),
                       hovertemplate = paste0("<b>%{y}</b><br>Long: %{x:.1%}<extra></extra>")) %>%
      plotly::add_bars(x = ~pct_normal, name = "Normal",
                       marker = list(color = PA_NORMAL),
                       hovertemplate = paste0("<b>%{y}</b><br>Normal: %{x:.1%}<extra></extra>")) %>%
      plotly::layout(
        barmode     = "stack",
        xaxis       = list(title = list(text = "Share of contracts", font = list(size = 13)),
                           tickformat = ".0%", range = c(0, 1), gridcolor = "#eeeeee",
                           tickfont = list(size = 13)),
        yaxis       = list(title = "", automargin = TRUE, tickfont = list(size = 13)),
        legend      = list(orientation = "h", yanchor = "bottom", y = 1.02,
                           xanchor = "center", x = 0.5, font = list(size = 13)),
        hovermode   = "y unified",
        margin      = list(l = 10, r = 20, t = 50, b = 40),
        font        = list(size = 13),
        plot_bgcolor  = "#ffffff",
        paper_bgcolor = "#ffffff"
      )
    admin$fig_dec_share <- fig %>% pa_config()
    admin$fig_dec_share
  })
  
  output$decision_long_plot <- renderPlotly({
    req(admin$thresholds)
    sel_procs   <- if (length(input$dec_proc_filter) == 0) "Open Procedure" else input$dec_proc_filter
    cutoffs_dec <- admin_dec_cutoffs()
    
    # Use decision days (bid deadline → award) — NOT submission days
    tp_base <- admin_decision_data() %>%
      dplyr::mutate(tender_proceduretype = recode_procedure_type(tender_proceduretype)) %>%
      dplyr::filter(tender_proceduretype %in% sel_procs,
                    !is.na(tender_days_dec), tender_days_dec >= 0)
    req(nrow(tp_base) > 0)
    
    tp_flagged_l <- tp_base %>%
      dplyr::left_join(cutoffs_dec, by = "tender_proceduretype") %>%
      dplyr::mutate(status = factor(
        dplyr::if_else(!is.na(long_cut) & tender_days_dec >= long_cut, "Long", "Normal"),
        levels = c("Long", "Normal")
      ))
    
    procs_l <- sort(unique(tp_flagged_l$tender_proceduretype))
    req(length(procs_l) > 0)
    ncols_l <- min(3L, length(procs_l))
    nrows_l <- ceiling(length(procs_l) / ncols_l)
    
    # ── Build one subplot per procedure ───────────────────────────────────
    sub_figs_l <- lapply(procs_l, function(proc) {
      d_proc  <- tp_flagged_l %>% dplyr::filter(tender_proceduretype == proc)
      thr_val <- dplyr::first(na.omit(
        dplyr::pull(dplyr::filter(cutoffs_dec, tender_proceduretype == proc), long_cut)))
      if (length(thr_val) == 0) thr_val <- NA_real_
      
      pct_l <- mean(d_proc$status == "Long", na.rm = TRUE)
      n_tot <- nrow(d_proc)
      
      # x-range: 99th percentile, but always at least 1.5× threshold
      x_max <- quantile(d_proc$tender_days_dec, 0.99, na.rm = TRUE)
      x_max <- max(x_max, if (!is.na(thr_val)) thr_val * 1.5 else 100, 50)
      
      # Tight bins: 2-3 days for decision periods
      binw <- min(3, max(1, round(x_max / 80)))
      
      traces <- plotly::plot_ly()
      for (st in c("Normal", "Long")) {
        d_st <- d_proc %>% dplyr::filter(status == st) %>%
          dplyr::pull(tender_days_dec) %>% .[. <= x_max]
        if (length(d_st) == 0) next
        col <- if (st == "Long") PA_FLAG else PA_NORMAL
        traces <- traces %>% plotly::add_histogram(
          x      = d_st,
          xbins  = list(start = 0, end = x_max + binw, size = binw),
          name   = st, legendgroup = st, showlegend = FALSE,
          marker = list(color = col, line = list(color = "white", width = 0.3)),
          hovertemplate = paste0("<b>", st, "</b>: %{y} contracts<extra></extra>")
        )
      }
      
      shape_list <- list()
      if (!is.na(thr_val)) {
        shape_list <- list(
          list(type = "line", x0 = thr_val, x1 = thr_val, y0 = 0, y1 = 1,
               xref = "x", yref = "paper",
               line = list(color = "#cc0000", width = 1.8, dash = "dash")),
          list(type = "rect", x0 = thr_val, x1 = x_max + binw, y0 = 0, y1 = 1,
               xref = "x", yref = "paper",
               fillcolor = "rgba(220,50,50,0.07)", line = list(width = 0))
        )
      }
      
      # Subtitle line under title: threshold + share, kept on x-axis as annotation
      sub_txt <- paste0(
        if (!is.na(thr_val)) paste0("\u2265", round(thr_val), " days threshold  \u2502  ") else "",
        scales::percent(pct_l, accuracy = 0.1), " long  ",
        "(n = ", scales::comma(n_tot), ")"
      )
      
      traces %>% plotly::layout(
        barmode = "stack",
        shapes  = shape_list,
        # Title baked into xaxis title so it sits below the panel, not above —
        # avoids subplot annotation coordinate collision entirely
        xaxis = list(
          title      = list(text = sub_txt, font = list(size = 11, color = "#555")),
          tickfont   = list(size = 11),
          gridcolor  = "#eeeeee",
          rangemode  = "nonnegative",
          automargin = TRUE
        ),
        yaxis = list(tickfont = list(size = 11), title = NULL, rangemode = "nonnegative")
      )
    })
    
    # ── Combine subplots ─────────────────────────────────────────────────
    # Use a generous row gap so titles don't bleed into neighbour panels
    row_gap <- if (nrows_l == 1) 0.06 else 0.14
    fig <- plotly::subplot(
      sub_figs_l,
      nrows   = nrows_l,
      shareX  = FALSE, shareY = FALSE,
      titleX  = TRUE,  titleY = FALSE,
      margin  = c(0.04, 0.04, row_gap, 0.06)
    )
    
    # ── Add per-panel procedure-name titles via domain arithmetic ─────────
    # subplot() lays panels left-to-right, top-to-bottom.
    # Domain of axis i (1-based) can be read from fig$x$layout.
    # We compute it manually: equal columns, equal rows.
    col_w  <- (1 - 0.04 * (ncols_l - 1)) / ncols_l
    row_h  <- (1 - row_gap * (nrows_l - 1)) / nrows_l
    panel_annotations <- lapply(seq_along(procs_l), function(i) {
      col_i  <- ((i - 1) %% ncols_l)          # 0-based
      row_i  <- ((i - 1) %/% ncols_l)          # 0-based, 0 = top
      x_mid  <- col_i * (col_w + 0.04) + col_w / 2
      # rows go top-to-bottom: row 0 has y_top near 1
      y_top  <- 1 - row_i * (row_h + row_gap)
      list(
        text      = paste0("<b>", procs_l[i], "</b>"),
        x         = x_mid,
        y         = y_top + 0.015,
        xref      = "paper", yref = "paper",
        xanchor   = "center", yanchor = "bottom",
        showarrow = FALSE,
        font      = list(size = 12, color = "#2c3e50")
      )
    })
    
    legend_ann <- list(
      text      = paste0(
        "<span style='color:", PA_FLAG,   ";'>▪ Long (flagged)</span>   ",
        "<span style='color:", PA_NORMAL, ";'>▪ Normal</span>   ",
        "<span style='color:#cc0000;'>‒ ‒ threshold</span>"
      ),
      x = 0.5, y = -0.03, xref = "paper", yref = "paper",
      xanchor = "center", yanchor = "top", showarrow = FALSE,
      font = list(size = 12)
    )
    
    fig <- fig %>% plotly::layout(
      font          = list(size = 12),
      hoverlabel    = list(bgcolor = "white", font = list(size = 12)),
      hovermode     = "closest",
      margin        = list(l = 45, r = 20, t = 35, b = 55),
      plot_bgcolor  = "#ffffff",
      paper_bgcolor = "#ffffff",
      annotations   = c(panel_annotations, list(legend_ann))
    )
    admin$fig_dec_long <- fig %>% pa_config(); admin$fig_dec_long
  })
  
  output$buyer_long_plot <- renderPlotly({
    req(admin$thresholds)
    cutoffs_dec <- admin_dec_cutoffs()
    tp_base     <- admin_subm_open_data() %>% dplyr::filter(tender_proceduretype %in% admin$global_proc_filter)
    req(nrow(tp_base) > 0)
    tp_buyer <- tp_base %>% dplyr::left_join(cutoffs_dec,by="tender_proceduretype") %>%
      dplyr::mutate(long_decision=tender_days_open>=long_cut, buyer_group=add_buyer_group(buyer_buyertype))
    req(nrow(tp_buyer)>0)
    by_count <- tp_buyer %>%
      dplyr::group_by(buyer_group,tender_proceduretype) %>%
      dplyr::summarise(n_long=sum(long_decision,na.rm=TRUE),n_total=dplyr::n(),share_long=mean(long_decision,na.rm=TRUE),.groups="drop") %>%
      dplyr::mutate(share_other=1-share_long,
                    tip_l=paste0(buyer_group," | ",tender_proceduretype,"<br><b>Long: ",scales::percent(share_long,accuracy=0.1),"</b> (",n_long," of ",n_total,")"),
                    tip_n=paste0(buyer_group," | ",tender_proceduretype,"<br>Normal: ",scales::percent(1-share_long,accuracy=0.1)),metric="Count")
    by_value <- tp_buyer %>%
      dplyr::group_by(buyer_group,tender_proceduretype) %>%
      dplyr::summarise(total_value=sum(bid_priceusd,na.rm=TRUE),long_value=sum(bid_priceusd[long_decision %in% TRUE],na.rm=TRUE),.groups="drop") %>%
      dplyr::mutate(share_long=dplyr::if_else(total_value>0,long_value/total_value,0),share_other=1-share_long,
                    tip_l=paste0(buyer_group," | ",tender_proceduretype,"<br><b>Long: ",scales::percent(share_long,accuracy=0.1),"</b><br>",
                                 scales::dollar(long_value,scale=1e-6,suffix="M",accuracy=0.1)," of ",scales::dollar(total_value,scale=1e-6,suffix="M",accuracy=0.1)),
                    tip_n=paste0(buyer_group," | ",tender_proceduretype,"<br>Normal: ",scales::percent(1-share_long,accuracy=0.1)),metric="Contract Value")
    
    # Perceptually distinct palette — same as buyer_short_plot for consistency
    PA_PROC_PAL <- c(
      "#1A6FAF",  # steel blue
      "#2CA02C",  # green
      "#D62728",  # red
      "#FF7F0E",  # orange
      "#9467BD",  # purple
      "#17BECF",  # cyan
      "#E377C2",  # pink
      "#8C564B",  # brown
      "#7F7F7F"   # grey
    )
    all_procs_l <- sort(unique(by_count$tender_proceduretype))
    proc_pal_l  <- setNames(PA_PROC_PAL[seq_along(all_procs_l)], all_procs_l)
    buyers_ord_l <- sort(unique(by_count$buyer_group))
    
    make_buyer_bar_long <- function(df, share_col, n_col, hover_label, show_legend = TRUE) {
      df <- df %>% dplyr::mutate(
        hover_txt = paste0(
          "<b>", buyer_group, " | ", tender_proceduretype, "</b><br>",
          hover_label, ": ", scales::percent(.data[[share_col]], accuracy=0.1), "<br>",
          "N: ", scales::comma(.data[[n_col]])
        )
      )
      fig <- plotly::plot_ly()
      procs_l3 <- sort(unique(df$tender_proceduretype))
      for (proc in procs_l3) {
        d_p <- df %>% dplyr::filter(tender_proceduretype == proc) %>%
          dplyr::arrange(match(buyer_group, buyers_ord_l))
        fig <- fig %>% plotly::add_bars(
          x           = ~buyer_group, y = ~.data[[share_col]],
          data        = d_p,
          name        = proc, legendgroup = proc,
          showlegend  = show_legend,
          marker      = list(color=proc_pal_l[[proc]], line=list(color="white",width=0.5)),
          hovertext   = ~hover_txt, hoverinfo="text"
        )
      }
      fig %>% plotly::layout(
        barmode = "group",
        xaxis   = list(title=NULL, tickfont=list(size=13), automargin=TRUE),
        yaxis   = list(title = list(text=hover_label, font=list(size=13)),
                       tickformat=".0%", range=c(0,1),
                       tickfont=list(size=13), gridcolor="#eeeeee"),
        plot_bgcolor  = "#ffffff",
        paper_bgcolor = "#ffffff",
        margin        = list(l=65, r=10, t=10, b=10)
      )
    }
    
    p_count_l3 <- make_buyer_bar_long(
      by_count %>% dplyr::rename(n_col=n_total),
      "share_long", "n_col", "% long by count", show_legend = TRUE)
    p_value_l3 <- make_buyer_bar_long(
      by_value %>% dplyr::mutate(n_col=round(total_value/1e6)),
      "share_long", "n_col", "% long by value", show_legend = FALSE)
    
    view_sel <- input$dec_buyer_view %||% "count"
    p_out <- if (view_sel == "count") {
      p_count_l3 %>% plotly::layout(
        hoverlabel = list(bgcolor="white", font=list(size=13)),
        font = list(size=13),
        legend = list(orientation="h", yanchor="bottom", y=1.02,
                      xanchor="center", x=0.5, font=list(size=14)),
        margin = list(l=65, r=20, t=60, b=60))
    } else if (view_sel == "value") {
      p_value_l3 %>% plotly::layout(
        hoverlabel = list(bgcolor="white", font=list(size=13)),
        font = list(size=13),
        legend = list(orientation="h", yanchor="bottom", y=1.02,
                      xanchor="center", x=0.5, font=list(size=14)),
        margin = list(l=65, r=20, t=60, b=60))
    } else {
      plotly::subplot(p_count_l3, p_value_l3, nrows=2,
                      shareX=TRUE, shareY=FALSE,
                      titleX=FALSE, titleY=TRUE, margin=0.06) %>%
        plotly::layout(
          hoverlabel = list(bgcolor="white", font=list(size=13)),
          font = list(size=13),
          legend = list(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="center", x=0.5, font=list(size=14)),
          margin = list(l=65, r=20, t=60, b=80))
    }
    admin$fig_buyer_long <- p_out %>% pa_config(); admin$fig_buyer_long
  })
  
  # ============================================================
  # ADMIN REGRESSION OUTPUTS
  # ============================================================
  
  output$short_reg_plot_ui <- renderUI({
    if (!is.null(admin$filtered_analysis$plot_short_reg)) plotOutput("short_reg_plot", height="600px")
    else p("No regression results available. Click 'Run / Re-run Regressions' above.")
  })
  output$short_reg_plot <- renderPlot({ req(admin$filtered_analysis$plot_short_reg); print(admin$filtered_analysis$plot_short_reg) })
  output$dl_short_reg_ui <- renderUI({
    if (!is.null(admin$filtered_analysis$plot_short_reg))
      downloadButton("dl_short_reg", "Download Figure", class="download-btn btn-sm")
  })
  
  output$long_reg_plot_ui <- renderUI({
    if (!is.null(admin$filtered_analysis$plot_long_reg)) plotOutput("long_reg_plot", height="600px")
    else p("No regression results available.")
  })
  output$long_reg_plot <- renderPlot({ req(admin$filtered_analysis$plot_long_reg); print(admin$filtered_analysis$plot_long_reg) })
  output$dl_long_reg_ui <- renderUI({
    if (!is.null(admin$filtered_analysis$plot_long_reg))
      downloadButton("dl_long_reg", "Download Figure", class="download-btn btn-sm")
  })
  
  # Sensitivity analysis UI builder (ported verbatim — reads from admin$filtered_analysis)
  sensitivity_ui_builder <- function(bundle) {
    has_rows <- function(tbl) !is.null(tbl) && is.data.frame(tbl) && nrow(tbl) > 0
    share_pos  <- if (has_rows(bundle$overall)) bundle$overall$share_positive else NA
    share_p10  <- if (has_rows(bundle$overall)) { cn <- names(bundle$overall); col <- cn[grepl("share_p_le_0.1",cn,fixed=TRUE)]; if(length(col)>0) bundle$overall[[col[1]]] else NA } else NA
    sign_stable <- if (has_rows(bundle$sign)) bundle$sign$share_sign_stable else NA
    median_est  <- if (has_rows(bundle$overall)) bundle$overall$median_estimate else NA
    median_p    <- if (has_rows(bundle$overall)) bundle$overall$median_pvalue   else NA
    n_specs     <- if (has_rows(bundle$overall)) bundle$overall$n_specs         else NA
    if (!is.na(share_pos) && !is.na(share_p10) && !is.na(sign_stable)) {
      if      (share_pos>=0.7 && share_p10>=0.6 && sign_stable==1) { verdict <- "\u2713 Strong and robust evidence."; vcol <- "success" }
      else if (share_pos>=0.6 && share_p10>=0.3)                   { verdict <- "\u26a0 Moderate evidence.";          vcol <- "warning" }
      else                                                           { verdict <- "\u2717 Weak or mixed evidence.";    vcol <- "danger"  }
    } else { verdict <- "\u2139 Summary not available."; vcol <- "info" }
    tagList(
      div(class=paste0("alert alert-",vcol), style="margin-top:10px;",
          h4(style="margin-top:0;","Summary for Decision-Makers"),
          p(strong(verdict)),
          tags$ul(
            if (!is.na(share_pos))  tags$li(scales::percent(share_pos,accuracy=1)," of models show a positive estimate"),
            if (!is.na(share_p10))  tags$li(scales::percent(share_p10,accuracy=1)," are significant at p \u2264 0.10"),
            if (!is.na(median_est)) tags$li("Median estimate: ",sprintf("%.3f",median_est)),
            if (!is.na(median_p))   tags$li("Median p-value: ", sprintf("%.3f",median_p))
          )),
      tags$details(style="margin-bottom:12px;",
                   tags$summary(style="cursor:pointer;font-weight:bold;font-size:13px;","What is sensitivity analysis?"),
                   div(style="margin-top:8px;font-size:13px;",
                       p("Tests whether the relationship holds across different modeling choices (fixed effects, clustering, controls).")))
    )
  }
  
  make_formula_ui <- function(m) {
    if (is.null(m)) return(NULL)
    fml   <- tryCatch(paste(deparse(formula(m),width.cutoff=120),collapse=" "), error=function(e) NULL)
    n_obs <- tryCatch(nobs(m), error=function(e) NA)
    div(style="margin-top:14px;padding-top:12px;border-top:1px solid #e8e8e8;",
        tags$details(tags$summary(style="cursor:pointer;font-size:12px;color:#666;font-weight:bold;","Model specification"),
                     div(style="margin-top:8px;",
                         p(style="font-size:12px;color:#555;","Family: binomial (logit)",
                           if (!is.na(n_obs)) tagList(" | Observations: ",tags$b(format(n_obs,big.mark=",")))),
                         div(style="background:#f8f9fa;border-left:3px solid #f0ad4e;padding:8px 12px;font-family:monospace;font-size:12px;", fml))))
  }
  
  output$short_reg_formula_ui <- renderUI({ make_formula_ui(admin$filtered_analysis$model_short_glm) })
  output$long_reg_formula_ui  <- renderUI({ make_formula_ui(admin$filtered_analysis$model_long_glm)  })
  output$sensitivity_short_ui <- renderUI({ req(admin$filtered_analysis$sensitivity_short); sensitivity_ui_builder(admin$filtered_analysis$sensitivity_short) })
  output$sensitivity_long_ui  <- renderUI({ req(admin$filtered_analysis$sensitivity_long);  sensitivity_ui_builder(admin$filtered_analysis$sensitivity_long)  })
  
  # ============================================================
  # ADMIN FIGURE DOWNLOAD HANDLERS (webshot2 for plotly figures)
  # ============================================================
  
  # ============================================================
  # ADMIN FIGURE DOWNLOAD HANDLERS
  # ============================================================
  
  # Alias to the shared helper defined above
  dl_admin_plotly <- dl_plotly_fig
  
  output$dl_contracts_year   <- dl_admin_plotly(function() admin$fig_contracts_year,   "contracts_year",  1200, 700)
  output$dl_proc_share_value <- dl_admin_plotly(function() admin$fig_proc_share_value, "proc_share_value", 900, 700)
  output$dl_proc_share_count <- dl_admin_plotly(function() admin$fig_proc_share_count, "proc_share_count", 900, 700)
  output$dl_proc_value_dist  <- dl_admin_plotly(function() admin$fig_proc_value_dist,  "proc_value_dist", 1400, 700)
  output$dl_subm_dist        <- dl_admin_plotly(function() admin$fig_subm_dist,        "subm_dist",       1200, 700)
  output$dl_subm_proc        <- dl_admin_plotly(function() admin$fig_subm_proc,        "subm_proc",       1400, 800)
  output$dl_subm_share       <- dl_admin_plotly(function() admin$fig_subm_share,       "subm_share",       900, 400)
  output$dl_subm_short       <- dl_admin_plotly(function() admin$fig_subm_short,       "subm_short",      1400, 900)
  output$dl_buyer_short      <- dl_admin_plotly(function() admin$fig_buyer_short,      "buyer_short",     1400, 800)
  output$dl_dec_dist         <- dl_admin_plotly(function() admin$fig_dec_dist,         "dec_dist",        1200, 700)
  output$dl_dec_proc         <- dl_admin_plotly(function() admin$fig_dec_proc,         "dec_proc",        1400, 800)
  output$dl_dec_share        <- dl_admin_plotly(function() admin$fig_dec_share,        "dec_share",        900, 400)
  output$dl_dec_long         <- dl_admin_plotly(function() admin$fig_dec_long,         "dec_long",        1400, 900)
  output$dl_buyer_long       <- dl_admin_plotly(function() admin$fig_buyer_long,       "buyer_long",      1400, 800)
  
  output$dl_short_reg <- downloadHandler(
    filename = function() paste0("short_reg_", admin$country_code, ".png"),
    content  = function(file) { req(admin$filtered_analysis$plot_short_reg); ggsave(file, admin$filtered_analysis$plot_short_reg, width=10, height=8, dpi=300) }
  )
  output$dl_long_reg <- downloadHandler(
    filename = function() paste0("long_reg_", admin$country_code, ".png"),
    content  = function(file) { req(admin$filtered_analysis$plot_long_reg); ggsave(file, admin$filtered_analysis$plot_long_reg, width=10, height=8, dpi=300) }
  )
  
  # ============================================================
  # ADMIN REPORT DOWNLOADS
  # ============================================================
  
  
  output$dl_admin_word <- downloadHandler(
    filename = function() paste0("admin_efficiency_", admin$country_code, "_", format(Sys.Date(), "%Y%m%d"), ".docx"),
    content  = function(file) {
      req(admin$data, admin$analysis, admin$country_code)
      withProgress(message = "Generating administrative efficiency Word report...", value = 0, {
        incProgress(0.2, detail = "Filtering data...")
        exp_data <- admin_get_export_data()
        incProgress(0.5, detail = "Collecting figures...")
        # Use the same ggplot objects the app rendered — these ARE what the user sees.
        # Each renderPlotly block stores its ggplot as admin$gg_* alongside the plotly fig.
        # Fall back to admin_regenerate_plots for any that haven't been rendered yet.
        plots <- admin_build_word_plots(
          filtered_data      = exp_data,
          thresholds         = admin$thresholds,
          global_proc_filter = admin$global_proc_filter,
          subm_cutoffs       = isolate(admin_subm_cutoffs()),
          dec_cutoffs        = isolate(admin_dec_cutoffs()),
          price_thresholds   = admin$price_thresholds
        )
        plots$plot_short_reg  <- admin$filtered_analysis$plot_short_reg
        plots$plot_long_reg   <- admin$filtered_analysis$plot_long_reg
        # Fallback: if ggplot bunching panels weren't built, pass the stored plotly fig
        # so generate_admin_word_report can render it via webshot2
        if (is.null(plots$bunching))
          plots$bunching_fig_fallback <- admin$bunching_fig
        message("[word_export] plots$bunching: ",
                if (is.null(plots$bunching)) "NULL (will try plotly fallback)" else paste0("list of ", length(plots$bunching), " ggplots"))
        incProgress(0.7, detail = "Creating document...")
        filter_desc  <- get_filter_description(admin_filters$active)
        filters_text <- if (filter_desc == "No filters applied") "" else paste0("Applied Filters: ", filter_desc)
        ok <- generate_admin_word_report(
          filtered_data     = exp_data,
          filtered_analysis = plots,
          country_code      = admin$country_code,
          output_file       = file,
          filters_text      = filters_text
        )
        output$export_status <- renderText(if (ok) "Administrative efficiency Word report generated!" else "Error generating admin Word report.")
      })
    }
  )
  
  output$dl_admin_zip <- downloadHandler(
    filename = function() paste0("admin_figures_", admin$country_code, "_", format(Sys.Date(), "%Y%m%d"), ".zip"),
    content  = function(file) {
      req(admin$data, admin$analysis, admin$country_code)
      withProgress(message = "Creating admin figures ZIP...", value = 0, {
        incProgress(0.1, detail = "Filtering data...")
        exp_data <- admin_get_export_data()
        temp_dir <- tempfile(); dir.create(temp_dir)
        cc       <- admin$country_code
        regen <- admin_build_word_plots(
          filtered_data      = exp_data,
          thresholds         = admin$thresholds,
          global_proc_filter = admin$global_proc_filter,
          subm_cutoffs       = isolate(admin_subm_cutoffs()),
          dec_cutoffs        = isolate(admin_dec_cutoffs()),
          price_thresholds   = admin$price_thresholds
        )
        plot_list <- list(
          list(regen$sh,               "proc_share_value",  10, 6),
          list(regen$p_count,          "proc_share_count",  10, 6),
          list(regen$subm,             "subm_dist",         10, 6),
          list(regen$subm_proc_facet_q,"subm_proc",         10, 6),
          list(regen$subm_r,           "subm_short",        10, 6),
          list(regen$buyer_short,      "buyer_short",       12, 8),
          list(regen$decp,             "dec_dist",          10, 6),
          list(regen$decp_proc_facet_q,"dec_proc",          10, 6),
          list(regen$decp_r,           "dec_long",          10, 6),
          list(regen$buyer_long,       "buyer_long",        12, 8),
          list(admin$filtered_analysis$plot_short_reg, "short_reg", 10, 8),
          list(admin$filtered_analysis$plot_long_reg,  "long_reg",  10, 8)
        )
        saved <- 0
        for (i in seq_along(plot_list)) {
          incProgress(0.2 + i / length(plot_list) * 0.7, detail = paste("Saving figure", i))
          pl <- plot_list[[i]]
          if (!is.null(pl[[1]])) tryCatch({
            ggsave(file.path(temp_dir, paste0(pl[[2]], "_", cc, ".png")),
                   pl[[1]], width = pl[[3]], height = pl[[4]], dpi = 300)
            saved <- saved + 1
          }, error = function(e) NULL)
        }
        if (saved > 0) {
          zip::zip(zipfile = file, files = list.files(temp_dir, full.names = TRUE), mode = "cherry-pick")
          output$export_status <- renderText(paste0(saved, " admin figures saved to ZIP."))
        } else {
          showNotification("No figures were saved. View the charts first, then download the ZIP.", type="warning", duration=8)
          output$export_status <- renderText("No figures saved. View charts first, then download.")
        }
        unlink(temp_dir, recursive = TRUE)
      })
    }
  )
  
  # ============================================================
  # EXPORT STATUS
  # ============================================================
  
  output$export_status <- renderText({ "No exports yet. Use the buttons above to generate reports." })
  
  
  # ============================================================
  # INTEGRITY — FILTER UI GENERATION
  # ============================================================
  
  integ_tabs <- c("missing", "interop", "risky", "prices")
  
  make_integ_filter_outputs <- function(tabs = integ_tabs) {
    for (tab in tabs) {
      local({
        t <- tab
        p <- "integ_"
        
        output[[paste0(p, "year_filter_", t)]] <- renderUI({
          req(integ$data)
          year_col <- if ("tender_year" %in% names(integ$data)) "tender_year"
          else if ("year" %in% names(integ$data)) "year"
          else if ("cal_year" %in% names(integ$data)) "cal_year" else NULL
          if (!is.null(year_col)) {
            years <- sort(unique(integ$data[[year_col]])); years <- years[!is.na(years)]
            if (length(years) > 0)
              sliderInput("integ_year_range", "Year Range:",
                          min=min(years), max=max(years), value=c(min(years),max(years)), step=1, sep="")
          }
        })
        
        output[[paste0(p, "market_filter_", t)]] <- renderUI({
          req(econ$filtered_data)
          if ("cpv_cluster" %in% names(econ$filtered_data)) {
            cpv_codes <- sort(unique(econ$filtered_data$cpv_cluster))
            cpv_codes <- cpv_codes[!is.na(cpv_codes) & cpv_codes != ""]
            if (length(cpv_codes) > 0) {
              cpv_choices <- setNames(cpv_codes, sapply(cpv_codes, get_cpv_label))
              pickerInput("integ_market_filter", "Market (CPV):",
                          choices = c("All" = "All", cpv_choices), selected = "All",
                          multiple = TRUE,
                          options = list(`actions-box` = TRUE, `live-search` = TRUE))
            }
          }
        })
        
        output[[paste0(p, "value_filter_", t)]] <- renderUI({
          req(integ$data)
          price_col <- detect_price_col(integ$data, .PRICE_COLS_ADMIN)
          if (!is.na(price_col) && !is.null(price_col)) {
            prices <- integ$data[[price_col]]; prices <- prices[!is.na(prices) & prices > 0]
            if (length(prices) > 0) {
              cur  <- input$integ_value_currency %||% "USD"
              rate <- if (cur == "BGN") .BGN_PER_USD else 1
              max_val_raw <- quantile(prices * rate, 0.99, na.rm = TRUE)
              div <- 1e3
              integ$value_divisor <- div / rate
              integ$value_max_k   <- ceiling(max(prices) * rate / div)
              make_value_filter_widget(prices, "integ_value_currency", "integ_value_range", cur)
            }
          }
        })
        
        output[[paste0(p, "buyer_type_filter_", t)]] <- renderUI({
          req(integ$data)
          if ("buyer_buyertype" %in% names(integ$data)) {
            bg <- integ$data %>% mutate(bg = add_buyer_group(buyer_buyertype)) %>%
              pull(bg) %>% as.character() %>% unique() %>% sort()
            bg <- bg[!is.na(bg)]
            if (length(bg) > 0)
              pickerInput("integ_buyer_type_filter", "Buyer Type:",
                          choices=c("All", bg), selected="All", multiple=TRUE,
                          options=list(`actions-box`=TRUE))
          }
        })
        
        output[[paste0(p, "procedure_type_filter_", t)]] <- renderUI({
          req(integ$data)
          if ("tender_proceduretype" %in% names(integ$data)) {
            raw_types <- unique(integ$data$tender_proceduretype)
            raw_types <- raw_types[!is.na(raw_types)]
            if (length(raw_types) > 0) {
              df_map <- data.frame(raw     = raw_types,
                                   cleaned = recode_procedure_type(raw_types),
                                   stringsAsFactors = FALSE)
              types <- sort(unique(df_map$cleaned[!is.na(df_map$cleaned)]))
              pickerInput("integ_procedure_type_filter", "Procedure Type:",
                          choices  = c("All", types), selected = "All",
                          multiple = TRUE,
                          options  = list(`actions-box` = TRUE, `live-search` = TRUE))
            }
          }
        })
        
        output[[paste0(p, "filter_status_", t)]] <- renderText({
          paste(" ", get_filter_description(integ_filters[[t]]))
        })
      })
    }
  }
  make_integ_filter_outputs()
  
  # run_integrity_pipeline_fast_local is defined globally above the UI
  
  apply_integ_filters <- function(tab_name) {
    req(integ$data)
    current_filters <- list(
      year           = input$integ_year_range,
      market         = input$integ_market_filter,
      value = {
        mn_k <- input$integ_value_range_min_k
        mx_k <- input$integ_value_range_max_k
        # Blank = use data min (0) or max
        mn <- if (is.null(mn_k) || is.na(mn_k)) 0 else mn_k
        mx <- if (is.null(mx_k) || is.na(mx_k)) (integ$value_max_k %||% 1e9) else mx_k
        c(mn, mx)
      },
      buyer_type     = input$integ_buyer_type_filter,
      procedure_type = input$integ_procedure_type_filter
    )
    integ_filters$active      <- current_filters
    integ_filters[[tab_name]] <- current_filters
    filtered_df <- integrity_filter_data(integ$data,
                                         year_range     = current_filters$year,
                                         market         = current_filters$market,
                                         value_range    = current_filters$value,
                                         buyer_type     = current_filters$buyer_type,
                                         procedure_type = current_filters$procedure_type,
                                         value_divisor  = isolate(integ$value_divisor))
    if (nrow(filtered_df) == 0) {
      showNotification("No data matches the selected filters.", type="warning", duration=5)
      return(NULL)
    }
    showNotification(paste0("Filters applied! ", formatC(nrow(filtered_df), format="d", big.mark=","), " contracts."),
                     type="message", duration=3)
    withProgress(message="Re-running integrity analysis with filters...", value=0, {
      incProgress(0.2, detail="Preparing filtered dataset...")
      new_results <- run_integrity_pipeline_fast_local(filtered_df, integ$country_code, tempdir())
      integ$filtered_data     <- new_results$data
      integ$filtered_analysis <- new_results
      integ$network_done      <- FALSE
      integ$regression_done   <- FALSE
      integ$missing_advanced_done <- FALSE
      incProgress(1.0, detail="Complete!")
    })
  }
  
  reset_integ_filters <- function(tab_name) {
    empty <- list(year=NULL, market=NULL, value=NULL, buyer_type=NULL, procedure_type=NULL)
    integ_filters$active      <- empty
    integ_filters[[tab_name]] <- empty
    integ$filtered_data     <- integ$data
    integ$filtered_analysis <- integ$analysis
    showNotification("Filters reset", type="message", duration=2)
  }
  
  for (tn in integ_tabs) {
    local({
      t <- tn
      observeEvent(input[[paste0("integ_apply_filters_", t)]], { apply_integ_filters(t) })
      observeEvent(input[[paste0("integ_reset_filters_",  t)]], { reset_integ_filters(t) })
    })
  }
  
  # ============================================================
  # INTEGRITY — DATA OVERVIEW OUTPUTS
  # ============================================================
  
  output$integ_n_contracts <- renderValueBox({
    df <- integ_filtered_data(); req(df)
    valueBox(formatC(nrow(df), format="d", big.mark=","), "Total Contracts",
             icon=icon("file-contract"), color="blue")
  })
  output$integ_n_buyers <- renderValueBox({
    df <- integ_filtered_data(); req(df)
    n <- if ("buyer_masterid" %in% names(df)) dplyr::n_distinct(df$buyer_masterid, na.rm=TRUE) else NA
    valueBox(formatC(n, format="d", big.mark=","), "Buyers", icon=icon("building"), color="teal")
  })
  output$integ_n_suppliers <- renderValueBox({
    df <- integ_filtered_data(); req(df)
    n <- if ("bidder_masterid" %in% names(df)) dplyr::n_distinct(df$bidder_masterid, na.rm=TRUE) else NA
    valueBox(formatC(n, format="d", big.mark=","), "Suppliers", icon=icon("truck"), color="olive")
  })
  output$integ_n_years <- renderValueBox({
    df <- integ_filtered_data(); req(df)
    year_col <- if ("tender_year" %in% names(df)) "tender_year" else NULL
    n <- if (!is.null(year_col)) { yrs <- unique(df[[year_col]]); length(yrs[!is.na(yrs)]) } else NA
    valueBox(ifelse(is.na(n) || n==0, "N/A", as.character(n)), "Period", icon=icon("calendar"), color="navy")
  })
  output$integ_contracts_year_plot <- renderPlotly({
    df <- integ_filtered_data(); req(df, "tender_year" %in% names(df))
    yc <- df %>% dplyr::count(tender_year, name="n_observations")
    p  <- ggplot2::ggplot(yc, ggplot2::aes(x=tender_year, y=n_observations,
                                           text=paste0("Year: ",tender_year,"<br>Contracts: ",formatC(n_observations,format="d",big.mark=",")))) +
      ggplot2::geom_col(fill="#c0392b") +
      ggplot2::labs(x="Year", y="Contracts", title="Contracts per Year") +
      pa_theme() +
      ggplot2::scale_y_continuous(labels=scales::comma)
    ggplotly(p, tooltip="text") %>% layout(hoverlabel=list(bgcolor="white"), hovermode="closest") %>%
      pa_config() -> .stored_fig
    integ$fig_contracts_year <- .stored_fig
    .stored_fig
  })
  output$integ_summary_table <- DT::renderDataTable({
    df <- integ_filtered_data(); req(df)
    rows <- list(data.frame(Metric="Total Contracts", Value=format(nrow(df), big.mark=",")))
    if ("buyer_masterid"  %in% names(df)) rows[[length(rows)+1]] <- data.frame(Metric="Unique Buyers",   Value=format(dplyr::n_distinct(df$buyer_masterid,  na.rm=TRUE), big.mark=","))
    if ("bidder_masterid" %in% names(df)) rows[[length(rows)+1]] <- data.frame(Metric="Unique Bidders",  Value=format(dplyr::n_distinct(df$bidder_masterid, na.rm=TRUE), big.mark=","))
    if ("tender_year" %in% names(df)) {
      yrs <- sort(unique(df$tender_year[!is.na(df$tender_year)]))
      if (length(yrs)>0) {
        rows[[length(rows)+1]] <- data.frame(Metric="Year Range",  Value=paste(min(yrs),"-",max(yrs)))
        rows[[length(rows)+1]] <- data.frame(Metric="Total Years", Value=as.character(length(yrs)))
      }
    }
    DT::datatable(do.call(rbind, rows), options=list(pageLength=20, dom="t"), rownames=FALSE)
  })
  output$integ_variables_list <- renderUI({
    req(integ$filtered_analysis$summary_stats$vars_present)
    vars <- integ$filtered_analysis$summary_stats$vars_present
    HTML(paste("<ul>", paste0("<li>", vars, "</li>", collapse=""), "</ul>"))
  })
  output$integ_dl_contracts_year <- downloadHandler(
    filename = function() paste0("integ_contracts_year_", integ$country_code %||% "export",
                                 "_", format(Sys.Date(), "%Y%m%d"), ".png"),
    content  = function(file) {
      fig <- tryCatch(integ$fig_contracts_year, error = function(e) NULL)
      .require_fig(fig, "Contracts per Year")
      .save_fig_png(fig, file)
    }
  )
  
  # ============================================================
  # INTEGRITY — DEFERRED: MISSING ADVANCED
  # ============================================================
  
  observeEvent(input$integ_run_missing_advanced, {
    req(integ$filtered_data, integ$filtered_analysis)
    withProgress(message="Running advanced missingness tests...", value=0, {
      incProgress(0.1, detail="MCAR test...")
      config <- safe_pipeline_config(integ$country_code)
      adv <- tryCatch(
        run_missing_advanced_tests(integ$filtered_data, config, tempdir()),
        error=function(e) { showNotification(paste("Advanced tests error:", e$message), type="error", duration=10); NULL }
      )
      incProgress(0.9, detail="Updating results...")
      if (!is.null(adv)) {
        integ$filtered_analysis$missing$mcar_test        <- adv$mcar_test
        integ$filtered_analysis$missing$cooccurrence_data <- adv$cooccurrence_data
        integ$filtered_analysis$missing$cooccurrence_plot <- adv$cooccurrence_plot
        integ$filtered_analysis$missing$mar_results      <- adv$mar_results
        integ$filtered_analysis$missing$mar_plot         <- adv$mar_plot
        integ$missing_advanced_done <- TRUE
        showNotification("Advanced missingness tests complete!", type="message", duration=5)
      }
    })
  })
  
  output$integ_mcar_summary_card <- renderUI({
    if (!isTRUE(integ$missing_advanced_done))
      return(div(class="deferred-box", icon("clock"), " Click 'Run Advanced Missingness Tests' above."))
    mcar <- integ$filtered_analysis$missing$mcar_test
    if (is.null(mcar) || is.na(mcar$p_value))
      return(div(class="alert alert-info", "MCAR test could not be computed."))
    p_val      <- mcar$p_value
    status_col <- if (p_val < 0.05) "#c0392b" else if (p_val < 0.10) "#e67e22" else "#27ae60"
    div(style=paste0("border-left:5px solid ",status_col,"; padding:14px 20px; background:#fafafa; border-radius:4px;"),
        tags$p(tags$b(paste("Little's MCAR Test — p-value:", round(p_val, 4))), style="margin-bottom:6px; font-size:15px;"),
        tags$p(mcar$interpretation, style=paste0("margin-top:10px; color:",status_col,"; font-style:italic;")))
  })
  
  # ============================================================
  # INTEGRITY — MISSING VALUES OUTPUTS
  # ============================================================
  
  output$integ_missing_overall_plot <- renderPlotly({
    req(integ$filtered_analysis$missing$overall_plot)
    n_vars  <- nrow(integ$filtered_analysis$missing$overall_long %||% data.frame())
    h       <- max(250, min(600, n_vars*18+60))
    post_process_plotly(
      ggplotly(integ$filtered_analysis$missing$overall_plot, tooltip="text", height=h)
    ) %>% layout(hoverlabel=list(bgcolor="white",font=list(size=12)),
                 margin=list(l=210,b=50,t=20,r=20),
                 font=list(size=12),
                 xaxis=list(titlefont=list(size=12), tickfont=list(size=11)),
                 yaxis=list(tickfont=list(size=11))) %>%
      pa_config() -> .stored_fig
    integ$fig_miss_overall <- .stored_fig
    .stored_fig
  })
  output$integ_missing_overall_height_spacer <- renderUI({
    req(integ$filtered_analysis$missing$overall_long)
    n <- nrow(integ$filtered_analysis$missing$overall_long)
    h <- max(270, min(620, n*18+60))
    tags$style(paste0("#integ_missing_overall_plot { height: ",h,"px !important; }"))
  })
  output$integ_missing_buyer_slider_ui <- renderUI({
    req(integ$filtered_analysis$missing$by_buyer_n_vars_max)
    n_max <- integ$filtered_analysis$missing$by_buyer_n_vars_max
    sliderInput("integ_missing_buyer_n_vars","Number of variables to show:", min=5, max=n_max, value=min(15,n_max), step=5, width="70%")
  })
  output$integ_missing_buyer_plot <- renderPlotly({
    req(integ$filtered_analysis$missing$by_buyer, integ$filtered_analysis$missing$by_buyer_var_order)
    n_vars <- input$integ_missing_buyer_n_vars %||% min(15, integ$filtered_analysis$missing$by_buyer_n_vars_max)
    req(n_vars); h <- max(200, n_vars*20+60)
    p <- make_groupvar_heatmap(long_df=integ$filtered_analysis$missing$by_buyer, group_var="buyer_group",
                               var_order=integ$filtered_analysis$missing$by_buyer_var_order, top_n=n_vars,
                               title=paste("Missing share by buyer type -", integ$country_code %||% ""), x_lab="Buyer type")
    post_process_plotly(ggplotly(p, tooltip="text", height=h)) %>% layout(hoverlabel=list(bgcolor="white",font=list(size=12)), font=list(size=12), margin=list(l=210,r=130,b=70,t=30), xaxis=list(tickfont=list(size=11)), yaxis=list(tickfont=list(size=11))) %>%
      pa_config() -> .stored_fig
    integ$fig_miss_buyer <- .stored_fig
    .stored_fig
  })
  output$integ_missing_buyer_plot_height <- renderUI({
    n <- input$integ_missing_buyer_n_vars %||% 15; h <- max(220, n*30+80)
    tags$style(paste0("#integ_missing_buyer_plot { height: ",h,"px !important; }"))
  })
  output$integ_missing_procedure_slider_ui <- renderUI({
    req(integ$filtered_analysis$missing$by_procedure_n_vars_max)
    n_max <- integ$filtered_analysis$missing$by_procedure_n_vars_max
    sliderInput("integ_missing_procedure_n_vars","Number of variables to show:", min=5, max=n_max, value=min(15,n_max), step=5, width="70%")
  })
  output$integ_missing_procedure_plot <- renderPlotly({
    req(integ$filtered_analysis$missing$by_procedure, integ$filtered_analysis$missing$by_procedure_var_order)
    n_vars <- input$integ_missing_procedure_n_vars %||% min(15, integ$filtered_analysis$missing$by_procedure_n_vars_max)
    req(n_vars); h <- max(200, n_vars*20+60)
    p <- make_groupvar_heatmap(long_df=integ$filtered_analysis$missing$by_procedure, group_var="proc_group_label",
                               var_order=integ$filtered_analysis$missing$by_procedure_var_order, top_n=n_vars,
                               title=paste("Missing share by procedure type -", integ$country_code %||% ""), x_lab="Procedure type")
    post_process_plotly(ggplotly(p, tooltip="text", height=h)) %>% layout(hoverlabel=list(bgcolor="white",font=list(size=12)), font=list(size=12), margin=list(l=210,r=130,b=70,t=30), xaxis=list(tickfont=list(size=11)), yaxis=list(tickfont=list(size=11))) %>%
      pa_config() -> .stored_fig
    integ$fig_miss_proc <- .stored_fig
    .stored_fig
  })
  output$integ_missing_procedure_plot_height <- renderUI({
    n <- input$integ_missing_procedure_n_vars %||% 15; h <- max(220, n*30+80)
    tags$style(paste0("#integ_missing_procedure_plot { height: ",h,"px !important; }"))
  })
  output$integ_missing_time_slider_ui <- renderUI({
    req(integ$filtered_analysis$missing$by_year_n_vars_max)
    n_max <- integ$filtered_analysis$missing$by_year_n_vars_max
    s_max <- min(50, n_max); s_max <- ceiling(s_max/5)*5; s_val <- min(15, s_max)
    sliderInput("integ_missing_time_n_vars","Number of variables to show:", min=5, max=s_max, value=s_val, step=5, width="70%")
  })
  output$integ_missing_time_plot <- renderPlotly({
    req(integ$filtered_analysis$missing$by_year, integ$filtered_analysis$missing$by_year_var_order)
    n_vars <- input$integ_missing_time_n_vars %||% min(15, integ$filtered_analysis$missing$by_year_n_vars_max)
    req(n_vars); h <- max(200, n_vars*20+60)
    p <- make_year_heatmap(by_year_df=integ$filtered_analysis$missing$by_year,
                           var_order=integ$filtered_analysis$missing$by_year_var_order,
                           top_n=n_vars, country=integ$country_code %||% "")
    post_process_plotly(ggplotly(p, tooltip="text", height=h)) %>%
      layout(hoverlabel=list(bgcolor="white",font=list(size=12)), font=list(size=12), margin=list(l=210,r=130,b=60,t=40), xaxis=list(tickfont=list(size=11)), yaxis=list(tickfont=list(size=11)),
             coloraxis=list(colorbar=list(len=0.8, thickness=15))) %>%
      pa_config() -> .stored_fig
    integ$fig_miss_time <- .stored_fig
    .stored_fig
  })
  output$integ_missing_time_height_spacer <- renderUI({
    n <- input$integ_missing_time_n_vars %||% 15; h <- max(220, n*24+70)
    tags$style(paste0("#integ_missing_time_plot { height: ",h,"px !important; }"))
  })
  output$integ_missing_cooccurrence_ui <- renderUI({
    if (!isTRUE(integ$missing_advanced_done))
      div(class="deferred-box", icon("clock"), " Click 'Run Advanced Missingness Tests' above.")
    else if (is.null(integ$filtered_analysis$missing$cooccurrence_plot))
      div(class="alert alert-warning", "Co-occurrence plot could not be generated.")
    else tagList(
      uiOutput("integ_missing_cooc_slider_ui"),
      div(style="overflow-y:auto; max-height:700px;", plotlyOutput("integ_missing_cooccurrence_plot", height="auto"))
    )
  })
  output$integ_missing_cooccurrence_download_ui <- renderUI({
    if (isTRUE(integ$missing_advanced_done) && !is.null(integ$filtered_analysis$missing$cooccurrence_plot))
      downloadButton("integ_dl_missing_cooccurrence", "Download Figure", class="download-btn btn-sm")
  })
  output$integ_missing_cooc_slider_ui <- renderUI({
    req(integ$filtered_analysis$missing$cooccurrence_data)
    co    <- integ$filtered_analysis$missing$cooccurrence_data
    j_min <- floor(min(co$jaccard, na.rm=TRUE)*100/5)*5
    sliderInput("integ_cooc_min_jaccard","Show only pairs with co-occurrence score at or above:",
                min=j_min, max=100, value=50, step=5, post="%", width="55%")
  })
  output$integ_missing_cooccurrence_plot <- renderPlotly({
    req(integ$missing_advanced_done, integ$filtered_analysis$missing$cooccurrence_data)
    min_j <- (input$integ_cooc_min_jaccard %||% 0)/100
    p <- plot_cooccurrence_from_data(co_df=integ$filtered_analysis$missing$cooccurrence_data,
                                     top_n=50, min_jaccard=min_j,
                                     title=paste("Variable Pairs Missing Together -", integ$country_code %||% ""))
    if (is.null(p)) return(plotly::plot_ly(type = "scatter", mode = "markers") %>% plotly::layout(title="No pairs meet the selected threshold."))
    n_pairs <- nrow(integ$filtered_analysis$missing$cooccurrence_data %>% dplyr::filter(jaccard >= min_j))
    h <- max(280, min(700, n_pairs*22+80))
    post_process_plotly(ggplotly(p, tooltip="text", height=h)) %>% layout(hoverlabel=list(bgcolor="white",font=list(size=11)), margin=list(l=220)) %>%
      pa_config() -> .stored_fig
    integ$fig_miss_cooc <- .stored_fig
    .stored_fig
  })
  output$integ_missing_mar_ui <- renderUI({
    if (!isTRUE(integ$missing_advanced_done))
      div(class="deferred-box", icon("clock"), " Click 'Run Advanced Missingness Tests' above.")
    else if (is.null(integ$filtered_analysis$missing$mar_plot))
      div(class="alert alert-warning", "MAR predictability plot could not be generated.")
    else {
      n_vars <- nrow(integ$filtered_analysis$missing$mar_results %||% data.frame())
      h <- max(300, min(520, n_vars*22+80))
      tagList(div(style="width:100%; overflow-x:hidden;",
                  plotlyOutput("integ_missing_mar_plot", height=paste0(h,"px"), width="100%")))
    }
  })
  output$integ_missing_mar_download_ui <- renderUI({
    if (isTRUE(integ$missing_advanced_done) && !is.null(integ$filtered_analysis$missing$mar_plot))
      downloadButton("integ_dl_missing_mar", "Download Figure", class="download-btn btn-sm")
  })
  output$integ_missing_mar_plot <- renderPlotly({
    req(integ$missing_advanced_done, integ$filtered_analysis$missing$mar_plot)
    n_vars <- nrow(integ$filtered_analysis$missing$mar_results %||% data.frame())
    h <- max(300, min(520, n_vars*22+80))
    post_process_plotly(
      ggplotly(integ$filtered_analysis$missing$mar_plot, tooltip="text", height=h)
    ) %>% plotly::config(responsive=TRUE) %>%
      layout(hoverlabel=list(bgcolor="white",font=list(size=10)),
             margin=list(l=180,r=10,t=20,b=50), autosize=TRUE) %>%
      pa_config() -> .stored_fig
    integ$fig_miss_mar <- .stored_fig
    .stored_fig
  })
  
  # ── Missing download handlers ──────────────────────────────────────────
  # ── Integ missing value downloads — download exactly what's displayed ──
  .dl_integ_plotly <- function(fig_expr, fname, vw = 1200, vh = 700) {
    downloadHandler(
      filename = function() paste0(fname, "_", integ$country_code %||% "export",
                                   "_", format(Sys.Date(), "%Y%m%d"), ".png"),
      content  = function(file) {
        fig <- tryCatch(fig_expr(), error = function(e) NULL)
        .require_fig(fig, fname)
        .save_fig_png(fig, file)
      }
    )
  }
  output$integ_dl_missing_overall    <- .dl_integ_plotly(function() integ$fig_miss_overall, "integ_missing_overall",    1200, 700)
  output$integ_dl_missing_buyer      <- .dl_integ_plotly(function() integ$fig_miss_buyer,   "integ_missing_buyer",      1200, 800)
  output$integ_dl_missing_procedure  <- .dl_integ_plotly(function() integ$fig_miss_proc,    "integ_missing_procedure",  1000, 600)
  output$integ_dl_missing_time       <- .dl_integ_plotly(function() integ$fig_miss_time,    "integ_missing_time",       1200, 700)
  output$integ_dl_missing_cooccurrence <- .dl_integ_plotly(function() integ$fig_miss_cooc,  "integ_missing_cooccurrence", 1000, 800)
  output$integ_dl_missing_mar        <- .dl_integ_plotly(function() integ$fig_miss_mar,     "integ_missing_mar",        1000, 800)
  
  # ============================================================
  # INTEGRITY — INTEROPERABILITY OUTPUT
  # ============================================================
  
  output$integ_interoperability_table <- DT::renderDataTable({
    req(integ$filtered_analysis$interoperability$org_missing)
    org_data <- integ$filtered_analysis$interoperability$org_missing %>%
      dplyr::mutate(`Missing share` = ifelse(is.na(missing_share), "Not available",
                                             scales::percent(missing_share, accuracy=1))) %>%
      dplyr::select(`Organization type`=organization_type, `ID type`=id_type, `Missing share`)
    DT::datatable(org_data, options=list(pageLength=10, dom="t"), rownames=FALSE)
  })
  
  # ============================================================
  # INTEGRITY — DEFERRED: NETWORK ANALYSIS
  # ============================================================
  
  observeEvent(input$integ_run_network_analysis, {
    req(integ$filtered_data, integ$country_code)
    withProgress(message="Running network analysis...", value=0, {
      incProgress(0.2, detail="Computing market entry patterns...")
      tryCatch({
        config      <- safe_pipeline_config(integ$country_code)
        net_results <- safely_run_module(analyze_markets, integ$filtered_data, config, tempdir())
        integ$filtered_analysis$markets <- net_results
        integ$network_done <- TRUE
        incProgress(1.0, detail="Done.")
        showNotification("Network analysis complete!", type="message", duration=5)
      }, error=function(e) showNotification(paste("Network error:", e$message), type="error", duration=10))
    })
  })
  
  output$integ_network_status_ui <- renderUI({
    if (isTRUE(integ$network_done))
      div(class="alert alert-success", icon("check-circle"), tags$strong(" Network analysis complete. Plots shown below."))
    else
      div(class="alert alert-warning", icon("info-circle"), tags$strong(" Network analysis not yet run."), " Click the button below to compute.")
  })
  output$integ_network_done_flag <- reactive({ isTRUE(integ$network_done) })
  outputOptions(output, "integ_network_done_flag", suspendWhenHidden=FALSE)
  
  output$integ_net_cluster_filter_ui <- renderUI({
    req(integ$filtered_analysis$markets$unusual_matrix)
    mat <- integ$filtered_analysis$markets$unusual_matrix
    clusters <- sort(unique(c(mat$home_cpv_cluster, mat$target_cpv_cluster)))
    selectInput("integ_net_cluster_filter","Filter to specific clusters (leave blank = all):",
                choices=clusters, selected=NULL, multiple=TRUE, selectize=TRUE, width="100%")
  })
  output$integ_net_min_bidders_ui <- renderUI({
    req(integ$filtered_analysis$markets$unusual_matrix)
    mat <- integ$filtered_analysis$markets$unusual_matrix
    max_bid <- max(mat$n_bidders, na.rm=TRUE)
    sliderInput("integ_net_min_bidders","Min suppliers to show a route:", min=1, max=max(20,ceiling(max_bid/2)),
                value=min(4,ceiling(max_bid*0.1)), step=1, ticks=FALSE, width="100%")
  })
  output$integ_net_top_clusters_ui <- renderUI({
    req(integ$filtered_analysis$markets$unusual_matrix)
    mat <- integ$filtered_analysis$markets$unusual_matrix
    n_cl <- dplyr::n_distinct(c(mat$home_cpv_cluster, mat$target_cpv_cluster))
    sliderInput("integ_net_top_clusters","Max market clusters to show:", min=5, max=max(50,n_cl),
                value=min(20,n_cl), step=5, ticks=FALSE, width="100%")
  })
  
  integ_matrix_df <- reactive({
    req(integ$filtered_analysis$markets$unusual_matrix)
    mat       <- integ$filtered_analysis$markets$unusual_matrix
    min_bid   <- input$integ_net_min_bidders  %||% 4
    top_n     <- input$integ_net_top_clusters %||% 20
    cl_filter <- input$integ_net_cluster_filter
    edges <- mat %>% dplyr::rename(from=home_cpv_cluster, to=target_cpv_cluster) %>%
      dplyr::filter(n_bidders >= min_bid, from != to)
    if (!is.null(cl_filter) && length(cl_filter) > 0)
      edges <- edges %>% dplyr::filter(from %in% cl_filter | to %in% cl_filter)
    top_clusters <- edges %>%
      tidyr::pivot_longer(c(from,to), values_to="cluster") %>%
      dplyr::count(cluster, wt=n_bidders, sort=TRUE) %>%
      dplyr::slice_head(n=top_n) %>% dplyr::pull(cluster)
    edges %>%
      dplyr::filter(from %in% top_clusters, to %in% top_clusters) %>%
      dplyr::mutate(from=factor(from,levels=rev(top_clusters)), to=factor(to,levels=top_clusters),
                    tooltip=paste0("<b>",from," → ",to,"</b><br>Suppliers: <b>",n_bidders,"</b><br>Avg surprise (z): ",round(mean_surprise,2)))
  })
  
  output$integ_flow_matrix_plot_ui <- renderUI({
    req(integ$network_done, integ$filtered_analysis$markets$unusual_matrix)
    df <- tryCatch(integ_matrix_df(), error=function(e) NULL)
    if (is.null(df) || nrow(df)==0)
      return(div(class="alert alert-warning","No routes meet the current filter settings."))
    n <- dplyr::n_distinct(levels(df$from)); h <- max(280, min(600, n*26+80))
    plotlyOutput("integ_flow_matrix_plot", height=paste0(h,"px"), width="100%")
  })
  output$integ_flow_matrix_plot <- renderPlotly({
    df <- integ_matrix_df(); req(nrow(df) > 0)
    n_cl <- dplyr::n_distinct(levels(df$from))
    txt_size  <- max(3.2, min(6.0, 56/max(n_cl,1)))
    axis_size <- max(9,   min(14, 120/max(n_cl,1)))
    h <- max(280, min(600, n_cl*26+80))
    p <- ggplot2::ggplot(df, ggplot2::aes(x=to, y=from, fill=n_bidders, text=tooltip)) +
      ggplot2::geom_tile(colour="white", linewidth=0.6) +
      ggplot2::geom_text(ggplot2::aes(label=n_bidders,
                                      colour=dplyr::if_else(n_bidders>max(n_bidders,na.rm=TRUE)*0.55,"l","d")),
                         size=txt_size, fontface="bold", show.legend=FALSE) +
      ggplot2::scale_colour_manual(values=c(l="white",d="#1a252f"), guide="none") +
      ggplot2::scale_fill_gradientn(colours=c("#f0f7ff","#93c6e0","#2471a3","#1a5276"), na.value="grey95",
                                    name="Suppliers crossing") +
      ggplot2::scale_x_discrete(position="top") +
      ggplot2::labs(x="↓ Target market", y="Home market →") +
      pa_theme() +
      ggplot2::theme(axis.text.x=ggplot2::element_text(angle=40,hjust=0,size=axis_size,face="bold"),
                     axis.text.y=ggplot2::element_text(size=axis_size,face="bold"),
                     panel.grid=ggplot2::element_blank(), legend.position="right")
    ggplotly(p, tooltip="text", height=h) %>%
      plotly::config(responsive=TRUE) %>%
      layout(hoverlabel=list(bgcolor="white",font=list(size=12)), margin=list(l=5,r=5,b=5,t=15), autosize=TRUE) %>%
      pa_config()
  })
  output$integ_network_plot_ui <- renderUI({
    req(integ$network_done, integ$filtered_analysis$markets$unusual_matrix)
    plotOutput("integ_network_plot", height="720px")
  })
  output$integ_network_plot <- renderPlot({
    req(integ$filtered_analysis$markets$unusual_matrix)
    set.seed(42)
    build_network_graph_from_matrix(
      unusual_matrix = integ$filtered_analysis$markets$unusual_matrix,
      min_bidders    = input$integ_net_min_bidders  %||% 4,
      top_n          = input$integ_net_top_clusters %||% 20,
      cl_filter      = if (length(input$integ_net_cluster_filter)==0) NULL else input$integ_net_cluster_filter,
      country        = integ$country_code %||% ""
    )
  }, res=110)
  output$integ_download_network_ui <- renderUI({
    req(integ$network_done)
    downloadButton("integ_dl_network", "Download Flow Matrix", class="download-btn btn-sm")
  })
  output$integ_download_network_graph_ui <- renderUI({
    req(integ$network_done, integ$filtered_analysis$markets$unusual_matrix)
    downloadButton("integ_dl_network_graph", "Download Network Graph", class="download-btn btn-sm")
  })
  output$integ_supplier_unusual_plot_ui <- renderUI({
    if (isTRUE(integ$network_done) && !is.null(integ$filtered_analysis$markets$supplier_unusual_plot))
      plotlyOutput("integ_supplier_unusual_plot", height="420px")
    else div(class="deferred-box", icon("clock"), " Run Network Analysis above to see this plot.")
  })
  output$integ_supplier_unusual_plot <- renderPlotly({
    req(integ$network_done, integ$filtered_analysis$markets$supplier_unusual_plot)
    post_process_plotly(
      ggplotly(integ$filtered_analysis$markets$supplier_unusual_plot, tooltip="text")
    ) %>% layout(hoverlabel=list(bgcolor="white",font=list(size=10)),
                 margin=list(l=120,r=40,b=60,t=20)) %>%
      pa_config() -> .stored_fig
    integ$fig_supp_unusual <- .stored_fig
    .stored_fig
  })
  output$integ_download_supplier_unusual_ui <- renderUI({
    req(integ$network_done)
    downloadButton("integ_dl_supplier_unusual", "Download Figure", class="download-btn btn-sm")
  })
  output$integ_market_unusual_plot_ui <- renderUI({
    if (isTRUE(integ$network_done) && !is.null(integ$filtered_analysis$markets$market_unusual_plot))
      plotlyOutput("integ_market_unusual_plot", height="420px")
    else div(class="deferred-box", icon("clock"), " Run Network Analysis above to see this plot.")
  })
  output$integ_market_unusual_plot <- renderPlotly({
    req(integ$network_done, integ$filtered_analysis$markets$market_unusual_plot)
    post_process_plotly(
      ggplotly(integ$filtered_analysis$markets$market_unusual_plot, tooltip="text")
    ) %>% layout(hoverlabel=list(bgcolor="white",font=list(size=10)),
                 margin=list(l=60,r=40,b=60,t=20)) %>%
      pa_config() -> .stored_fig
    integ$fig_mkt_unusual <- .stored_fig
    .stored_fig
  })
  output$integ_download_market_unusual_ui <- renderUI({
    req(integ$network_done)
    downloadButton("integ_dl_market_unusual", "Download Figure", class="download-btn btn-sm")
  })
  
  # ============================================================
  # INTEGRITY — CONCENTRATION PLOT
  # ============================================================
  
  output$integ_conc_n_buyers_slider_ui <- renderUI({
    req(integ$filtered_analysis$competition$concentration_yearly_data)
    sliderInput("integ_conc_n_buyers","Buyers per year to show:", min=5, max=30, value=10, step=1, ticks=FALSE, width="90%")
  })
  output$integ_conc_min_contracts_slider_ui <- renderUI({
    req(integ$filtered_analysis$competition$concentration_yearly_data)
    d <- integ$filtered_analysis$competition$concentration_yearly_data
    mean_c <- round(mean(d$total_contracts, na.rm=TRUE)/10)*10
    sliderInput("integ_conc_min_contracts",
                paste0("Min contracts per buyer-year (dataset mean: ", scales::comma(mean_c), "):"),
                min=1, max=200, value=min(50, max(1,mean_c)), step=1, ticks=FALSE, width="90%")
  })
  output$integ_concentration_plot <- renderPlotly({
    req(integ$filtered_analysis$competition$concentration_yearly_data)
    d         <- integ$filtered_analysis$competition$concentration_yearly_data
    n_buyers  <- input$integ_conc_n_buyers      %||% 10
    min_contr <- input$integ_conc_min_contracts %||% 1
    p <- build_concentration_yearly_plot(yearly_data=d, n_buyers=n_buyers, min_contracts=min_contr, country=integ$country_code %||% "")
    if (is.null(p)) return(plotly::plot_ly(type = "scatter", mode = "markers") %>% plotly::layout(title="No data meets the current filters."))
    n_years <- dplyr::n_distinct(d %>% dplyr::filter(total_contracts >= min_contr) %>% dplyr::pull(tender_year))
    n_cols  <- min(max(n_years,1), 3); n_rows <- ceiling(n_years/n_cols)
    h       <- max(300, n_rows*(n_buyers*16+80))
    
    # Detect buyer name column in the source data
    buy_nm_col <- if ("buyer_name"           %in% names(integ$filtered_data)) "buyer_name"
    else if ("buyer_normalized_name" %in% names(integ$filtered_data)) "buyer_normalized_name"
    else if ("buyer_normalizedname"  %in% names(integ$filtered_data)) "buyer_normalizedname"
    else NULL
    
    # Build a buyer_id -> buyer_name lookup from filtered data
    buyer_lkp <- if (!is.null(buy_nm_col) && "buyer_masterid" %in% names(integ$filtered_data)) {
      integ$filtered_data %>%
        dplyr::filter(!is.na(buyer_masterid), !is.na(.data[[buy_nm_col]])) %>%
        dplyr::distinct(buyer_masterid = as.character(buyer_masterid),
                        buyer_name_lkp = as.character(.data[[buy_nm_col]]))
    } else NULL
    
    # Convert ggplot to plotly, then patch hover text to include buyer name
    fig_raw <- post_process_plotly(ggplotly(p, tooltip="text", height=h))
    if (!is.null(buyer_lkp)) {
      pb <- plotly::plotly_build(fig_raw)
      for (i in seq_along(pb$x$data)) {
        tr <- pb$x$data[[i]]
        if (is.null(tr$text) || length(tr$text) == 0) next
        # Extract buyer ID from first <b>...</b> per tooltip element
        buyer_ids <- regmatches(tr$text,
                                regexpr("(?<=<b>)[^<]+(?=</b>)", tr$text, perl = TRUE))
        if (length(buyer_ids) != length(tr$text)) next
        matched <- buyer_lkp$buyer_name_lkp[match(buyer_ids, buyer_lkp$buyer_masterid)]
        has_name <- !is.na(matched)
        if (any(has_name)) {
          # Use mapply so replacement is scalar per element
          pb$x$data[[i]]$text <- mapply(function(txt, nm, has) {
            if (!has) return(txt)
            # Prepend the human name before the existing tooltip (which starts with the ID)
            paste0("<b>", nm, "</b><br>", txt)
          }, tr$text, matched, has_name, SIMPLIFY = TRUE, USE.NAMES = FALSE)
          pb$x$data[[i]]$hoverinfo <- "text"
        }
      }
      fig_raw <- pb
    }
    
    fig_raw %>%
      layout(hoverlabel=list(bgcolor="white",font=list(size=11)),
             margin=list(l=100,r=160,b=60,t=20),
             legend=list(orientation="v",x=1.02,y=0.5,xanchor="left",font=list(size=10))) %>%
      pa_config() -> .stored_fig
    integ$fig_concentration <- .stored_fig
    .stored_fig
  })
  output$integ_concentration_plot_height <- renderUI({
    req(integ$filtered_analysis$competition$concentration_yearly_data)
    d <- integ$filtered_analysis$competition$concentration_yearly_data
    n_buyers  <- input$integ_conc_n_buyers      %||% 10
    min_contr <- input$integ_conc_min_contracts %||% 1
    n_years   <- dplyr::n_distinct(d %>% dplyr::filter(total_contracts >= min_contr) %>% dplyr::pull(tender_year))
    n_cols    <- min(max(n_years,1),3); n_rows <- ceiling(n_years/n_cols)
    h         <- max(300, n_rows*(n_buyers*16+80))   # matches renderPlotly
    tags$style(paste0("#integ_concentration_plot { height: ",h,"px !important; }"))
  })
  output$integ_dl_concentration <- .dl_integ_plotly(
    function() integ$fig_concentration, "integ_concentration", 1400, 900
  )
  
  # ============================================================
  # INTEGRITY — DEFERRED: REGRESSION ANALYSIS
  # ============================================================
  
  output$integ_regression_status_box <- renderUI({
    if (isTRUE(integ$regression_done))
      div(class = "reg-status-ok", icon("check-circle"), " Regression results available and up to date.")
    else
      div(class = "reg-status-wait", icon("clock"), " No results yet. Set your filters, then click Run.")
  })
  observeEvent(input$integ_run_regressions_now, {
    req(integ$filtered_data, integ$country_code)
    withProgress(message="Running integrity regression analysis...", value=0, {
      tryCatch({
        config <- safe_pipeline_config(integ$country_code)
        incProgress(0.1, detail="Single-bidding panel data...")
        comp_results <- safely_run_module(analyze_competition, integ$filtered_data, config, tempdir(),
                                          run_regressions=TRUE, save_plots=FALSE)
        if (!is.null(comp_results)) {
          integ$filtered_analysis$competition$singleb_data        <- comp_results$singleb_data
          integ$filtered_analysis$competition$singleb_specs       <- comp_results$singleb_specs
          integ$filtered_analysis$competition$singleb_sensitivity <- comp_results$singleb_sensitivity
          integ$filtered_analysis$competition$singleb_plot        <- comp_results$singleb_plot
        }
        incProgress(0.5, detail="Price regressions...")
        price_results <- safely_run_module(analyze_prices, integ$filtered_data, config, tempdir())
        integ$filtered_analysis$prices <- price_results
        integ$regression_done <- TRUE
        incProgress(1.0, detail="Done.")
        showNotification("Regression analysis complete!", type="message", duration=5)
      }, error=function(e) showNotification(paste("Regression error:", e$message), type="error", duration=10))
    })
  })
  
  output$integ_singleb_plot_ui <- renderUI({
    if (isTRUE(integ$regression_done) && !is.null(integ$filtered_analysis$prices$singleb_plot))
      plotlyOutput("integ_singleb_plot", height="500px")
    else if (isTRUE(integ$regression_done))
      div(class="alert alert-warning", icon("info-circle"), " The single-bidding model could not be produced with the current filters.")
    else div(class="deferred-box", icon("clock"), " Click 'Run / Re-run Regression Analysis' above.")
  })
  output$integ_singleb_plot <- renderPlotly({
    req(integ$regression_done, integ$filtered_analysis$prices$singleb_plot)
    post_process_plotly(
      ggplotly(integ$filtered_analysis$prices$singleb_plot)
    ) %>% layout(hoverlabel=list(bgcolor="white",font=list(size=10)),
                 margin=list(l=80,r=30,b=60,t=20)) %>%
      pa_config() -> .stored_fig
    integ$fig_singleb <- .stored_fig
    .stored_fig
  })
  output$integ_download_singleb_ui <- renderUI({
    req(integ$regression_done, integ$filtered_analysis$prices$singleb_plot)
    downloadButton("integ_dl_singleb", "Download Figure", class="download-btn btn-sm")
  })
  output$integ_relprice_plot_ui <- renderUI({
    if (isTRUE(integ$regression_done) && !is.null(integ$filtered_analysis$prices$rel_price_plot))
      plotlyOutput("integ_relprice_plot", height="500px")
    else if (isTRUE(integ$regression_done))
      div(class="alert alert-warning", icon("info-circle"), " The relative price model could not be produced with the current filters.")
    else div(class="deferred-box", icon("clock"), " Click 'Run / Re-run Regression Analysis' above.")
  })
  output$integ_relprice_plot <- renderPlotly({
    req(integ$regression_done, integ$filtered_analysis$prices$rel_price_plot)
    post_process_plotly(
      ggplotly(integ$filtered_analysis$prices$rel_price_plot)
    ) %>% layout(hoverlabel=list(bgcolor="white",font=list(size=10)),
                 margin=list(l=80,r=30,b=60,t=20)) %>%
      pa_config() -> .stored_fig
    integ$fig_relprice <- .stored_fig
    .stored_fig
  })
  output$integ_download_relprice_ui <- renderUI({
    req(integ$regression_done, integ$filtered_analysis$prices$rel_price_plot)
    downloadButton("integ_dl_relprice", "Download Figure", class="download-btn btn-sm")
  })
  
  output$integ_singleb_sensitivity_table <- renderUI({
    if (!isTRUE(integ$regression_done))
      return(div(class="deferred-box", icon("clock"), " Run regression analysis to see sensitivity results."))
    bundle <- integ$filtered_analysis$competition$singleb_sensitivity %||%
      integ$filtered_analysis$prices$singleb_sensitivity
    if (is.null(bundle)) return(p("Sensitivity bundle not available."))
    p("Sensitivity analysis complete. Results available in the regression output.")
  })
  output$integ_relprice_sensitivity_table <- renderUI({
    if (!isTRUE(integ$regression_done))
      return(div(class="deferred-box", icon("clock"), " Run regression analysis to see sensitivity results."))
    bundle <- integ$filtered_analysis$prices$relprice_sensitivity
    if (is.null(bundle)) return(p("Sensitivity bundle not available."))
    p("Relative price sensitivity analysis complete.")
  })
  
  # ── Integrity download handlers — all use stored plotly figs ──────────
  output$integ_dl_singleb          <- .dl_integ_plotly(function() integ$fig_singleb,      "integ_singleb",          1000, 700)
  output$integ_dl_relprice         <- .dl_integ_plotly(function() integ$fig_relprice,      "integ_relprice",         1000, 700)
  output$integ_dl_supplier_unusual <- .dl_integ_plotly(function() integ$fig_supp_unusual,  "integ_supplier_unusual", 1000, 700)
  output$integ_dl_market_unusual   <- .dl_integ_plotly(function() integ$fig_mkt_unusual,   "integ_market_unusual",   1000, 700)
  output$integ_dl_network <- downloadHandler(
    filename = function() paste0("integ_market_flow_matrix_", integ$country_code, ".png"),
    content  = function(file) {
      req(integ$filtered_analysis$markets$unusual_matrix)
      df <- tryCatch(integ_matrix_df(), error=function(e) NULL); req(df, nrow(df)>0)
      n_cl <- dplyr::n_distinct(levels(df$from)); axis_size <- max(9, min(14, 120/max(n_cl,1))); txt_size <- max(3.2, min(6.0, 56/max(n_cl,1)))
      p <- ggplot2::ggplot(df, ggplot2::aes(x=to, y=from, fill=n_bidders)) +
        ggplot2::geom_tile(colour="white", linewidth=0.6) +
        ggplot2::geom_text(ggplot2::aes(label=n_bidders, colour=dplyr::if_else(n_bidders>max(n_bidders,na.rm=TRUE)*0.55,"l","d")),
                           size=txt_size, fontface="bold", show.legend=FALSE) +
        ggplot2::scale_colour_manual(values=c(l="white",d="#1a252f"), guide="none") +
        ggplot2::scale_fill_gradientn(colours=c("#f0f7ff","#93c6e0","#2471a3","#1a5276"), na.value="grey95") +
        ggplot2::scale_x_discrete(position="top") +
        ggplot2::labs(x="↓ Target market", y="Home market →",
                      title=paste("Unusual Market Entry Flow -", integ$country_code %||% "")) +
        pa_theme() +
        ggplot2::theme(axis.text.x=ggplot2::element_text(angle=40,hjust=0,size=axis_size,face="bold"),
                       axis.text.y=ggplot2::element_text(size=axis_size,face="bold"),
                       panel.grid=ggplot2::element_blank(), plot.background=ggplot2::element_rect(fill="white",colour=NA))
      h_in <- max(6, min(16, n_cl*0.55+3)); ggplot2::ggsave(file, p, width=14, height=h_in, dpi=300, bg="white")
    }
  )
  output$integ_dl_network_graph <- downloadHandler(
    filename = function() paste0("integ_market_network_", integ$country_code, ".png"),
    content  = function(file) {
      req(integ$filtered_analysis$markets$unusual_matrix)
      set.seed(42)
      p <- build_network_graph_from_matrix(unusual_matrix=integ$filtered_analysis$markets$unusual_matrix,
                                           min_bidders=isolate(input$integ_net_min_bidders) %||% 4,
                                           top_n=isolate(input$integ_net_top_clusters) %||% 20,
                                           cl_filter=isolate(input$integ_net_cluster_filter), country=integ$country_code %||% "")
      ggplot2::ggsave(file, p, width=14, height=11, dpi=300, bg="white")
    }
  )
  
  # ============================================================
  # INTEGRITY — EXPORT (Word report + ZIP)
  # ============================================================
  
  output$integ_dl_word_report <- downloadHandler(
    filename = function() paste0("procurement_integrity_", integ$country_code,
                                 "_", format(Sys.Date(), "%Y%m%d"), ".docx"),
    content  = function(file) {
      req(integ$data, integ$filtered_analysis, integ$country_code)
      withProgress(message = "Generating procurement integrity Word report...", value = 0, {
        incProgress(0.3, detail = "Preparing data...")
        filter_desc  <- get_filter_description(integ_filters$active)
        filters_text <- if (filter_desc == "No filters applied") "" else paste0("Applied Filters: ", filter_desc)
        incProgress(0.6, detail = "Creating document...")
        ok <- generate_integrity_word_report(
          filtered_data     = integ$filtered_data,
          filtered_analysis = integ$filtered_analysis,
          country_code      = integ$country_code,
          output_file       = file,
          filters_text      = filters_text
        )
        output$export_status <- renderText(if (ok) "Procurement integrity Word report generated!" else "Error generating integrity Word report.")
      })
    }
  )
  output$integ_dl_all_figures <- downloadHandler(
    filename = function() paste0("integ_all_figures_", integ$country_code, "_", format(Sys.Date(), "%Y%m%d"), ".zip"),
    content  = function(file) {
      req(integ$data, integ$filtered_analysis, integ$country_code)
      withProgress(message = "Creating integrity figures ZIP...", value = 0, {
        incProgress(0.1, detail = "Collecting figures...")
        temp_dir <- tempfile(); dir.create(temp_dir)
        cc <- integ$country_code
        
        # Use the stored plotly figs (exactly what is displayed)
        figs <- list(
          list(fig = integ$fig_contracts_year, name = "integ_contracts_year",   vw = 1200, vh = 600),
          list(fig = integ$fig_miss_overall,   name = "integ_missing_overall",  vw = 1200, vh = 700),
          list(fig = integ$fig_miss_buyer,     name = "integ_missing_buyer",    vw = 1200, vh = 800),
          list(fig = integ$fig_miss_time,      name = "integ_missing_time",     vw = 1200, vh = 700),
          list(fig = integ$fig_miss_proc,      name = "integ_missing_procedure",vw = 1000, vh = 600),
          list(fig = integ$fig_miss_cooc,      name = "integ_missing_cooccurrence", vw = 1000, vh = 800),
          list(fig = integ$fig_miss_mar,       name = "integ_missing_mar",      vw = 1000, vh = 800),
          list(fig = integ$fig_supp_unusual,   name = "integ_supplier_unusual", vw = 1000, vh = 700),
          list(fig = integ$fig_mkt_unusual,    name = "integ_market_unusual",   vw = 1000, vh = 700),
          list(fig = integ$fig_concentration,  name = "integ_concentration",    vw = 1400, vh = 900),
          list(fig = integ$fig_singleb,        name = "integ_singleb",          vw = 1000, vh = 700),
          list(fig = integ$fig_relprice,       name = "integ_relprice",         vw = 1000, vh = 700)
        )
        
        n_figs <- length(figs); saved <- 0
        for (i in seq_along(figs)) {
          incProgress(0.1 + i / n_figs * 0.85, detail = paste("Saving figure", i, "of", n_figs))
          entry <- figs[[i]]
          if (!is.null(entry$fig)) tryCatch({
            fp <- file.path(temp_dir, paste0(entry$name, "_", cc, ".png"))
            .save_fig_png(entry$fig, fp, entry$vw, entry$vh)
            saved <- saved + 1
          }, error = function(e) message("Could not save ", entry$name, ": ", e$message))
        }
        if (saved > 0) {
          zip::zip(zipfile = file, files = list.files(temp_dir, full.names = TRUE), mode = "cherry-pick")
          output$export_status <- renderText(paste0(saved, " integrity figures saved to ZIP."))
        } else {
          showNotification("No figures were saved — view the integrity charts first, then download.", type="warning", duration=8)
          output$export_status <- renderText("No figures saved. View charts first, then download.")
        }
        unlink(temp_dir, recursive = TRUE)
      })
    }
  )
  
} # close server

# ============================================================
shinyApp(ui = ui, server = server)