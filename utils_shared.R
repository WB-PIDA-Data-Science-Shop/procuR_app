# ========================================================================
# SHARED UTILITIES — Unified Procurement Analysis App
# ========================================================================
# Functions used by both the Economic Outcomes and Administrative
# Efficiency sections of the app.
# ========================================================================

`%ni%` <- Negate(`%in%`)
`%||%` <- function(a, b) if (!is.null(a)) a else b

# ========================================================================
# CPV LABEL LOOKUP
# ========================================================================

CPV_DESCRIPTIONS <- c(
  "03" = "Agricultural products",       "09" = "Petroleum, fuel, electricity",
  "14" = "Mining products",             "15" = "Food, beverages, tobacco",
  "16" = "Agricultural machinery",      "18" = "Clothing, footwear",
  "19" = "Leather, textile products",   "22" = "Printed matter",
  "24" = "Chemical products",           "30" = "Office/computing machinery",
  "31" = "Electrical machinery",        "32" = "Radio, TV, communication",
  "33" = "Medical equipment",           "34" = "Transport equipment",
  "35" = "Security equipment",          "37" = "Musical instruments",
  "38" = "Laboratory equipment",        "39" = "Furniture",
  "41" = "Collected water",             "42" = "Industrial machinery",
  "43" = "Mining/extraction machinery", "44" = "Construction structures",
  "45" = "Construction work",           "48" = "Software packages",
  "50" = "Repair/maintenance",          "51" = "Installation services",
  "55" = "Hotel/restaurant services",   "60" = "Transport services",
  "63" = "Supporting transport",        "64" = "Postal/telecom services",
  "65" = "Utility services",            "66" = "Financial services",
  "70" = "Real estate services",        "71" = "Architectural services",
  "72" = "IT services",                 "73" = "Research services",
  "75" = "Administration services",     "76" = "Services to oil/gas",
  "77" = "Agricultural services",       "79" = "Business services",
  "80" = "Education/training",          "85" = "Health/social services",
  "90" = "Sewage/refuse services",      "92" = "Recreational services",
  "98" = "Other services",              "99" = "Other"
)

get_cpv_label <- function(code) {
  desc <- CPV_DESCRIPTIONS[as.character(code)]
  desc <- ifelse(is.na(desc), paste0("CPV ", code), desc)
  paste0(code, " - ", desc)
}

# ========================================================================
# FILTER HELPERS
# ========================================================================

get_filter_description <- function(filter_list) {
  parts <- c()
  if (!is.null(filter_list$year)           && length(filter_list$year) == 2)
    parts <- c(parts, paste0("Years: ", filter_list$year[1], "-", filter_list$year[2]))
  if (!is.null(filter_list$market)         && length(filter_list$market) > 0 && "All" %ni% filter_list$market)
    parts <- c(parts, paste0("Markets: ", paste(filter_list$market, collapse = ", ")))
  if (!is.null(filter_list$value)          && length(filter_list$value) == 2)
    parts <- c(parts, "Value range applied")
  if (!is.null(filter_list$buyer_type)     && length(filter_list$buyer_type) > 0 && "All" %ni% filter_list$buyer_type)
    parts <- c(parts, paste0("Buyer types: ", paste(filter_list$buyer_type, collapse = ", ")))
  if (!is.null(filter_list$procedure_type) && length(filter_list$procedure_type) > 0 && "All" %ni% filter_list$procedure_type)
    parts <- c(parts, paste0("Procedures: ", paste(filter_list$procedure_type, collapse = ", ")))
  if (length(parts) == 0) return("No filters applied")
  paste(parts, collapse = "; ")
}

# ========================================================================
# VALUE SCALE HELPERS
# ========================================================================

compute_value_scale <- function(prices) {
  max_price <- max(prices, na.rm = TRUE)
  if (max_price > 1e9) {
    list(divisor = 1e9, label = "Contract Value (Billions USD):", pre = "$", post = "B",
         display_label = "Billions")
  } else if (max_price > 1e6) {
    list(divisor = 1e6, label = "Contract Value (Millions USD):", pre = "$", post = "M",
         display_label = "Millions")
  } else if (max_price > 1e3) {
    list(divisor = 1e3, label = "Contract Value (Thousands USD):", pre = "$", post = "K",
         display_label = "Thousands")
  } else {
    list(divisor = 1, label = "Contract Value (USD):", pre = "$", post = "",
         display_label = "USD")
  }
}

fmt_value <- function(v) {
  dplyr::case_when(
    v >= 1e9 ~ paste0(round(v / 1e9, 1), "B"),
    v >= 1e6 ~ paste0(round(v / 1e6, 1), "M"),
    v >= 1e3 ~ paste0(round(v / 1e3, 1), "K"),
    TRUE     ~ as.character(round(v))
  )
}

fmt_value_log <- function(lv) fmt_value(10^lv)

# ========================================================================
# DATA LOADING (shared — identical needs in econ + admin utils)
# ========================================================================

load_data <- function(input_path) {
  data <- data.table::fread(
    input            = input_path,
    keepLeadingZeros = TRUE,
    encoding         = "UTF-8",
    stringsAsFactors = FALSE,
    showProgress     = TRUE,
    na.strings       = c("", "-", "NA")
  )
  dup_cols <- duplicated(names(data))
  if (any(dup_cols)) data <- data[, !dup_cols, with = FALSE]
  data
}

# ========================================================================
# PROCEDURE TYPE RECODING (canonical — used by all three pipelines)
# ========================================================================

recode_procedure_type <- function(x) {
  LUT <- c(
    OPEN                           = "Open Procedure",
    RESTRICTED                     = "Restricted Procedure",
    NEGOTIATED_WITH_PUBLICATION    = "Negotiated with publications",
    NEGOTIATED_WITHOUT_PUBLICATION = "Negotiated without publications",
    NEGOTIATED                     = "Negotiated (unspecified)",
    COMPETITIVE_DIALOG             = "Competitive Dialogue",
    INNOVATION_PARTNERSHIP         = "Innovation Partnership",
    OUTRIGHT_AWARD                 = "Direct Award",
    OTHER                          = "Other",
    # pass-through already-recoded labels
    "Open Procedure"               = "Open Procedure",
    "Restricted Procedure"         = "Restricted Procedure",
    "Negotiated with publications" = "Negotiated with publications",
    "Negotiated without publications" = "Negotiated without publications",
    "Negotiated (unspecified)"     = "Negotiated (unspecified)",
    "Negotiated"                   = "Negotiated (unspecified)",
    "Competitive Dialogue"         = "Competitive Dialogue",
    "Competitive Dialog"           = "Competitive Dialogue",
    "Innovation Partnership"       = "Innovation Partnership",
    "Direct Award"                 = "Direct Award",
    "Other"                        = "Other",
    "Other Procedures"             = "Other"
  )
  raw <- as.character(x)
  idx <- match(raw, names(LUT))
  out <- LUT[idx]
  out[is.na(idx)] <- "Other"
  out[is.na(x)]   <- NA_character_
  unname(out)
}

# ========================================================================
# BUYER GROUPING (shared — identical in econ + admin utils)
# ========================================================================

add_buyer_group <- function(buyer_buyertype) {
  group <- dplyr::case_when(
    grepl("(?i)national",  buyer_buyertype) ~ "National Buyer",
    grepl("(?i)regional",  buyer_buyertype) ~ "Regional Buyer",
    grepl("(?i)utilities", buyer_buyertype) ~ "Utilities",
    grepl("(?i)european",  buyer_buyertype) ~ "EU agency",
    TRUE                                    ~ "Other Public Bodies"
  )
  factor(group, levels = c("National Buyer", "Regional Buyer", "Utilities",
                           "EU agency", "Other Public Bodies"))
}

# ========================================================================
# TENDER YEAR EXTRACTION (shared — flexible version)
# ========================================================================

add_tender_year <- function(df,
                            date_cols = c(
                              "tender_publications_firstcallfortenderdate",
                              "tender_awarddecisiondate",
                              "tender_biddeadline"
                            )) {
  get_year <- function(x) stringr::str_extract(x, "^\\d{4}")
  cols_present <- intersect(date_cols, names(df))
  if (length(cols_present) == 0) { df$tender_year <- NA_integer_; return(df) }
  year_vec <- purrr::reduce(
    cols_present,
    .init = rep(NA_character_, nrow(df)),
    .f    = function(acc, col) dplyr::coalesce(acc, get_year(df[[col]]))
  )
  df %>% dplyr::mutate(tender_year = as.integer(year_vec))
}

# ========================================================================
# DIRECTORY HELPER (shared)
# ========================================================================

dir_ensure <- function(path) {
  if (!dir.exists(path)) dir.create(path, recursive = TRUE, showWarnings = FALSE)
  invisible(path)
}

# ========================================================================
# WORD REPORT HELPERS (shared — avoids repeating inside each report fn)
# ========================================================================

#' Returns a named list of officer document-building closures.
#' cw = usable page width in inches (default 6.5 = 8.5 - 2*1 margins).
#' The returned list: h1_, h2_, par_, br_, pg_, add_fig
make_word_doc_fns <- function(cw = 6.5) {
  list(
    h1_  = function(d, t) officer::body_add_par(d, t, style = "heading 1"),
    h2_  = function(d, t) officer::body_add_par(d, t, style = "heading 2"),
    par_ = function(d, t) officer::body_add_par(d, t, style = "Normal"),
    br_  = function(d)    officer::body_add_par(d, "",  style = "Normal"),
    pg_  = function(d)    officer::body_add_break(d),
    add_fig = function(d, p, aspect = 0.60, label = NULL, max_h = 5.5) {
      if (is.null(p)) return(d)
      tmp <- tempfile(fileext = ".png")
      h   <- min(max_h, cw * aspect)
      tryCatch({
        ggplot2::ggsave(tmp, plot = p, width = cw, height = h, dpi = 180, bg = "white")
        if (!is.null(label)) d <- officer::body_add_par(d, label, style = "heading 2")
        d <- officer::body_add_img(d, src = tmp, width = cw, height = h)
        d <- officer::body_add_par(d, "", style = "Normal")
      }, error = function(e) message("add_fig error (", label, "): ", e$message))
      d
    }
  )
}

#' Compute summary stats for a Word report cover page.
#' Returns list: n_c, n_b, n_s, yr
compute_report_stats <- function(filtered_data) {
  list(
    n_c = format(nrow(filtered_data), big.mark = ","),
    n_b = if ("buyer_masterid"  %in% names(filtered_data))
      format(dplyr::n_distinct(filtered_data$buyer_masterid,  na.rm = TRUE), big.mark = ",")
    else "N/A",
    n_s = if ("bidder_masterid" %in% names(filtered_data))
      format(dplyr::n_distinct(filtered_data$bidder_masterid, na.rm = TRUE), big.mark = ",")
    else "N/A",
    yr  = if ("tender_year" %in% names(filtered_data)) {
      y <- sort(unique(filtered_data$tender_year[!is.na(filtered_data$tender_year)]))
      if (length(y) > 1) paste0(min(y), "\u2013", max(y)) else as.character(y[1L])
    } else "N/A"
  )
}