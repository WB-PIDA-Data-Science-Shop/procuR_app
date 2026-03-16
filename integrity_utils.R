# ========================================================================
# PROCUREMENT INTEGRITY ANALYSIS PIPELINE
# Version: 2.0
# Refactored for consistency, efficiency, and best practices
# ========================================================================

# ========================================================================
# PART 1: CONFIGURATION MANAGEMENT
# ========================================================================

#' Create pipeline configuration
#' 
#' @param country_code Two-letter country code
#' @return List of configuration parameters
create_pipeline_config <- function(country_code) {
  list(
    country = toupper(country_code),
    
    # Analysis thresholds
    thresholds = list(
      min_buyer_contracts = 100,
      min_suppliers_for_buyer_conc = 3,
      min_buyer_years = 3,
      cpv_digits = 3,
      min_bidders_for_edge = 4,
      top_n_buyers = 30,
      top_n_suppliers = 30,
      top_n_markets = 30,
      top_n_vars = 10,
      marginal_share_threshold = 0.05,
      max_wins_atypical = 3,
      min_history_threshold = 4,
      max_relative_price = 5,
      min_relative_price = 0
    ),
    
    # Year filters by component
    years = get_year_range(country_code, "default"),
    years_singleb = get_year_range(country_code, "singleb"),
    years_relprice = get_year_range(country_code, "rel_price"),
    
    # Model settings
    models = list(
      p_max = 0.10,
      fe_set = c("buyer", "year", "buyer+year", "buyer#year"),
      cluster_set = c("none", "buyer", "year", "buyer_year", "buyer_buyertype"),
      controls_set = c("x_only", "base", "base_extra"),
      model_types_relprice = c("ols_level", "ols_log", "gamma_log")
    ),
    
    # Plot settings
    plots = list(
      width = 10,
      height = 6,
      width_large = 12,
      height_large = 12,
      dpi = 300,
      base_size = 14
    )
  )
}

#' Year filter configuration for each country
year_filter_config <- tibble::tribble(
  ~component,  ~country_code, ~min_year, ~max_year,
  # default catch-all
  "default",   "BG",          NA,        NA,
  "default",   "UY",          NA,        NA,
  "default",   "UG",          NA,        NA,
  # component-specific overrides
  "singleb",   "BG",          2011,      2018,
  "singleb",   "UY",          2014,      NA,
  "singleb",   "UG",          NA,        NA,
  "rel_price", "BG",          2011,      2018,
  "rel_price", "UY",          2014,      NA,
  "rel_price", "UG",          NA,        NA
)

#' Get year range for specific component and country
#' 
#' @param country_code Two-letter country code
#' @param component One of "singleb", "rel_price", "default"
#' @return List with min_year and max_year
get_year_range <- function(country_code,
                           component = c("singleb", "rel_price", "default")) {
  component <- match.arg(component)
  cc <- toupper(country_code)
  
  # Try component-specific rule
  row_spec <- year_filter_config %>%
    dplyr::filter(component == !!component, country_code == !!cc) %>%
    dplyr::slice_head(n = 1)
  
  # Fall back to default for that country
  if (nrow(row_spec) == 0) {
    row_spec <- year_filter_config %>%
      dplyr::filter(component == "default", country_code == !!cc) %>%
      dplyr::slice_head(n = 1)
  }
  
  # If still nothing, no filtering
  if (nrow(row_spec) == 0) {
    return(list(min_year = -Inf, max_year = Inf))
  }
  
  min_y <- if (is.na(row_spec$min_year)) -Inf else row_spec$min_year
  max_y <- if (is.na(row_spec$max_year)) Inf else row_spec$max_year
  
  list(min_year = min_y, max_year = max_y)
}

# ========================================================================
# PART 2: LABEL DEFINITIONS AND LOOKUPS
# ========================================================================

#' Variable label lookup for display
label_lookup <- c(
  tender_id_missing_share = "Tender ID",
  tender_year_missing_share = "Tender Year",
  lot_number_missing_share = "Lot Number",
  bid_number_missing_share = "Number of Bids",
  bid_iswinning_missing_share = "Winning Bid",
  tender_country_missing_share = "Tender Country",
  tender_awarddecisiondate_missing_share = "Award Decision Date",
  tender_contractsignaturedate_missing_share = "Contract Signature Date",
  tender_biddeadline_missing_share = "Bid Deadline",
  tender_proceduretype_missing_share = "Procedure Type",
  tender_nationalproceduretype_missing_share = "National Procedure Type",
  tender_supplytype_missing_share = "Supply Type",
  tender_publications_firstcallfortenderdate_missing_share = "First Call for Tender Date",
  notice_url_missing_share = "Notice URL",
  source_missing_share = "Source",
  tender_publications_firstdcontractawarddate_missing_share = "First Contract Award Date",
  tender_publications_lastcontractawardurl_missing_share = "Last Contract Award URL",
  buyer_masterid_missing_share = "Buyer Master ID",
  buyer_id_missing_share = "Buyer ID",
  buyer_city_missing_share = "Buyer City",
  buyer_postcode_missing_share = "Buyer Postcode",
  buyer_country_missing_share = "Buyer Country",
  buyer_nuts_missing_share = "Buyer NUTS",
  buyer_name_missing_share = "Buyer Name",
  buyer_buyertype_missing_share = "Buyer Type",
  buyer_mainactivities_missing_share = "Buyer Main Activities",
  tender_addressofimplementation_country_missing_share = "Implementation Country",
  tender_addressofimplementation_nuts_missing_share = "Implementation NUTS",
  bidder_masterid_missing_share = "Bidder Master ID",
  bidder_id_missing_share = "Bidder ID",
  bidder_country_missing_share = "Bidder Country",
  bidder_nuts_missing_share = "Bidder NUTS",
  bidder_name_missing_share = "Bidder Name",
  bid_price_missing_share = "Bid Price",
  bid_priceusd_missing_share = "Bid Price (USD)",
  bid_pricecurrency_missing_share = "Bid Price Currency",
  lot_productcode_missing_share = "Product Code",
  lot_localproductcode_type_missing_share = "Local Product Code Type",
  lot_localproductcode_missing_share = "Local Product Code",
  submp_missing_share = "Submission Period",
  decp_missing_share = "Decision Period",
  is_capital_missing_share = "Capital City Indicator",
  lot_title_missing_share = "Lot Title",
  lot_estimatedpriceusd_missing_share = "Estimated Price (USD)",
  lot_estimatedprice_missing_share = "Estimated Price",
  lot_estimatedpricecurrency_missing_share = "Estimated Price Currency"
)

#' Apply label lookup to variable names
#' 
#' @param vec Character vector of variable names
#' @param lookup Named character vector of labels
#' @return Character vector with labels applied
label_with_lookup <- function(vec, lookup) {
  out <- lookup[vec]
  # For names with no label match, strip _missing_share suffix and clean underscores
  no_match      <- is.na(out)
  fallback      <- gsub("_missing_share$", "", vec[no_match])
  fallback      <- gsub("_", " ", fallback)
  fallback      <- tools::toTitleCase(fallback)
  out[no_match] <- fallback
  unname(out)
}

#' Procedure type labels for display
procedure_type_labels <- c(
  "OPEN" = "Open (competitive bidding)",
  "RESTRICTED" = "Restricted (limited competition)",
  "NEGOTIATED_WITH_PUBLICATION" = "Negotiated with publication (limited competition)",
  "NEGOTIATED_WITHOUT_PUBLICATION" = "Negotiated without publication (no competition)",
  "NEGOTIATED" = "Negotiated (limited competition)",
  "COMPETITIVE_DIALOG" = "Competitive dialogue (limited competition)",
  "OUTRIGHT_AWARD" = "Outright award (direct purchase)",
  "OTHER" = "Other (special or exceptional procedures)",
  "Missing procedure type" = "Missing procedure type"
)

# ========================================================================
# PART 2A: STANDARD PLOT THEME SETTINGS
# ========================================================================

#' Standard text sizes for all plots
#' These are calibrated for fig.width=14, fig.height=12
# In PLOT_SIZES (at the top of the file):
PLOT_SIZES <- list(
  base_size = 11,
  title_size = 13,
  subtitle_size = 10,
  axis_title_size = 11,
  axis_text_size = 10,
  legend_title_size = 10,
  legend_text_size = 9,
  geom_text_size = 3.5,
  geom_text_large = 4,
  line_size = 1.2,
  point_size = 3
)

#' Apply standard theme to a ggplot object
#' 
#' @param base_size Base font size
#' @return ggplot2 theme
standard_plot_theme <- function(base_size = PLOT_SIZES$base_size) {
  ggplot2::theme_minimal(base_size = base_size) +
    ggplot2::theme(
      plot.title       = ggplot2::element_text(size = PLOT_SIZES$title_size, face = "bold"),
      plot.subtitle    = ggplot2::element_text(size = PLOT_SIZES$subtitle_size),
      axis.title       = ggplot2::element_text(size = PLOT_SIZES$axis_title_size),
      axis.text        = ggplot2::element_text(size = PLOT_SIZES$axis_text_size),
      legend.title     = ggplot2::element_text(size = PLOT_SIZES$legend_title_size),
      legend.text      = ggplot2::element_text(size = PLOT_SIZES$legend_text_size),
      plot.background  = ggplot2::element_rect(fill = "white", colour = NA),
      panel.background = ggplot2::element_rect(fill = "white", colour = NA),
      plot.margin      = ggplot2::margin(4, 8, 2, 4)
    )
}

#' Add explicit white background to any ggplot (prevents blue tint in PNG exports)
white_bg <- function() {
  ggplot2::theme(
    plot.background  = ggplot2::element_rect(fill = "white", colour = NA),
    panel.background = ggplot2::element_rect(fill = "white", colour = NA)
  )
}

# ========================================================================
# PART 3: DATA LOADING AND VALIDATION
# ========================================================================

#' Load data with consistent settings
#' 
#' @param input_path Path to input file
#' @return data.table
load_data <- function(input_path) {
  data <- data.table::fread(
    input = input_path,
    keepLeadingZeros = TRUE,
    encoding = "UTF-8",
    stringsAsFactors = FALSE,
    showProgress = TRUE,
    na.strings = c("", "-", "NA")
  )
  
  # Drop duplicated column names
  dup_cols <- duplicated(names(data))
  if (any(dup_cols)) {
    warning("Dropping ", sum(dup_cols), " duplicated columns")
    data <- data[, !dup_cols, with = FALSE]
  }
  
  return(data)
}

#' Validate required columns exist
#' 
#' @param df Data frame to check
#' @param required_cols Character vector of required column names
#' @param action_name Name of action requiring columns (for messages)
#' @return Logical indicating whether all columns exist
validate_required_columns <- function(df, required_cols, action_name = "this action") {
  missing <- setdiff(required_cols, names(df))
  
  if (length(missing) > 0) {
    message(sprintf(
      "Skipping %s: missing columns [%s]",
      action_name,
      paste(missing, collapse = ", ")
    ))
    return(FALSE)
  }
  TRUE
}

#' Check overall data quality
#' 
#' @param df Data frame to check
#' @param config Configuration list
#' @return List of quality metrics
check_data_quality <- function(df, config) {
  list(
    n_rows = nrow(df),
    n_cols = ncol(df),
    has_buyer_id = "buyer_masterid" %in% names(df),
    has_bidder_id = "bidder_masterid" %in% names(df),
    has_tender_year = "tender_year" %in% names(df),
    has_price = "bid_price" %in% names(df) | "bid_priceusd" %in% names(df),
    year_range = if ("tender_year" %in% names(df)) {
      range(df$tender_year, na.rm = TRUE)
    } else {
      NULL
    },
    n_unique_buyers = if ("buyer_masterid" %in% names(df)) {
      dplyr::n_distinct(df$buyer_masterid, na.rm = TRUE)
    } else {
      NA_integer_
    },
    n_unique_bidders = if ("bidder_masterid" %in% names(df)) {
      dplyr::n_distinct(df$bidder_masterid, na.rm = TRUE)
    } else {
      NA_integer_
    }
  )
}

# ========================================================================
# PART 4: DATA PREPARATION HELPERS
# ========================================================================

#' Add tender year from available date fields
#' 
#' @param df Data frame
#' @return Data frame with tender_year column
add_tender_year <- function(df) {
  df %>%
    dplyr::mutate(
      tender_year = dplyr::coalesce(
        stringr::str_extract(tender_publications_firstcallfortenderdate, "^\\d{4}"),
        stringr::str_extract(tender_awarddecisiondate, "^\\d{4}"),
        stringr::str_extract(tender_biddeadline, "^\\d{4}")
      ),
      tender_year = as.integer(tender_year)
    )
}

#' Add buyer group classification
#' 
#' @param buyer_buyertype Character vector of buyer types
#' @return Factor with buyer groups
add_buyer_group <- function(buyer_buyertype) {
  group <- dplyr::case_when(
    grepl("(?i)national", buyer_buyertype) ~ "National Buyer",
    grepl("(?i)regional", buyer_buyertype) ~ "Regional Buyer",
    grepl("(?i)utilities", buyer_buyertype) ~ "Utilities",
    grepl("(?i)European", buyer_buyertype) ~ "EU agency",
    TRUE ~ "Other Public Bodies"
  )
  
  factor(
    group,
    levels = c(
      "National Buyer",
      "Regional Buyer",
      "Utilities",
      "EU agency",
      "Other Public Bodies"
    )
  )
}

#' Standardize missing values
#' 
#' @param df Data frame
#' @return Data frame with standardized NAs
standardize_missing_values <- function(df) {
  df %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::where(is.character),
        ~ dplyr::na_if(., "")
      )
    )
}

#' Add commonly used derived variables
#' 
#' @param df Data frame
#' @return Data frame with derived variables
add_derived_variables <- function(df) {
  df <- df %>%
    dplyr::mutate(
      has_buyer_id = !is.na(buyer_masterid),
      has_bidder_id = !is.na(bidder_masterid),
      has_price = !is.na(bid_price) | !is.na(bid_priceusd)
    )
  
  # Add buyer group if buyertype exists
  if ("buyer_buyertype" %in% names(df)) {
    df <- df %>%
      dplyr::mutate(buyer_group = add_buyer_group(buyer_buyertype))
  }
  
  df
}

#' Clean NUTS3 codes
#' 
#' @param df Data frame
#' @param nuts_col Column name containing NUTS codes
#' @return Data frame with cleaned nuts3 column
clean_nuts3 <- function(df, nuts_col = buyer_nuts) {
  nuts_quo <- rlang::enquo(nuts_col)
  
  df %>%
    dplyr::mutate(
      buyer_nuts = as.character(!!nuts_quo),
      nuts_clean = buyer_nuts %>%
        stringr::str_replace_all("\\[|\\]", "") %>%
        stringr::str_replace_all('"', "") %>%
        stringr::str_squish(),
      nuts3 = dplyr::if_else(
        stringr::str_detect(nuts_clean, "^[A-Z]{2}[0-9]{3}$"),
        nuts_clean,
        NA_character_
      )
    )
}

#' Prepare data for analysis
#' 
#' @param df Data frame
#' @return Prepared data frame
prepare_data <- function(df) {
  df %>%
    add_tender_year() %>%
    standardize_missing_values() %>%
    add_derived_variables()
}

# ========================================================================
# PART 5: MISSING VALUE ANALYSIS
# ========================================================================

#' Compute missing shares across columns
#' 
#' @param df Data frame
#' @param cols Columns to analyze (tidy-select)
#' @return Data frame with missing share columns
#' Compute missing shares across columns
summarise_missing_shares <- function(df, cols = !dplyr::starts_with("ind_")) {
  df %>%
    dplyr::summarise(
      dplyr::across(
        .cols = c({{ cols }}) & !dplyr::ends_with("_missing_share"),
        .fns = ~ mean(is.na(.)),
        .names = "{.col}_missing_share"
      ),
      .groups = "drop"
    )
}

#' Pivot missing shares to long format
#' 
#' @param df Data frame with missing share columns
#' @param id_vars ID columns to keep
#' @return Long format data frame
pivot_missing_long <- function(df, id_vars = NULL) {
  if (is.null(id_vars)) {
    df %>%
      tidyr::pivot_longer(
        cols = dplyr::everything(),
        names_to = "variable",
        values_to = "missing_share"
      )
  } else {
    df %>%
      tidyr::pivot_longer(
        cols = -dplyr::all_of(id_vars),
        names_to = "variable",
        values_to = "missing_share"
      )
  }
}

#' Compute organization-level missing shares for interoperability
#' 
#' @param df Data frame
#' @return Data frame with organization type and missing shares
compute_org_missing <- function(df) {
  miss_or_na <- function(col) {
    if (col %in% names(df)) {
      mean(is.na(df[[col]]))
    } else {
      NA_real_
    }
  }
  
  tibble::tribble(
    ~organization_type, ~id_type, ~missing_share,
    "Supplier", "Source ID", miss_or_na("bidder_id"),
    "Supplier", "Generated ID", miss_or_na("bidder_masterid"),
    "Supplier", "Name", miss_or_na("bidder_name"),
    "Supplier", "Address (postcode)", miss_or_na("bidder_nuts"),
    "Buyer", "Source ID", miss_or_na("buyer_id"),
    "Buyer", "Generated ID", miss_or_na("buyer_masterid"),
    "Buyer", "Name", miss_or_na("buyer_name"),
    "Buyer", "Address (postcode)", miss_or_na("buyer_postcode")
  )
}

#' Compute missing correlations
#' 
#' @param df Data frame
#' @return Correlation matrix
compute_missing_correlations <- function(df) {
  miss_matrix <- df %>%
    dplyr::select(!dplyr::starts_with("ind_")) %>%
    dplyr::mutate(dplyr::across(dplyr::everything(), ~ as.numeric(is.na(.))))
  
  corrr::correlate(miss_matrix)
}

# ========================================================================
# PART 6: PLOTTING FUNCTIONS
# ========================================================================

#' Create plot saver function
#' 
#' @param output_dir Output directory
#' @param config Configuration list
#' @return Function to save plots
create_plot_saver <- function(output_dir, config) {
  function(plot, filename, width = NULL, height = NULL) {
    if (is.null(plot)) return(invisible(NULL))
    # Skip saving if output directory doesn't exist (e.g. in Shiny where we don't need disk output)
    if (!dir.exists(output_dir)) return(invisible(NULL))
    tryCatch(
      ggplot2::ggsave(
        filename = file.path(output_dir, paste0(filename, "_", config$country, ".png")),
        plot = plot,
        width = width %||% config$plots$width,
        height = height %||% config$plots$height,
        dpi = config$plots$dpi
      ),
      error = function(e) invisible(NULL)
    )
    invisible(TRUE)
  }
}

#' Plot missing share — lollipop with severity bands
#'
#' Variables sorted ascending by missing share. Coloured by severity:
#' green < 5%, amber 5–20%, red >= 20%. Dashed threshold lines and
#' labelled zones make the chart self-explanatory at a glance.
#'
#' @param data_long   Long-format df with columns 'variable' and 'missing_share'
#' @param label_lookup Named character vector for human-readable labels
#' @param title        Plot title
#' @param n_total      Total rows in data (used for abs-count tooltip); optional
#' @return ggplot object
plot_missing_bar <- function(data_long, label_lookup,
                             title   = "Missing Value Share per Variable",
                             n_total = NULL) {
  
  AMBER <- 0.05
  RED   <- 0.20
  
  plot_df <- data_long %>%
    dplyr::mutate(
      var_label = label_with_lookup(variable, label_lookup),
      severity  = dplyr::case_when(
        missing_share >= RED   ~ "High \u226520%",
        missing_share >= AMBER ~ "Moderate 5\u201320%",
        TRUE                   ~ "Low <5%"
      ),
      severity = factor(severity,
                        levels = c("Low <5%", "Moderate 5\u201320%", "High \u226520%")),
      tooltip  = paste0(
        "<b>", var_label, "</b><br>",
        "Missing: ", scales::percent(missing_share, accuracy = 0.1),
        if (!is.null(n_total)) paste0(" (", scales::comma(round(missing_share * n_total)), " of ",
                                      scales::comma(n_total), " records)") else ""
      )
    ) %>%
    dplyr::arrange(missing_share) %>%
    dplyr::mutate(var_label = factor(var_label, levels = unique(var_label)))
  
  n_vars <- nrow(plot_df)
  
  pal <- c("Low <5%"           = "#27ae60",
           "Moderate 5\u201320%" = "#e67e22",
           "High \u226520%"    = "#c0392b")
  
  ggplot2::ggplot(plot_df,
                  ggplot2::aes(x = missing_share, y = var_label, colour = severity, text = tooltip)) +
    
    # Coloured risk-zone bands
    ggplot2::annotate("rect", xmin = 0,     xmax = AMBER, ymin = -Inf, ymax = Inf,
                      fill = "#eafaf1", alpha = 0.55) +
    ggplot2::annotate("rect", xmin = AMBER, xmax = RED,   ymin = -Inf, ymax = Inf,
                      fill = "#fef9e7", alpha = 0.55) +
    ggplot2::annotate("rect", xmin = RED,   xmax = 1,     ymin = -Inf, ymax = Inf,
                      fill = "#fdedec", alpha = 0.55) +
    
    # Threshold lines
    ggplot2::geom_vline(xintercept = AMBER, linetype = "dashed",
                        colour = "#e67e22", linewidth = 0.45) +
    ggplot2::geom_vline(xintercept = RED,   linetype = "dashed",
                        colour = "#c0392b", linewidth = 0.45) +
    
    # Zone labels along the top
    ggplot2::annotate("text", x = AMBER / 2,         y = Inf,
                      label = "Low", vjust = 1.6, size = 3,
                      colour = "#27ae60", fontface = "bold") +
    ggplot2::annotate("text", x = (AMBER + RED) / 2, y = Inf,
                      label = "Moderate", vjust = 1.6, size = 3,
                      colour = "#e67e22", fontface = "bold") +
    ggplot2::annotate("text", x = (RED + 1) / 2,     y = Inf,
                      label = "High", vjust = 1.6, size = 3,
                      colour = "#c0392b", fontface = "bold") +
    
    # Lollipop stick + dot only — no text labels (hover for values)
    ggplot2::geom_segment(
      ggplot2::aes(x = 0, xend = missing_share, y = var_label, yend = var_label),
      colour = "grey72", linewidth = 0.55) +
    ggplot2::geom_point(size = 2.5) +
    
    ggplot2::scale_colour_manual(values = pal, name = "Severity") +
    ggplot2::scale_x_continuous(
      labels = scales::percent_format(accuracy = 1),
      limits = c(0, 1),
      expand = ggplot2::expansion(mult = c(0, 0.04))) +
    ggplot2::labs(
      title    = title,
      subtitle = "Dashed lines mark 5% (notable) and 20% (high-risk) thresholds. Hover for exact values.",
      x        = "Share of records with missing value",
      y        = NULL) +
    standard_plot_theme() +
    ggplot2::theme(
      axis.text.y        = ggplot2::element_text(size = 11),
      axis.text.x        = ggplot2::element_text(size = 11),
      axis.title.x       = ggplot2::element_text(size = 11),
      panel.grid.major.y = ggplot2::element_blank(),
      panel.grid.minor   = ggplot2::element_blank(),
      legend.position    = "bottom",
      legend.text        = ggplot2::element_text(size = 11),
      plot.margin        = ggplot2::margin(4, 6, 2, 4))
}

#' Clean missing-share heatmap — labels only on notable cells
#'
#' Variables (y-axis) are sorted by average missing share so the worst sit
#' at the top. Percentage labels only appear in cells >= label_threshold,
#' avoiding a wall of tiny numbers. Uses a 3-stop colour scale (white →
#' amber → red) that makes severity immediately readable.
#'
#' @param data             Long-format df
#' @param x_var            Column name for x-axis grouping variable
#' @param y_var            Column name for variable labels (y-axis)
#' @param fill_var         Column name for missing share (0–1)
#' @param title / x_lab / y_lab   Aesthetic labels
#' @param label_threshold  Only print text in cells at or above this share (default 0.10)
#' @param height_per_row   Kept for back-compat; no longer used
#' @param text_size        Kept for back-compat; no longer used
#' @return ggplot object
plot_missing_heatmap <- function(data, x_var, y_var, fill_var,
                                 title, x_lab, y_lab,
                                 label_threshold = 0.10,
                                 height_per_row  = 0.3,
                                 text_size       = NULL) {
  x_sym    <- rlang::sym(x_var)
  y_sym    <- rlang::sym(y_var)
  fill_sym <- rlang::sym(fill_var)
  
  # Sort y-axis so highest-miss variables are at the top
  y_order <- data %>%
    dplyr::group_by(!!y_sym) %>%
    dplyr::summarise(avg = mean(!!fill_sym, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(avg) %>%
    dplyr::pull(!!y_sym)
  
  data <- data %>%
    dplyr::mutate(
      !!y_sym  := factor(!!y_sym, levels = y_order),
      tooltip   = paste0(
        "<b>", !!y_sym, "</b><br>",
        !!x_sym, ": ", dplyr::if_else(
          is.na(!!fill_sym), "n/a",
          scales::percent(!!fill_sym, accuracy = 0.1)
        )
      )
    )
  
  high_cells <- data %>% dplyr::filter(!!fill_sym >= label_threshold)
  # Use dark text on lightly-filled cells (≤30%), white on heavily-filled
  high_dark  <- high_cells %>% dplyr::filter(!!fill_sym <= 0.30)
  high_light <- high_cells %>% dplyr::filter(!!fill_sym >  0.30)
  
  ggplot2::ggplot(data,
                  ggplot2::aes(x = !!x_sym, y = !!y_sym, fill = !!fill_sym, text = tooltip)) +
    
    ggplot2::geom_tile(colour = "white", linewidth = 0.35) +
    
    # Dark text for lightly-coloured cells
    ggplot2::geom_text(
      data     = high_dark,
      ggplot2::aes(label = scales::percent(!!fill_sym, accuracy = 1)),
      size     = 3.3, colour = "grey20", fontface = "bold") +
    # White text for heavily-coloured cells
    ggplot2::geom_text(
      data     = high_light,
      ggplot2::aes(label = scales::percent(!!fill_sym, accuracy = 1)),
      size     = 3.3, colour = "white", fontface = "bold") +
    
    # Green (0%) -> white (5%) -> amber (20%) -> red (100%)
    ggplot2::scale_fill_gradientn(
      colours  = c("#27ae60", "#eafaf1", "#ffe082", "#e53935"),
      values   = scales::rescale(c(0, 0.05, 0.20, 1)),
      limits   = c(0, 1),
      labels   = scales::percent_format(accuracy = 1),
      name     = "Missing\nshare",
      na.value = "grey88") +
    
    ggplot2::labs(title = title, x = x_lab, y = NULL) +
    standard_plot_theme() +
    ggplot2::theme(
      axis.text.x     = ggplot2::element_text(angle = 35, hjust = 1, size = 12),
      axis.text.y     = ggplot2::element_text(size = 12),
      panel.grid      = ggplot2::element_blank(),
      legend.position = "right")
}
# ========================================================================
# PART 6B: MISSINGNESS CO-OCCURRENCE, MCAR TEST, MAR PREDICTABILITY
# ========================================================================

#' Missing co-occurrence — Jaccard similarity lollipop
#'
#' Shows which variable pairs most often go missing on the *same row*.
#' Jaccard = (rows where both are NA) / (rows where either is NA).
#' A high Jaccard means the two variables have a shared data-collection
#' failure — useful for identifying structural gaps in source systems.
#'
#' @param df           Raw data frame (non-indicator columns are used)
#' @param label_lookup Named character vector for labels
#' @param top_n        How many top pairs to show (default 20)
#' @param title        Plot title
#' @return ggplot object, or NULL if < 2 columns have any NAs
plot_missing_cooccurrence <- function(df, label_lookup,
                                      top_n = 20,
                                      title = "Variable Pairs Most Often Missing Together") {
  co_df <- compute_cooccurrence_data(df, label_lookup)
  if (is.null(co_df)) return(NULL)
  plot_cooccurrence_from_data(co_df, top_n = top_n, min_jaccard = 0, title = title)
}

#' Compute raw Jaccard co-occurrence data (no plotting)
#' Returns a tibble with var1, var2, n_both, jaccard, pct_rows — or NULL if < 2 cols have NAs
compute_cooccurrence_data <- function(df, label_lookup) {
  miss_cols <- names(df)[sapply(df, function(x) sum(is.na(x)) > 0)]
  miss_cols <- miss_cols[!startsWith(miss_cols, "ind_")]
  if (length(miss_cols) < 2) return(NULL)
  
  if (length(miss_cols) > 50) {
    na_counts <- sapply(df[miss_cols], function(x) sum(is.na(x)))
    miss_cols <- names(sort(na_counts, decreasing = TRUE))[1:50]
  }
  
  n     <- nrow(df)
  pairs <- utils::combn(miss_cols, 2, simplify = FALSE)
  
  get_label <- function(v) {
    key <- paste0(v, "_missing_share")
    val <- label_lookup[key]
    if (length(val) == 0 || is.na(val)) v else unname(val)
  }
  
  co_df <- purrr::map_dfr(pairs, function(p) {
    a      <- is.na(df[[p[1]]])
    b      <- is.na(df[[p[2]]])
    both   <- sum(a & b)
    either <- sum(a | b)
    tibble::tibble(
      var1     = get_label(p[1]),
      var2     = get_label(p[2]),
      n_both   = both,
      jaccard  = if (either > 0) both / either else 0,
      pct_rows = both / n
    )
  }) %>% dplyr::filter(n_both > 0)
  
  if (nrow(co_df) == 0) return(NULL)
  co_df
}

#' Build co-occurrence lollipop from pre-computed data
#' @param co_df      Output of compute_cooccurrence_data()
#' @param top_n      Max pairs to show
#' @param min_jaccard Filter to pairs with jaccard >= this value (0–1)
#' @param title      Plot title
plot_cooccurrence_from_data <- function(co_df, top_n = 20, min_jaccard = 0,
                                        title = "Variable Pairs Most Often Missing Together") {
  if (is.null(co_df) || nrow(co_df) == 0) return(NULL)
  
  plot_data <- co_df %>%
    dplyr::filter(jaccard >= min_jaccard) %>%
    dplyr::arrange(dplyr::desc(jaccard)) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::mutate(
      pair_label = paste0(var1, "  \u00d7  ", var2),
      pair_label = factor(pair_label, levels = rev(unique(pair_label))),
      strength   = dplyr::case_when(
        jaccard >= 0.70 ~ "Strong (\u226570%)",
        jaccard >= 0.40 ~ "Moderate (40\u201370%)",
        TRUE            ~ "Weak (<40%)"
      ),
      strength = factor(strength,
                        levels = c("Weak (<40%)", "Moderate (40\u201370%)", "Strong (\u226570%)")),
      tooltip  = paste0(
        "<b>", var1, " \u00d7 ", var2, "</b><br>",
        "Co-occurrence score: ", scales::percent(jaccard, accuracy = 1), "<br>",
        "Both missing in: ", scales::comma(n_both), " rows (",
        scales::percent(pct_rows, accuracy = 0.1), " of all records)<br>",
        "<i>", strength, " co-occurrence</i>"
      )
    )
  
  if (nrow(plot_data) == 0) return(NULL)
  
  str_pal <- c("Weak (<40%)"           = "#aed6f1",
               "Moderate (40\u201370%)" = "#e67e22",
               "Strong (\u226570%)"    = "#c0392b")
  
  ggplot2::ggplot(plot_data,
                  ggplot2::aes(x = jaccard, y = pair_label, fill = strength, text = tooltip)) +
    ggplot2::geom_col(width = 0.65) +
    ggplot2::scale_fill_manual(values = str_pal, name = "Co-occurrence") +
    ggplot2::scale_x_continuous(
      labels = scales::percent_format(accuracy = 1),
      limits = c(0, 1),
      expand = ggplot2::expansion(mult = c(0, 0.05))) +
    ggplot2::labs(
      title    = title,
      subtitle = "Jaccard = rows where BOTH variables are missing / rows where EITHER is missing. Hover for counts.",
      x        = "Jaccard similarity (co-missing rate)",
      y        = NULL) +
    standard_plot_theme() +
    ggplot2::theme(
      panel.grid.major.y = ggplot2::element_blank(),
      legend.position    = "bottom")
}

#' Little's MCAR test (chi-squared approximation)
#'
#' Tests H\u2080: data are Missing Completely At Random.
#' A significant result (p < 0.05) means missingness is NOT random — it is
#' structured by observed values, i.e. MAR or MNAR.
#'
#' Uses the pattern-mean deviation approach from Little (1988).
#' Only numeric, non-indicator columns with actual NAs are included.
#'
#' @param df       Data frame
#' @param max_cols Cap on numeric columns tested (default 20, for speed)
#' @return Named list: statistic, df, p_value, n_cols, n_rows, interpretation
run_little_mcar_test <- function(df, max_cols = 20) {
  # Coerce to numeric where possible (year, price often come as character/factor)
  df_core <- df %>% dplyr::select(!dplyr::starts_with("ind_"))
  df_num  <- df_core %>%
    dplyr::mutate(dplyr::across(dplyr::everything(),
                                ~ suppressWarnings(as.numeric(.)))) %>%
    dplyr::select(dplyr::where(is.numeric)) %>%
    dplyr::select(dplyr::where(~ any(is.na(.)))) %>%
    dplyr::select(seq_len(min(ncol(.), max_cols)))
  
  if (ncol(df_num) < 2 || nrow(df_num) < 10) {
    return(list(statistic = NA_real_, df = NA_integer_, p_value = NA_real_,
                n_cols = ncol(df_num), n_rows = nrow(df_num),
                interpretation = paste0(
                  "MCAR test requires at least 2 numeric columns with missing values. ",
                  "Found ", ncol(df_num), " usable column(s). ",
                  "This typically means your data has very few numeric fields with NAs, ",
                  "or that price/year columns could not be converted to numbers. ",
                  "The MAR regression results below are still valid and more informative."
                )))
  }
  
  tryCatch({
    n        <- nrow(df_num)
    p        <- ncol(df_num)
    mu_hat   <- colMeans(df_num, na.rm = TRUE)
    sig_hat  <- stats::cov(df_num, use = "pairwise.complete.obs")
    
    # Missingness patterns
    pat_str  <- apply(is.na(df_num), 1, paste, collapse = "")
    pat_tab  <- table(pat_str)
    
    d2 <- 0; df_acc <- 0
    for (pat in names(pat_tab)) {
      obs  <- !as.logical(strsplit(pat, "")[[1]])
      if (sum(obs) == 0) next
      n_k  <- as.integer(pat_tab[pat])
      rows <- which(pat_str == pat)
      xbar <- colMeans(df_num[rows, obs, drop = FALSE], na.rm = TRUE)
      diff <- xbar - mu_hat[obs]
      sig_k <- sig_hat[obs, obs, drop = FALSE]
      if (any(is.na(sig_k))) next
      tryCatch({
        d2    <- d2 + n_k * as.numeric(t(diff) %*% solve(sig_k) %*% diff)
        df_acc <- df_acc + sum(obs)
      }, error = function(e) NULL)
    }
    
    df_adj <- max(1L, df_acc - p)
    p_val  <- stats::pchisq(d2, df = df_adj, lower.tail = FALSE)
    
    interp <- dplyr::case_when(
      is.na(p_val)  ~ "Could not compute.",
      p_val < 0.001 ~ "Very strong evidence against MCAR (p < 0.001). Missingness is highly systematic.",
      p_val < 0.05  ~ "Evidence against MCAR (p < 0.05). Missingness likely structured (MAR or MNAR).",
      p_val < 0.10  ~ "Borderline evidence against MCAR (p < 0.10). Interpret cautiously.",
      TRUE          ~ "No significant evidence against MCAR (p \u2265 0.10). Missingness may be random."
    )
    
    list(statistic = round(d2, 2), df = df_adj, p_value = round(p_val, 4),
         n_cols = p, n_rows = n, interpretation = interp)
    
  }, error = function(e) {
    list(statistic = NA_real_, df = NA_integer_, p_value = NA_real_,
         n_cols = NA_integer_, n_rows = nrow(df_num),
         interpretation = paste("MCAR test failed:", conditionMessage(e)))
  })
}

#' MAR predictability test — logistic regression per variable
#'
#' For each variable with missing values, fits:
#'   is.na(var) ~ tender_year + buyer_type + procedure_type + log(price + 1)
#'
#' McFadden pseudo-R\u00b2 measures how well missingness is explained by observed
#' covariates.  High R\u00b2 => variable is likely MAR (or MNAR), not MCAR.
#' The dominant predictor (largest absolute coefficient) tells you *what* is
#' driving the missingness pattern.
#'
#' @param df           Data frame
#' @param label_lookup Named character vector for labels
#' @param min_miss     Minimum missing share to include a variable (default 0.01)
#' @return Tibble: variable, label, n_missing, missing_share, pseudo_r2,
#'                 dominant_predictor, interpretation — sorted by pseudo_r2 desc
run_mar_predictability <- function(df, label_lookup, min_miss = 0.01) {
  # Build covariate columns safely — check existence BEFORE mutate
  cov_df <- df %>% dplyr::select(!dplyr::starts_with("ind_"))
  
  if ("tender_year" %in% names(cov_df))
    cov_df$year_num <- as.numeric(cov_df$tender_year)
  else
    cov_df$year_num <- NA_real_
  
  if ("buyer_buyertype" %in% names(cov_df))
    cov_df$buyer_type <- as.character(cov_df$buyer_buyertype)
  else
    cov_df$buyer_type <- NA_character_
  
  if ("tender_proceduretype" %in% names(cov_df))
    cov_df$proc_type <- as.character(cov_df$tender_proceduretype)
  else
    cov_df$proc_type <- NA_character_
  
  if ("bid_priceusd" %in% names(cov_df))
    cov_df$log_price <- log1p(pmax(suppressWarnings(as.numeric(cov_df$bid_priceusd)), 0, na.rm = TRUE))
  else if ("bid_price" %in% names(cov_df))
    cov_df$log_price <- log1p(pmax(suppressWarnings(as.numeric(cov_df$bid_price)), 0, na.rm = TRUE))
  else
    cov_df$log_price <- NA_real_
  
  pred_candidates <- c("year_num", "buyer_type", "proc_type", "log_price")
  pred_avail <- pred_candidates[sapply(pred_candidates, function(v)
    v %in% names(cov_df) && sum(!is.na(cov_df[[v]])) > 50)]
  
  if (length(pred_avail) == 0)
    return(tibble::tibble(variable = character(), label = character(),
                          n_missing = integer(), missing_share = numeric(),
                          pseudo_r2 = numeric(), dominant_predictor = character(),
                          interpretation = character()))
  
  n_rows      <- nrow(df)
  target_vars <- df %>%
    dplyr::select(!dplyr::starts_with("ind_")) %>%
    names() %>%
    setdiff(pred_avail) %>%
    purrr::keep(~ {
      ms <- mean(is.na(df[[.x]]))
      ms >= min_miss && ms <= 0.999
    })
  
  if (length(target_vars) == 0) return(tibble::tibble())
  
  purrr::map_dfr(target_vars, function(var) {
    y      <- as.integer(is.na(df[[var]]))
    fit_df <- cov_df %>%
      dplyr::select(dplyr::all_of(pred_avail)) %>%
      dplyr::mutate(y = y) %>%
      tidyr::drop_na()
    
    if (nrow(fit_df) < 50 || stats::var(fit_df$y) == 0) return(NULL)
    
    fml    <- stats::as.formula(paste("y ~", paste(pred_avail, collapse = " + ")))
    m_full <- tryCatch(stats::glm(fml, data = fit_df,
                                  family = stats::binomial()), error = function(e) NULL)
    m_null <- tryCatch(stats::glm(y ~ 1, data = fit_df,
                                  family = stats::binomial()), error = function(e) NULL)
    if (is.null(m_full) || is.null(m_null)) return(NULL)
    
    ll_full <- as.numeric(stats::logLik(m_full))
    ll_null <- as.numeric(stats::logLik(m_null))
    r2      <- if (ll_null != 0) round(1 - ll_full / ll_null, 3) else NA_real_
    
    # Which predictor has the largest absolute standardised coefficient?
    dom <- tryCatch({
      cf  <- stats::coef(m_full)
      cf  <- cf[names(cf) != "(Intercept)"]
      nm  <- names(cf)[which.max(abs(cf))]
      dplyr::case_when(
        grepl("year",                    nm) ~ "Year (temporal trend)",
        grepl("buyer_type|buyer",        nm) ~ paste0("Buyer type (", gsub("buyer_type", "", nm), ")"),
        grepl("proc_type|proc",          nm) ~ paste0("Procedure (", gsub("proc_type",  "", nm), ")"),
        grepl("log_price|price",         nm) ~ "Contract value",
        TRUE                                 ~ nm)
    }, error = function(e) "unknown")
    
    # Capture top 3 categories with strongest effect for tooltip detail
    cat_detail <- tryCatch({
      cf    <- stats::coef(m_full)
      cf    <- cf[names(cf) != "(Intercept)"]
      top3  <- head(sort(abs(cf), decreasing = TRUE), 3)
      nms   <- names(top3)
      clean <- gsub("buyer_type|proc_type|log_price|year_num", "", nms)
      clean <- clean[clean != ""]
      if (length(clean) > 0) paste(clean, collapse = ", ") else ""
    }, error = function(e) "")
    
    key   <- paste0(var, "_missing_share")
    lbl   <- label_lookup[key]
    lbl   <- if (is.na(lbl)) var else unname(lbl)
    n_mis <- sum(is.na(df[[var]]))
    
    interp <- dplyr::case_when(
      is.na(r2)   ~ "Could not test — too few records.",
      r2 >= 0.10  ~ paste0("Clear non-random pattern: which records have this field missing is strongly ",
                           "explained by ", dom, ". This may signal a reporting gap tied to specific ",
                           "buyer types, procedures, or time periods. Worth investigating."),
      r2 >= 0.03  ~ paste0("Some pattern detected: missingness is partly explained by ", dom,
                           ". May be worth checking for differences across sub-groups."),
      TRUE        ~ paste0("No clear pattern: missingness appears unrelated to known factors. ",
                           "This is consistent with random data entry gaps (MCAR) and is ",
                           "generally less concerning for analysis.")
    )
    
    tibble::tibble(variable = var, label = lbl, n_missing = n_mis,
                   missing_share = n_mis / n_rows,
                   pseudo_r2 = r2, dominant_predictor = dom,
                   category_detail = cat_detail,
                   interpretation = interp)
  }) %>%
    dplyr::arrange(dplyr::desc(pseudo_r2))
}

#' MAR predictability dot-plot
#'
#' Lollipop of McFadden pseudo-R\u00b2 per variable, coloured by signal strength.
#' Companion chart to the main missing-share lollipop: one tells you *how much*
#' is missing, this tells you *whether that missingness is random*.
#'
#' @param mar_df  Output of run_mar_predictability()
#' @param title   Plot title
#' @return ggplot object, or NULL if no data
plot_mar_predictability <- function(mar_df,
                                    title = "Is Missingness Random? (MAR vs MCAR)") {
  if (is.null(mar_df) || nrow(mar_df) == 0) return(NULL)
  
  plot_df <- mar_df %>%
    dplyr::filter(!is.na(pseudo_r2)) %>%
    dplyr::mutate(
      signal = dplyr::case_when(
        pseudo_r2 >= 0.10 ~ "Strong pattern (MAR/MNAR)",
        pseudo_r2 >= 0.03 ~ "Moderate pattern",
        TRUE              ~ "Appears random (MCAR)"
      ),
      signal    = factor(signal,
                         levels = c("Appears random (MCAR)", "Moderate pattern",
                                    "Strong pattern (MAR/MNAR)")),
      label     = factor(label, levels = label[order(pseudo_r2)]),
      r2_pct    = scales::percent(pseudo_r2, accuracy = 1),
      # Tooltip for policy makers — plain language, no jargon
      tooltip   = paste0(
        "<b>", label, "</b><br>",
        "Pattern score: <b>", scales::percent(pseudo_r2, accuracy = 1), "</b><br>",
        "Main driver: ", dominant_predictor, "<br>",
        scales::comma(n_missing), " missing records (",
        scales::percent(missing_share, accuracy = 0.1), " of total)<br>",
        "<i>", interpretation, "</i>"
      )
    )
  
  if (nrow(plot_df) == 0) return(NULL)
  
  sig_pal <- c("Appears random (MCAR)"      = "#27ae60",
               "Moderate pattern"            = "#e67e22",
               "Strong pattern (MAR/MNAR)"   = "#c0392b")
  
  x_max <- max(c(plot_df$pseudo_r2 * 1.35, 0.25), na.rm = TRUE)
  
  ggplot2::ggplot(plot_df,
                  ggplot2::aes(x = pseudo_r2, y = label, colour = signal, text = tooltip)) +
    
    ggplot2::annotate("rect", xmin = 0,    xmax = 0.03,  ymin = -Inf, ymax = Inf,
                      fill = "#eafaf1", alpha = 0.5) +
    ggplot2::annotate("rect", xmin = 0.03, xmax = 0.10,  ymin = -Inf, ymax = Inf,
                      fill = "#fef9e7", alpha = 0.5) +
    ggplot2::annotate("rect", xmin = 0.10, xmax = x_max, ymin = -Inf, ymax = Inf,
                      fill = "#fdedec", alpha = 0.5) +
    
    ggplot2::geom_vline(xintercept = c(0.03, 0.10), linetype = "dashed",
                        colour = "grey55", linewidth = 0.45) +
    
    ggplot2::annotate("text", x = 0.015,  y = Inf, label = "Looks random",
                      vjust = 1.6, size = 2.8, colour = "#27ae60", fontface = "bold") +
    ggplot2::annotate("text", x = 0.065,  y = Inf, label = "Some pattern",
                      vjust = 1.6, size = 2.8, colour = "#e67e22", fontface = "bold") +
    ggplot2::annotate("text", x = (0.10 + x_max) / 2, y = Inf, label = "Clear pattern",
                      vjust = 1.6, size = 2.8, colour = "#c0392b", fontface = "bold") +
    
    ggplot2::geom_segment(
      ggplot2::aes(x = 0, xend = pseudo_r2, y = label, yend = label),
      colour = "grey72", linewidth = 0.55) +
    ggplot2::geom_point(size = 3.8) +
    
    ggplot2::scale_colour_manual(values = sig_pal, guide = "none") +
    ggplot2::scale_x_continuous(
      limits = c(0, x_max),
      labels = scales::percent_format(accuracy = 1),
      expand = ggplot2::expansion(mult = c(0, 0.04))) +
    ggplot2::labs(
      title    = title,
      subtitle = paste0(
        "Pattern score: how well can we predict which records have missing data, using year, buyer type, procedure & contract value?\n",
        "Score near 0% = missing data appears random (no policy concern). Score above 10% = clear non-random pattern (investigate why)."),
      x = "Pattern score (% of missingness explained by observable factors)",
      y = NULL) +
    standard_plot_theme() +
    ggplot2::theme(
      axis.text.y        = ggplot2::element_text(size = 9),
      panel.grid.major.y = ggplot2::element_blank(),
      panel.grid.minor   = ggplot2::element_blank(),
      legend.position    = "none")
}

#' Plot top N bar chart
#' 
#' @param df Data frame
#' @param x_var Name of x variable
#' @param y_var Name of y variable
#' @param label_var Name of label variable
#' @param title Plot title
#' @param x_lab X-axis label
#' @param y_lab Y-axis label
#' @param fill_color Bar fill color
#' @param y_limit Y-axis limits
#' @param percent Whether to format labels as percent
#' @return ggplot object
plot_top_bar <- function(df, x_var, y_var, label_var,
                         title, x_lab, y_lab,
                         fill_color = "skyblue1",
                         y_limit = c(0, 1.05),
                         percent = TRUE) {
  x_sym <- rlang::sym(x_var)
  y_sym <- rlang::sym(y_var)
  lab_sym <- rlang::sym(label_var)
  
  p <- ggplot2::ggplot(
    df,
    ggplot2::aes(
      x = reorder(!!x_sym, !!y_sym),
      y = !!y_sym
    )
  ) +
    ggplot2::geom_col(fill = fill_color) +
    ggplot2::geom_text(
      ggplot2::aes(
        label = if (percent) {
          scales::percent(!!y_sym, accuracy = 0.1)
        } else {
          !!lab_sym
        }
      ),
      hjust = -0.1,
      size = PLOT_SIZES$geom_text_large,
      check_overlap = TRUE
    ) +
    ggplot2::coord_flip() +
    ggplot2::labs(
      title = title,
      x = x_lab,
      y = y_lab
    ) +
    ggplot2::theme_bw(base_size = PLOT_SIZES$base_size) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(size = PLOT_SIZES$title_size, face = "bold"),
      axis.title = ggplot2::element_text(size = PLOT_SIZES$axis_title_size),
      axis.text  = ggplot2::element_text(size = PLOT_SIZES$axis_text_size)
    ) +
    white_bg()
  
  if (!is.null(y_limit)) {
    p <- p + ggplot2::ylim(y_limit)
  }
  
  p
}

#' Plot ggeffects line with confidence band
#' 
#' @param pred ggeffects prediction object
#' @param title Plot title
#' @param subtitle Plot subtitle
#' @param x_lab X-axis label
#' @param y_lab Y-axis label
#' @param caption Plot caption
#' @param wrap_width Width for text wrapping
#' @return ggplot object
plot_ggeffects_line <- function(pred,
                                title,
                                subtitle,
                                x_lab,
                                y_lab,
                                caption = NULL,
                                wrap_width = 110) {
  
  subtitle_w <- if (!is.null(subtitle)) stringr::str_wrap(subtitle, width = wrap_width) else NULL
  caption_w <- if (!is.null(caption)) stringr::str_wrap(caption, width = wrap_width) else NULL
  
  ggplot2::ggplot(pred, ggplot2::aes(x, predicted)) +
    ggplot2::geom_line(size = PLOT_SIZES$line_size, color = "lightblue") +
    ggplot2::geom_ribbon(ggplot2::aes(ymin = conf.low, ymax = conf.high), alpha = 0.2) +
    ggplot2::labs(
      title = title,
      subtitle = subtitle_w,
      x = x_lab,
      y = y_lab,
      caption = caption_w
    ) +
    standard_plot_theme() +
    ggplot2::theme(
      plot.subtitle = ggplot2::element_text(size = PLOT_SIZES$subtitle_size, lineheight = 1.05),
      plot.caption = ggplot2::element_text(size = PLOT_SIZES$subtitle_size - 2, lineheight = 1.05),
      plot.margin = ggplot2::margin(10, 18, 10, 10)
    )
}
# ========================================================================
# PART 7: MODEL SPECIFICATION HELPERS
# ========================================================================

#' Build fixed effects formula part
#' 
#' @param fe Fixed effects specification
#' @return Character string for formula
make_fe_part <- function(fe) {
  switch(
    fe,
    "0" = "0",
    "buyer" = "buyer_masterid",
    "year" = "tender_year",
    "buyer+year" = "buyer_masterid + tender_year",
    "buyer#year" = "buyer_masterid^tender_year",
    stop("Unknown FE spec: ", fe)
  )
}

#' Build cluster formula
#' 
#' @param cluster Cluster specification
#' @return Formula or NULL
make_cluster <- function(cluster) {
  switch(
    cluster,
    "none" = NULL,
    "buyer" = stats::as.formula("~ buyer_masterid"),
    "year" = stats::as.formula("~ tender_year"),
    "buyer_year" = stats::as.formula("~ buyer_masterid + tender_year"),
    "buyer_buyertype" = stats::as.formula("~ buyer_masterid + buyer_buyertype"),
    stop("Unknown cluster spec: ", cluster)
  )
}

#' Safe fixest model fitting
#' 
#' @param expr Expression to evaluate
#' @return Model or NULL
safe_fixest <- function(expr) {
  tryCatch(expr, error = function(e) NULL)
}

#' Extract effect from fixest model
#' 
#' @param model fixest model object
#' @param x_name Name of variable to extract
#' @param data_used Data used for estimation
#' @param y_name Name of outcome variable
#' @return List with estimate, pvalue, nobs, std_slope
extract_effect_fixest <- function(model, x_name, data_used, y_name = NULL) {
  s <- summary(model)
  ct <- s$coeftable
  
  if (!(x_name %in% rownames(ct))) {
    return(list(
      estimate = NA_real_,
      pvalue = NA_real_,
      nobs = s$nobs,
      std_slope = NA_real_
    ))
  }
  
  est <- as.numeric(ct[x_name, "Estimate"])
  pv <- as.numeric(ct[x_name, "Pr(>|t|)"])
  if (is.na(pv) && "Pr(>|z|)" %in% colnames(ct)) {
    pv <- as.numeric(ct[x_name, "Pr(>|z|)"])
  }
  
  sx <- stats::sd(data_used[[x_name]], na.rm = TRUE)
  std_slope <- est * sx
  
  list(
    estimate = est,
    pvalue = pv,
    nobs = s$nobs,
    std_slope = std_slope
  )
}

#' Extract effect with specific vcov
#' 
#' @param model fixest model
#' @param x_name Variable name
#' @param data_used Data frame
#' @param vcov VCOV specification
#' @return List with estimate, se, t, p
extract_effect_fixest_vcov <- function(model, x_name, data_used, vcov) {
  ct <- tryCatch(
    fixest::coeftable(model, vcov = vcov),
    error = function(e) NULL
  )
  
  if (is.null(ct) || !(x_name %in% rownames(ct))) {
    return(list(estimate = NA_real_, se = NA_real_, t = NA_real_, p = NA_real_))
  }
  
  est <- as.numeric(ct[x_name, "Estimate"])
  se <- as.numeric(ct[x_name, "Std. Error"])
  
  tv <- if ("t value" %in% colnames(ct)) as.numeric(ct[x_name, "t value"]) else NA_real_
  pv <- if ("Pr(>|t|)" %in% colnames(ct)) as.numeric(ct[x_name, "Pr(>|t|)"]) else
    if ("Pr(>|z|)" %in% colnames(ct)) as.numeric(ct[x_name, "Pr(>|z|)"]) else NA_real_
  
  list(estimate = est, se = se, t = tv, p = pv)
}

#' Compute effect at P10 vs P90
#' 
#' @param model Model object
#' @param data_used Data frame
#' @param x_name Variable name
#' @return Numeric effect size
effect_p10_p90 <- function(model, data_used, x_name) {
  qs <- stats::quantile(data_used[[x_name]], probs = c(.1, .9), na.rm = TRUE)
  x_lo <- unname(qs[1])
  x_hi <- unname(qs[2])
  
  typical <- data_used[1, , drop = FALSE]
  for (nm in names(typical)) {
    if (nm == x_name) next
    v <- data_used[[nm]]
    if (is.numeric(v)) {
      typical[[nm]] <- stats::median(v, na.rm = TRUE)
    } else if (is.factor(v) || is.character(v)) {
      tab <- sort(table(v), decreasing = TRUE)
      typical[[nm]] <- names(tab)[1]
      if (is.factor(v)) typical[[nm]] <- factor(typical[[nm]], levels = levels(v))
    }
  }
  
  d_lo <- typical
  d_hi <- typical
  d_lo[[x_name]] <- x_lo
  d_hi[[x_name]] <- x_hi
  
  p_lo <- suppressWarnings(stats::predict(model, newdata = d_lo, type = "response"))
  p_hi <- suppressWarnings(stats::predict(model, newdata = d_hi, type = "response"))
  
  as.numeric(p_hi - p_lo)
}

#' Get default VCOV menu for robustness checks
#' 
#' @param has_buyertype Whether buyer type variable exists
#' @return List of VCOV specifications
get_default_vcov_menu <- function(has_buyertype = TRUE) {
  out <- list(
    "hetero",
    stats::as.formula("~ buyer_masterid"),
    stats::as.formula("~ tender_year"),
    stats::as.formula("~ buyer_masterid + tender_year")
  )
  if (has_buyertype) {
    out <- c(out, list(stats::as.formula("~ buyer_masterid + buyer_buyertype")))
  }
  out
}

#' Compute robustness summary
#' 
#' @param model Model object
#' @param x_name Variable name
#' @param data_used Data frame
#' @param vcov_list List of VCOV specs
#' @param p_max P-value threshold
#' @return List with robustness metrics
robustness_summary <- function(model, x_name, data_used, vcov_list, p_max = 0.10) {
  res <- lapply(vcov_list, function(v) extract_effect_fixest_vcov(model, x_name, data_used, v))
  
  pvals <- vapply(res, function(z) z$p, numeric(1))
  ests <- vapply(res, function(z) z$estimate, numeric(1))
  
  worst_p <- suppressWarnings(max(pvals, na.rm = TRUE))
  pass_count <- sum(!is.na(pvals) & pvals <= p_max)
  tested_count <- sum(!is.na(pvals))
  pass_share <- if (tested_count > 0) pass_count / tested_count else NA_real_
  
  sign_consistent <- {
    s <- sign(ests[!is.na(ests)])
    length(unique(s)) <= 1
  }
  
  list(
    robust_p_worst = if (is.infinite(worst_p)) NA_real_ else worst_p,
    robust_pass_count = pass_count,
    robust_tested = tested_count,
    robust_pass_share = pass_share,
    robust_sign_stable = sign_consistent
  )
}

# ========================================================================
# PART 8: MODEL SELECTION FUNCTIONS
# ========================================================================

#' Pick best model from specifications
#' 
#' @param results_df Data frame of model results
#' @param require_positive Whether to require positive estimate
#' @param p_max Maximum p-value
#' @param strength_col Column name for effect strength
#' @return Single row data frame or NULL
pick_best_model <- function(results_df,
                            require_positive = TRUE,
                            p_max = 0.10,
                            strength_col = c("effect_strength", "std_slope")) {
  strength_col <- match.arg(strength_col)
  
  df <- results_df
  if (require_positive) df <- df[df$estimate > 0, , drop = FALSE]
  df <- df[!is.na(df$pvalue) & df$pvalue <= p_max, , drop = FALSE]
  df <- df[!is.na(df[[strength_col]]), , drop = FALSE]
  if (nrow(df) == 0) return(NULL)
  
  df <- df[order(df[[strength_col]], decreasing = TRUE), , drop = FALSE]
  df[["rank"]] <- seq_len(nrow(df))
  df[1, , drop = FALSE]
}

#' Pick most robust model
#' 
#' @param results_df Data frame with robustness metrics
#' @param require_positive Require positive estimate
#' @param p_max P-value threshold
#' @param strength_col Column for effect strength
#' @return Single row or NULL
pick_most_robust_model <- function(results_df,
                                   require_positive = TRUE,
                                   p_max = 0.10,
                                   strength_col = c("effect_strength", "std_slope")) {
  strength_col <- match.arg(strength_col)
  
  df <- results_df
  if (require_positive) df <- df[df$estimate > 0, , drop = FALSE]
  
  df <- df[!is.na(df$robust_p_worst), , drop = FALSE]
  df <- df[df$robust_p_worst <= p_max, , drop = FALSE]
  df <- df[!is.na(df[[strength_col]]), , drop = FALSE]
  
  if (nrow(df) == 0) return(NULL)
  
  df <- df[order(
    -df$robust_pass_share,
    df$robust_p_worst,
    -df[[strength_col]]
  ), , drop = FALSE]
  
  df[1, , drop = FALSE]
}

#' Model diagnostics
#' 
#' @param specs_df Specification results
#' @param require_positive Require positive estimate
#' @param p_max P-value threshold
#' @param strength_col Strength column name
#' @return List of diagnostic counts
model_diagnostics <- function(specs_df, require_positive = TRUE, p_max = 0.10, strength_col) {
  if (is.null(specs_df) || nrow(specs_df) == 0L) {
    return(list(total = 0L, after_sign = 0L, after_p = 0L, after_strength = 0L))
  }
  
  df <- specs_df
  total <- nrow(df)
  
  df1 <- if (require_positive) df[df$estimate > 0, , drop = FALSE] else df
  after_sign <- nrow(df1)
  
  df2 <- df1[!is.na(df1$pvalue) & df1$pvalue <= p_max, , drop = FALSE]
  after_p <- nrow(df2)
  
  ok_strength <- !is.na(df2[[strength_col]])
  df3 <- df2[ok_strength, , drop = FALSE]
  after_strength <- nrow(df3)
  
  list(total = total, after_sign = after_sign, after_p = after_p, after_strength = after_strength)
}

# ========================================================================
# PART 9: SENSITIVITY ANALYSIS FUNCTIONS
# ========================================================================

#' Add strength column to specifications
#' 
#' @param specs Specifications data frame
#' @return Data frame with strength column
add_strength_column <- function(specs) {
  if (is.null(specs) || nrow(specs) == 0L) return(specs)
  
  if ("effect_strength" %in% names(specs)) {
    specs$strength <- specs$effect_strength
  } else if ("std_slope" %in% names(specs)) {
    specs$strength <- specs$std_slope
  } else {
    specs$strength <- NA_real_
  }
  specs
}

#' Summarize sensitivity overall
#' 
#' @param specs Specifications data frame
#' @param p_levels P-value levels to check
#' @return Summary tibble
summarise_sensitivity_overall <- function(specs, p_levels = c(0.05, 0.10, 0.20)) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  specs <- add_strength_column(specs)
  
  tibble::tibble(
    n_specs = nrow(specs),
    share_positive = mean(specs$estimate > 0, na.rm = TRUE),
    share_negative = mean(specs$estimate < 0, na.rm = TRUE),
    median_estimate = median(specs$estimate, na.rm = TRUE),
    median_pvalue = median(specs$pvalue, na.rm = TRUE),
    median_strength = median(specs$strength, na.rm = TRUE),
    !!!setNames(
      lapply(p_levels, function(p) mean(specs$pvalue <= p, na.rm = TRUE)),
      paste0("share_p_le_", p_levels)
    )
  )
}

#' Summarize sign instability
#' 
#' @param specs Specifications data frame
#' @return Summary tibble
summarise_sign_instability <- function(specs) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  s <- sign(specs$estimate)
  s <- s[!is.na(s) & s != 0]
  tibble::tibble(
    share_sign_stable = if (length(s) == 0) NA_real_ else as.numeric(length(unique(s)) <= 1),
    n_nonzero = length(s)
  )
}

#' Summarize by fixed effects
#' 
#' @param specs Specifications data frame
#' @return Summary tibble
summarise_by_fe <- function(specs) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  specs <- add_strength_column(specs)
  
  specs %>%
    dplyr::group_by(fe) %>%
    dplyr::summarise(
      n_specs = dplyr::n(),
      share_positive = mean(estimate > 0, na.rm = TRUE),
      share_p10 = mean(pvalue <= 0.10, na.rm = TRUE),
      median_p = median(pvalue, na.rm = TRUE),
      median_strength = median(strength, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(share_p10))
}

#' Summarize by cluster
#' 
#' @param specs Specifications data frame
#' @return Summary tibble
summarise_by_cluster <- function(specs) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  specs <- add_strength_column(specs)
  
  specs %>%
    dplyr::group_by(cluster) %>%
    dplyr::summarise(
      n_specs = dplyr::n(),
      share_positive = mean(estimate > 0, na.rm = TRUE),
      share_p10 = mean(pvalue <= 0.10, na.rm = TRUE),
      median_p = median(pvalue, na.rm = TRUE),
      median_strength = median(strength, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(median_p)
}

#' Summarize by controls
#' 
#' @param specs Specifications data frame
#' @return Summary tibble
summarise_by_controls <- function(specs) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  specs <- add_strength_column(specs)
  
  specs %>%
    dplyr::group_by(controls) %>%
    dplyr::summarise(
      n_specs = dplyr::n(),
      share_positive = mean(estimate > 0, na.rm = TRUE),
      share_p10 = mean(pvalue <= 0.10, na.rm = TRUE),
      median_p = median(pvalue, na.rm = TRUE),
      median_strength = median(strength, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(share_p10))
}

#' Classify specifications
#' 
#' @param specs Specifications data frame
#' @param p_cut P-value cutoff
#' @return Classification tibble
classify_specs <- function(specs, p_cut = 0.10) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  specs %>%
    dplyr::mutate(
      class = dplyr::case_when(
        estimate > 0 & pvalue <= p_cut ~ "Positive & significant",
        estimate > 0 ~ "Positive but insignificant",
        estimate < 0 & pvalue <= p_cut ~ "Negative & significant",
        estimate < 0 ~ "Negative but insignificant",
        TRUE ~ "Missing/NA"
      )
    ) %>%
    dplyr::count(class) %>%
    dplyr::mutate(share = n / sum(n))
}

#' Find top specification cells
#' 
#' @param specs Specifications data frame
#' @param p_cut P-value cutoff
#' @param n_top Number of top cells
#' @return Top cells tibble
top_cells <- function(specs, p_cut = 0.10, n_top = 10) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  specs %>%
    dplyr::mutate(p_ok = pvalue <= p_cut) %>%
    dplyr::group_by(fe, cluster, controls) %>%
    dplyr::summarise(
      n = dplyr::n(),
      share_pok = mean(p_ok, na.rm = TRUE),
      median_p = median(pvalue, na.rm = TRUE),
      median_est = median(estimate, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(share_pok), median_p) %>%
    dplyr::slice_head(n = n_top)
}

#' Build sensitivity bundle
#' 
#' @param specs Specifications data frame
#' @return List of sensitivity summaries
build_sensitivity_bundle <- function(specs) {
  if (is.null(specs) || nrow(specs) == 0L) return(list())
  specs <- add_strength_column(specs)
  
  list(
    overall = summarise_sensitivity_overall(specs),
    sign = summarise_sign_instability(specs),
    by_fe = summarise_by_fe(specs),
    by_cluster = summarise_by_cluster(specs),
    by_controls = summarise_by_controls(specs),
    classes = classify_specs(specs),
    top_cells = top_cells(specs)
  )
}

# ========================================================================
# PART 10: DISPLAY HELPERS
# ========================================================================

#' Pretty model name
#' 
#' @param model_type Model type string
#' @return Readable name
pretty_model_name <- function(model_type) {
  switch(
    model_type,
    "fractional_logit" = "Fractional Logit (quasi-binomial, logit link)",
    "ols_level" = "OLS (levels)",
    "ols_log" = "OLS (log-transformed outcome)",
    "gamma_log" = "Gamma GLM (log link)",
    model_type
  )
}

#' Pretty controls label
#' 
#' @param ctrl Controls specification
#' @return Readable label
pretty_controls_label <- function(ctrl) {
  switch(
    ctrl,
    "x_only" = "No controls",
    "base" = "Core controls",
    "base_extra" = "Core + extra controls",
    ctrl
  )
}

#' Pretty FE label
#' 
#' @param fe Fixed effects specification
#' @return Readable label
pretty_fe_label <- function(fe) {
  switch(
    fe,
    "0" = "No fixed effects",
    "buyer" = "Buyer FE",
    "year" = "Year FE",
    "buyer+year" = "Buyer + Year FE",
    "buyer#year" = "Buyer×Year FE",
    fe
  )
}

#' Controls note for captions
#' 
#' @param ctrl Controls specification
#' @return Note string
controls_note <- function(ctrl) {
  switch(
    ctrl,
    "x_only" = "Controls: none (missingness only).",
    "base" = "Controls: log(contract value), buyer type, procedure type.",
    "base_extra" = "Controls: log(contracts), log(avg contract value), log(total value), buyer type.",
    paste0("Controls: ", ctrl, ".")
  )
}

#' FE counts note
#' 
#' @param data Estimation data
#' @param fe Fixed effects specification
#' @return Note string
fe_counts_note <- function(data, fe) {
  if (is.null(data) || nrow(data) == 0) return("FE counts: N/A")
  
  if (fe == "buyer") {
    return(paste0("FE groups: buyers=", dplyr::n_distinct(data$buyer_masterid)))
  }
  if (fe == "year") {
    return(paste0("FE groups: years=", dplyr::n_distinct(data$tender_year)))
  }
  if (fe == "buyer+year") {
    return(paste0(
      "FE groups: buyers=", dplyr::n_distinct(data$buyer_masterid),
      ", years=", dplyr::n_distinct(data$tender_year)
    ))
  }
  if (fe == "buyer#year") {
    return(paste0(
      "FE groups: buyer×year=",
      dplyr::n_distinct(paste(data$buyer_masterid, data$tender_year, sep = "_"))
    ))
  }
  "FE counts: N/A"
}

# ========================================================================
# PART 11: MODULE - MISSING VALUE ANALYSIS (with dynamic heights)
# ========================================================================

#' Analyze missing values — enhanced with co-occurrence, MCAR test, MAR regressions
#'
#' New outputs vs old:
#'   results$mcar_test         — Little's MCAR test (list with p_value, interpretation)
#'   results$mar_results       — Tibble of per-variable MAR predictability (pseudo-R²)
#'   results$mar_plot          — Dot-plot of MAR predictability
#' Build a variable x group heatmap for a given top-n variables
#' Works for buyer-type and procedure-type breakdowns.
#' @param long_df        Long-format data with columns: group_var, variable_label, missing_share
#' @param group_var      Column name of the x-axis grouping (string)
#' @param var_order      Character vector of variable labels ranked desc by avg missingness
#' @param top_n          How many variables to show
#' @param title          Plot title
#' @param x_lab          X-axis label
make_groupvar_heatmap <- function(long_df, group_var, var_order, top_n,
                                  title = "", x_lab = "") {
  top_vars  <- head(var_order, top_n)
  lev_order <- rev(top_vars)           # worst at top of y-axis
  g_sym     <- rlang::sym(group_var)
  
  plot_df <- long_df %>%
    dplyr::filter(variable_label %in% top_vars) %>%
    dplyr::mutate(
      # Wrap long group labels (e.g. procedure types) to prevent squeezing
      !!g_sym := stringr::str_wrap(as.character(!!g_sym), width = 18),
      variable_label = factor(variable_label, levels = lev_order),
      tooltip = paste0(
        "<b>", variable_label, "</b><br>",
        !!g_sym, ": ", scales::percent(missing_share, accuracy = 0.1)
      )
    )
  
  high_dark  <- plot_df %>% dplyr::filter(missing_share >= 0.10, missing_share <= 0.30)
  high_light <- plot_df %>% dplyr::filter(missing_share >  0.30)
  
  ggplot2::ggplot(plot_df,
                  ggplot2::aes(x = !!g_sym, y = variable_label,
                               fill = missing_share, text = tooltip)) +
    ggplot2::geom_tile(colour = "white", linewidth = 0.35) +
    ggplot2::geom_text(data = high_dark,
                       ggplot2::aes(label = scales::percent(missing_share, accuracy = 1)),
                       size = 3.0, colour = "grey20", fontface = "bold") +
    ggplot2::geom_text(data = high_light,
                       ggplot2::aes(label = scales::percent(missing_share, accuracy = 1)),
                       size = 3.0, colour = "white", fontface = "bold") +
    ggplot2::scale_fill_gradientn(
      colours  = c("#27ae60", "#eafaf1", "#ffe082", "#e53935"),
      values   = scales::rescale(c(0, 0.05, 0.20, 1)),
      limits   = c(0, 1),
      labels   = scales::percent_format(accuracy = 1),
      name     = "Missing\nshare", na.value = "grey88") +
    ggplot2::labs(title = title, x = x_lab, y = NULL) +
    standard_plot_theme() +
    ggplot2::theme(
      axis.text.x     = ggplot2::element_text(angle = 45, hjust = 1, size = 9),
      axis.text.y     = ggplot2::element_text(size = max(7, min(11, 150 / top_n))),
      panel.grid      = ggplot2::element_blank(),
      legend.position = "right")
}


#' @param by_year_df   Long-format data from analyze_missing_values$by_year
#' @param var_order    Character vector of variable labels ranked by avg missingness (desc)
#' @param top_n        How many variables to show
#' @param country      Country label for title
#' @return ggplot object
make_year_heatmap <- function(by_year_df, var_order, top_n, country = "") {
  top_vars  <- head(var_order, top_n)
  # ascending order so worst variable is at TOP of y-axis after coord flip
  lev_order <- rev(top_vars)
  
  plot_df <- by_year_df %>%
    dplyr::filter(variable_label %in% top_vars) %>%
    dplyr::mutate(
      variable_label = factor(variable_label, levels = lev_order),
      tooltip = paste0(
        "<b>", variable_label, "</b> | Year: ", tender_year, "<br>",
        "Missing: ", scales::percent(missing_share, accuracy = 0.1)
      )
    )
  
  ggplot2::ggplot(plot_df,
                  ggplot2::aes(x = factor(tender_year), y = variable_label,
                               fill = missing_share, text = tooltip)) +
    ggplot2::geom_tile(colour = "white", linewidth = 0.3) +
    ggplot2::geom_text(
      data = ~ dplyr::filter(.x, missing_share >= 0.10),
      ggplot2::aes(label = scales::percent(missing_share, accuracy = 1)),
      size = 2.8, colour = "white", fontface = "bold") +
    ggplot2::scale_fill_gradientn(
      colours  = c("#27ae60", "#eafaf1", "#ffe082", "#e53935"),
      values   = scales::rescale(c(0, 0.05, 0.20, 1)),
      limits   = c(0, 1),
      labels   = scales::percent_format(accuracy = 1),
      name     = "Missing\nshare",
      na.value = "grey88") +
    ggplot2::labs(
      title    = paste0("Missing share by variable and year (top ", top_n, " variables)",
                        if (nchar(country) > 0) paste0(" \u2013 ", country) else ""),
      subtitle = "Green = complete data, red = high missingness. Hover for exact values.",
      x        = "Year",
      y        = NULL) +
    standard_plot_theme() +
    ggplot2::theme(
      axis.text.x     = ggplot2::element_text(angle = 45, hjust = 1),
      axis.text.y     = ggplot2::element_text(size = max(7, min(10, 140 / top_n))),
      panel.grid      = ggplot2::element_blank(),
      legend.position = "right")
}

#' Analyze missing values
#'   results$mcar_test         — Little's MCAR test (list with p_value, interpretation)
#'   results$mar_results       — Tibble of per-variable MAR predictability (pseudo-R2)
#'   results$mar_plot          — Dot-plot of MAR predictability
#'   results$cooccurrence_plot — Jaccard co-missingness lollipop
#'
#' @param df Data frame
#' @param config Configuration list
#' @param output_dir Output directory
#' @return List of results
analyze_missing_values <- function(df, config, output_dir, save_plots = TRUE) {
  results   <- list()
  save_plot <- if (isTRUE(save_plots)) create_plot_saver(output_dir, config) else function(...) invisible(NULL)
  
  # ── Overall missing shares ─────────────────────────────────────────────
  df_clean             <- df %>% dplyr::select(!dplyr::ends_with("_missing_share"))
  missing_shares       <- df_clean %>% summarise_missing_shares(cols = !dplyr::starts_with("ind_"))
  results$overall      <- missing_shares
  results$overall_long <- pivot_missing_long(missing_shares)
  
  results$overall_plot <- plot_missing_bar(
    data_long    = results$overall_long,
    label_lookup = label_lookup,
    title        = paste("Missing Value Share per Variable \u2013", config$country),
    n_total      = nrow(df)
  )
  save_plot(results$overall_plot, "missing_shares", width = 10, height = 8)
  
  # MCAR test, MAR regressions, and co-occurrence are deferred (run on demand)
  results$mcar_test         <- NULL
  results$mar_results       <- NULL
  results$mar_plot          <- NULL
  results$cooccurrence_plot <- NULL
  
  # ── By buyer type ──────────────────────────────────────────────────────
  if (validate_required_columns(df, "buyer_buyertype", "missing by buyer type")) {
    df_clean <- df %>% dplyr::select(!dplyr::ends_with("_missing_share"))
    results$by_buyer <- df_clean %>%
      dplyr::mutate(buyer_group = add_buyer_group(buyer_buyertype)) %>%
      dplyr::group_by(buyer_group) %>%
      summarise_missing_shares(cols = -dplyr::starts_with("ind_")) %>%
      pivot_missing_long(id_vars = "buyer_group") %>%
      dplyr::mutate(variable_label = label_with_lookup(variable, label_lookup))
    
    results$by_buyer_var_order <- results$by_buyer %>%
      dplyr::group_by(variable_label) %>%
      dplyr::summarise(avg = mean(missing_share, na.rm = TRUE), .groups = "drop") %>%
      dplyr::arrange(dplyr::desc(avg)) %>%
      dplyr::pull(variable_label)
    results$by_buyer_n_vars_max <- length(results$by_buyer_var_order)
    
    results$by_buyer_plot <- plot_missing_heatmap(
      data     = results$by_buyer,
      x_var    = "buyer_group",
      y_var    = "variable_label",
      fill_var = "missing_share",
      title    = paste("Missing value share per variable by buyer type \u2013", config$country),
      x_lab    = "Buyer type",
      y_lab    = "Variable"
    )
  }
  
  # ── Top buyers by missing share ────────────────────────────────────────
  if (validate_required_columns(df, c("buyer_masterid", "buyer_buyertype"), "top missing buyers")) {
    results$top_buyers_missing <- df %>%
      dplyr::select(!dplyr::starts_with("ind_")) %>%
      dplyr::group_by(buyer_masterid, buyer_buyertype) %>%
      dplyr::filter(dplyr::n() >= config$thresholds$min_buyer_contracts) %>%
      dplyr::summarise(
        missing_share = mean(is.na(dplyr::cur_data_all()), na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::arrange(dplyr::desc(missing_share)) %>%
      dplyr::slice_head(n = config$thresholds$top_n_buyers) %>%
      dplyr::mutate(
        buyer_group       = add_buyer_group(buyer_buyertype),
        buyer_group_label = as.character(buyer_group),
        buyer_label       = paste0(buyer_masterid, "\n", buyer_group_label),
        buyer_label_short = dplyr::if_else(
          nchar(buyer_masterid) > 20,
          paste0(substr(buyer_masterid, 1, 20), "\u2026\n", buyer_group_label),
          buyer_label)
      )
    
    results$top_buyers_plot <- plot_top_bar(
      df         = results$top_buyers_missing,
      x_var      = "buyer_label_short",
      y_var      = "missing_share",
      label_var  = "missing_share",
      title      = paste("Top buyers with highest overall missing share \u2013", config$country),
      x_lab      = "Buyer",
      y_lab      = "Missing share",
      fill_color = "skyblue1",
      y_limit    = c(0, 1.05),
      percent    = TRUE
    )
  }
  
  if (!is.null(results$by_buyer_plot) && !is.null(results$top_buyers_plot)) {
    results$combined_buyers_plot <- results$by_buyer_plot + results$top_buyers_plot +
      patchwork::plot_layout(nrow = 2)
    save_plot(results$combined_buyers_plot, "buyers_missing", width = 12, height = 16)
  }
  
  # ── By procedure type ──────────────────────────────────────────────────
  if (validate_required_columns(df, "tender_proceduretype", "missing by procedure type")) {
    df_clean <- df %>% dplyr::select(!dplyr::ends_with("_missing_share"))
    results$by_procedure <- df_clean %>%
      dplyr::mutate(
        proc_group = ifelse(is.na(tender_proceduretype),
                            "Missing procedure type", as.character(tender_proceduretype))
      ) %>%
      dplyr::group_by(proc_group) %>%
      summarise_missing_shares(cols = -dplyr::starts_with("ind_")) %>%
      pivot_missing_long(id_vars = "proc_group") %>%
      dplyr::mutate(
        proc_group_label = ifelse(
          proc_group %in% names(procedure_type_labels),
          procedure_type_labels[proc_group], proc_group),
        variable_label   = label_with_lookup(variable, label_lookup),
        proc_group_label = factor(proc_group_label, levels = c(
          "Open (competitive bidding)",
          "Restricted (limited competition)",
          "Negotiated with publication (limited competition)",
          "Negotiated without publication (no competition)",
          "Negotiated (limited competition)",
          "Competitive dialogue (limited competition)",
          "Outright award (direct purchase)",
          "Other (special or exceptional procedures)",
          "Missing procedure type"))
      )
    
    results$by_procedure_var_order <- results$by_procedure %>%
      dplyr::group_by(variable_label) %>%
      dplyr::summarise(avg = mean(missing_share, na.rm = TRUE), .groups = "drop") %>%
      dplyr::arrange(dplyr::desc(avg)) %>%
      dplyr::pull(variable_label)
    results$by_procedure_n_vars_max <- length(results$by_procedure_var_order)
    
    results$by_procedure_plot <- plot_missing_heatmap(
      data     = results$by_procedure,
      x_var    = "proc_group_label",
      y_var    = "variable_label",
      fill_var = "missing_share",
      title    = paste("Missing value share per variable by procedure type \u2013", config$country),
      x_lab    = "Procedure Type",
      y_lab    = "Variable"
    )
    save_plot(results$by_procedure_plot, "missing_by_proc", width = 12, height = 16)
  }
  
  # ── NA correlation matrix — DEFERRED: expensive corrr::correlate on large data ──
  # Stored as NULL; populated lazily on first render of the correlation plot tab
  results$correlations     <- NULL
  results$correlation_plot <- NULL
  results$correlation_top  <- NULL
  
  # ── Missing by year ────────────────────────────────────────────────────
  if (validate_required_columns(df, "tender_year", "missing by year")) {
    df_clean <- df %>% dplyr::select(!dplyr::ends_with("_missing_share"))
    results$by_year <- df_clean %>%
      dplyr::filter(!is.na(tender_year)) %>%
      dplyr::group_by(tender_year) %>%
      summarise_missing_shares(cols = !dplyr::starts_with("ind_")) %>%
      pivot_missing_long(id_vars = "tender_year") %>%
      dplyr::mutate(variable_label = label_with_lookup(variable, label_lookup))
    
    results$by_year_var_order <- results$by_year %>%
      dplyr::group_by(variable_label) %>%
      dplyr::summarise(avg_missing = mean(missing_share, na.rm = TRUE), .groups = "drop") %>%
      dplyr::arrange(dplyr::desc(avg_missing)) %>%
      dplyr::pull(variable_label)
    
    results$by_year_n_vars_max <- length(results$by_year_var_order)
    
    top_n_default <- min(config$thresholds$top_n_vars, results$by_year_n_vars_max)
    results$by_year_plot <- make_year_heatmap(
      results$by_year, results$by_year_var_order, top_n_default, config$country)
    
    save_plot(results$by_year_plot, "year_miss", width = 12, height = 7)
  }
  
  results
}

#' Run advanced missingness tests on demand (MCAR, MAR, co-occurrence)
#'
#' Called from the Shiny app when the user clicks "Run Advanced Tests".
#' Separated from analyze_missing_values() because MAR regressions can be
#' slow on large datasets (one GLM per variable).
#'
#' @param df           Data frame (already prepared)
#' @param config       Pipeline config list
#' @param output_dir   Output directory
#' @return List: mcar_test, mar_results, mar_plot, cooccurrence_plot
run_missing_advanced_tests <- function(df, config, output_dir) {
  save_plot <- create_plot_saver(output_dir, config)
  out <- list()
  
  # Little's MCAR test
  message("  Running Little's MCAR test...")
  out$mcar_test <- tryCatch(
    run_little_mcar_test(df, max_cols = 20),
    error = function(e) {
      message("  MCAR test error: ", e$message)
      list(statistic = NA, df = NA, p_value = NA, n_cols = NA, n_rows = nrow(df),
           interpretation = paste("Test failed:", e$message))
    }
  )
  
  # Co-occurrence (fast - no modelling)
  message("  Computing missingness co-occurrence...")
  out$cooccurrence_data <- tryCatch(
    compute_cooccurrence_data(df, label_lookup),
    error = function(e) { message("  Co-occurrence error: ", e$message); NULL }
  )
  out$cooccurrence_plot <- if (!is.null(out$cooccurrence_data)) {
    tryCatch(
      plot_cooccurrence_from_data(out$cooccurrence_data, top_n = 20, min_jaccard = 0,
                                  title = paste("Variable Pairs Most Often Missing Together \u2013", config$country)),
      error = function(e) NULL
    )
  } else NULL
  if (!is.null(out$cooccurrence_plot))
    save_plot(out$cooccurrence_plot, "missing_cooccurrence", width = 10, height = 8)
  
  # MAR predictability regressions (slower)
  message("  Running MAR predictability regressions...")
  out$mar_results <- tryCatch(
    run_mar_predictability(df, label_lookup, min_miss = 0.01),
    error = function(e) { message("  MAR test error: ", e$message); NULL }
  )
  out$mar_plot <- if (!is.null(out$mar_results) && nrow(out$mar_results) > 0) {
    tryCatch(
      plot_mar_predictability(out$mar_results,
                              title = paste("Is Missingness Random? \u2013", config$country)),
      error = function(e) NULL
    )
  } else NULL
  if (!is.null(out$mar_plot))
    save_plot(out$mar_plot, "missing_mar", width = 10, height = 8)
  
  out
}

# ========================================================================
# PART 12: MODULE - INTEROPERABILITY ANALYSIS
# ========================================================================

#' Analyze interoperability (organization-level matching)
#' 
#' @param df Data frame
#' @param config Configuration list
#' @param output_dir Output directory
#' @return List of results
analyze_interoperability <- function(df, config, output_dir) {
  results <- list()
  
  results$org_missing <- compute_org_missing(df)
  
  results
}

# ========================================================================
# PART 13: MODULE - COMPETITION ANALYSIS
# ========================================================================

#' Build CPV cluster data frame
#' 
#' @param df Data frame
#' @param cpv_digits Number of CPV digits
#' @return Data frame with CPV clusters
build_cpv_df <- function(df, cpv_digits = 3) {
  cols_to_keep <- intersect(
    c("tender_id", "lot_number", "bidder_id", "bidder_name",
      "lot_productcode", "bid_priceusd"),
    names(df)
  )
  df %>%
    dplyr::select(dplyr::all_of(cols_to_keep)) %>%
    dplyr::mutate(
      cpv_cluster = stringr::str_sub(lot_productcode, 1, cpv_digits)
    )
}

#' Build the yearly concentration bar chart from pre-computed data
#' Called from the app with slider-controlled n_buyers and min_contracts
#' @param yearly_data   Output of analyze_buyer_supplier_concentration$yearly_data
#' @param n_buyers      Max buyers per year panel to show
#' @param min_contracts Minimum total contracts per buyer-year to include
#' @param country       Country label for title
build_concentration_yearly_plot <- function(yearly_data, n_buyers = 15,
                                            min_contracts = 1, country = "") {
  if (is.null(yearly_data) || nrow(yearly_data) == 0) return(NULL)
  
  plot_df <- yearly_data %>%
    dplyr::filter(total_contracts >= min_contracts) %>%
    dplyr::group_by(tender_year) %>%
    dplyr::slice_max(max_conc, n = n_buyers, with_ties = FALSE) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      tooltip = paste0(
        "<b>", buyer_masterid, "</b>",
        dplyr::if_else(
          !is.na(buyer_name) & buyer_name != "" & buyer_name != buyer_masterid,
          paste0("<br><i>", buyer_name, "</i>"),
          ""
        ), "<br>",
        "Year: <b>", tender_year, "</b><br>",
        "Max supplier concentration: <b>", scales::percent(max_conc, accuracy = 1), "</b><br>",
        "Contracts this year: ", scales::comma(total_contracts), "<br>",
        dplyr::if_else(
          repeated,
          paste0("<b>Also in: ", years_list, "</b><br>",
                 "<i>Persistent concentration risk</i>"),
          "<i>Single-year appearance</i>"
        )
      ),
      contracts_label = scales::comma(total_contracts)
    )
  
  if (nrow(plot_df) == 0) return(NULL)
  
  n_years <- dplyr::n_distinct(plot_df$tender_year)
  n_cols  <- min(n_years, 3)
  
  ggplot2::ggplot(plot_df,
                  ggplot2::aes(
                    x    = tidytext::reorder_within(buyer_short, max_conc, tender_year),
                    y    = max_conc,
                    fill = repeat_label,
                    text = tooltip
                  )
  ) +
    ggplot2::geom_col(width = 0.72) +
    ggplot2::geom_text(
      ggplot2::aes(label = contracts_label),
      hjust  = -0.15, size = 2.6, colour = "grey30"
    ) +
    ggplot2::coord_flip() +
    ggplot2::facet_wrap(~ tender_year, scales = "free_y", ncol = n_cols) +
    tidytext::scale_x_reordered() +
    ggplot2::scale_fill_manual(
      values = c("Appears in multiple years" = "#e74c3c",
                 "Single year only"          = "#5dade2"),
      name   = NULL
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::percent_format(accuracy = 1),
      expand = ggplot2::expansion(mult = c(0, 0.30))
    ) +
    ggplot2::labs(
      title    = NULL,
      subtitle = paste0("Red = buyer appears in multiple years (persistent risk). ",
                        "Labels = number of contracts. Hover for full detail."),
      x        = NULL,
      y        = "Max share of spending\ngoing to one supplier"
    ) +
    standard_plot_theme() +
    ggplot2::theme(
      axis.text.y        = ggplot2::element_text(size = 8),
      axis.text.x        = ggplot2::element_text(size = 8),
      axis.title.y       = ggplot2::element_text(size = 8, lineheight = 1.1),
      strip.text         = ggplot2::element_text(size = 10, face = "bold"),
      strip.placement    = "outside",
      legend.position    = "none",
      panel.grid.major.y = ggplot2::element_blank(),
      plot.margin        = ggplot2::margin(4, 8, 4, 4)
    )
}

#' Analyze buyer-supplier concentration
#' 
#' @param df Data frame
#' @param config Configuration list
#' @return List of concentration data and plots
analyze_buyer_supplier_concentration <- function(df, config) {
  results <- list()
  
  if (!validate_required_columns(
    df,
    c("buyer_masterid", "bidder_masterid", "bid_priceusd"),
    "buyer-supplier concentration"
  )) {
    return(results)
  }
  
  results$data <- df %>%
    dplyr::group_by(tender_year, buyer_masterid, bidder_masterid) %>%
    dplyr::summarise(
      n_contracts = dplyr::n(),
      total_spend = sum(bid_priceusd, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::group_by(tender_year, buyer_masterid) %>%
    dplyr::mutate(
      buyer_total_spend = sum(total_spend, na.rm = TRUE),
      buyer_total_contract = dplyr::n(),
      buyer_concentration = ifelse(
        buyer_total_spend > 0,
        total_spend / buyer_total_spend,
        NA_real_
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(tender_year, buyer_masterid) %>%
    dplyr::mutate(
      n_suppliers = dplyr::n(),
      buyer_concentration_display = ifelse(
        n_suppliers < config$thresholds$min_suppliers_for_buyer_conc,
        NA_real_,
        buyer_concentration
      )
    ) %>%
    dplyr::ungroup() %>%
    tidyr::drop_na()
  
  # Overall concentration
  top_buyers <- results$data %>%
    dplyr::group_by(buyer_masterid) %>%
    dplyr::summarise(
      max_conc = max(buyer_concentration_display, na.rm = TRUE),
      total_contracts = sum(n_contracts, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::slice_max(max_conc, n = config$thresholds$top_n_buyers)
  
  results$overall_plot <- plot_top_bar(
    df = top_buyers,
    x_var = "buyer_masterid",
    y_var = "max_conc",
    label_var = "total_contracts",
    title = paste("Top Buyers by Maximum Supplier Concentration –", config$country),
    x_lab = "Buyer ID",
    y_lab = "Max share of spending to one supplier",
    fill_color = "skyblue1",
    y_limit = c(0, 1.05),
    percent = FALSE
  )
  
  # Yearly concentration — store raw data so app can filter interactively
  top_buyers_yearly_raw <- results$data %>%
    dplyr::filter(tender_year > 2014) %>%
    dplyr::group_by(tender_year, buyer_masterid) %>%
    dplyr::summarise(
      max_conc        = max(buyer_concentration_display, na.rm = TRUE),
      total_contracts = sum(n_contracts, na.rm = TRUE),
      buyer_name      = dplyr::first(if ("buyer_name" %in% names(dplyr::cur_data_all()))
        buyer_name else NA_character_),
      .groups = "drop"
    )
  
  # Pre-compute which years each buyer appears in (across ALL years, before any n filter)
  buyer_year_appearances <- top_buyers_yearly_raw %>%
    dplyr::group_by(buyer_masterid) %>%
    dplyr::summarise(
      n_years_appeared = dplyr::n(),
      years_list       = paste(sort(unique(tender_year)), collapse = ", "),
      .groups = "drop"
    )
  
  results$yearly_data <- top_buyers_yearly_raw %>%
    dplyr::left_join(buyer_year_appearances, by = "buyer_masterid") %>%
    dplyr::mutate(
      repeated     = n_years_appeared > 1,
      buyer_short  = substr(buyer_masterid, 1, 12),
      repeat_label = dplyr::if_else(repeated, "Appears in multiple years", "Single year only")
    )
  
  results$yearly_plot <- NULL   # built dynamically in app with sliders
  
  results
}

#' Run single-bidding specifications
#' 
#' @param buyer_analysis_fe Buyer analysis data with FE-eligible buyers
#' @param config Configuration list
#' @return Data frame of specification results
run_singleb_specs <- function(buyer_analysis_fe, config) {
  out <- list()
  k <- 0L
  
  for (fe in config$models$fe_set) {
    fe_part <- make_fe_part(fe)
    
    for (cl in config$models$cluster_set) {
      cl_fml <- make_cluster(cl)
      
      for (ctrl in config$models$controls_set) {
        rhs_terms <- switch(
          ctrl,
          "x_only" = c("cumulative_missing_share"),
          "base" = c("cumulative_missing_share", "log1p(n_contracts)", "log1p(avg_contract_value)"),
          "base_extra" = c("cumulative_missing_share", "log1p(n_contracts)", "log1p(avg_contract_value)",
                           "log1p(total_contract_value)", "buyer_buyertype")
        )
        
        rhs_terms <- rhs_terms[rhs_terms %in% names(buyer_analysis_fe)]
        rhs <- paste(rhs_terms, collapse = " + ")
        fml <- stats::as.formula(paste0("cumulative_singleb_rate ~ ", rhs, " | ", fe_part))
        
        m <- safe_fixest(
          fixest::feglm(
            fml,
            family = quasibinomial(link = "logit"),
            data = buyer_analysis_fe,
            cluster = cl_fml
          )
        )
        
        if (is.null(m)) next
        
        eff <- extract_effect_fixest(
          model = m,
          x_name = "cumulative_missing_share",
          data_used = buyer_analysis_fe
        )
        
        eff_strength <- safe_fixest(effect_p10_p90(m, buyer_analysis_fe, "cumulative_missing_share"))
        if (is.null(eff_strength)) eff_strength <- NA_real_
        
        k <- k + 1L
        out[[k]] <- data.frame(
          outcome = "singleb",
          model_type = "fractional_logit",
          fe = fe,
          cluster = cl,
          controls = ctrl,
          estimate = eff$estimate,
          pvalue = eff$pvalue,
          nobs = eff$nobs,
          std_slope = eff$std_slope,
          effect_strength = eff_strength,
          stringsAsFactors = FALSE
        )
      }
    }
  }
  
  if (length(out) == 0) return(data.frame())
  do.call(rbind, out)
}

#' Analyze single bidding — data preparation only (no regressions)
#' Regressions are deferred to run_regressions button via analyze_competition(run_regressions=TRUE)
#' 
#' @param df Data frame
#' @param config Configuration list
#' @return List with $data (buyer-year panel ready for regression)
analyze_singleb_data <- function(df, config) {
  results <- list()
  
  if (!validate_required_columns(df, "ind_corr_singleb", "single-bidding analysis")) {
    return(results)
  }
  
  # Scale to 0-1
  df <- df %>%
    dplyr::mutate(
      ind_corr_singleb = dplyr::if_else(
        !is.na(ind_corr_singleb),
        ind_corr_singleb / 100,
        NA_real_
      )
    )
  
  # Filter by year
  yr <- config$years_singleb
  df_filtered <- df %>%
    dplyr::filter(
      !is.na(tender_year),
      tender_year >= yr$min_year,
      tender_year <= yr$max_year
    )
  
  if (nrow(df_filtered) == 0L) {
    message("No observations after year filter for single-bidding analysis")
    return(results)
  }
  
  # Compute buyer-level aggregates
  buyer_missing_share <- df_filtered %>%
    dplyr::group_by(buyer_masterid, tender_year) %>%
    summarise_missing_shares(cols = !dplyr::starts_with("ind_")) %>%
    dplyr::mutate(
      cumulative_missing_share = rowMeans(
        dplyr::across(dplyr::ends_with("_missing_share")),
        na.rm = TRUE
      )
    )
  
  buyer_singleb_rate <- df_filtered %>%
    dplyr::group_by(buyer_masterid, tender_year) %>%
    dplyr::summarise(
      cumulative_singleb_rate = mean(ind_corr_singleb, na.rm = TRUE),
      .groups = "drop"
    )
  
  buyer_controls <- df_filtered %>%
    dplyr::group_by(buyer_masterid, tender_year, buyer_buyertype) %>%
    dplyr::summarise(
      n_contracts          = dplyr::n(),
      avg_contract_value   = mean(bid_priceusd, na.rm = TRUE),
      total_contract_value = sum(bid_priceusd, na.rm = TRUE),
      .groups = "drop"
    )
  
  buyer_analysis <- buyer_missing_share %>%
    dplyr::select(buyer_masterid, tender_year, cumulative_missing_share) %>%
    dplyr::inner_join(buyer_singleb_rate, by = c("buyer_masterid", "tender_year")) %>%
    dplyr::inner_join(buyer_controls,     by = c("buyer_masterid", "tender_year")) %>%
    dplyr::mutate(
      buyer_buyertype = forcats::fct_explicit_na(as.factor(buyer_buyertype), "Unknown")
    )
  
  # Filter to buyers with sufficient history for FE models
  eligible_buyers <- buyer_analysis %>%
    dplyr::group_by(buyer_masterid) %>%
    dplyr::summarise(n_years = dplyr::n_distinct(tender_year), .groups = "drop") %>%
    dplyr::filter(n_years >= config$thresholds$min_buyer_years) %>%
    dplyr::pull(buyer_masterid)
  
  buyer_analysis_fe <- buyer_analysis %>%
    dplyr::filter(buyer_masterid %in% eligible_buyers)
  
  if (nrow(buyer_analysis_fe) == 0L) {
    message("No buyers with sufficient years of data for single-bidding analysis")
    return(results)
  }
  
  results$data <- buyer_analysis_fe
  results
}

#' @deprecated Use analyze_singleb_data() for data prep; regressions run via analyze_competition(run_regressions=TRUE)
analyze_singleb <- analyze_singleb_data

#' Analyze competition
#' 
#' @param df Data frame
#' @param config Configuration list
#' @param output_dir Output directory
#' @return List of results
analyze_competition <- function(df, config, output_dir, run_regressions = FALSE, save_plots = TRUE) {
  results <- list()
  save_plot <- if (isTRUE(save_plots)) create_plot_saver(output_dir, config) else function(...) invisible(NULL)
  
  # ── Buyer-supplier concentration (fast — data transforms only) ────────────
  conc <- analyze_buyer_supplier_concentration(df, config)
  if (length(conc) > 0) {
    results$concentration <- conc$data
    results$concentration_overall_plot <- conc$overall_plot
    results$concentration_yearly_data <- conc$yearly_data
    save_plot(conc$overall_plot, "bc_overall")
    save_plot(conc$yearly_plot, "bc_yearly")
  }
  
  # ── Single-bidding data prep and regressions both DEFERRED ───────────────
  # No singleb UI is shown before "Run Regression Analysis" is clicked.
  # analyze_singleb_data() + run_singleb_specs() run together when button pressed.
  
  # ── Regressions are DEFERRED — only run when explicitly requested ─────────
  if (isTRUE(run_regressions)) {
    message("Preparing single-bidding panel...")
    singleb <- analyze_singleb_data(df, config)
    if (length(singleb) > 0) results$singleb_data <- singleb$data
    
    if (!is.null(results$singleb_data) && nrow(results$singleb_data) > 0) {
      n_specs <- length(config$models$fe_set) *
        length(config$models$cluster_set) *
        length(config$models$controls_set)
      message("Running ", n_specs, " single-bidding specifications...")
      specs <- run_singleb_specs(results$singleb_data, config)
      results$singleb_specs       <- specs
      results$singleb_sensitivity <- build_sensitivity_bundle(specs)
      
      if (!is.null(specs) && nrow(specs) > 0) {
        best_row <- pick_best_model(specs, require_positive = TRUE,
                                    p_max = config$models$p_max,
                                    strength_col = "effect_strength")
        if (!is.null(best_row)) {
          fe_part   <- make_fe_part(best_row$fe)
          cl_fml    <- make_cluster(best_row$cluster)
          rhs_terms <- switch(
            best_row$controls,
            "x_only"     = c("cumulative_missing_share"),
            "base"       = c("cumulative_missing_share", "log1p(n_contracts)", "log1p(avg_contract_value)"),
            "base_extra"  = c("cumulative_missing_share", "log1p(n_contracts)", "log1p(avg_contract_value)",
                              "log1p(total_contract_value)", "buyer_buyertype")
          )
          fml   <- stats::as.formula(paste0("cumulative_singleb_rate ~ ",
                                            paste(rhs_terms, collapse = " + "),
                                            " | ", fe_part))
          model <- fixest::feglm(fml, family = quasibinomial(link = "logit"),
                                 data = results$singleb_data, cluster = cl_fml)
          pred  <- tryCatch(ggeffects::ggpredict(model, terms = "cumulative_missing_share"),
                            error = function(e) NULL)
          if (!is.null(pred)) {
            years_used <- range(results$singleb_data$tender_year, na.rm = TRUE)
            results$singleb_plot <- plot_ggeffects_line(
              pred     = pred,
              title    = paste("Predicted Single-Bidding by Missing Share \u2013", config$country),
              subtitle = paste0(
                "(BEST) Model: ", pretty_model_name(best_row$model_type),
                " | Years: ", years_used[1], "\u2013", years_used[2],
                " | N=", best_row$nobs,
                " | FE: ", pretty_fe_label(best_row$fe),
                " | Cluster: ", best_row$cluster,
                " | ", fe_counts_note(results$singleb_data, best_row$fe)
              ),
              x_lab   = "Buyer Missing Share",
              y_lab   = "Predicted Single-Bidding Share",
              caption = paste0(
                "Filters: tender_year in [", config$years_singleb$min_year, ", ",
                config$years_singleb$max_year,
                "]; buyers with \u2265", config$thresholds$min_buyer_years, " years of data. ",
                controls_note(best_row$controls)
              )
            )
            save_plot(results$singleb_plot, "reg_singleb_missing_share")
          }
        }
      }
    }  # end if singleb_data not null
  }  # end if run_regressions
  
  results
}

# ========================================================================
# PART 14: MODULE - MARKET ANALYSIS
# ========================================================================

#' Detect unusual market entries
#' 
#' @param cpv_data CPV cluster data
#' @param config Configuration list
#' @return Data frame of unusual entries
detect_unusual_entries <- function(cpv_data, config) {
  # Bidder-CPV statistics
  bidder_cpv_stats <- cpv_data %>%
    dplyr::group_by(bidder_id, cpv_cluster) %>%
    dplyr::summarise(
      n_awards = dplyr::n(),
      total_value = sum(bid_priceusd, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::group_by(bidder_id) %>%
    dplyr::mutate(
      bidder_total_awards = sum(n_awards),
      bidder_total_value = sum(total_value),
      share_awards = n_awards / bidder_total_awards,
      share_value = ifelse(
        bidder_total_value > 0,
        total_value / bidder_total_value,
        NA_real_
      )
    ) %>%
    dplyr::ungroup()
  
  # Flag atypical clusters
  df_with_flags <- cpv_data %>%
    dplyr::left_join(
      bidder_cpv_stats %>%
        dplyr::select(bidder_id, cpv_cluster, n_awards, bidder_total_awards, share_awards, share_value),
      by = c("bidder_id", "cpv_cluster")
    ) %>%
    dplyr::mutate(
      enough_history = bidder_total_awards >= config$thresholds$min_history_threshold,
      cpv_cluster_atypical = enough_history &
        share_awards < config$thresholds$marginal_share_threshold &
        n_awards <= config$thresholds$max_wins_atypical
    )
  
  # Cluster-supplier counts
  cluster_supplier_counts <- cpv_data %>%
    dplyr::group_by(cpv_cluster, bidder_id) %>%
    dplyr::summarise(n_ic = dplyr::n(), .groups = "drop")
  
  cluster_totals <- cpv_data %>%
    dplyr::group_by(cpv_cluster) %>%
    dplyr::summarise(
      n_c = dplyr::n(),
      n_suppliers_c = dplyr::n_distinct(bidder_id),
      .groups = "drop"
    )
  
  # Surprise scores — standardised WITHIN each cluster so scores are comparable
  cluster_surprise <- cluster_supplier_counts %>%
    dplyr::left_join(cluster_totals, by = "cpv_cluster") %>%
    dplyr::mutate(
      p_i_given_c   = (n_ic + 1) / (n_c + n_suppliers_c),
      surprise_score = -log(p_i_given_c)
    ) %>%
    dplyr::group_by(cpv_cluster) %>%
    dplyr::mutate(
      # Within-cluster z-score: how unusual is this supplier relative to others in same market
      surprise_z = if (dplyr::n() > 1 && stats::sd(surprise_score, na.rm = TRUE) > 0)
        (surprise_score - mean(surprise_score, na.rm = TRUE)) / stats::sd(surprise_score, na.rm = TRUE)
      else 0
    ) %>%
    dplyr::ungroup()
  
  df_with_surprise <- df_with_flags %>%
    dplyr::left_join(
      cluster_surprise %>% dplyr::select(cpv_cluster, bidder_id, surprise_score, surprise_z),
      by = c("cpv_cluster", "bidder_id")
    )
  
  # Home market
  home_market <- bidder_cpv_stats %>%
    dplyr::group_by(bidder_id) %>%
    dplyr::slice_max(n_awards, n = 1, with_ties = FALSE) %>%
    dplyr::ungroup() %>%
    dplyr::transmute(bidder_id, home_cpv_cluster = cpv_cluster)
  
  # Unusual entries
  df_unusual <- df_with_surprise %>%
    dplyr::left_join(home_market, by = "bidder_id") %>%
    dplyr::mutate(target_cpv_cluster = cpv_cluster) %>%
    dplyr::filter(enough_history, cpv_cluster_atypical, !is.na(surprise_z))
  
  df_unusual
}

#' Build unusual entry matrix
#' 
#' @param df_unusual Unusual entries data
#' @param config Configuration list
#' @return Data frame of unusual entry flows
build_unusual_matrix <- function(df_unusual, config) {
  df_unusual %>%
    dplyr::group_by(home_cpv_cluster, target_cpv_cluster) %>%
    dplyr::summarise(
      n_bidders = dplyr::n_distinct(bidder_id),
      n_awards = dplyr::n(),
      mean_surprise = mean(surprise_z, na.rm = TRUE),
      .groups = "drop"
    )
}

#' Build ggraph network plot from unusual_matrix with dynamic filters
#' Called from the app reactively when sliders change
#' @param unusual_matrix  Output of build_unusual_matrix()
#' @param min_bidders     Minimum suppliers per edge
#' @param top_n           Top N clusters to include
#' @param cl_filter       Optional character vector of cluster codes to focus on
#' @param country         Country label for title
build_network_graph_from_matrix <- function(unusual_matrix, min_bidders = 4,
                                            top_n = 20, cl_filter = NULL,
                                            country = "") {
  edges <- unusual_matrix %>%
    dplyr::rename(from = home_cpv_cluster, to = target_cpv_cluster) %>%
    dplyr::filter(n_bidders >= min_bidders, from != to)
  
  if (!is.null(cl_filter) && length(cl_filter) > 0)
    edges <- edges %>% dplyr::filter(from %in% cl_filter | to %in% cl_filter)
  
  top_clusters <- edges %>%
    tidyr::pivot_longer(c(from, to), values_to = "cluster") %>%
    dplyr::count(cluster, wt = n_bidders, sort = TRUE) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::pull(cluster)
  
  edges <- edges %>%
    dplyr::filter(from %in% top_clusters, to %in% top_clusters)
  
  if (nrow(edges) == 0) return(NULL)
  
  g <- igraph::graph_from_data_frame(edges, directed = TRUE)
  igraph::V(g)$degree <- igraph::degree(g, mode = "all")
  
  n_nodes   <- igraph::vcount(g)
  lbl_size  <- max(3.2, min(5.5, 44 / max(n_nodes, 1)))
  node_range <- c(max(5, min(8, 80 / max(n_nodes, 1))),
                  max(10, min(18, 160 / max(n_nodes, 1))))
  
  set.seed(42)  # stable Fruchterman-Reingold layout every render
  
  ggraph::ggraph(g, layout = "fr") +
    ggraph::geom_edge_link(
      ggplot2::aes(width = n_bidders, colour = mean_surprise),
      arrow     = grid::arrow(length = grid::unit(0.22, "cm"), type = "closed"),
      end_cap   = ggraph::circle(4, "mm"),
      start_cap = ggraph::circle(2, "mm"),
      alpha     = 0.82
    ) +
    ggraph::geom_node_point(
      ggplot2::aes(size = degree),
      fill = "#d5e8f5", colour = "#1a5276", shape = 21, stroke = 1.5
    ) +
    ggraph::geom_node_text(
      ggplot2::aes(label = name), repel = TRUE,
      size        = lbl_size,
      colour      = "#1a252f",
      fontface    = "bold",
      bg.colour   = "white",
      bg.r        = 0.18,
      box.padding = 0.5,
      force       = 2
    ) +
    ggraph::scale_edge_width(range = c(0.6, 4),
                             name  = "Suppliers\ncrossing route",
                             guide = ggplot2::guide_legend(order = 1,
                                                           override.aes = list(colour = "#555555"))) +
    ggraph::scale_edge_colour_gradient2(
      low      = "#aab7b8",
      mid      = "#e67e22",
      high     = "#c0392b",
      midpoint = 0.5,
      name     = "Unusualness\n(avg surprise z-score)",
      labels   = c("Low\n(expected)", "", "Moderate", "", "High\n(unusual)"),
      guide    = ggplot2::guide_colourbar(order = 2, barwidth = 0.8, barheight = 7,
                                          title.hjust = 0.5)
    ) +
    ggplot2::scale_size_continuous(range = node_range, guide = "none") +
    ggplot2::labs(
      title   = paste("Network of Unusual Market Entry Flows \u2013", country),
      subtitle = paste0(
        "How to read: Each node is a CPV market cluster. ",
        "An arrow A \u2192 B means suppliers whose home market is A won contracts atypically in B.\n",
        "Thicker arrows = more suppliers crossing that route. ",
        "Grey arrows = expected level of surprise; orange/red = unusually high surprise score.\n",
        "Larger nodes = more connections. Node labels = CPV cluster codes. ",
        "See Flow Matrix tab for exact counts."
      ),
      caption = paste0(
        "Top ", length(top_clusters), " clusters by activity shown. ",
        "Min ", min_bidders, " suppliers per route. ",
        "Layout: Fruchterman-Reingold, fixed seed=42 (stable across filter changes)."
      )
    ) +
    ggplot2::theme_void(base_size = 12) +
    ggplot2::theme(
      plot.title       = ggplot2::element_text(size = 14, face = "bold",
                                               margin = ggplot2::margin(b = 4)),
      plot.subtitle    = ggplot2::element_text(size = 9.5, colour = "grey25",
                                               lineheight = 1.35,
                                               margin = ggplot2::margin(b = 6)),
      plot.caption     = ggplot2::element_text(size = 8, colour = "grey50", lineheight = 1.2),
      legend.text      = ggplot2::element_text(size = 9),
      legend.title     = ggplot2::element_text(size = 9.5, face = "bold"),
      plot.margin      = ggplot2::margin(12, 20, 10, 12),
      plot.background  = ggplot2::element_rect(fill = "white", colour = NA),
      panel.background = ggplot2::element_rect(fill = "white", colour = NA)
    )
}


#' 
#' @param df Data frame
#' @param config Configuration list
#' @param output_dir Output directory
#' @return List of results
analyze_markets <- function(df, config, output_dir) {
  results <- list()
  save_plot <- create_plot_saver(output_dir, config)
  
  required_cols <- c("tender_id", "lot_number", "bidder_id", "lot_productcode", "bid_priceusd")
  
  if (!validate_required_columns(df, required_cols, "market analysis")) {
    return(results)
  }
  
  # Build CPV data
  results$cpv_data <- build_cpv_df(df, config$thresholds$cpv_digits)
  
  # Detect unusual entries
  df_unusual <- detect_unusual_entries(results$cpv_data, config)
  
  if (nrow(df_unusual) == 0) {
    message("No unusual market entries detected")
    return(results)
  }
  
  results$unusual_entries <- df_unusual
  results$unusual_matrix <- build_unusual_matrix(df_unusual, config)
  
  # Network plot
  edges_markets <- results$unusual_matrix %>%
    dplyr::filter(n_bidders >= config$thresholds$min_bidders_for_edge) %>%
    dplyr::rename(from = home_cpv_cluster, to = target_cpv_cluster)
  
  if (nrow(edges_markets) > 0) {
    
    # ── 1. FLOW MATRIX: home × target heatmap ───────────────────────────────
    top_clusters <- edges_markets %>%
      tidyr::pivot_longer(c(from, to), values_to = "cluster") %>%
      dplyr::count(cluster, wt = n_bidders, sort = TRUE) %>%
      dplyr::slice_head(n = 20) %>%
      dplyr::pull(cluster)
    
    matrix_df <- edges_markets %>%
      dplyr::filter(from %in% top_clusters, to %in% top_clusters, from != to) %>%
      dplyr::mutate(
        from    = factor(from, levels = rev(top_clusters)),
        to      = factor(to,   levels = top_clusters),
        tooltip = paste0(
          "<b>", from, " \u2192 ", to, "</b><br>",
          "Suppliers crossing: <b>", n_bidders, "</b><br>",
          "Avg surprise score: ", round(mean_surprise, 2), " \u03c3<br>",
          "Total unusual wins: ", n_awards, "<br>",
          "<i>These ", n_bidders, " supplier(s) primarily work in '", from,
          "' but won contracts atypically in '", to, "'</i>"
        )
      )
    
    if (nrow(matrix_df) > 0) {
      n_clusters <- length(top_clusters)
      cell_size  <- max(6, min(11, 160 / n_clusters))
      
      results$flow_matrix_plot <- ggplot2::ggplot(
        matrix_df,
        ggplot2::aes(x = to, y = from, fill = n_bidders, text = tooltip)
      ) +
        ggplot2::geom_tile(colour = "white", linewidth = 0.5) +
        ggplot2::geom_text(
          ggplot2::aes(
            label  = n_bidders,
            colour = dplyr::if_else(n_bidders > max(n_bidders) * 0.6, "light", "dark")
          ),
          size = 3.2, fontface = "bold", show.legend = FALSE
        ) +
        ggplot2::scale_colour_manual(values = c(light = "white", dark = "grey20")) +
        ggplot2::scale_fill_gradientn(
          colours  = c("#eaf4fb", "#aed6f1", "#2980b9", "#1a5276"),
          na.value = "grey96",
          name     = "Suppliers\ncrossing route"
        ) +
        ggplot2::scale_x_discrete(position = "top") +
        ggplot2::labs(
          title    = paste("Unusual Market Entry Flow Map \u2013", config$country),
          subtitle = paste0(
            "Each cell shows how many suppliers whose main market is the row cluster ",
            "won contracts atypically in the column cluster. ",
            "Darker = more suppliers crossing that route. Hover for details."
          ),
          x       = "Target market (where they entered unusually)",
          y       = "Home market (where they normally operate)",
          caption = paste0(
            "Top ", n_clusters, " most-connected clusters shown. ",
            "Atypicality flags: supplier has \u22654 total wins overall, ",
            "\u22643 wins in this market AND <5% of their total awards there. ",
            "Surprise score = \u2212log[(n_ic + 1)/(n_c + n_suppliers_c)], ",
            "z-standardised within each target cluster."
          )
        ) +
        ggplot2::theme_minimal(base_size = PLOT_SIZES$base_size) +
        ggplot2::theme(
          axis.text.x     = ggplot2::element_text(angle = 40, hjust = 0,
                                                  size = cell_size, face = "bold"),
          axis.text.y     = ggplot2::element_text(size = cell_size, face = "bold"),
          axis.title.x    = ggplot2::element_text(size = 9, colour = "grey40"),
          axis.title.y    = ggplot2::element_text(size = 9, colour = "grey40"),
          panel.grid      = ggplot2::element_blank(),
          plot.title      = ggplot2::element_text(size = PLOT_SIZES$title_size, face = "bold"),
          plot.subtitle   = ggplot2::element_text(size = PLOT_SIZES$subtitle_size,
                                                  colour = "grey30", lineheight = 1.3),
          plot.caption    = ggplot2::element_text(size = 7.5, colour = "grey50", lineheight = 1.2),
          legend.position = "right",
          plot.margin     = ggplot2::margin(10, 10, 10, 10)
        ) +
        white_bg()
    }
    
    # ── 2. NETWORK GRAPH: ggraph directed network ────────────────────────────
    g_markets <- igraph::graph_from_data_frame(edges_markets, directed = TRUE)
    igraph::V(g_markets)$degree <- igraph::degree(g_markets, mode = "all")
    
    results$network_plot <- ggraph::ggraph(g_markets, layout = "fr") +
      ggraph::geom_edge_link(
        ggplot2::aes(width = n_bidders, colour = mean_surprise),
        arrow     = grid::arrow(length = grid::unit(0.18, "cm"), type = "closed"),
        end_cap   = ggraph::circle(3, "mm"),
        start_cap = ggraph::circle(1, "mm"),
        alpha     = 0.7
      ) +
      ggraph::geom_node_point(
        ggplot2::aes(size = degree),
        fill = "#f39c12", colour = "#2c3e50", shape = 21, stroke = 1.1
      ) +
      ggraph::geom_node_text(
        ggplot2::aes(label = name), repel = TRUE,
        size = 3.0, colour = "#1a252f", fontface = "bold",
        bg.colour = "white", bg.r = 0.12
      ) +
      ggraph::scale_edge_width(range = c(0.4, 3), name = "Suppliers\ncrossing") +
      ggraph::scale_edge_colour_gradient2(
        low = "#2980b9", mid = "#f39c12", high = "#c0392b",
        midpoint = 0, name = "Avg surprise\n(within-cluster z)"
      ) +
      ggplot2::scale_size_continuous(range = c(4, 11), name = "Node\nconnections") +
      ggplot2::labs(
        title    = paste("Network of Unusual Market Entry Flows \u2013", config$country),
        subtitle = paste0(
          "Nodes = CPV market clusters. Arrow A \u2192 B = suppliers whose home market is A ",
          "won contracts atypically in B. Arrow width = number of such suppliers. ",
          "Arrow colour = avg within-cluster surprise score (red = more unusual). ",
          "Node size = total connections. See Flow Matrix tab for exact counts."
        ),
        caption  = paste0(
          "Surprise score = \u2212log[(n_ic + 1)/(n_c + n_suppliers_c)], z-standardised within each target cluster. ",
          "Atypicality flags: \u22654 total wins overall, \u22643 wins in this market, <5% of portfolio."
        )
      ) +
      ggplot2::theme_void(base_size = PLOT_SIZES$base_size) +
      ggplot2::theme(
        plot.title    = ggplot2::element_text(size = PLOT_SIZES$title_size, face = "bold",
                                              margin = ggplot2::margin(b = 5)),
        plot.subtitle = ggplot2::element_text(size = PLOT_SIZES$subtitle_size, colour = "grey30",
                                              lineheight = 1.3, margin = ggplot2::margin(b = 4)),
        plot.caption  = ggplot2::element_text(size = 7.5, colour = "grey50", lineheight = 1.2),
        legend.text   = ggplot2::element_text(size = PLOT_SIZES$legend_text_size),
        legend.title  = ggplot2::element_text(size = PLOT_SIZES$legend_title_size, face = "bold"),
        plot.margin   = ggplot2::margin(12, 12, 8, 12)
      ) +
      white_bg()
  }
  
  # Supplier-level unusual behavior — drop NAs, use mean score, include bidder name
  results$supplier_unusual <- df_unusual %>%
    dplyr::filter(!is.na(bidder_id), bidder_id != "") %>%
    dplyr::group_by(bidder_id, home_cpv_cluster) %>%
    dplyr::summarise(
      bidder_name        = dplyr::first(if ("bidder_name" %in% names(dplyr::cur_data_all()))
        bidder_name else NA_character_),
      max_surprise_z     = max(surprise_z,  na.rm = TRUE),
      mean_surprise_z    = mean(surprise_z, na.rm = TRUE),
      n_atypical_markets = dplyr::n_distinct(target_cpv_cluster),
      n_atypical_awards  = dplyr::n(),
      .groups = "drop"
    )
  
  top_suppliers <- results$supplier_unusual %>%
    dplyr::filter(n_atypical_awards >= 3, !is.na(mean_surprise_z)) %>%
    dplyr::arrange(dplyr::desc(mean_surprise_z)) %>%
    dplyr::slice_head(n = config$thresholds$top_n_suppliers) %>%
    dplyr::mutate(
      # Axis: always ID (clean, stable)
      id_short  = substr(bidder_id, 1, 16),
      id_short  = factor(id_short, levels = rev(unique(id_short))),
      # Tooltip: ID + name if different
      full_label = dplyr::if_else(
        !is.na(bidder_name) & nchar(trimws(bidder_name)) > 0 & bidder_name != bidder_id,
        paste0(bidder_name, "\n(", bidder_id, ")"),
        bidder_id
      ),
      tooltip = paste0(
        "<b>", full_label, "</b><br>",
        "Home market: <b>", home_cpv_cluster, "</b><br>",
        "Atypical markets entered: <b>", n_atypical_markets, "</b><br>",
        "Atypical contract wins: <b>", n_atypical_awards, "</b><br>",
        "<b>Avg surprise score: ", round(mean_surprise_z, 2), " \u03c3</b>",
        "  |  Peak: ", round(max_surprise_z, 2), " \u03c3<br>",
        "<i>Avg score = mean of within-cluster z-scores across all atypical entries.<br>",
        "Peak score = highest single z-score recorded for this supplier.<br>",
        "Higher = more unusual relative to all other entrants in that market.</i>"
      )
    )
  
  results$supplier_unusual_plot <- ggplot2::ggplot(
    top_suppliers,
    ggplot2::aes(x = id_short, y = mean_surprise_z,
                 fill = n_atypical_markets, text = tooltip)
  ) +
    ggplot2::geom_col(width = 0.72) +
    ggplot2::geom_text(
      ggplot2::aes(label = paste0(n_atypical_awards, " wins")),
      hjust = -0.12, size = 2.8, colour = "grey25"
    ) +
    ggplot2::coord_flip() +
    ggplot2::scale_fill_gradient(
      low  = "#aed6f1",   # light blue
      high = "#e67e22",   # amber — stays light enough for dark labels
      name = "Distinct markets\nentered atypically"
    ) +
    ggplot2::scale_y_continuous(expand = ggplot2::expansion(mult = c(0, 0.28))) +
    ggplot2::labs(
      title    = paste("Suppliers with Unusually Diversified Market Entries \u2013", config$country),
      subtitle = paste0(
        "Ranked by average surprise score across all atypical entries \u2014 higher means more unusual. ",
        "Bar labels show total atypical wins. Colour shows distinct markets entered unexpectedly. ",
        "Hover for supplier name, home market, and full score breakdown."
      ),
      x        = NULL,
      y        = "Average surprise score (within-market z-score)",
      caption  = paste0(
        "Shown: suppliers with \u22653 atypical wins. A win is atypical if it falls outside the supplier\u2019s ",
        "core portfolio (\u22644 total career wins in this market, <5% of their total awards). ",
        "Score reflects how rare this supplier is compared to all other entrants in those markets."
      )
    ) +
    standard_plot_theme() +
    ggplot2::theme(
      panel.grid.major.y = ggplot2::element_blank(),
      plot.caption = ggplot2::element_text(size = 8, colour = "grey50", lineheight = 1.2)
    )
  
  save_plot(results$supplier_unusual_plot, "supp_unusual_suppliers")
  
  # Market-level unusual entries
  results$market_unusual <- df_unusual %>%
    dplyr::group_by(target_cpv_cluster) %>%
    dplyr::summarise(
      mean_surprise_z = mean(surprise_z, na.rm = TRUE),
      max_surprise_z = max(surprise_z, na.rm = TRUE),
      n_unusual_suppliers = dplyr::n_distinct(bidder_id),
      n_unusual_awards = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::mutate(market_risk_index = mean_surprise_z * log1p(n_unusual_suppliers))
  
  top_markets <- results$market_unusual %>%
    dplyr::slice_max(market_risk_index, n = config$thresholds$top_n_markets) %>%
    dplyr::mutate(
      tooltip = paste0(
        "<b>CPV cluster: ", target_cpv_cluster, "</b><br>",
        "Unusual suppliers entering: <b>", n_unusual_suppliers, "</b><br>",
        "Atypical contract wins: <b>", n_unusual_awards, "</b><br>",
        "Avg surprise score: <b>", round(mean_surprise_z, 2), " \u03c3</b>",
        "  |  Peak: ", round(max_surprise_z, 2), " \u03c3<br>",
        "Market risk index: ", round(market_risk_index, 2), "<br>",
        "<i>Risk index = avg surprise \u00d7 log(suppliers + 1)</i>"
      )
    )
  
  results$market_unusual_plot <- ggplot2::ggplot(
    top_markets,
    ggplot2::aes(
      x      = n_unusual_suppliers,
      y      = mean_surprise_z,
      size   = n_unusual_awards,
      colour = market_risk_index,
      label  = target_cpv_cluster,
      text   = tooltip
    )
  ) +
    ggplot2::geom_point(alpha = 0.82) +
    ggrepel::geom_text_repel(
      size        = 3.0, colour = "grey15",
      bg.colour   = "white", bg.r = 0.12,
      box.padding = 0.4, max.overlaps = 20
    ) +
    ggplot2::scale_size_continuous(
      name = "Atypical\ncontract wins", range = c(3, 14),
      guide = ggplot2::guide_legend(order = 1)
    ) +
    ggplot2::scale_colour_gradientn(
      colours = c("#2c7bb6", "#ffffbf", "#d7191c"),
      name    = "Market risk\nindex",
      guide   = ggplot2::guide_colourbar(order = 2)
    ) +
    ggplot2::labs(
      title    = paste("Markets Attracting Unusual Supplier Entries \u2013", config$country),
      subtitle = paste0(
        "Each bubble = a 3-digit CPV market cluster. ",
        "X axis = how many distinct suppliers entered atypically. ",
        "Y axis = how surprising those entries were on average (within-market z-score). ",
        "Bubble size = total atypical contract wins. Colour = overall market risk index ",
        "(combines surprise intensity with breadth of unusual entries)."
      ),
      x        = "Number of suppliers entering atypically",
      y        = "Average surprise score (within-market z-score)",
      caption  = "Risk index = avg surprise score \u00d7 log(1 + no. of unusual suppliers)"
    ) +
    standard_plot_theme() +
    ggplot2::theme(
      plot.caption = ggplot2::element_text(size = 8, colour = "grey50")
    )
  
  save_plot(results$market_unusual_plot, "supp_unusual_markets")
  
  results
}

# ========================================================================
# PART 15: MODULE - PRICE ANALYSIS
# ========================================================================

#' Prepare price data
#' 
#' @param df Data frame
#' @param config Configuration list
#' @return Prepared data frame
prepare_price_data <- function(df, config) {
  if (!validate_required_columns(df, c("bid_price", "lot_estimatedprice"), "price analysis")) {
    return(NULL)
  }
  
  # Determine value variable
  val_var <- dplyr::case_when(
    "bid_priceusd" %in% names(df) ~ "bid_priceusd",
    "bid_price" %in% names(df) ~ "bid_price",
    TRUE ~ NA_character_
  )
  
  # Compute total missing share
  key_vars <- names(label_lookup) %>% gsub("_missing_share", "", .)
  df <- df %>%
    dplyr::mutate(
      total_missing_share = rowMeans(
        dplyr::across(all_of(key_vars), ~ is.na(.)),
        na.rm = TRUE
      )
    )
  
  # Add log contract value
  if (!is.na(val_var)) {
    df <- df %>%
      dplyr::mutate(log_contract_value = log1p(.data[[val_var]]))
  }
  
  # Filter by year
  yrp <- config$years_relprice
  rel_price_data <- df %>%
    dplyr::mutate(
      relative_price = bid_price / lot_estimatedprice,
      relative_price = ifelse(
        relative_price > config$thresholds$max_relative_price |
          relative_price <= config$thresholds$min_relative_price,
        NA,
        relative_price
      )
    ) %>%
    dplyr::filter(
      !is.na(tender_year),
      tender_year >= yrp$min_year,
      tender_year <= yrp$max_year
    )
  
  # Relevel factors
  if ("tender_proceduretype" %in% names(rel_price_data)) {
    rel_price_data <- rel_price_data %>%
      dplyr::mutate(
        tender_proceduretype = stats::relevel(as.factor(tender_proceduretype), ref = "OPEN")
      )
  }
  if ("buyer_buyertype" %in% names(rel_price_data)) {
    rel_price_data <- rel_price_data %>%
      dplyr::mutate(
        buyer_buyertype = stats::relevel(as.factor(buyer_buyertype), ref = "UTILITIES")
      )
  }
  
  rel_price_data
}


#' Run relative price specifications
#' 
#' @param rel_price_data Price data
#' @param config Configuration list
#' @return Data frame of specification results
run_relprice_specs <- function(rel_price_data, config) {
  out <- list()
  k <- 0L
  
  fe_set <- c("0", config$models$fe_set)
  
  for (mt in config$models$model_types_relprice) {
    for (fe in fe_set) {
      fe_part <- make_fe_part(fe)
      
      for (cl in config$models$cluster_set) {
        if (fe == "0" && cl == "none") next
        
        cl_fml <- make_cluster(cl)
        
        for (ctrl in config$models$controls_set) {
          rhs_terms <- switch(
            ctrl,
            "x_only" = c("total_missing_share"),
            "base" = c("total_missing_share", "log_contract_value",
                       "buyer_buyertype", "tender_proceduretype")
          )
          
          rhs_terms <- rhs_terms[rhs_terms %in% names(rel_price_data)]
          
          # ADDED: Skip if no valid terms
          if (length(rhs_terms) == 0) {
            message("Skipping spec: no valid RHS terms for controls=", ctrl)
            next
          }
          
          rhs <- paste(rhs_terms, collapse = " + ")
          
          m <- NULL
          d_used <- NULL
          x_name <- "total_missing_share"
          
          if (mt == "ols_level") {
            fml <- stats::as.formula(paste0("relative_price ~ ", rhs, " | ", fe_part))
            m <- safe_fixest(fixest::feols(fml, data = rel_price_data, cluster = cl_fml))
            d_used <- rel_price_data
            
          } else if (mt == "ols_log") {
            d_used <- rel_price_data %>%
              dplyr::mutate(log_relative_price = log(relative_price))
            d_used$log_relative_price[!is.finite(d_used$log_relative_price)] <- NA_real_
            d_used <- d_used[!is.na(d_used$log_relative_price), , drop = FALSE]
            if (nrow(d_used) == 0) next
            
            fml <- stats::as.formula(paste0("log_relative_price ~ ", rhs, " | ", fe_part))
            m <- safe_fixest(fixest::feols(fml, data = d_used, cluster = cl_fml))
            
          } else if (mt == "gamma_log") {
            fml <- stats::as.formula(paste0("relative_price ~ ", rhs, " | ", fe_part))
            m <- safe_fixest(fixest::feglm(
              fml, data = rel_price_data, family = Gamma(link = "log"), cluster = cl_fml
            ))
            d_used <- rel_price_data
          }
          
          if (is.null(m)) next
          
          # ADDED: Check if x_name is actually in the model
          if (!(x_name %in% names(coef(m)))) {
            message("Skipping spec: ", x_name, " not in model coefficients")
            next
          }
          
          eff <- extract_effect_fixest(model = m, x_name = x_name, data_used = d_used)
          
          k <- k + 1L
          out[[k]] <- data.frame(
            outcome = "rel_price",
            model_type = mt,
            fe = fe,
            cluster = cl,
            controls = ctrl,
            estimate = eff$estimate,
            pvalue = eff$pvalue,
            nobs = eff$nobs,
            std_slope = eff$std_slope,
            stringsAsFactors = FALSE
          )
        }
      }
    }
  }
  
  if (length(out) == 0) return(data.frame())
  do.call(rbind, out)
}

#' Analyze prices
#' 
#' @param df Data frame
#' @param config Configuration list
#' @param output_dir Output directory
#' @return List of results
analyze_prices <- function(df, config, output_dir) {
  results <- list()
  
  # Create plot saver
  save_plot <- create_plot_saver(output_dir, config)
  
  results$data <- prepare_price_data(df, config)
  
  if (is.null(results$data) || nrow(results$data) == 0) {
    message("No data available for price analysis")
    return(results)
  }
  
  results$specs <- run_relprice_specs(results$data, config)
  
  # Check if specs were generated
  if (is.null(results$specs) || nrow(results$specs) == 0) {
    message("No relative price specifications generated for ", config$country)
    return(results)
  }
  
  results$relprice_sensitivity <- build_sensitivity_bundle(results$specs)
  
  # Print sensitivity
  if (!is.null(results$relprice_sensitivity)) {
    message("\n--- REL_PRICE sensitivity (", config$country, ") ---")
    print(results$relprice_sensitivity$overall)
    print(results$relprice_sensitivity$sign)
    print(results$relprice_sensitivity$by_fe)
    print(results$relprice_sensitivity$by_cluster)
    print(results$relprice_sensitivity$by_controls)
    print(results$relprice_sensitivity$classes)
    print(results$relprice_sensitivity$top_cells)
  }
  
  # Select and plot best model
  best_row <- pick_best_model(
    results$specs,
    require_positive = TRUE,
    p_max = config$models$p_max,
    strength_col = "std_slope"
  )
  
  if (is.null(best_row)) {
    message("No relative price model met selection criteria for ", config$country)
    return(results)
  }
  
  message("Relative price BEST spec for ", config$country, ": ",
          "model=", best_row$model_type,
          ", fe=", best_row$fe, 
          ", cluster=", best_row$cluster,
          ", controls=", best_row$controls,
          ", p=", signif(best_row$pvalue, 3))
  
  # Refit model
  fe_part <- make_fe_part(best_row$fe)
  cl_fml <- make_cluster(best_row$cluster)
  
  rhs_terms <- switch(
    best_row$controls,
    "x_only" = c("total_missing_share"),
    "base" = c("total_missing_share", "log_contract_value",
               "buyer_buyertype", "tender_proceduretype")
  )
  rhs_terms <- rhs_terms[rhs_terms %in% names(results$data)]
  rhs <- paste(rhs_terms, collapse = " + ")
  
  model <- NULL
  pred <- NULL
  y_lab <- NULL
  
  if (best_row$model_type == "ols_level") {
    fml <- stats::as.formula(paste0("relative_price ~ ", rhs, " | ", fe_part))
    model <- fixest::feols(fml, data = results$data, cluster = cl_fml)
    pred <- tryCatch(
      ggeffects::ggpredict(model, terms = "total_missing_share"),
      error = function(e) {
        message("Error in ggpredict: ", e$message)
        NULL
      }
    )
    y_lab <- "Predicted Relative Price"
    
  } else if (best_row$model_type == "ols_log") {
    d <- results$data %>% dplyr::mutate(log_relative_price = log(relative_price))
    d$log_relative_price[!is.finite(d$log_relative_price)] <- NA_real_
    d <- d[!is.na(d$log_relative_price), , drop = FALSE]
    
    if (nrow(d) > 0) {
      fml <- stats::as.formula(paste0("log_relative_price ~ ", rhs, " | ", fe_part))
      model <- fixest::feols(fml, data = d, cluster = cl_fml)
      pred <- tryCatch(
        ggeffects::ggpredict(model, terms = "total_missing_share"),
        error = function(e) {
          message("Error in ggpredict: ", e$message)
          NULL
        }
      )
      y_lab <- "Predicted log(Relative Price)"
    }
    
  } else if (best_row$model_type == "gamma_log") {
    fml <- stats::as.formula(paste0("relative_price ~ ", rhs, " | ", fe_part))
    model <- fixest::feglm(
      fml, data = results$data, family = Gamma(link = "log"), cluster = cl_fml
    )
    pred <- tryCatch(
      ggeffects::ggpredict(model, terms = "total_missing_share"),
      error = function(e) {
        message("Error in ggpredict: ", e$message)
        NULL
      }
    )
    y_lab <- "Predicted Relative Price (Gamma-log)"
  }
  
  if (!is.null(pred)) {
    years_used <- range(results$data$tender_year, na.rm = TRUE)
    
    results$rel_price_plot <- plot_ggeffects_line(
      pred = pred,
      title = paste("Predicted Relative Price by Missing Share –", config$country),
      subtitle = paste0(
        "(BEST) Model: ", pretty_model_name(best_row$model_type),
        " | Years: ", years_used[1], "–", years_used[2],
        " | N=", best_row$nobs,
        " | FE: ", pretty_fe_label(best_row$fe),
        " | Cluster: ", best_row$cluster,
        " | Controls: ", pretty_controls_label(best_row$controls)
      ),
      x_lab = "Total Missing Share",
      y_lab = y_lab,
      caption = paste0(
        "Filters: tender_year in [", config$years_relprice$min_year, ", ",
        config$years_relprice$max_year, "]. ",
        controls_note(best_row$controls)
      )
    )
    
    # Save the plot
    tryCatch({
      save_plot(results$rel_price_plot, "reg_relative_price_missing_share")
      message("✓ Relative price plot saved successfully for ", config$country)
    }, error = function(e) {
      message("✗ Error saving relative price plot: ", e$message)
      # Fallback: save directly
      ggplot2::ggsave(
        filename = file.path(output_dir, paste0("reg_relative_price_missing_share_", config$country, ".png")),
        plot = results$rel_price_plot,
        width = 10,
        height = 8,
        dpi = 300
      )
      message("✓ Saved plot using fallback method")
    })
    
  } else {
    message("Could not generate predictions for relative price plot")
  }
  
  results
}

# ========================================================================
# PART 16: MODULE - REGIONAL ANALYSIS
# ========================================================================

#' Analyze regional patterns (NUTS3)
#' 
#' @param df Data frame
#' @param config Configuration list
#' @param output_dir Output directory
#' @return List of results
analyze_regional <- function(df, config, output_dir) {
  results <- list()
  save_plot <- create_plot_saver(output_dir, config)
  
  if (!validate_required_columns(df, "buyer_nuts", "regional analysis")) {
    return(results)
  }
  
  df_nuts <- df %>% clean_nuts3(buyer_nuts)
  
  results$missing_by_region <- df_nuts %>%
    dplyr::filter(!is.na(nuts3)) %>%
    dplyr::select(!dplyr::starts_with("ind_")) %>%
    dplyr::group_by(nuts3) %>%
    dplyr::summarise(
      missing_share = mean(is.na(dplyr::cur_data_all()), na.rm = TRUE),
      .groups = "drop"
    )
  
  # Try to get map data
  region_sf <- tryCatch(
    {
      eurostat::get_eurostat_geospatial(
        output_class = "sf",
        nuts_level = 3,
        year = 2021
      ) %>%
        dplyr::filter(CNTR_CODE == config$country)
    },
    error = function(e) {
      message("Could not fetch NUTS3 map data: ", e$message)
      NULL
    }
  )
  
  if (!is.null(region_sf) && nrow(region_sf) > 0) {
    results$region_map_data <- region_sf %>%
      dplyr::left_join(results$missing_by_region, by = c("NUTS_ID" = "nuts3"))
    
    results$region_missing_map <- ggplot2::ggplot(results$region_map_data) +
      ggplot2::geom_sf(ggplot2::aes(fill = missing_share), color = "grey40", size = 0.2) +
      ggplot2::scale_fill_distiller(
        palette = "Blues",
        direction = 1,
        na.value = "grey90",
        labels = scales::percent_format(accuracy = 1),
        name = "Missing share"
      ) +
      ggplot2::labs(
        title = paste("Overall share of missing values by region (NUTS3) –", config$country)
      ) +
      standard_plot_theme()  # CHANGED from theme_minimal()
    
    save_plot(results$region_missing_map, "region_missing_map")
  }
  
  results
}

# ========================================================================
# PART 17: SUMMARY STATISTICS MODULE
# ========================================================================

#' Log summary statistics
#' 
#' @param df Data frame
#' @param config Configuration list
#' @param output_dir Output directory
#' @return List of summary stats
log_summary_stats <- function(df, config, output_dir) {
  stats <- list()
  
  # Observations per year
  stats$n_obs_per_year <- df %>%
    dplyr::count(tender_year, name = "n_observations")
  
  # Unique entities
  stats$n_unique_buyers <- if ("buyer_masterid" %in% names(df)) {
    dplyr::n_distinct(df$buyer_masterid, na.rm = TRUE)
  } else {
    NA_integer_
  }
  
  stats$n_unique_bidders <- if ("bidder_masterid" %in% names(df)) {
    dplyr::n_distinct(df$bidder_masterid, na.rm = TRUE)
  } else {
    NA_integer_
  }
  
  # Tenders per year
  stats$tender_year_tenders <- if ("tender_id" %in% names(df)) {
    df %>%
      dplyr::group_by(tender_year) %>%
      dplyr::summarise(
        n_unique_tender_id = dplyr::n_distinct(tender_id),
        .groups = "drop"
      )
  } else {
    NULL
  }
  
  # Variables present
  stats$vars_present <- names(df)[!startsWith(names(df), "ind_")]
  
  # Print summary
  cat("\n", strrep("=", 70), "\n")
  cat("DATA SUMMARY FOR", config$country, "\n")
  cat(strrep("=", 70), "\n\n")
  
  cat("Contracts per year:\n")
  print(stats$n_obs_per_year)
  cat("\n")
  
  if (!is.na(stats$n_unique_buyers)) {
    cat("Number of unique buyers (buyer_masterid):", stats$n_unique_buyers, "\n\n")
  }
  
  if (!is.na(stats$n_unique_bidders)) {
    cat("Number of unique bidders (bidder_masterid):", stats$n_unique_bidders, "\n\n")
  }
  
  if (!is.null(stats$tender_year_tenders)) {
    cat("Number of unique tenders per year:\n")
    print(stats$tender_year_tenders)
    cat("\n")
  }
  
  cat("Variables present (excluding indicators):\n")
  cat(paste(stats$vars_present, collapse = ", "), "\n")
  cat("\n", strrep("=", 70), "\n\n")
  
  invisible(stats)
}

# ========================================================================
# PART 18: SAFE MODULE EXECUTION
# ========================================================================

#' Safely run analysis module
#' 
#' @param module_fn Module function
#' @param df Data frame
#' @param config Configuration list
#' @param output_dir Output directory
#' @return Module results or error info
safely_run_module <- function(module_fn, df, config, output_dir, ...) {
  module_name <- deparse(substitute(module_fn))
  
  tryCatch(
    {
      message("\n", strrep("-", 60))
      message("Running module: ", module_name)
      message(strrep("-", 60))
      
      result <- module_fn(df, config, output_dir, ...)
      
      message("✓ Module completed successfully: ", module_name)
      result
    },
    error = function(e) {
      message("✗ Module failed: ", module_name)
      message("Error: ", e$message)
      list(error = e$message, status = "failed")
    }
  )
}

# ========================================================================
# PART 19: ENSURE OUTPUT DIRECTORY
# ========================================================================

# dir_ensure() is defined in utils_shared.R.
# Kept as a thin alias for backward compatibility within this file.
ensure_output_directory <- function(output_dir) dir_ensure(output_dir)

# ========================================================================
# PART 20: MAIN PIPELINE
# ========================================================================

#' Run complete integrity analysis pipeline
#' 
#' @param df Data frame with procurement data
#' @param country_code Two-letter country code
#' @param output_dir Directory for outputs
#' @return List of analysis results
#' @export
run_integrity_pipeline <- function(df, country_code = "GEN", output_dir) {
  
  # Initialize
  message("\n", strrep("=", 70))
  message("PROCUREMENT INTEGRITY ANALYSIS PIPELINE")
  message("Country: ", country_code)
  message(strrep("=", 70), "\n")
  
  config <- create_pipeline_config(country_code)
  ensure_output_directory(output_dir)
  
  # Prepare data
  df <- prepare_data(df)
  data_quality <- check_data_quality(df, config)
  
  # Log summary statistics
  summary_stats <- log_summary_stats(df, config, output_dir)
  
  # Run analysis modules
  results <- list(
    config = config,
    data = df,
    data_quality = data_quality,
    summary_stats = summary_stats,
    missing = safely_run_module(analyze_missing_values, df, config, output_dir),
    interoperability = safely_run_module(analyze_interoperability, df, config, output_dir),
    competition = safely_run_module(analyze_competition, df, config, output_dir),
    markets = safely_run_module(analyze_markets, df, config, output_dir),
    prices = safely_run_module(analyze_prices, df, config, output_dir),
    regional = safely_run_module(analyze_regional, df, config, output_dir)
  )
  
  # Final message
  message("\n", strrep("=", 70))
  message("PIPELINE COMPLETED")
  message("Results saved to: ", output_dir)
  message(strrep("=", 70), "\n")
  
  invisible(results)
}

# ========================================================================
# END OF SCRIPT
# ========================================================================