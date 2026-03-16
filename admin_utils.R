# ========================================================================
# Administrative efficiency pipeline
# ========================================================================

# ------------------------------------------------------------------------
# load_data, add_tender_year, recode_procedure_type, add_buyer_group, %||%
# are all defined in utils_shared.R — not repeated here.
# ------------------------------------------------------------------------

# ------------------------------------------------------------------------
# Helper: convert app nested thresholds → flat pipeline format
#
# The app stores thresholds as:
#   list(
#     subm = list(open = list(days, medium = list(min, max), no_medium, ...), restricted = ..., neg_pub = ...),
#     dec  = list(open = list(days, ...), restricted = ..., neg_pub = ...)
#   )
#
# The pipeline expects a flat list:
#   list(subm_short_open, subm_short_restricted, subm_short_negotiated,
#        subm_medium_open_min, subm_medium_open_max, long_decision_days)
# ------------------------------------------------------------------------

app_thresholds_to_pipeline <- function(app_thr) {
  get_days <- function(lst) {
    v <- lst$days
    if (is.null(v) || (length(v) == 1 && is.na(v))) NA_real_ else as.numeric(v)
  }
  # Decision days: take the first non-NA across open / restricted / neg_pub
  dec_days <- NA_real_
  for (k in c("open", "restricted", "neg_pub")) {
    v <- get_days(app_thr$dec[[k]])
    if (!is.na(v)) { dec_days <- v; break }
  }
  list(
    subm_short_open         = get_days(app_thr$subm$open),
    subm_short_restricted   = get_days(app_thr$subm$restricted),
    subm_short_negotiated   = get_days(app_thr$subm$neg_pub),
    subm_medium_open_min    = app_thr$subm$open$medium$min  %||% NA_real_,
    subm_medium_open_max    = app_thr$subm$open$medium$max  %||% NA_real_,
    long_decision_days      = dec_days
  )
}


# ========================================================================
# SPECIFICATION TESTING AND SENSITIVITY ANALYSIS FUNCTIONS
# ========================================================================

make_fe_part <- function(fe) {
  switch(fe,
         "0"          = "0",
         "buyer"      = "buyer_id",
         "year"       = "tender_year",
         "buyer+year" = "buyer_id + tender_year",
         "buyer#year" = "buyer_id^tender_year",
         stop("Unknown FE spec: ", fe)
  )
}

make_cluster <- function(cluster) {
  switch(cluster,
         "none"            = NULL,
         "buyer"           = stats::as.formula("~ buyer_id"),
         "year"            = stats::as.formula("~ tender_year"),
         "buyer_year"      = stats::as.formula("~ buyer_id + tender_year"),
         "buyer_buyertype" = stats::as.formula("~ buyer_id + buyer_buyertype"),
         stop("Unknown cluster spec: ", cluster)
  )
}

safe_fixest <- function(expr) tryCatch(expr, error = function(e) NULL)

extract_effect_fixest <- function(model, x_name, data_used, y_name = NULL) {
  s  <- summary(model)
  ct <- s$coeftable
  if (!(x_name %in% rownames(ct)))
    return(list(estimate = NA_real_, pvalue = NA_real_, nobs = s$nobs, std_slope = NA_real_))
  est <- as.numeric(ct[x_name, "Estimate"])
  pv  <- as.numeric(ct[x_name, "Pr(>|t|)"])
  if (is.na(pv) && "Pr(>|z|)" %in% colnames(ct)) pv <- as.numeric(ct[x_name, "Pr(>|z|)"])
  list(estimate = est, pvalue = pv, nobs = s$nobs,
       std_slope = est * stats::sd(data_used[[x_name]], na.rm = TRUE))
}

effect_p10_p90 <- function(model, data_used, x_name) {
  qs   <- stats::quantile(data_used[[x_name]], probs = c(.1, .9), na.rm = TRUE)
  typical <- data_used[1, , drop = FALSE]
  for (nm in names(typical)) {
    if (nm == x_name) next
    v <- data_used[[nm]]
    if (is.numeric(v)) typical[[nm]] <- stats::median(v, na.rm = TRUE)
    else if (is.factor(v) || is.character(v)) {
      tab <- sort(table(v), decreasing = TRUE)
      typical[[nm]] <- names(tab)[1]
      if (is.factor(v)) typical[[nm]] <- factor(typical[[nm]], levels = levels(v))
    }
  }
  d_lo <- typical; d_lo[[x_name]] <- unname(qs[1])
  d_hi <- typical; d_hi[[x_name]] <- unname(qs[2])
  as.numeric(
    suppressWarnings(stats::predict(model, newdata = d_hi, type = "response")) -
      suppressWarnings(stats::predict(model, newdata = d_lo, type = "response"))
  )
}

add_strength_column <- function(specs) {
  if (is.null(specs) || nrow(specs) == 0L) return(specs)
  if ("effect_strength" %in% names(specs))  specs$strength <- specs$effect_strength
  else if ("std_slope"  %in% names(specs))  specs$strength <- specs$std_slope
  else                                       specs$strength <- NA_real_
  specs
}

summarise_sensitivity_overall <- function(specs, p_levels = c(0.05, 0.10, 0.20)) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  specs <- add_strength_column(specs)
  tibble::tibble(
    n_specs         = nrow(specs),
    share_positive  = mean(specs$estimate > 0, na.rm = TRUE),
    share_negative  = mean(specs$estimate < 0, na.rm = TRUE),
    median_estimate = median(specs$estimate,   na.rm = TRUE),
    median_pvalue   = median(specs$pvalue,     na.rm = TRUE),
    median_strength = median(specs$strength,   na.rm = TRUE),
    !!!setNames(
      lapply(p_levels, function(p) mean(specs$pvalue <= p, na.rm = TRUE)),
      paste0("share_p_le_", p_levels)
    )
  )
}

summarise_sign_instability <- function(specs) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  s <- sign(specs$estimate); s <- s[!is.na(s) & s != 0]
  tibble::tibble(
    share_sign_stable = if (length(s) == 0) NA_real_ else as.numeric(length(unique(s)) <= 1),
    n_nonzero = length(s)
  )
}

summarise_by_fe <- function(specs) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  add_strength_column(specs) %>%
    dplyr::group_by(fe) %>%
    dplyr::summarise(n_specs = dplyr::n(), share_positive = mean(estimate > 0, na.rm = TRUE),
                     share_p10 = mean(pvalue <= 0.10, na.rm = TRUE), median_p = median(pvalue, na.rm = TRUE),
                     median_strength = median(strength, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(share_p10))
}

summarise_by_cluster <- function(specs) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  add_strength_column(specs) %>%
    dplyr::group_by(cluster) %>%
    dplyr::summarise(n_specs = dplyr::n(), share_positive = mean(estimate > 0, na.rm = TRUE),
                     share_p10 = mean(pvalue <= 0.10, na.rm = TRUE), median_p = median(pvalue, na.rm = TRUE),
                     median_strength = median(strength, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(median_p)
}

summarise_by_controls <- function(specs) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  add_strength_column(specs) %>%
    dplyr::group_by(controls) %>%
    dplyr::summarise(n_specs = dplyr::n(), share_positive = mean(estimate > 0, na.rm = TRUE),
                     share_p10 = mean(pvalue <= 0.10, na.rm = TRUE), median_p = median(pvalue, na.rm = TRUE),
                     median_strength = median(strength, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(share_p10))
}

classify_specs <- function(specs, p_cut = 0.10) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  specs %>%
    dplyr::mutate(class = dplyr::case_when(
      estimate > 0 & pvalue <= p_cut ~ "Positive & significant",
      estimate > 0                   ~ "Positive but insignificant",
      estimate < 0 & pvalue <= p_cut ~ "Negative & significant",
      estimate < 0                   ~ "Negative but insignificant",
      TRUE                           ~ "Missing/NA")) %>%
    dplyr::count(class) %>% dplyr::mutate(share = n / sum(n))
}

top_cells <- function(specs, p_cut = 0.10, n_top = 10) {
  if (is.null(specs) || nrow(specs) == 0L) return(tibble::tibble())
  specs %>%
    dplyr::mutate(p_ok = pvalue <= p_cut) %>%
    dplyr::group_by(fe, cluster, controls) %>%
    dplyr::summarise(n = dplyr::n(), share_pok = mean(p_ok, na.rm = TRUE),
                     median_p = median(pvalue, na.rm = TRUE), median_est = median(estimate, na.rm = TRUE),
                     .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(share_pok), median_p) %>% dplyr::slice_head(n = n_top)
}

build_sensitivity_bundle <- function(specs) {
  if (is.null(specs) || nrow(specs) == 0L) return(list())
  specs <- add_strength_column(specs)
  list(overall = summarise_sensitivity_overall(specs), sign = summarise_sign_instability(specs),
       by_fe = summarise_by_fe(specs), by_cluster = summarise_by_cluster(specs),
       by_controls = summarise_by_controls(specs), classes = classify_specs(specs),
       top_cells = top_cells(specs))
}

pick_best_model <- function(results_df, require_positive = TRUE, p_max = 0.10,
                            strength_col = c("effect_strength", "std_slope")) {
  strength_col <- match.arg(strength_col)
  df <- results_df
  if (require_positive) df <- df[df$estimate > 0, , drop = FALSE]
  df <- df[!is.na(df$pvalue) & df$pvalue <= p_max, , drop = FALSE]
  df <- df[!is.na(df[[strength_col]]),              , drop = FALSE]
  if (nrow(df) == 0) return(NULL)
  df <- df[order(df[[strength_col]], decreasing = TRUE), , drop = FALSE]
  df[["rank"]] <- seq_len(nrow(df))
  df[1, , drop = FALSE]
}

run_specs <- function(reg_data, x_var,
                      fe_set       = c("0","buyer","year","buyer+year"),
                      cluster_set  = c("none","buyer","year","buyer_year","buyer_buyertype"),
                      controls_set = c("x_only","base")) {
  out <- list(); k <- 0L
  for (fe in fe_set) {
    fe_part <- make_fe_part(fe)
    for (cl in cluster_set) {
      cl_fml <- make_cluster(cl)
      for (ctrl in controls_set) {
        rhs_terms <- switch(ctrl,
                            "x_only" = c(x_var),
                            "base"   = c(x_var, "buyer_buyertype", "tender_proceduretype"))
        rhs_terms <- rhs_terms[rhs_terms %in% names(reg_data)]
        fml <- stats::as.formula(paste0("ind_corr_binary ~ ", paste(rhs_terms, collapse = " + "), " | ", fe_part))
        m   <- safe_fixest(fixest::feglm(fml, family = quasibinomial(link = "logit"),
                                         data = reg_data, cluster = cl_fml))
        if (is.null(m)) next
        eff          <- extract_effect_fixest(m, x_var, reg_data)
        eff_strength <- safe_fixest(effect_p10_p90(m, reg_data, x_var)) %||% NA_real_
        k <- k + 1L
        out[[k]] <- data.frame(outcome = x_var, model_type = "fractional_logit",
                               fe = fe, cluster = cl, controls = ctrl,
                               estimate = eff$estimate, pvalue = eff$pvalue, nobs = eff$nobs,
                               std_slope = eff$std_slope, effect_strength = eff_strength,
                               stringsAsFactors = FALSE)
      }
    }
  }
  if (length(out) == 0) return(data.frame())
  do.call(rbind, out)
}

run_short_subm_specs <- function(reg_data, fe_set = c("0","buyer","year","buyer+year"),
                                 cluster_set = c("none","buyer","year","buyer_year","buyer_buyertype"),
                                 controls_set = c("x_only","base")) {
  run_specs(reg_data, "short_submission_period", fe_set, cluster_set, controls_set)
}

run_long_dec_specs <- function(reg_data, fe_set = c("0","buyer","year","buyer+year"),
                               cluster_set = c("none","buyer","year","buyer_year","buyer_buyertype"),
                               controls_set = c("x_only","base")) {
  run_specs(reg_data, "long_decision_period", fe_set, cluster_set, controls_set)
}

# ------------------------------------------------------------------------
# 4. Generic "days between" helper
# ------------------------------------------------------------------------

compute_tender_days <- function(df, from_col, to_col, new_col) {
  from_quo   <- rlang::enquo(from_col)
  to_quo     <- rlang::enquo(to_col)
  new_col_nm <- rlang::as_name(rlang::enquo(new_col))
  df %>%
    dplyr::mutate(!!from_quo := as.Date(!!from_quo), !!to_quo := as.Date(!!to_quo)) %>%
    dplyr::filter(!is.na(!!from_quo), !is.na(!!to_quo)) %>%
    dplyr::mutate(!!new_col_nm := as.numeric(!!to_quo - !!from_quo)) %>%
    dplyr::filter(!!rlang::sym(new_col_nm) >= 0, !!rlang::sym(new_col_nm) < 365)
}

# ------------------------------------------------------------------------
# 5–6. Flag helpers (unchanged)
# ------------------------------------------------------------------------

add_short_deadline_flags <- function(df, days_col = tender_days_open,
                                     proc_col = tender_proceduretype, thr) {
  days_col <- rlang::enquo(days_col); proc_col <- rlang::enquo(proc_col)
  med_open       <- df %>% dplyr::filter(!!proc_col == "Open Procedure")              %>% dplyr::summarise(m = stats::median(!!days_col, na.rm = TRUE)) %>% dplyr::pull(m)
  med_restricted <- df %>% dplyr::filter(!!proc_col == "Restricted Procedure")        %>% dplyr::summarise(m = stats::median(!!days_col, na.rm = TRUE)) %>% dplyr::pull(m)
  med_negotiated <- df %>% dplyr::filter(!!proc_col == "Negotiated with publications") %>% dplyr::summarise(m = stats::median(!!days_col, na.rm = TRUE)) %>% dplyr::pull(m)
  short_open_cutoff <- if (is.na(thr$subm_short_open))         med_open       else thr$subm_short_open
  short_rest_cutoff <- if (is.na(thr$subm_short_restricted))   med_restricted else thr$subm_short_restricted
  short_neg_cutoff  <- if (is.null(thr$subm_short_negotiated) || is.na(thr$subm_short_negotiated)) med_negotiated else thr$subm_short_negotiated
  medium_min <- thr$subm_medium_open_min; medium_max <- thr$subm_medium_open_max
  use_medium <- !is.na(medium_min) & !is.na(medium_max)
  df %>% dplyr::mutate(
    short_deadline = dplyr::case_when(
      !!proc_col == "Open Procedure"               & !!days_col < short_open_cutoff ~ TRUE,
      !!proc_col == "Restricted Procedure"         & !!days_col < short_rest_cutoff ~ TRUE,
      !!proc_col == "Negotiated with publications" & !!days_col < short_neg_cutoff  ~ TRUE,
      TRUE ~ FALSE),
    medium_deadline = dplyr::case_when(
      use_medium & !!proc_col == "Open Procedure" &
        !!days_col >= medium_min & !!days_col < medium_max ~ TRUE,
      TRUE ~ FALSE))
}

add_long_decision_flag <- function(df, days_col = tender_days_dec,
                                   proc_col = tender_proceduretype, thr) {
  days_col <- rlang::enquo(days_col); proc_col <- rlang::enquo(proc_col)
  df %>% dplyr::mutate(long_decision = dplyr::case_when(
    !!proc_col %in% c("Open Procedure","Restricted Procedure","Negotiated with publications") &
      !!days_col >= thr$long_decision_days ~ TRUE,
    TRUE ~ FALSE))
}

# ------------------------------------------------------------------------
# 7–8. Plot helpers (unchanged from original)
# ------------------------------------------------------------------------

plot_days_hist_with_quartiles <- function(data, days_var, facet_var = NULL, title,
                                          x_lab, y_lab = "Number of tenders",
                                          caption = NULL, binwidth = 5, xlim = c(0, 365)) {
  days_sym <- rlang::sym(days_var)
  base <- ggplot2::ggplot(data, ggplot2::aes(x = !!days_sym)) +
    ggplot2::geom_histogram(binwidth = binwidth, fill = "lightblue", color = "white", boundary = 0) +
    ggplot2::scale_y_continuous(expand = ggplot2::expansion(mult = c(0, 0.25))) +
    ggplot2::coord_cartesian(xlim = xlim, clip = "off") +
    ggplot2::labs(title = title, x = x_lab, y = y_lab, caption = caption) +
    ggplot2::theme_minimal(base_size = 14) +
    ggplot2::theme(plot.title.position = "plot",
                   plot.title   = ggplot2::element_text(margin = ggplot2::margin(b = 20)),
                   plot.caption = ggplot2::element_text(hjust = 0, face = "italic", size = 10,
                                                        margin = ggplot2::margin(t = 10)))
  if (is.null(facet_var)) {
    q <- stats::quantile(data[[days_var]], probs = c(0.25, 0.5, 0.75), na.rm = TRUE)
    base +
      ggplot2::geom_vline(xintercept = q, color = "blue",
                          linetype = c("dashed","solid","dashed"), size = 1) +
      ggplot2::annotate("text", x = q, y = Inf,
                        label = paste0(names(q), ": ", round(q, 1), " days"),
                        color = "blue", size = 4, angle = 45, vjust = -1, hjust = 0)
  } else {
    facet_sym <- rlang::sym(facet_var)
    q_by_facet <- data %>%
      dplyr::group_by(!!facet_sym) %>%
      dplyr::summarise(q25 = stats::quantile(!!days_sym, 0.25, na.rm = TRUE),
                       q50 = stats::quantile(!!days_sym, 0.50, na.rm = TRUE),
                       q75 = stats::quantile(!!days_sym, 0.75, na.rm = TRUE), .groups = "drop") %>%
      tidyr::pivot_longer(cols = dplyr::starts_with("q"), names_to = "quartile", values_to = "xint") %>%
      dplyr::mutate(
        quartile_label = dplyr::case_when(quartile == "q25" ~ "25%", quartile == "q50" ~ "50% (median)",
                                          quartile == "q75" ~ "75%", TRUE ~ quartile),
        linetype = dplyr::if_else(quartile == "q50", "solid", "dashed"))
    base +
      ggplot2::geom_vline(data = q_by_facet,
                          ggplot2::aes(xintercept = xint, linetype = quartile_label),
                          color = "blue", size = 0.9) +
      ggrepel::geom_text_repel(data = q_by_facet,
                               ggplot2::aes(x = xint, y = Inf, label = paste0(quartile_label, ": ", round(xint, 1), " days")),
                               inherit.aes = FALSE, color = "blue", size = 3.3, angle = 90, vjust = 1.2,
                               min.segment.length = 0, segment.color = "blue", box.padding = 0.5,
                               direction = "x", max.overlaps = Inf) +
      ggplot2::facet_wrap(stats::as.formula(paste("~", facet_var)), scales = "free_y") +
      ggplot2::scale_linetype_manual(name = NULL,
                                     values = c("25%" = "dashed", "50% (median)" = "solid", "75%" = "dashed"))
  }
}

build_proc_share_data <- function(df) {
  df %>%
    dplyr::mutate(
      tender_proceduretype = recode_procedure_type(tender_proceduretype),
      tender_proceduretype = forcats::fct_explicit_na(as.factor(tender_proceduretype), na_level = "Missing value")) %>%
    dplyr::group_by(tender_proceduretype) %>%
    dplyr::summarise(total_value = sum(bid_priceusd, na.rm = TRUE), n_contracts = dplyr::n(), .groups = "drop") %>%
    dplyr::mutate(share_value = total_value / sum(total_value), share_contracts = n_contracts / sum(n_contracts))
}

plot_proc_share_value <- function(plot_data) {
  ggplot2::ggplot(plot_data, ggplot2::aes(x = stats::reorder(tender_proceduretype, share_value), y = share_value)) +
    ggplot2::geom_col(ggplot2::aes(fill = tender_proceduretype), show.legend = FALSE, width = 0.6) +
    ggplot2::geom_text(ggplot2::aes(label = paste0(scales::percent(share_value, accuracy = 0.1),
                                                   " (", scales::dollar(total_value, scale = 1e-6, suffix = "M"), ")")),
                       hjust = -0.05, size = 4) +
    ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                                expand = ggplot2::expansion(mult = c(0, 0.4))) +
    ggplot2::scale_fill_brewer(palette = "Blues", direction = -1) +
    ggplot2::coord_flip() +
    ggplot2::labs(title = "Share of contracts value", x = NULL, y = "Share of total value",
                  caption = "Values in millions of USD") +
    ggplot2::theme_minimal(base_size = 14) +
    ggplot2::theme(plot.margin = ggplot2::margin(10, 30, 10, 10),
                   axis.text.y = ggplot2::element_text(size = 14))
}

plot_proc_share_count <- function(plot_data) {
  ggplot2::ggplot(plot_data, ggplot2::aes(x = stats::reorder(tender_proceduretype, share_value), y = share_contracts)) +
    ggplot2::geom_col(ggplot2::aes(fill = tender_proceduretype), show.legend = FALSE, width = 0.6) +
    ggplot2::geom_text(ggplot2::aes(label = paste0(scales::percent(share_contracts, accuracy = 0.1),
                                                   " (", n_contracts, " contracts)")),
                       hjust = -0.05, size = 4) +
    ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                                expand = ggplot2::expansion(mult = c(0, 0.4))) +
    ggplot2::scale_fill_brewer(palette = "Blues", direction = -1) +
    ggplot2::coord_flip() +
    ggplot2::labs(title = "Share of number of contracts", x = NULL, y = "Share of contracts") +
    ggplot2::theme_minimal(base_size = 14) +
    ggplot2::theme(plot.margin = ggplot2::margin(10, 30, 10, 0),
                   axis.text.y = ggplot2::element_text(size = 14))
}

# ------------------------------------------------------------------------
# 9–10. Threshold config and year filter (unchanged)
# ------------------------------------------------------------------------

admin_threshold_config <- tibble::tribble(
  ~country_code, ~subm_short_open, ~subm_short_restricted, ~subm_short_negotiated,
  ~subm_medium_open_min, ~subm_medium_open_max, ~long_decision_days,
  "DEFAULT", 30, 30, 30, 30, 30, 60,
  "UY",      21, 14, 14, 21, 28, 56,
  "BG",      30, 30, 30, 30, 30, NA,
  "ID",       3,  3, NA,  3,  5, NA
)

get_admin_thresholds <- function(country_code) {
  cc  <- toupper(country_code)
  row <- admin_threshold_config %>%
    dplyr::filter(country_code %in% c(cc, "DEFAULT")) %>%
    dplyr::arrange(dplyr::desc(country_code == cc)) %>%
    dplyr::slice(1)
  as.list(dplyr::select(row, -country_code))
}

year_filter_config <- tibble::tribble(
  ~component, ~country_code, ~min_year, ~max_year,
  "default",  "BG",          NA,        NA,
  "default",  "UY",          NA,        NA,
  "default",  "ID",          NA,        NA,
  "singleb",  "BG",          2011,      2018,
  "singleb",  "UY",          2014,      NA,
  "singleb",  "ID",          2012,      2018
)

get_year_range <- function(country_code, component = c("singleb", "default")) {
  component <- match.arg(component)
  cc        <- toupper(country_code)
  row_spec  <- year_filter_config %>%
    dplyr::filter(component == !!component, country_code == !!cc) %>% dplyr::slice_head(n = 1)
  if (nrow(row_spec) == 0)
    row_spec <- year_filter_config %>%
    dplyr::filter(component == "default", country_code == !!cc) %>% dplyr::slice_head(n = 1)
  if (nrow(row_spec) == 0) return(list(min_year = -Inf, max_year = Inf))
  list(min_year = if (is.na(row_spec$min_year)) -Inf else row_spec$min_year,
       max_year = if (is.na(row_spec$max_year))  Inf else row_spec$max_year)
}

# ========================================================================
# Unified administrative efficiency pipeline
#
# NEW PARAMETER: thresholds (optional)
#   Pass results$thresholds from the Shiny app to use the values set in
#   the Configuration tab rather than the hardcoded country defaults.
#   If NULL (default), falls back to get_admin_thresholds(country_code).
# ========================================================================

run_admin_efficiency_pipeline <- function(df, country_code = "GEN", output_dir,
                                          run_regressions = TRUE,
                                          thresholds = NULL) {
  
  # ── Resolve thresholds ──────────────────────────────────────────────
  # If the app passes its nested thresholds object, convert to flat format.
  # Otherwise fall back to the hardcoded country config.
  if (!is.null(thresholds)) {
    thr        <- app_thresholds_to_pipeline(thresholds)
    thr_source <- "app configuration"
  } else {
    thr        <- get_admin_thresholds(country_code)
    thr_source <- paste0("country defaults (", country_code, ")")
  }
  
  message("Running administrative efficiency pipeline for ", country_code,
          " [thresholds: ", thr_source, "]",
          if (!run_regressions) " [descriptive only]" else "", " ...")
  
  df  <- df %>% add_tender_year()
  
  yr_singleb       <- get_year_range(country_code, component = "singleb")
  min_year_singleb <- yr_singleb$min_year
  max_year_singleb <- yr_singleb$max_year
  
  # ── Summary stats ────────────────────────────────────────────────────
  n_obs_per_year   <- df %>% dplyr::count(tender_year, name = "n_observations")
  n_unique_buyers  <- if ("buyer_masterid"  %in% names(df)) dplyr::n_distinct(df$buyer_masterid,  na.rm = TRUE) else NA_integer_
  n_unique_bidders <- if ("bidder_masterid" %in% names(df)) dplyr::n_distinct(df$bidder_masterid, na.rm = TRUE) else NA_integer_
  tender_year_tenders <- if ("tender_id" %in% names(df))
    df %>% dplyr::group_by(tender_year) %>%
    dplyr::summarise(n_unique_tender_id = dplyr::n_distinct(tender_id), .groups = "drop")
  else NULL
  
  vars_present <- names(df)[!startsWith(names(df), "ind_")]
  summary_stats <- list(n_obs_per_year = n_obs_per_year, n_unique_buyers = n_unique_buyers,
                        tender_year_tenders = tender_year_tenders,
                        n_unique_bidders = n_unique_bidders, vars_present = vars_present)
  
  if (!is.na(n_unique_buyers))  cat("Unique buyers:  ", n_unique_buyers,  "\n\n") else cat("buyer_masterid not found.\n\n")
  if (!is.null(tender_year_tenders)) { cat("Unique tenders per year:\n"); print(tender_year_tenders); cat("\n") } else cat("tender_id not found.\n\n")
  if (!is.na(n_unique_bidders)) cat("Unique bidders: ", n_unique_bidders, "\n\n") else cat("bidder_masterid not found.\n\n")
  
  # ── A) Procedure type shares ─────────────────────────────────────────
  proc_share_data <- build_proc_share_data(df)
  sh            <- plot_proc_share_value(proc_share_data)
  p_count       <- plot_proc_share_count(proc_share_data)
  combined_proc <- sh + p_count + patchwork::plot_layout(ncol = 2)
  ggplot2::ggsave(file.path(output_dir, "share_value_vs_contracts.png"),
                  combined_proc, width = 19, height = 8, dpi = 300)
  
  # ── B) Submission period distribution ────────────────────────────────
  tender_periods_open <- compute_tender_days(
    df, tender_publications_firstcallfortenderdate, tender_biddeadline, tender_days_open)
  subm <- plot_days_hist_with_quartiles(tender_periods_open, "tender_days_open", NULL,
                                        "Days for bid submission", "Days between call opening and bid submission deadline",
                                        caption = "Vertical lines indicate the 25th, 50th (median), and 75th percentiles")
  ggplot2::ggsave(file.path(output_dir, "subm.png"), subm, width = 10, height = 6, dpi = 300)
  
  # ── C) By procedure type ─────────────────────────────────────────────
  tender_periods_open_proc <- tender_periods_open %>%
    dplyr::mutate(tender_proceduretype = recode_procedure_type(tender_proceduretype)) %>%
    tidyr::drop_na(tender_proceduretype)
  subm_proc_facet_q <- plot_days_hist_with_quartiles(tender_periods_open_proc, "tender_days_open",
                                                     "tender_proceduretype", "Days for bid submission by procedure type",
                                                     "Days between call opening and bid submission deadline",
                                                     caption = "Blue lines indicate quartiles within each procedure type")
  ggplot2::ggsave(file.path(output_dir, "subm_proc_fac.png"), subm_proc_facet_q, width = 10, height = 6, dpi = 300)
  
  # ── D) Short submission flags ─────────────────────────────────────────
  tender_periods_short <- tender_periods_open_proc %>%
    dplyr::filter(tender_proceduretype %in% c("Open Procedure","Restricted Procedure","Negotiated with publications")) %>%
    add_short_deadline_flags(days_col = tender_days_open, proc_col = tender_proceduretype, thr = thr)
  
  subm_r <- ggplot2::ggplot(tender_periods_short,
                            ggplot2::aes(x = tender_days_open,
                                         fill = dplyr::case_when(short_deadline ~ "red", medium_deadline ~ "yellow", TRUE ~ "lightblue"))) +
    ggplot2::geom_histogram(binwidth = 1, boundary = 0, colour = "white") +
    ggplot2::facet_wrap(~ tender_proceduretype, scales = "free_y") +
    ggplot2::scale_fill_identity() + ggplot2::xlim(0, 60) +
    ggplot2::labs(x = "Days", y = "Number of tenders",
                  title = "Distribution of tender open periods by procedure type",
                  subtitle = paste0("Red = short deadline (<",
                                    thr$subm_short_open, "d open / <", thr$subm_short_restricted,
                                    "d restricted); yellow = medium band")) +
    ggplot2::theme_minimal(base_size = 14) + ggplot2::theme(legend.position = "none")
  share_labels_short <- tender_periods_short %>%
    dplyr::group_by(tender_proceduretype) %>%
    dplyr::summarise(share_short = mean(short_deadline, na.rm = TRUE) * 100, .groups = "drop")
  subm_r <- subm_r +
    ggplot2::geom_text(data = share_labels_short,
                       ggplot2::aes(x = 50, y = Inf, label = paste0("Share with short deadlines: ", round(share_short, 1), "%")),
                       vjust = 2, hjust = 1, size = 4.5, fontface = "bold", inherit.aes = FALSE)
  ggplot2::ggsave(file.path(output_dir, "subm_r.png"), subm_r, width = 10, height = 6, dpi = 300)
  
  tender_periods_buyer <- tender_periods_short %>% dplyr::mutate(buyer_group = add_buyer_group(buyer_buyertype))
  short_share_buyer_proc <- tender_periods_buyer %>%
    dplyr::group_by(buyer_group, tender_proceduretype) %>%
    dplyr::summarise(share_short = mean(short_deadline, na.rm = TRUE), n_tenders = dplyr::n(), .groups = "drop") %>%
    dplyr::mutate(share_other = 1 - share_short) %>%
    tidyr::pivot_longer(c(share_short, share_other), names_to = "deadline_type", values_to = "share")
  buyer_short <- ggplot2::ggplot(short_share_buyer_proc, ggplot2::aes(x = buyer_group, y = share, fill = deadline_type)) +
    ggplot2::geom_col(position = "fill") +
    ggplot2::geom_text(ggplot2::aes(label = scales::percent(share, accuracy = 1)),
                       position = ggplot2::position_fill(vjust = 0.5), color = "white", size = 4, fontface = "bold") +
    ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
    ggplot2::scale_fill_manual(values = c("share_short" = "tomato2", "share_other" = "steelblue2"),
                               breaks = c("share_short","share_other"),
                               labels = c("Short submission period","Other submission periods")) +
    ggplot2::facet_wrap(~ tender_proceduretype) +
    ggplot2::labs(x = "Buyer group", y = "Share of tenders (100%)", fill = NULL,
                  title = "Short tender submission periods (by contract count)") +
    ggplot2::theme_minimal(base_size = 14) +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1), legend.position = "top")
  short_share_value_buyer_proc <- tender_periods_buyer %>%
    dplyr::group_by(buyer_group, tender_proceduretype) %>%
    dplyr::summarise(total_value = sum(bid_priceusd, na.rm = TRUE),
                     short_value = sum(bid_priceusd[short_deadline %in% TRUE], na.rm = TRUE),
                     share_short = dplyr::if_else(total_value > 0, short_value / total_value, NA_real_),
                     n_contracts = dplyr::n(), .groups = "drop") %>%
    dplyr::mutate(share_other = 1 - share_short) %>%
    tidyr::pivot_longer(c(share_short, share_other), names_to = "deadline_type", values_to = "share")
  buyer_short_v <- ggplot2::ggplot(short_share_value_buyer_proc, ggplot2::aes(x = buyer_group, y = share, fill = deadline_type)) +
    ggplot2::geom_col(position = "fill") +
    ggplot2::geom_text(ggplot2::aes(label = scales::percent(share, accuracy = 1)),
                       position = ggplot2::position_fill(vjust = 0.5), color = "white", size = 4, fontface = "bold") +
    ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
    ggplot2::scale_fill_manual(values = c("share_short" = "tomato2", "share_other" = "steelblue2"),
                               breaks = c("share_short","share_other"),
                               labels = c("Short submission period","Other submission periods")) +
    ggplot2::facet_wrap(~ tender_proceduretype) +
    ggplot2::labs(x = "Buyer group", y = "Share of contract value (100%)", fill = NULL,
                  title = "Short tender submission periods (by contract value)") +
    ggplot2::theme_minimal(base_size = 14) +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1), legend.position = "top")
  combined_short_buyer <- buyer_short + buyer_short_v + patchwork::plot_layout(nrow = 2)
  ggplot2::ggsave(file.path(output_dir, "short_submission_buyer.png"), combined_short_buyer,
                  width = 12, height = 12, dpi = 300)
  
  # ── E–F) Decision period distribution ────────────────────────────────
  if (!"tender_contractsignaturedate" %in% names(df))
    df <- df %>% dplyr::mutate(tender_contractsignaturedate = as.Date(NA))
  df_with_end_date <- df %>%
    dplyr::mutate(decision_end_date = dplyr::coalesce(
      as.Date(tender_contractsignaturedate), as.Date(tender_awarddecisiondate)))
  tender_periods_dec <- compute_tender_days(df_with_end_date, tender_biddeadline, decision_end_date, tender_days_dec)
  decp <- plot_days_hist_with_quartiles(tender_periods_dec, "tender_days_dec", NULL,
                                        "Days for award decision", "Days between bid submission deadline and contract award",
                                        caption = "Vertical lines indicate quartiles")
  ggplot2::ggsave(file.path(output_dir, "decp.png"), decp, width = 10, height = 6, dpi = 300)
  tender_periods_dec_proc <- tender_periods_dec %>%
    dplyr::mutate(tender_proceduretype = recode_procedure_type(tender_proceduretype)) %>%
    tidyr::drop_na(tender_proceduretype)
  decp_proc_facet_q <- plot_days_hist_with_quartiles(tender_periods_dec_proc, "tender_days_dec",
                                                     "tender_proceduretype", "Days for award decision",
                                                     "Days between bid submission deadline and contract award",
                                                     caption = "Blue lines indicate quartiles within each procedure type")
  ggplot2::ggsave(file.path(output_dir, "decp_proc_fac.png"), decp_proc_facet_q, width = 10, height = 6, dpi = 300)
  
  # ── G) Long decision flags ────────────────────────────────────────────
  long_threshold_open <- if (is.na(thr$long_decision_days)) {
    tender_periods_open_proc %>%
      dplyr::filter(tender_proceduretype %in% c("Open Procedure","Restricted Procedure","Negotiated with publications")) %>%
      dplyr::summarise(m = stats::median(tender_days_open, na.rm = TRUE)) %>% dplyr::pull(m)
  } else thr$long_decision_days
  thr_long_open <- thr; thr_long_open$long_decision_days <- long_threshold_open
  tender_periods_long <- tender_periods_open_proc %>%
    dplyr::filter(tender_proceduretype %in% c("Open Procedure","Restricted Procedure","Negotiated with publications")) %>%
    add_long_decision_flag(days_col = tender_days_open, proc_col = tender_proceduretype, thr = thr_long_open)
  long_thr_label_ge <- paste0("\u2265 ", round(long_threshold_open), " days")
  long_thr_label_lt <- paste0("< ",      round(long_threshold_open), " days")
  decp_r <- ggplot2::ggplot(tender_periods_long,
                            ggplot2::aes(x = tender_days_open,
                                         fill = dplyr::case_when(
                                           tender_proceduretype %in% c("Open Procedure","Restricted Procedure") &
                                             tender_days_open >= long_threshold_open ~ "red",
                                           TRUE ~ "lightblue"))) +
    ggplot2::geom_histogram(binwidth = 4, boundary = 0, colour = "white") +
    ggplot2::facet_wrap(~ tender_proceduretype, scales = "free_y") +
    ggplot2::scale_fill_identity() + ggplot2::xlim(0, 300) +
    ggplot2::labs(x = "Days", y = "Number of tenders",
                  title = "Distribution of tender decision periods by procedure type",
                  subtitle = paste0("Bars in red: periods ", long_thr_label_ge, " (long threshold)")) +
    ggplot2::theme_minimal(base_size = 14) + ggplot2::theme(legend.position = "none")
  share_labels_long <- tender_periods_long %>%
    dplyr::group_by(tender_proceduretype) %>%
    dplyr::summarise(share_long = mean(tender_days_open >= long_threshold_open, na.rm = TRUE) * 100, .groups = "drop")
  decp_r <- decp_r +
    ggplot2::geom_text(data = share_labels_long,
                       ggplot2::aes(x = 200, y = Inf, label = paste0("Share delayed: ", round(share_long, 1), "%")),
                       vjust = 2, hjust = 0.75, size = 4.5, fontface = "bold", inherit.aes = FALSE)
  ggplot2::ggsave(file.path(output_dir, "decp_r.png"), decp_r, width = 10, height = 6, dpi = 300)
  
  # ── H) Long decision by buyer ─────────────────────────────────────────
  tender_periods_labeled_dec <- tender_periods_long %>% dplyr::mutate(buyer_group = add_buyer_group(buyer_buyertype))
  long_share_buyer_proc <- tender_periods_labeled_dec %>%
    dplyr::group_by(buyer_group, tender_proceduretype) %>%
    dplyr::summarise(share_long = mean(long_decision, na.rm = TRUE), n_tenders = dplyr::n(), .groups = "drop") %>%
    dplyr::mutate(share_other = 1 - share_long) %>%
    tidyr::pivot_longer(c(share_long, share_other), names_to = "decision_type", values_to = "share")
  buyer_long <- ggplot2::ggplot(long_share_buyer_proc, ggplot2::aes(x = buyer_group, y = share, fill = decision_type)) +
    ggplot2::geom_col(position = "fill") +
    ggplot2::geom_text(ggplot2::aes(label = scales::percent(share, accuracy = 1)),
                       position = ggplot2::position_fill(vjust = 0.5), color = "white", size = 4, fontface = "bold") +
    ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
    ggplot2::scale_fill_manual(values = c("share_long" = "tomato2","share_other" = "steelblue2"),
                               breaks = c("share_long","share_other"),
                               labels = c(long_thr_label_ge, long_thr_label_lt)) +
    ggplot2::facet_wrap(~ tender_proceduretype) +
    ggplot2::labs(x = "Buyer group", y = "Share of tenders (100%)", fill = NULL,
                  title = paste0("Long tender decision periods (", long_thr_label_ge, ") — by count")) +
    ggplot2::theme_minimal(base_size = 14) +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1), legend.position = "top")
  long_share_value_buyer_proc <- tender_periods_labeled_dec %>%
    dplyr::group_by(buyer_group, tender_proceduretype) %>%
    dplyr::summarise(total_value = sum(bid_priceusd, na.rm = TRUE),
                     long_value  = sum(bid_priceusd[long_decision %in% TRUE], na.rm = TRUE),
                     share_long  = dplyr::if_else(total_value > 0, long_value / total_value, NA_real_),
                     n_contracts = dplyr::n(), .groups = "drop") %>%
    dplyr::mutate(share_other = 1 - share_long) %>%
    tidyr::pivot_longer(c(share_long, share_other), names_to = "decision_type", values_to = "share")
  buyer_long_v <- ggplot2::ggplot(long_share_value_buyer_proc, ggplot2::aes(x = buyer_group, y = share, fill = decision_type)) +
    ggplot2::geom_col(position = "fill") +
    ggplot2::geom_text(ggplot2::aes(label = scales::percent(share, accuracy = 1)),
                       position = ggplot2::position_fill(vjust = 0.5), color = "white", size = 4, fontface = "bold") +
    ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
    ggplot2::scale_fill_manual(values = c("share_long" = "tomato2","share_other" = "steelblue2"),
                               breaks = c("share_long","share_other"),
                               labels = c(long_thr_label_ge, long_thr_label_lt)) +
    ggplot2::facet_wrap(~ tender_proceduretype) +
    ggplot2::labs(x = "Buyer group", y = "Share of contract value (100%)", fill = NULL,
                  title = paste0("Long tender decision periods (", long_thr_label_ge, ") — by value")) +
    ggplot2::theme_minimal(base_size = 14) +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1), legend.position = "top")
  combined_dec_plot <- buyer_long + buyer_long_v + patchwork::plot_layout(nrow = 2)
  ggplot2::ggsave(file.path(output_dir, "long_decision_buyer.png"), combined_dec_plot,
                  width = 12, height = 12, dpi = 300)
  
  # ── I–J) Regressions ─────────────────────────────────────────────────
  specs_short <- specs_long <- sensitivity_short <- sensitivity_long <- NULL
  model_short_glm <- model_long_glm <- plot_short_reg <- plot_long_reg <- NULL
  
  run_one_regression <- function(reg_data, x_var, label) {
    if (nrow(reg_data) == 0) { message("No data for ", label); return(list()) }
    specs       <- run_specs(reg_data, x_var)
    if (is.null(specs) || nrow(specs) == 0) return(list())
    sensitivity <- build_sensitivity_bundle(specs)
    message("\n--- ", label, " Sensitivity (", country_code, ") ---")
    print(sensitivity$overall); print(sensitivity$sign); print(sensitivity$by_fe)
    print(sensitivity$by_cluster); print(sensitivity$by_controls)
    print(sensitivity$classes);  print(sensitivity$top_cells)
    best_row <- pick_best_model(specs, require_positive = TRUE, p_max = 0.10,
                                strength_col = "effect_strength")
    if (is.null(best_row)) return(list(specs = specs, sensitivity = sensitivity))
    fe_part   <- make_fe_part(best_row$fe)
    cl_fml    <- make_cluster(best_row$cluster)
    rhs_terms <- switch(best_row$controls,
                        "x_only" = c(x_var),
                        "base"   = c(x_var, "buyer_buyertype", "tender_proceduretype"))
    rhs_terms <- rhs_terms[rhs_terms %in% names(reg_data)]
    fml <- stats::as.formula(paste0("ind_corr_binary ~ ", paste(rhs_terms, collapse = " + "), " | ", fe_part))
    model_glm <- fixest::feglm(fml, family = quasibinomial(link = "logit"),
                               data = reg_data, cluster = cl_fml)
    pred <- tryCatch({
      raw <- ggeffects::ggpredict(model_glm, terms = x_var)
      # Normalize ggeffects output to a plain data.frame with stable column names.
      # ggeffects < 1.3 uses 'predicted'; >= 1.3 uses 'predicted' as an S3 print alias
      # but the underlying tibble may vary. Force to data.frame with expected names.
      df_pred <- as.data.frame(raw)
      if (!"predicted" %in% names(df_pred) && "estimate" %in% names(df_pred))
        df_pred <- dplyr::rename(df_pred, predicted = estimate)
      if (!"conf.low"  %in% names(df_pred) && "conf.low"  %in% names(raw)) df_pred$conf.low  <- raw$conf.low
      if (!"conf.high" %in% names(df_pred) && "conf.high" %in% names(raw)) df_pred$conf.high <- raw$conf.high
      if (!"x" %in% names(df_pred) && "x" %in% names(raw)) df_pred$x <- raw$x
      df_pred
    }, error = function(e) NULL)
    plot_reg <- NULL
    if (!is.null(pred) && "predicted" %in% names(pred)) {
      caption_txt <- paste0("N=", nrow(reg_data), "; years ",
                            min(reg_data$tender_year, na.rm = TRUE), "\u2013", max(reg_data$tender_year, na.rm = TRUE),
                            ". Best model: FE=", best_row$fe, ", Cluster=", best_row$cluster,
                            ", Controls=", best_row$controls,
                            ". Thresholds: ", thr_source)
      plot_reg <- ggplot2::ggplot(pred, ggplot2::aes(x = x, y = predicted)) +
        ggplot2::geom_line(size = 1.5, color = "lightblue") +
        ggplot2::geom_ribbon(ggplot2::aes(ymin = conf.low, ymax = conf.high), alpha = 0.2) +
        ggplot2::labs(title   = paste0("Predicted probability of single bidding by ", label),
                      subtitle = "(Best Model from Specification Testing)",
                      x = paste0(x_var, " (0 = normal, 1 = flagged)"),
                      y = "Predicted probability", caption = caption_txt) +
        ggplot2::scale_y_continuous(labels = scales::percent_format()) +
        ggplot2::theme_minimal(base_size = 20)
    }
    list(specs = specs, sensitivity = sensitivity, model_glm = model_glm, plot_reg = plot_reg)
  }
  
  if (run_regressions) {
    message("\n", strrep("-", 60))
    message("Running specification testing for SHORT submission period...")
    message("  Cutoffs: open=", thr$subm_short_open, "d, restricted=",
            thr$subm_short_restricted, "d, neg_pub=", thr$subm_short_negotiated, "d")
    message(strrep("-", 60))
    
    reg_short_base <- df %>%
      dplyr::mutate(
        tender_publications_firstcallfortenderdate = as.Date(tender_publications_firstcallfortenderdate),
        tender_biddeadline = as.Date(tender_biddeadline),
        tender_days_open   = as.numeric(tender_biddeadline - tender_publications_firstcallfortenderdate)) %>%
      add_tender_year() %>%
      dplyr::filter(tender_year >= min_year_singleb, tender_year <= max_year_singleb,
                    !is.na(tender_days_open), tender_days_open >= 0, tender_days_open < 365,
                    tender_proceduretype %in% c("OPEN","RESTRICTED","NEGOTIATED_WITH_PUBLICATION"))
    
    # Use app thresholds directly (already resolved above), fall back to median if NA
    short_open_reg <- if (!is.na(thr$subm_short_open)) thr$subm_short_open else
      stats::median(reg_short_base$tender_days_open[reg_short_base$tender_proceduretype == "OPEN"], na.rm = TRUE)
    short_rest_reg <- if (!is.na(thr$subm_short_restricted)) thr$subm_short_restricted else
      stats::median(reg_short_base$tender_days_open[reg_short_base$tender_proceduretype == "RESTRICTED"], na.rm = TRUE)
    short_neg_reg  <- if (!is.na(thr$subm_short_negotiated)) thr$subm_short_negotiated else
      stats::median(reg_short_base$tender_days_open[reg_short_base$tender_proceduretype == "NEGOTIATED_WITH_PUBLICATION"], na.rm = TRUE)
    
    reg_short <- reg_short_base %>%
      dplyr::mutate(short_submission_period = dplyr::case_when(
        tender_proceduretype == "OPEN"                        & tender_days_open < short_open_reg ~ 1L,
        tender_proceduretype == "RESTRICTED"                  & tender_days_open < short_rest_reg ~ 1L,
        tender_proceduretype == "NEGOTIATED_WITH_PUBLICATION" & tender_days_open < short_neg_reg  ~ 1L,
        tender_proceduretype %in% c("OPEN","RESTRICTED","NEGOTIATED_WITH_PUBLICATION") ~ 0L,
        TRUE ~ NA_integer_)) %>%
      dplyr::filter(!is.na(short_submission_period), !is.na(ind_corr_singleb), !is.na(buyer_id)) %>%
      dplyr::mutate(ind_corr_binary = ind_corr_singleb / 100)
    
    res_short       <- run_one_regression(reg_short, "short_submission_period", "short submission period")
    specs_short     <- res_short$specs
    sensitivity_short <- res_short$sensitivity
    model_short_glm <- res_short$model_glm
    plot_short_reg  <- res_short$plot_reg
    
    message("\n", strrep("-", 60))
    message("Running specification testing for LONG decision period...")
    message("  Cutoff: ", thr$long_decision_days, " days")
    message(strrep("-", 60))
    
    reg_long_base <- df %>%
      dplyr::mutate(
        tender_publications_firstcallfortenderdate = as.Date(tender_publications_firstcallfortenderdate),
        tender_biddeadline = as.Date(tender_biddeadline),
        tender_days_dec    = as.numeric(tender_biddeadline - tender_publications_firstcallfortenderdate)) %>%
      add_tender_year() %>%
      dplyr::filter(tender_year >= min_year_singleb, tender_year <= max_year_singleb,
                    tender_days_dec >= 0, tender_days_dec < 365,
                    tender_proceduretype %in% c("OPEN","RESTRICTED","NEGOTIATED_WITH_PUBLICATION"))
    
    long_threshold_dec <- if (!is.na(thr$long_decision_days)) thr$long_decision_days else
      stats::median(reg_long_base$tender_days_dec, na.rm = TRUE)
    
    reg_long <- reg_long_base %>%
      dplyr::mutate(long_decision_period = dplyr::case_when(
        !is.na(tender_days_dec) & tender_days_dec >= 0 & tender_days_dec < 365 &
          tender_days_dec >= long_threshold_dec ~ 1L,
        !is.na(tender_days_dec) & tender_days_dec >= 0 & tender_days_dec < 365 &
          tender_days_dec <  long_threshold_dec ~ 0L,
        TRUE ~ NA_integer_)) %>%
      dplyr::filter(!is.na(long_decision_period), !is.na(ind_corr_singleb), !is.na(buyer_id)) %>%
      dplyr::mutate(ind_corr_binary = ind_corr_singleb / 100)
    
    res_long        <- run_one_regression(reg_long, "long_decision_period", "long decision period")
    specs_long      <- res_long$specs
    sensitivity_long  <- res_long$sensitivity
    model_long_glm  <- res_long$model_glm
    plot_long_reg   <- res_long$plot_reg
  }
  
  # ── K) Return ─────────────────────────────────────────────────────────
  invisible(list(
    country_code = country_code, data = df, thresholds = thr, thr_source = thr_source,
    proc_share_data = proc_share_data,
    tender_periods_open = tender_periods_open, tender_periods_open_proc = tender_periods_open_proc,
    tender_periods_short = tender_periods_short, tender_periods_dec = tender_periods_dec,
    tender_periods_dec_proc = tender_periods_dec_proc, tender_periods_long = tender_periods_long,
    tender_periods_labeled_dec = tender_periods_labeled_dec,
    sh = sh, p_count = p_count, combined_proc = combined_proc,
    subm = subm, subm_proc_facet_q = subm_proc_facet_q, subm_r = subm_r,
    buyer_short = buyer_short, buyer_short_v = buyer_short_v, combined_short_buyer = combined_short_buyer,
    decp = decp, decp_proc_facet_q = decp_proc_facet_q, decp_r = decp_r,
    buyer_long = buyer_long, buyer_long_v = buyer_long_v, combined_dec_plot = combined_dec_plot,
    plot_short_reg = plot_short_reg, plot_long_reg = plot_long_reg,
    model_short_glm = model_short_glm, model_long_glm = model_long_glm,
    specs_short = specs_short, sensitivity_short = sensitivity_short,
    specs_long = specs_long, sensitivity_long = sensitivity_long,
    summary_stats = summary_stats
  ))
}