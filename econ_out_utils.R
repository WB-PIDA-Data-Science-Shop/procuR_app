# ========================================================================
# PROACT Economic Efficiency Utilities
# ========================================================================
# Purpose:
#   A reusable, modular toolkit for procurement exports (e.g., PROACT).
#   Focus areas ("economic efficiency"):
#     - CPV market sizing (counts, total value, typical contract size)
#     - Supplier entry dynamics (new vs repeat suppliers)
#     - Buyer–supplier network snapshots by year (top buyers)
#     - Relative prices (contract vs estimated) diagnostics (BG-style)
#     - Competition diagnostics (single-bid incidence by procedure/price/buyer)
#
# Design principles:
#   - Small pure helpers + a single orchestrator pipeline that returns a list
#     of objects for printing in an Rmd (similar to your admin pipeline).
#   - No hard-coded file paths; all I/O is parameterized.
#   - CPV lookup is built once and reused.
#   - Guardrails for missing columns and pathological values.
# ========================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(purrr)
  library(forcats)
  library(ggplot2)
  library(scales)
  library(ggrepel)
  library(tidygraph)
  library(ggraph)
  library(patchwork)
})

# ------------------------------------------------------------------------
# Register 'text' as a known optional aesthetic for plotly hover labels.
# ggplot2 silently ignores aesthetics listed in optional_aes rather than
# warning "Ignoring unknown aesthetics: text". This must run once after
# ggplot2 is loaded. The 'text' aesthetic is consumed by ggplotly() to
# build hover tooltips and is intentionally invisible to ggplot2 itself.
# ------------------------------------------------------------------------
local({
  geoms_needing_text <- list(
    ggplot2::GeomPoint,
    ggplot2::GeomCol,
    ggplot2::GeomBar,
    ggplot2::GeomLine,
    ggplot2::GeomPath,
    ggplot2::GeomSegment,
    ggplot2::GeomErrorbarh
  )
  for (g in geoms_needing_text) {
    if (!"text" %in% g$optional_aes) {
      g$optional_aes <- c(g$optional_aes, "text")
    }
  }
})

# load_data, dir_ensure defined in utils_shared.R

save_plot <- function(plot, out_dir, filename, width = 10, height = 7, dpi = 300) {
  dir_ensure(out_dir)
  ggplot2::ggsave(
    filename = file.path(out_dir, filename),
    plot     = plot,
    width    = width,
    height   = height,
    dpi      = dpi
  )
  invisible(file.path(out_dir, filename))
}

# ------------------------------------------------------------------------
# 1) Relabeling / recoding helpers (keep near the top)
# ------------------------------------------------------------------------

# recode_procedure_type, add_buyer_group, add_tender_year defined in utils_shared.R

make_cpv_cluster_legend <- function(market_summary) {
  market_summary %>%
    dplyr::select(cpv_cluster, cpv_category) %>%
    dplyr::distinct() %>%
    dplyr::arrange(cpv_cluster)
}


# ------------------------------------------------------------------------
# 2) Standard feature engineering
# ------------------------------------------------------------------------




add_cpv_cluster <- function(df, cpv_col = "lot_productcode", digits = 2) {
  if (!cpv_col %in% names(df)) {
    df$cpv_cluster <- NA_character_
    return(df)
  }
  df %>% dplyr::mutate(cpv_cluster = stringr::str_sub(.data[[cpv_col]], 1, digits))
}

add_single_bid_flag <- function(df, singleb_col = "ind_corr_singleb") {
  if (!singleb_col %in% names(df)) {
    df$single_bid <- NA_real_
    return(df)
  }
  df %>% dplyr::mutate(single_bid = .data[[singleb_col]] / 100)
}

add_price_bins_usd <- function(df, value_col = "bid_priceusd") {
  if (!value_col %in% names(df)) {
    df$price_bin <- NA
    return(df)
  }
  df %>%
    dplyr::mutate(
      price_bin = cut(
        .data[[value_col]],
        breaks = c(0, 5e3, 1e4, 5e4, 1e5, 5e5, 1e6, Inf),
        labels = c("< 5k", "5–10k", "10k–50k", "50k–100k", "100k–500k", "500k–1M", "> 1M"),
        right  = FALSE
      )
    )
}

wrap_strip <- function(x, width = 18) {
  stringr::str_wrap(x, width = width)
}

# ------------------------------------------------------------------------
# 3) CPV lookup (build once, reuse)
# ------------------------------------------------------------------------

build_cpv_lookup <- function(cpv_table, code_col = "CODE", label_col = "EN") {
  stopifnot(code_col %in% names(cpv_table), label_col %in% names(cpv_table))
  
  cpv_core <- sub("-.*", "", cpv_table[[code_col]])
  
  df <- cpv_table %>%
    dplyr::mutate(
      cpv_core = cpv_core,
      cpv_2d = dplyr::if_else(
        stringr::str_sub(cpv_core, 3, 8) == "000000",
        stringr::str_sub(cpv_core, 1, 2),
        NA_character_
      ),
      cpv_3d = dplyr::if_else(
        stringr::str_sub(cpv_core, 3, 3) != "0" & stringr::str_sub(cpv_core, 4, 8) == "00000",
        stringr::str_sub(cpv_core, 1, 3),
        NA_character_
      )
    )
  
  list(
    cpv_2d = df %>%
      dplyr::filter(!is.na(cpv_2d)) %>%
      dplyr::distinct(cpv_2d, .keep_all = TRUE) %>%
      dplyr::transmute(cpv_cluster = cpv_2d, cpv_category = .data[[label_col]]),
    cpv_3d = df %>%
      dplyr::filter(!is.na(cpv_3d)) %>%
      dplyr::distinct(cpv_3d, .keep_all = TRUE) %>%
      dplyr::transmute(cpv_cluster = cpv_3d, cpv_category = .data[[label_col]])
  )
}

attach_cpv_labels <- function(df,
                              cpv_lookup_2d,
                              cluster_col = "cpv_cluster",
                              other_code = "99",
                              other_label = "Other") {
  if (!cluster_col %in% names(df)) return(df)
  
  out <- df %>%
    dplyr::left_join(cpv_lookup_2d, by = setNames("cpv_cluster", cluster_col)) %>%
    dplyr::mutate(
      !!cluster_col := dplyr::if_else(is.na(cpv_category) | cpv_category == "", other_code, .data[[cluster_col]]),
      cpv_category  := dplyr::if_else(.data[[cluster_col]] == other_code, other_label, cpv_category)
    )
  
  out
}


# ------------------------------------------------------------------------
# 3.1) Year set up
# ------------------------------------------------------------------------

year_breaks_rule <- function(years, max_labels = 5) {
  yrs <- sort(unique(stats::na.omit(as.integer(years))))
  if (length(yrs) <= max_labels) return(yrs)
  # if too many years -> every 2nd year (you asked exactly this)
  yrs[seq(1, length(yrs), by = 2)]
}

NETWORK_YEAR_LIMITS <- list(
  GEN = c(NA, NA),
  UY  = c(2014, NA),
  BG  = c(2011, 2018)
)

get_network_year_limits <- function(country_code, default = c(-Inf, Inf)) {
  lim <- NETWORK_YEAR_LIMITS[[country_code]]
  if (is.null(lim) || length(lim) != 2) lim <- default
  
  # Treat NA as open-ended
  lim[1] <- ifelse(is.na(lim[1]), default[1], lim[1])
  lim[2] <- ifelse(is.na(lim[2]), default[2], lim[2])
  lim
}



# ------------------------------------------------------------------------
# 4) Market size summaries + plots
# ------------------------------------------------------------------------
summarise_market_size <- function(df,
                                  cluster_col = "cpv_cluster",
                                  category_col = "cpv_category",
                                  value_col = NULL) {
  stopifnot(cluster_col %in% names(df))
  
  # If no value column specified or found, create dummy column
  if (is.null(value_col) || !value_col %in% names(df)) {
    df$dummy_value <- NA_real_
    value_col <- "dummy_value"
  }
  
  if (!category_col %in% names(df)) df[[category_col]] <- NA_character_
  
  df %>%
    dplyr::group_by(.data[[cluster_col]], .data[[category_col]]) %>%
    dplyr::summarise(
      total_value = sum(.data[[value_col]], na.rm = TRUE),
      avg_value   = mean(.data[[value_col]], na.rm = TRUE),
      n_contracts = dplyr::n(),
      .groups     = "drop"
    ) %>%
    dplyr::rename(cpv_cluster = 1, cpv_category = 2)
}

plot_market_contract_counts <- function(market_summary) {
  ggplot2::ggplot(market_summary, ggplot2::aes(x = stats::reorder(cpv_category, n_contracts), y = n_contracts)) +
    ggplot2::geom_col(fill = "#5B8DB8") +
    ggplot2::coord_flip() +
    ggplot2::scale_y_continuous(labels = scales::comma) +
    ggplot2::labs(x = NULL, y = "Count of contracts") +
    ggplot2::theme_minimal(base_size = 10) +
    ggplot2::theme(axis.text.y = ggplot2::element_text(size = 8))
}

plot_market_total_value <- function(market_summary) {
  ggplot2::ggplot(market_summary, ggplot2::aes(x = stats::reorder(cpv_category, total_value), y = total_value)) +
    ggplot2::geom_col(fill = "#00897B") +
    ggplot2::coord_flip() +
    ggplot2::scale_y_continuous(labels = scales::comma) +
    ggplot2::labs(x = NULL, y = "Total contract value (USD)") +
    ggplot2::theme_minimal(base_size = 10) +
    ggplot2::theme(axis.text.y = ggplot2::element_text(size = 8))
}

plot_market_bubble <- function(market_summary) {
  # Check if market_summary is NULL or empty
  if (is.null(market_summary) || nrow(market_summary) == 0) {
    warning("market_summary is NULL or empty, cannot create bubble plot")
    return(NULL)
  }
  
  
  # Filter out rows with NA or zero/negative values (can't plot on log scale)
  plot_data <- market_summary %>%
    dplyr::filter(!is.na(n_contracts) & n_contracts > 0) %>%
    dplyr::filter(!is.na(avg_value) & avg_value > 0) %>%
    dplyr::filter(!is.na(total_value) & total_value > 0)
  
  
  # If no valid data, return simple message plot
  if (nrow(plot_data) == 0) {
    warning("No valid price data for bubble plot")
    # Return a simple ggplot that plotly can handle
    return(ggplot2::ggplot(data.frame(x = 1, y = 1, label = "No price data available"),
                           ggplot2::aes(x = x, y = y)) +
             ggplot2::geom_text(ggplot2::aes(label = label), size = 6) +
             ggplot2::theme_void() +
             ggplot2::labs())
  }
  
  ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x = n_contracts,
      y = avg_value,
      size = total_value,
      label = cpv_cluster,
      text = cpv_category  # Add cpv_category for plotly tooltip
    )
  ) +
    ggplot2::geom_point(alpha = 0.7, colour = "steelblue") +
    ggrepel::geom_text_repel(size = 3, max.overlaps = 30, colour = "gray20") +
    ggplot2::annotation_logticks(sides="b", colour="#aaaaaa", size=0.3) +
    ggplot2::scale_x_log10(
      breaks = c(1,5,10,50,100,500,1000,5000,10000,50000),
      labels = scales::label_comma()
    ) +
    ggplot2::scale_y_log10(
      breaks = c(1e3,1e4,1e5,1e6,1e7,1e8,1e9),
      labels = scales::label_dollar(scale_cut=scales::cut_short_scale(), accuracy=1)
    ) +
    ggplot2::scale_size_continuous(
      labels = scales::label_number(scale_cut = scales::cut_short_scale()),
      name = "Total market value"
    ) +
    ggplot2::labs(
      x = "Number of contracts",
      y = "Avg contract value (USD)"
    ) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(
      axis.title = ggplot2::element_text(size=11),
      axis.text  = ggplot2::element_text(size=10),
      panel.grid.minor = ggplot2::element_blank()
    ) +
    ggplot2::guides(size = ggplot2::guide_legend(order = 1))
}

# ------------------------------------------------------------------------
# 5) Supplier entry: new vs repeat suppliers
# ------------------------------------------------------------------------

compute_supplier_entry <- function(df,
                                   supplier_id_col = "bidder_masterid",
                                   cluster_col = "cpv_cluster",
                                   year_col = "tender_year") {
  cols <- c(supplier_id_col, cluster_col, year_col)
  missing <- setdiff(cols, names(df))
  if (length(missing) > 0) stop("Missing required columns: ", paste(missing, collapse = ", "))
  
  df2 <- df %>%
    dplyr::filter(!is.na(.data[[supplier_id_col]]),
                  !is.na(.data[[year_col]]),
                  !is.na(.data[[cluster_col]]))
  
  # Avoid left_join: compute first_year as a grouped window mutate (single pass)
  df2 <- df2 %>%
    dplyr::group_by(.data[[cluster_col]], .data[[supplier_id_col]]) %>%
    dplyr::mutate(first_year = min(.data[[year_col]], na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(is_new = .data[[year_col]] == first_year)
  
  df2 %>%
    dplyr::group_by(.data[[cluster_col]], .data[[year_col]]) %>%
    dplyr::summarise(
      n_suppliers        = dplyr::n_distinct(.data[[supplier_id_col]]),
      n_new_suppliers    = dplyr::n_distinct(.data[[supplier_id_col]][is_new]),
      n_repeat_suppliers = n_suppliers - n_new_suppliers,
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      share_new    = n_new_suppliers / n_suppliers,
      share_repeat = n_repeat_suppliers / n_suppliers
    ) %>%
    dplyr::rename(cpv_cluster = 1, tender_year = 2)
}

plot_supplier_shares_heatmap <- function(supplier_stats) {
  
  # Create heatmap showing % new suppliers by market and year
  ggplot2::ggplot(supplier_stats,
                  ggplot2::aes(x = factor(tender_year), y = cpv_cluster,
                               fill = share_new,
                               text = paste0("Market: ", cpv_cluster, "<br>",
                                             "Year: ", tender_year, "<br>",
                                             "New: ", scales::percent(share_new, accuracy = 0.1), "<br>",
                                             "Repeat: ", scales::percent(share_repeat, accuracy = 0.1), "<br>",
                                             "Total: ", n_suppliers))) +
    ggplot2::geom_tile(color = "white", linewidth = 0.5) +
    ggplot2::scale_fill_gradient2(
      low = "#2c3e50", mid = "#ecf0f1", high = "#e74c3c",
      midpoint = 0.5,
      labels = scales::percent,
      name = "% New\nSuppliers",
      limits = c(0, 1)
    ) +
    ggplot2::labs(
      x = "Year", y = "CPV Market"
    ) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(
      axis.text.y = ggplot2::element_text(size = 8),
      axis.text.x = ggplot2::element_text(angle = 45, hjust = 1),
      panel.grid = ggplot2::element_blank()
    )
}


plot_unique_suppliers_heatmap <- function(df,
                                          supplier_id_col = "bidder_masterid",
                                          cluster_col = "cpv_cluster",
                                          year_col = "tender_year") {
  
  plot_data <- df %>%
    dplyr::filter(!is.na(.data[[supplier_id_col]]),
                  !is.na(.data[[year_col]]),
                  !is.na(.data[[cluster_col]])) %>%
    dplyr::group_by(.data[[cluster_col]], .data[[year_col]]) %>%
    dplyr::summarise(n_suppliers = dplyr::n_distinct(.data[[supplier_id_col]]), .groups = "drop") %>%
    dplyr::rename(cpv_cluster = 1, tender_year = 2)
  
  # Create heatmap
  ggplot2::ggplot(plot_data,
                  ggplot2::aes(x = factor(tender_year), y = cpv_cluster,
                               fill = n_suppliers,
                               text = paste0("Market: ", cpv_cluster, "<br>",
                                             "Year: ", tender_year, "<br>",
                                             "Unique Suppliers: ", n_suppliers))) +
    ggplot2::geom_tile(color = "white", linewidth = 0.5) +
    ggplot2::scale_fill_gradient(
      low = "#ecf0f1", high = "#3498db",
      name = "Unique\nSuppliers",
      labels = scales::comma
    ) +
    ggplot2::labs(
      x = "Year", y = "CPV Market"
    ) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(
      axis.text.y = ggplot2::element_text(size = 8),
      axis.text.x = ggplot2::element_text(angle = 45, hjust = 1),
      panel.grid = ggplot2::element_blank()
    )
}

# ------------------------------------------------------------------------
# 6) Buyer–supplier networks (yearly snapshots)
# ------------------------------------------------------------------------

plot_buyer_supplier_networks <- function(df,
                                         cpv_focus,
                                         n_top_buyers = 20,
                                         ncol = 1,
                                         buyer_id_col = "buyer_id",
                                         supplier_id_col = "bidder_masterid",
                                         year_col = "tender_year",
                                         value_col = "bid_priceusd",
                                         country_code = "GEN",
                                         year_limits = NULL) {
  
  required <- c("cpv_cluster", buyer_id_col, supplier_id_col, year_col)
  missing <- setdiff(required, names(df))
  if (length(missing) > 0) stop("Missing required columns: ", paste(missing, collapse = ", "))
  
  if (is.null(year_limits)) year_limits <- get_network_year_limits(country_code)
  
  sub <- df %>%
    dplyr::filter(
      cpv_cluster == cpv_focus,
      !is.na(.data[[buyer_id_col]]),
      !is.na(.data[[supplier_id_col]]),
      !is.na(.data[[year_col]]),
      .data[[year_col]] >= year_limits[1],
      .data[[year_col]] <= year_limits[2]
    )
  
  if (nrow(sub) == 0) stop("No data for cpv_cluster = ", cpv_focus, " in selected year range.")
  
  years <- sort(unique(sub[[year_col]]))
  
  filter_top_buyers <- function(dat, yr) {
    dy <- dat %>% dplyr::filter(.data[[year_col]] == yr)
    if (nrow(dy) == 0) return(dy[0, , drop = FALSE])
    
    if (value_col %in% names(dy)) {
      top <- dy %>%
        dplyr::group_by(.data[[buyer_id_col]]) %>%
        dplyr::summarise(total_value = sum(.data[[value_col]], na.rm = TRUE), .groups = "drop") %>%
        dplyr::slice_max(total_value, n = n_top_buyers, with_ties = FALSE)
      dy %>% dplyr::filter(.data[[buyer_id_col]] %in% top[[buyer_id_col]])
    } else {
      top <- dy %>%
        dplyr::count(.data[[buyer_id_col]], name = "n") %>%
        dplyr::slice_max(n, n = n_top_buyers, with_ties = FALSE)
      dy %>% dplyr::filter(.data[[buyer_id_col]] %in% top[[buyer_id_col]])
    }
  }
  
  make_year_plot <- function(yr) {
    dat <- filter_top_buyers(sub, yr)
    if (nrow(dat) == 0) return(NULL)
    
    edges <- dat %>%
      dplyr::count(.data[[buyer_id_col]], .data[[supplier_id_col]], name = "weight") %>%
      dplyr::transmute(
        from   = paste0("B_", .data[[buyer_id_col]]),
        to     = paste0("S_", .data[[supplier_id_col]]),
        weight = weight
      )
    
    buyers <- dat %>%
      dplyr::distinct(.data[[buyer_id_col]]) %>%
      dplyr::transmute(name = paste0("B_", .data[[buyer_id_col]]), role = "Buyer")
    
    suppliers <- dat %>%
      dplyr::distinct(.data[[supplier_id_col]]) %>%
      dplyr::transmute(name = paste0("S_", .data[[supplier_id_col]]), role = "Supplier")
    
    nodes <- dplyr::bind_rows(buyers, suppliers) %>% dplyr::distinct(name, .keep_all = TRUE)
    
    g <- tidygraph::tbl_graph(nodes = nodes, edges = edges, directed = FALSE) %>%
      tidygraph::activate(nodes) %>%
      dplyr::mutate(degree = tidygraph::centrality_degree())
    
    ggraph::ggraph(g, layout = "stress") +
      ggraph::geom_edge_link(ggplot2::aes(alpha = weight), show.legend = FALSE) +
      ggraph::geom_node_point(ggplot2::aes(color = role, size = degree), show.legend = FALSE) +
      ggraph::geom_node_text(
        ggplot2::aes(label = ifelse(role == "Buyer", name, "")),
        repel = TRUE,
        size = 2.5,
        show.legend = FALSE
      ) +
      ggplot2::scale_color_manual(values = c("Buyer" = "steelblue", "Supplier" = "darkorange")) +
      ggplot2::scale_size_continuous(range = c(2, 6), limits = c(0, NA)) +
      ggplot2::theme_void() +
      ggplot2::ggtitle(paste("Year:", yr)) +
      ggplot2::theme(legend.position = "none")
  }
  
  plots <- purrr::map(years, make_year_plot) %>% purrr::compact()
  if (length(plots) == 0) stop("No non-empty yearly networks for cpv_cluster = ", cpv_focus)
  
  patchwork::wrap_plots(plots, ncol = ncol) +
    patchwork::plot_annotation(
      caption  = "Caption: Node color = role (Buyer vs Supplier). Node size = number of unique partners (degree). Edge thickness/opacity reflects number of contracts between buyer–supplier pairs."
    ) &
    ggplot2::theme(legend.position = "none")
}

# ------------------------------------------------------------------------
# 7) Relative price diagnostics (BG-style)
# ------------------------------------------------------------------------

add_relative_price <- function(df,
                               contract_price_col = "bid_price",
                               estimated_price_col = "lot_estimatedprice",
                               cap = 5) {
  required <- c(contract_price_col, estimated_price_col)
  missing <- setdiff(required, names(df))
  if (length(missing) > 0) stop("Missing required columns: ", paste(missing, collapse = ", "))
  
  df %>%
    dplyr::mutate(
      relative_price = .data[[contract_price_col]] / .data[[estimated_price_col]],
      relative_price = dplyr::if_else(relative_price > cap | relative_price <= 0, NA_real_, relative_price)
    )
}

# ── shared theme for all relative-price plots ────────────────────────────
.rel_theme <- function(base_size = 11) {
  ggplot2::theme_minimal(base_size = base_size) +
    ggplot2::theme(
      plot.title = ggplot2::element_blank(),
      axis.title = ggplot2::element_text(size = 11, colour = "#333333"),
      axis.text        = ggplot2::element_text(size = base_size, colour = "#444444"),
      panel.grid.major = ggplot2::element_line(colour = "#f0f0f0", linewidth = 0.4),
      panel.grid.minor = ggplot2::element_blank(),
      legend.text      = ggplot2::element_text(size = base_size),
      legend.title = ggplot2::element_text(size = 10, face = "bold"),
      plot.margin      = ggplot2::margin(6, 10, 6, 6)
    )
}

# colour helpers: green = under budget, amber = slight over, red = over
.rp_fill <- function(x) {
  dplyr::case_when(
    x <  1.0 ~ "#2196a6",   # teal-blue  – under budget
    x <= 1.2 ~ "#f59e0b",   # amber      – slightly over
    TRUE      ~ "#e03e3e"    # red        – clearly over
  )
}

# ── Plot 1: Bicolour density with zone annotations ───────────────────────
plot_relative_price_density <- function(df) {
  d <- df[!is.na(df$relative_price), "relative_price", drop = FALSE]
  if (nrow(d) < 5) return(ggplot2::ggplot() + ggplot2::labs())
  
  pct_under    <- mean(d$relative_price <  1,    na.rm = TRUE)
  pct_over     <- mean(d$relative_price >  1,    na.rm = TRUE)
  # "at budget" = within ±1% of the estimate (floating-point-safe)
  pct_at       <- mean(d$relative_price >= 0.99 & d$relative_price <= 1.01, na.rm = TRUE)
  med_val      <- stats::median(d$relative_price, na.rm = TRUE)
  n_total      <- nrow(d)
  
  x_max   <- min(stats::quantile(d$relative_price, 0.995, na.rm = TRUE) * 1.05, 5)
  dens    <- stats::density(d$relative_price, from = 0, to = x_max + 0.1, n = 512)
  dens_df <- data.frame(x = dens$x, y = dens$y)
  
  # ECDF for informative hover on the density curve
  ecdf_fn <- stats::ecdf(d$relative_price)
  dens_df$pct_below <- ecdf_fn(dens_df$x)
  dens_df$hover_text <- paste0(
    "Relative price: ", round(dens_df$x, 3), "<br>",
    scales::percent(dens_df$pct_below, accuracy = 0.1), " of contracts priced at or below this value"
  )
  
  under_df <- dens_df[dens_df$x <= 1, ]
  over_df  <- dens_df[dens_df$x >= 1, ]
  under_df <- rbind(under_df[, c("x","y")], data.frame(x = rev(under_df$x), y = 0))
  over_df  <- rbind(over_df[,  c("x","y")], data.frame(x = rev(over_df$x),  y = 0))
  
  y_top <- max(dens$y, na.rm = TRUE)
  
  x_lbl_under <- 0.25
  x_lbl_over  <- pmin(1.9, x_max * 0.82)
  
  # x position for "at budget" label — on the dashed line itself, above the other two labels
  # median caption — to the right of the amber line, clear of the dashed line
  median_caption_x <- med_val + 0.03
  
  ggplot2::ggplot(dens_df, ggplot2::aes(x, y)) +
    ggplot2::geom_polygon(data = under_df, ggplot2::aes(x, y),
                          fill = "#2196a6", alpha = 0.22, colour = NA) +
    ggplot2::geom_polygon(data = over_df,  ggplot2::aes(x, y),
                          fill = "#e03e3e", alpha = 0.18, colour = NA) +
    # density line — carries hover text for plotly
    ggplot2::geom_line(ggplot2::aes(text = hover_text),
                       linewidth = 0.9, colour = "#333333") +
    # Budget = 1.0 line (dashed, grey)
    ggplot2::geom_vline(xintercept = 1,       linetype = "dashed", linewidth = 0.8, colour = "#555555") +
    # Median line (solid amber)
    ggplot2::geom_vline(xintercept = med_val, linetype = "solid",  linewidth = 0.7, colour = "#f59e0b") +
    # Under budget label — left zone, same style as over budget
    ggplot2::annotate("text", x = x_lbl_under, y = y_top * 0.80,
                      label = paste0(scales::percent(pct_under, accuracy = 0.1), "\nunder budget"),
                      hjust = 0.5, size = 3.5, colour = "#2196a6", fontface = "bold") +
    # At budget label — centred on the dashed line, same font/size
    ggplot2::annotate("text", x = 1.2, y = y_top * 0.80,
                      label = paste0(scales::percent(pct_at, accuracy = 0.1), "\nat budget"),
                      hjust = 0.5, size = 3.5, colour = "#888888", fontface = "bold") +
    # Over budget label — right zone
    ggplot2::annotate("text", x = x_lbl_over, y = y_top * 0.80,
                      label = paste0(scales::percent(pct_over, accuracy = 0.1), "\nover budget"),
                      hjust = 0.5, size = 3.5, colour = "#e03e3e", fontface = "bold") +
    # Median caption — to the right of the amber line, never on the line
    ggplot2::annotate("text", x = median_caption_x * 0.8, y = y_top * 0.30,
                      label = paste0("Median = ", round(med_val, 2)),
                      hjust = 0, vjust = 0.5, size = 3.0, colour = "#b07800",
                      fontface = "italic") +
    ggplot2::labs(
      subtitle = paste0(
        "N = ", scales::comma(n_total), " contracts  |  ",
        "Dashed = Budget (1.0)  |  Amber = Median (", round(med_val, 2), ")  |  ",
        "At-budget: rel. price within \u00b11% of 1.0  |  Over budget: rel. price > 1.0"
      ),
      x = "Relative price  (contract price \u00f7 estimated price)",
      y = "Density"
    ) +
    ggplot2::coord_cartesian(xlim = c(0, x_max), clip = "off") +
    .rel_theme() +
    ggplot2::theme(plot.subtitle = ggplot2::element_text(size = 10))
}

# ── Plot 2: % over budget by year — line + CI ribbon ──────────────────────
plot_relative_price_by_year <- function(df) {
  d <- df %>% dplyr::filter(!is.na(relative_price), !is.na(tender_year))
  if (nrow(d) < 5) return(ggplot2::ggplot() + ggplot2::labs())
  
  overall_pct <- mean(d$relative_price > 1, na.rm = TRUE)
  
  yr_stats <- d %>%
    dplyr::group_by(tender_year) %>%
    dplyr::summarise(
      pct_over = mean(relative_price > 1, na.rm = TRUE),
      n        = dplyr::n(),
      .groups  = "drop"
    ) %>%
    dplyr::mutate(
      # 95% CI on a proportion
      se       = sqrt(pct_over * (1 - pct_over) / n),
      ci_lo    = pmax(pct_over - 1.96 * se, 0),
      ci_hi    = pmin(pct_over + 1.96 * se, 1),
      dot_col  = dplyr::if_else(pct_over >= overall_pct, "#e03e3e", "#2196a6"),
      hover_text = paste0(
        "Year: ",          tender_year, "<br>",
        "% over budget: ", scales::percent(pct_over, accuracy = 0.1), "<br>",
        "95% CI: [",       scales::percent(ci_lo, accuracy = 0.1), " \u2013 ",
        scales::percent(ci_hi, accuracy = 0.1), "]<br>",
        "N contracts: ",   scales::comma(n)
      )
    )
  
  ggplot2::ggplot(yr_stats, ggplot2::aes(x = tender_year)) +
    # 95% CI ribbon
    ggplot2::geom_ribbon(ggplot2::aes(ymin = ci_lo, ymax = ci_hi),
                         fill = "#aaaaaa", alpha = 0.25) +
    # overall average reference line (same number as in density chart)
    # overall avg dashed — invisible points along it carry hover text
    ggplot2::geom_hline(yintercept = overall_pct, linetype = "dashed",
                        linewidth = 0.7, colour = "#555555") +
    ggplot2::geom_point(data = yr_stats %>% dplyr::mutate(.hy = overall_pct),
                        ggplot2::aes(y = .hy,
                                     text = paste0("Overall avg: ",
                                                   scales::percent(overall_pct, accuracy = 0.1))),
                        size = 0, alpha = 0) +
    # line + dots
    ggplot2::geom_line(ggplot2::aes(y = pct_over),
                       linewidth = 1.0, colour = "#333333") +
    ggplot2::geom_point(ggplot2::aes(y = pct_over, colour = dot_col, text = hover_text),
                        size = 3.5, shape = 16) +
    ggplot2::scale_colour_identity() +
    ggplot2::scale_x_continuous(breaks = scales::pretty_breaks(n = 6)) +
    ggplot2::scale_y_continuous(
      labels = scales::percent_format(accuracy = 1),
      limits = c(0, NA),
      expand = ggplot2::expansion(mult = c(0, 0.10))
    ) +
    ggplot2::labs(
      x        = NULL,
      y        = "% contracts over budget"
    ) +
    .rel_theme()
}

# ── Plot 3: % over budget by CPV market — dot + 95% CI whisker ───────────
top_markets_by_relative_price <- function(df, n = 10) {
  # Use cpv_cluster as fallback label if cpv_category is absent/all-NA
  lbl_col <- if ("cpv_category" %in% names(df) && !all(is.na(df$cpv_category))) "cpv_category" else "cpv_cluster"
  if (!lbl_col %in% names(df)) return(character(0))
  df %>%
    dplyr::filter(!is.na(.data[[lbl_col]]), !is.na(relative_price)) %>%
    dplyr::group_by(.data[[lbl_col]]) %>%
    dplyr::summarise(pct_over = mean(relative_price > 1, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(pct_over)) %>%
    dplyr::pull(1)
}

plot_top_markets_relative_price <- function(df, top_markets) {
  # Determine which label column to use
  lbl_col <- if ("cpv_category" %in% names(df) && !all(is.na(df$cpv_category))) "cpv_category" else "cpv_cluster"
  if (!lbl_col %in% names(df)) {
    return(ggplot2::ggplot() + ggplot2::labs())
  }
  # Standardise column name
  df <- df %>% dplyr::mutate(.lbl = .data[[lbl_col]])
  
  overall_pct <- mean(df$relative_price > 1, na.rm = TRUE)
  
  d <- df %>%
    dplyr::filter(!is.na(.lbl), !is.na(relative_price)) %>%
    dplyr::group_by(.lbl) %>%
    dplyr::summarise(
      pct_over = mean(relative_price > 1, na.rm = TRUE),
      med      = stats::median(relative_price, na.rm = TRUE),
      n        = dplyr::n(),
      .groups  = "drop"
    ) %>%
    dplyr::mutate(
      .lbl     = factor(.lbl, levels = rev(top_markets)),
      .lbl_short = factor(stringr::str_trunc(as.character(.lbl), 32, side = "right", ellipsis = "\u2026"),
                          levels = stringr::str_trunc(rev(top_markets), 32, side = "right", ellipsis = "\u2026")),
      se       = sqrt(pct_over * (1 - pct_over) / n),
      ci_lo    = pmax(pct_over - 1.96 * se, 0),
      ci_hi    = pmin(pct_over + 1.96 * se, 1),
      dot_col  = dplyr::case_when(
        pct_over >= overall_pct + 0.10 ~ "#e03e3e",
        pct_over >= overall_pct        ~ "#f59e0b",
        TRUE                           ~ "#2196a6"
      ),
      hover_text = paste0(
        "Market: ",            .lbl, "<br>",
        "% over budget: ",     scales::percent(pct_over, accuracy = 0.1), "<br>",
        "95% CI: [",           scales::percent(ci_lo, accuracy = 0.1), " \u2013 ",
        scales::percent(ci_hi, accuracy = 0.1), "]<br>",
        "Median rel. price: ", round(med, 3), "<br>",
        "N contracts: ",       scales::comma(n)
      )
    )
  
  n_mkts <- nrow(d)
  # plotly doesn't render y=Inf annotations — use the actual top factor level
  y_top_lbl <- levels(d$.lbl_short)[length(levels(d$.lbl_short))]
  
  ggplot2::ggplot(d, ggplot2::aes(y = .lbl_short)) +
    # overall average reference line
    # overall avg dashed — hover-only
    ggplot2::geom_vline(xintercept = overall_pct, linetype = "dashed",
                        linewidth = 0.6, colour = "#555555") +
    ggplot2::geom_point(data = d %>% dplyr::mutate(.vx = overall_pct),
                        ggplot2::aes(x = .vx, y = .lbl_short,
                                     text = paste0("Overall avg: ",
                                                   scales::percent(overall_pct, accuracy = 0.1))),
                        size = 0, alpha = 0) +
    # 95% CI whisker
    ggplot2::geom_errorbarh(ggplot2::aes(xmin = ci_lo, xmax = ci_hi),
                            height = 0.3, linewidth = 0.7, colour = "#bbbbbb") +
    # % over budget dot
    ggplot2::geom_point(ggplot2::aes(x = pct_over, colour = dot_col,
                                     size = log1p(n), text = hover_text),
                        shape = 16, alpha = 0.9) +
    # contract count label
    ggplot2::geom_text(ggplot2::aes(x = ci_hi + 0.005, label = scales::comma(n)),
                       hjust = 0, size = 2.8, colour = "#888888") +
    ggplot2::scale_colour_identity() +
    ggplot2::scale_size_continuous(range = c(2, 4.5), guide = "none") +
    ggplot2::scale_y_discrete(expand = ggplot2::expansion(add = c(0.8, 1.2))) +
    ggplot2::scale_x_continuous(
      labels = scales::percent_format(accuracy = 1),
      expand = ggplot2::expansion(mult = c(0.01, 0.14))
    ) +
    ggplot2::labs(
      x        = "% contracts over budget",
      y        = NULL
    ) +
    .rel_theme(base_size = 11) +
    ggplot2::theme(
      axis.text.y     = ggplot2::element_text(size = 9),
      legend.position = "none"
    )
}

# ── Plot 4 helpers + lollipop chart ──────────────────────────────────────
top_buyers_by_relative_price <- function(df,
                                         buyer_name_col = "buyer_name",
                                         min_contracts  = 10,
                                         n              = 20) {
  # Detect the buyer name column — accept common alternatives
  col_candidates <- c(buyer_name_col, "buyer_name", "buyer_id", "buyer_masterid", "buyername")
  buyer_col <- col_candidates[col_candidates %in% names(df)][1]
  if (is.na(buyer_col)) return(dplyr::tibble())   # no buyer column → empty
  
  # Detect a contract value column (prefer USD)
  value_col <- intersect(c("bid_priceusd", "bid_price", "lot_estimatedpriceusd", "lot_estimatedprice"),
                         names(df))[1]
  has_value <- length(value_col) > 0 && !is.na(value_col)
  
  df %>%
    dplyr::group_by(.data[[buyer_col]]) %>%
    dplyr::summarise(
      pct_over_budget      = mean(relative_price > 1, na.rm = TRUE),
      mean_relative_price  = mean(relative_price, na.rm = TRUE),
      n_contracts          = dplyr::n(),
      total_contract_value = if (has_value) sum(.data[[value_col]], na.rm = TRUE) else NA_real_,
      .groups              = "drop"
    ) %>%
    dplyr::filter(n_contracts >= min_contracts, !is.na(pct_over_budget)) %>%
    dplyr::arrange(dplyr::desc(pct_over_budget)) %>%
    dplyr::slice_head(n = n) %>%
    dplyr::rename(buyer_name = 1)
}

plot_top_buyers_relative_price <- function(top_buyers_df, label_max_chars = 30) {
  if (is.null(top_buyers_df) || nrow(top_buyers_df) == 0)
    return(ggplot2::ggplot() + ggplot2::labs())
  
  has_value <- "total_contract_value" %in% names(top_buyers_df) &&
    !all(is.na(top_buyers_df$total_contract_value))
  
  dfp <- top_buyers_df %>%
    dplyr::arrange(mean_relative_price) %>%
    dplyr::mutate(
      buyer_name_short = stringr::str_trunc(buyer_name, width = label_max_chars,
                                            side = "right", ellipsis = "\u2026"),
      buyer_name_short = make.unique(buyer_name_short),
      buyer_name_short = factor(buyer_name_short, levels = buyer_name_short),
      dot_col  = .rp_fill(mean_relative_price),
      dot_size = scales::rescale(n_contracts, to = c(3, 9)),
      hover_text = paste0(
        "Buyer: ",               buyer_name, "<br>",
        "Mean relative price: ", round(mean_relative_price, 3), "<br>",
        "% over budget: ",       scales::percent(pct_over_budget, accuracy = 1), "<br>",
        "N contracts: ",         scales::comma(n_contracts),
        if (has_value)
          paste0("<br>Total contract value: $",
                 scales::label_number(scale_cut = scales::cut_short_scale(),
                                      accuracy  = 0.1)(total_contract_value))
        else ""
      )
    )
  
  max_x     <- max(dfp$mean_relative_price, na.rm = TRUE)
  # plotly doesn't render y = Inf on factor axes — use top factor level (mirrors market chart)
  y_top_lbl <- levels(dfp$buyer_name_short)[nlevels(dfp$buyer_name_short)]
  
  ggplot2::ggplot(dfp, ggplot2::aes(y = buyer_name_short)) +
    # reference line at budget = 1.0
    # Budget = 1.0 dashed — hover-only
    ggplot2::geom_vline(xintercept = 1, linetype = "dashed",
                        linewidth = 0.7, colour = "#888888") +
    ggplot2::geom_point(data = dfp %>% dplyr::mutate(.bx = 1),
                        ggplot2::aes(x = .bx, y = buyer_name_short,
                                     text = "Budget = 1.0"),
                        size = 0, alpha = 0) +
    # lollipop stick — from 1.0 (budget) to mean relative price
    ggplot2::geom_segment(ggplot2::aes(x = 1, xend = mean_relative_price,
                                       yend = buyer_name_short),
                          linewidth = 0.6, colour = "#dddddd") +
    # lollipop head — size ∝ contract count, colour = price level
    ggplot2::geom_point(ggplot2::aes(x = mean_relative_price,
                                     colour = dot_col,
                                     size   = dot_size,
                                     text   = hover_text),
                        shape = 16, alpha = 0.9) +
    # value label at the head
    ggplot2::geom_text(ggplot2::aes(x = mean_relative_price + max_x * 0.02,
                                    label = round(mean_relative_price, 2)),
                       hjust = 0, size = 2.9, colour = "#444444") +
    ggplot2::scale_colour_identity() +
    ggplot2::scale_size_identity() +
    ggplot2::scale_x_continuous(
      labels = scales::label_number(accuracy = 0.01),
      expand = ggplot2::expansion(mult = c(0.06, 0.20))
    ) +
    ggplot2::labs(
      x        = "Mean relative price  (contract \u00f7 estimate)",
      y        = NULL
    ) +
    .rel_theme() +
    ggplot2::theme(
      axis.text.y     = ggplot2::element_text(size = 9),
      legend.position = "none"
    )
}


# ------------------------------------------------------------------------
# 8) Competition diagnostics (single-bid shares)
# ------------------------------------------------------------------------

# ── Shared theme (mirrors .rel_theme) ────────────────────────────────────
.comp_theme <- function(base_size = 13) {
  ggplot2::theme_minimal(base_size = base_size) +
    ggplot2::theme(
      plot.title       = ggplot2::element_blank(),
      plot.subtitle    = ggplot2::element_text(size = base_size - 2, colour = "#1a1a2e"),
      axis.title       = ggplot2::element_text(size = 11, colour = "#333333"),
      axis.text        = ggplot2::element_text(size = base_size - 2, colour = "#444444"),
      panel.grid.major = ggplot2::element_line(colour = "#eeeeee"),
      panel.grid.minor = ggplot2::element_blank(),
      legend.position  = "none",
      plot.margin      = ggplot2::margin(12, 16, 12, 12)
    )
}

# ── Colour helper: shade single-bid % relative to overall average ─────────
.sb_col <- function(x, overall) {
  dplyr::case_when(
    x >= overall + 0.10 ~ "#e03e3e",   # red   — 10pp+ above average
    x >= overall        ~ "#f59e0b",   # amber — above average
    TRUE                ~ "#2196a6"    # teal  — below average
  )
}

# ── Shared lollipop builder (procedure, buyer group, CPV market) ──────────
.comp_lollipop <- function(dat,
                           y_var,
                           x_var      = "share_single_bid",
                           n_var      = "n",
                           overall    = NULL,
                           x_lab      = "Single-bid rate",
                           y_lab      = NULL,
                           label_max  = 35) {
  
  overall <- overall %||% mean(dat[[x_var]], na.rm = TRUE)
  
  dat <- dat %>%
    dplyr::mutate(
      .y_fct   = factor(.data[[y_var]],
                        levels = .data[[y_var]][order(.data[[x_var]])]),
      .col     = .sb_col(.data[[x_var]], overall),
      .n       = .data[[n_var]],
      .x       = .data[[x_var]],
      hover_text = paste0(
        .data[[y_var]], "<br>",
        "Single-bid rate: ", scales::percent(.data[[x_var]], accuracy = 0.1), "<br>",
        "N contracts: ", scales::comma(.data[[n_var]])
      )
    )
  
  if (nrow(dat) == 0)
    return(ggplot2::ggplot() + ggplot2::labs(subtitle = "No data available"))
  
  max_x <- max(dat$.x, na.rm = TRUE)
  # plotly requires a factor level string on a discrete axis — Inf / integer indices don't work
  y_top_lbl <- levels(dat$.y_fct)[nlevels(dat$.y_fct)]
  
  ggplot2::ggplot(dat, ggplot2::aes(y = .y_fct)) +
    # overall average reference line
    ggplot2::geom_vline(xintercept = overall, linetype = "dashed",
                        linewidth = 0.6, colour = "#555555") +
    # overall avg — hover-only
    ggplot2::geom_point(data = dat %>% dplyr::mutate(.vx = overall),
                        ggplot2::aes(x = .vx, y = .y_fct,
                                     text = paste0("Overall avg: ",
                                                   scales::percent(overall, accuracy = 0.1))),
                        size = 0, alpha = 0) +
    # stick from 0 to rate
    ggplot2::geom_segment(ggplot2::aes(x = 0, xend = .x, yend = .y_fct),
                          linewidth = 0.5, colour = "#dddddd") +
    # dot
    ggplot2::geom_point(ggplot2::aes(x = .x, colour = .col,
                                     size = log1p(.n), text = hover_text),
                        shape = 16, alpha = 0.9) +
    # value label
    ggplot2::geom_text(ggplot2::aes(x = .x + max_x * 0.02,
                                    label = scales::percent(.x, accuracy = 1)),
                       hjust = 0, size = 2.9, colour = "#444444") +
    ggplot2::scale_colour_identity() +
    ggplot2::scale_size_continuous(range = c(3, 7), guide = "none") +
    ggplot2::scale_x_continuous(
      labels = scales::percent_format(accuracy = 1),
      limits = c(0, NA),
      expand = ggplot2::expansion(mult = c(0, 0.18))
    ) +
    ggplot2::labs(x = x_lab, y = y_lab) +
    .comp_theme()
}

# ── Plot 1: Overall trend by year ────────────────────────────────────────
plot_single_bid_overall <- function(df) {
  d <- df %>% dplyr::filter(!is.na(single_bid), !is.na(tender_year))
  if (nrow(d) < 5) return(ggplot2::ggplot() + ggplot2::labs())
  
  overall <- mean(d$single_bid, na.rm = TRUE)
  n_total <- nrow(d)
  
  yr <- d %>%
    dplyr::group_by(tender_year) %>%
    dplyr::summarise(
      rate = mean(single_bid, na.rm = TRUE),
      n    = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      se    = sqrt(rate * (1 - rate) / n),
      ci_lo = pmax(rate - 1.96 * se, 0),
      ci_hi = pmin(rate + 1.96 * se, 1),
      dot_col = dplyr::if_else(rate >= overall, "#e03e3e", "#2196a6"),
      hover_text = paste0(
        "Year: ", tender_year, "<br>",
        "Single-bid rate: ", scales::percent(rate, accuracy = 0.1), "<br>",
        "95% CI: [", scales::percent(ci_lo, accuracy = 0.1),
        " \u2013 ", scales::percent(ci_hi, accuracy = 0.1), "]<br>",
        "N contracts: ", scales::comma(n)
      )
    )
  
  ggplot2::ggplot(yr, ggplot2::aes(x = tender_year)) +
    ggplot2::geom_ribbon(ggplot2::aes(ymin = ci_lo, ymax = ci_hi),
                         fill = "#aaaaaa", alpha = 0.20) +
    # overall avg dashed — hover-only
    ggplot2::geom_hline(yintercept = overall, linetype = "dashed",
                        linewidth = 0.7, colour = "#555555") +
    ggplot2::geom_point(data = yr %>% dplyr::mutate(.hy = overall),
                        ggplot2::aes(y = .hy,
                                     text = paste0("Overall avg: ",
                                                   scales::percent(overall, accuracy = 0.1))),
                        size = 0, alpha = 0) +
    ggplot2::geom_line(ggplot2::aes(y = rate),
                       linewidth = 1.0, colour = "#333333") +
    ggplot2::geom_point(ggplot2::aes(y = rate, colour = dot_col, text = hover_text),
                        size = 3.5, shape = 16) +
    ggplot2::scale_colour_identity() +
    ggplot2::scale_x_continuous(breaks = scales::pretty_breaks(n = 6)) +
    ggplot2::scale_y_continuous(
      labels = scales::percent_format(accuracy = 1),
      limits = c(0, NA),
      expand = ggplot2::expansion(mult = c(0, 0.10))
    ) +
    ggplot2::labs(
      subtitle = paste0(
        "N = ", scales::comma(n_total), " contracts  |  ",
        "Overall avg: ", scales::percent(overall, accuracy = 0.1), "  |  ",
        "Red dots = years above average  |  Shaded band = 95% CI"
      ),
      x = NULL,
      y = "Single-bid rate"
    ) +
    .comp_theme()
}

# ── Plot 2: By procedure type — bubble row chart ─────────────────────────
# Large filled circles, % rate printed inside in white bold, CI whiskers,
# sorted ascending, overall avg dashed line.
plot_single_bid_by_procedure <- function(df, proc_col = "tender_proceduretype") {
  if (!proc_col %in% names(df)) stop("Missing required column: ", proc_col)
  
  overall <- mean(df$single_bid, na.rm = TRUE)
  
  dat <- df %>%
    dplyr::mutate(procedure_type = recode_procedure_type(.data[[proc_col]])) %>%
    dplyr::filter(!is.na(single_bid), !is.na(procedure_type)) %>%
    dplyr::group_by(procedure_type) %>%
    dplyr::summarise(
      share_single_bid = mean(single_bid, na.rm = TRUE),
      n                = dplyr::n(),
      .groups          = "drop"
    ) %>%
    dplyr::mutate(
      se         = sqrt(share_single_bid * (1 - share_single_bid) / n),
      ci_lo      = pmax(share_single_bid - 1.96 * se, 0),
      ci_hi      = pmin(share_single_bid + 1.96 * se, 1),
      bubble_col = .sb_col(share_single_bid, overall),
      proc_fct   = factor(procedure_type, levels = procedure_type[order(share_single_bid)]),
      pct_label  = scales::percent(share_single_bid, accuracy = 1),
      hover_text = paste0(
        "Procedure: ",       procedure_type, "<br>",
        "Single-bid rate: ", scales::percent(share_single_bid, accuracy = 0.1), "<br>",
        "95% CI: [",         scales::percent(ci_lo, accuracy = 0.1), " \u2013 ",
        scales::percent(ci_hi, accuracy = 0.1), "]<br>",
        "N contracts: ",     scales::comma(n)
      )
    )
  
  if (nrow(dat) == 0)
    return(ggplot2::ggplot() + ggplot2::labs(subtitle = "No data available"))
  
  y_top_lbl <- levels(dat$proc_fct)[nlevels(dat$proc_fct)]
  
  ggplot2::ggplot(dat, ggplot2::aes(y = proc_fct)) +
    # overall avg reference line
    ggplot2::geom_vline(xintercept = overall, linetype = "dashed",
                        linewidth = 0.7, colour = "#aaaaaa") +
    # overall avg — hover-only
    ggplot2::geom_point(data = dat %>% dplyr::mutate(.vx = overall),
                        ggplot2::aes(x = .vx, y = proc_fct,
                                     text = paste0("Overall avg: ",
                                                   scales::percent(overall, accuracy = 0.1))),
                        size = 0, alpha = 0) +
    # 95% CI whisker — drawn first so bubble sits on top
    ggplot2::geom_errorbarh(ggplot2::aes(xmin = ci_lo, xmax = ci_hi),
                            height = 0, linewidth = 5,
                            colour = "#dddddd", alpha = 0.7) +
    # thin whisker cap line
    ggplot2::geom_errorbarh(ggplot2::aes(xmin = ci_lo, xmax = ci_hi),
                            height = 0.28, linewidth = 0.5, colour = "#aaaaaa") +
    # large filled bubble — carries hover text
    ggplot2::geom_point(ggplot2::aes(x = share_single_bid, fill = bubble_col,
                                     text = hover_text),
                        shape = 21, size = 11,
                        colour = "white", stroke = 0.5, alpha = 0.92) +
    # % label inside the bubble
    ggplot2::geom_text(ggplot2::aes(x = share_single_bid, label = pct_label),
                       size = 2.8, fontface = "bold", colour = "white") +
    ggplot2::scale_fill_identity() +
    ggplot2::scale_x_continuous(
      labels = scales::percent_format(accuracy = 1),
      expand = ggplot2::expansion(mult = c(0.02, 0.18))
    ) +
    ggplot2::labs(
      x        = "Single-bid rate",
      y        = NULL
    ) +
    .comp_theme() +
    ggplot2::theme(
      panel.grid.major.y = ggplot2::element_blank(),
      axis.text.y        = ggplot2::element_text(size = 10, face = "plain", colour = "#333333")
    )
}

# ── Plot 3: By contract value — gradient bar chart with trend line ─────────
plot_single_bid_by_price <- function(df) {
  overall <- mean(df$single_bid, na.rm = TRUE)
  
  dat <- df %>%
    dplyr::filter(!is.na(price_bin), !is.na(single_bid)) %>%
    dplyr::group_by(price_bin) %>%
    dplyr::summarise(
      share_single_bid = mean(single_bid, na.rm = TRUE),
      n                = dplyr::n(),
      .groups          = "drop"
    ) %>%
    dplyr::mutate(
      se         = sqrt(share_single_bid * (1 - share_single_bid) / n),
      ci_lo      = pmax(share_single_bid - 1.96 * se, 0),
      ci_hi      = pmin(share_single_bid + 1.96 * se, 1),
      bar_col    = .sb_col(share_single_bid, overall),
      hover_text = paste0(
        "Value band: ",      price_bin, "<br>",
        "Single-bid rate: ", scales::percent(share_single_bid, accuracy = 0.1), "<br>",
        "95% CI: [",         scales::percent(ci_lo, accuracy = 0.1), " \u2013 ",
        scales::percent(ci_hi, accuracy = 0.1), "]<br>",
        "N contracts: ",     scales::comma(n)
      )
    )
  
  if (nrow(dat) == 0)
    return(ggplot2::ggplot() + ggplot2::labs(subtitle = "No price data available"))
  
  x_last_bin <- as.character(dat$price_bin[nrow(dat)])
  
  ggplot2::ggplot(dat, ggplot2::aes(x = price_bin)) +
    # filled bars coloured by position relative to average
    ggplot2::geom_col(ggplot2::aes(y = share_single_bid, fill = bar_col,
                                   text = hover_text),
                      width = 0.72, alpha = 0.85) +
    # overall avg reference line
    # overall avg dashed — hover-only
    ggplot2::geom_hline(yintercept = overall, linetype = "dashed",
                        linewidth = 0.7, colour = "#555555") +
    ggplot2::geom_point(data = dat %>% dplyr::mutate(.hy = overall),
                        ggplot2::aes(x = price_bin, y = .hy,
                                     text = paste0("Overall avg: ",
                                                   scales::percent(overall, accuracy = 0.1))),
                        size = 0, alpha = 0) +
    # value label above each bar — anchored at ci_hi so it always clears the bar top
    ggplot2::geom_text(ggplot2::aes(y = ci_hi,
                                    label = scales::percent(share_single_bid, accuracy = 1)),
                       vjust = -0.6, size = 2.9, colour = "#444444") +
    ggplot2::scale_fill_identity() +
    ggplot2::scale_y_continuous(
      labels = scales::percent_format(accuracy = 1),
      limits = c(0, NA),
      expand = ggplot2::expansion(mult = c(0, 0.16))
    ) +
    ggplot2::labs(
      x        = "Contract value (USD)",
      y        = "Single-bid rate"
    ) +
    .comp_theme() +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 35, hjust = 1))
}

# ── Plot 4: By buyer group ────────────────────────────────────────────────
plot_single_bid_by_buyer_group <- function(df, buyer_type_col = "buyer_buyertype") {
  if (!buyer_type_col %in% names(df)) stop("Missing required column: ", buyer_type_col)
  
  overall <- mean(df$single_bid, na.rm = TRUE)
  
  dat <- df %>%
    dplyr::filter(!is.na(single_bid), !is.na(.data[[buyer_type_col]])) %>%
    dplyr::mutate(buyer_group = as.character(add_buyer_group(.data[[buyer_type_col]]))) %>%
    dplyr::group_by(buyer_group) %>%
    dplyr::summarise(
      share_single_bid = mean(single_bid, na.rm = TRUE),
      n                = dplyr::n(),
      .groups          = "drop"
    )
  
  .comp_lollipop(
    dat,
    y_var    = "buyer_group",
    overall  = overall,
    x_lab    = "Single-bid rate"
  )
}

# ── Plot 5: By CPV market ─────────────────────────────────────────────────
top_markets_by_single_bid <- function(df, top_n = 5) {
  lbl_col <- if ("cpv_category" %in% names(df) && !all(is.na(df$cpv_category)))
    "cpv_category" else "cpv_cluster"
  if (!lbl_col %in% names(df)) return(character(0))
  df %>%
    dplyr::filter(!is.na(.data[[lbl_col]]), !is.na(single_bid)) %>%
    dplyr::group_by(.data[[lbl_col]]) %>%
    dplyr::summarise(share_single_bid = mean(single_bid, na.rm = TRUE),
                     n = dplyr::n(), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(share_single_bid)) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::pull(1)
}

plot_single_bid_by_market <- function(df, label_max_chars = 30) {
  lbl_col <- if ("cpv_category" %in% names(df) && !all(is.na(df$cpv_category)))
    "cpv_category" else "cpv_cluster"
  if (!lbl_col %in% names(df))
    return(ggplot2::ggplot() + ggplot2::labs())
  
  overall <- mean(df$single_bid, na.rm = TRUE)
  
  d <- df %>%
    dplyr::filter(!is.na(.data[[lbl_col]]), !is.na(single_bid)) %>%
    dplyr::group_by(.data[[lbl_col]]) %>%
    dplyr::summarise(
      pct_single   = mean(single_bid, na.rm = TRUE),
      n            = dplyr::n(),
      .groups      = "drop"
    ) %>%
    dplyr::arrange(pct_single) %>%
    dplyr::mutate(
      .lbl_short = stringr::str_trunc(.data[[lbl_col]], width = label_max_chars,
                                      side = "right", ellipsis = "\u2026"),
      .lbl_short = factor(.lbl_short, levels = unique(.lbl_short)),
      se         = sqrt(pct_single * (1 - pct_single) / n),
      ci_lo      = pmax(pct_single - 1.96 * se, 0),
      ci_hi      = pmin(pct_single + 1.96 * se, 1),
      dot_col    = dplyr::case_when(
        pct_single >= overall + 0.10 ~ "#e03e3e",
        pct_single >= overall        ~ "#f59e0b",
        TRUE                         ~ "#2196a6"
      ),
      hover_text = paste0(
        "Market: ", .data[[lbl_col]], "<br>",
        "Single-bid rate: ", scales::percent(pct_single, accuracy = 0.1), "<br>",
        "95% CI: [", scales::percent(ci_lo, accuracy = 0.1), " \u2013 ",
        scales::percent(ci_hi, accuracy = 0.1), "]<br>",
        "N contracts: ", scales::comma(n)
      )
    )
  
  if (nrow(d) == 0) return(ggplot2::ggplot() + ggplot2::labs())
  
  y_top_lbl <- levels(d$.lbl_short)[nlevels(d$.lbl_short)]
  
  ggplot2::ggplot(d, ggplot2::aes(y = .lbl_short)) +
    # overall avg dashed — hover-only
    ggplot2::geom_vline(xintercept = overall, linetype = "dashed",
                        linewidth = 0.6, colour = "#555555") +
    ggplot2::geom_point(data = d %>% dplyr::mutate(.vx = overall),
                        ggplot2::aes(x = .vx, y = .lbl_short,
                                     text = paste0("Overall avg: ",
                                                   scales::percent(overall, accuracy = 0.1))),
                        size = 0, alpha = 0) +
    # 95% CI whisker
    ggplot2::geom_errorbarh(ggplot2::aes(xmin = ci_lo, xmax = ci_hi),
                            height = 0.25, linewidth = 0.7, colour = "#bbbbbb") +
    # lollipop dot — size proportional to log(n)
    ggplot2::geom_point(ggplot2::aes(x = pct_single, colour = dot_col,
                                     size = log1p(n), text = hover_text),
                        shape = 16, alpha = 0.9) +
    # contract count label
    ggplot2::geom_text(ggplot2::aes(x = ci_hi + 0.005, label = scales::comma(n)),
                       hjust = 0, size = 2.8, colour = "#888888") +
    ggplot2::scale_colour_identity() +
    ggplot2::scale_size_continuous(range = c(2, 4.5), guide = "none") +
    ggplot2::scale_x_continuous(
      labels = scales::percent_format(accuracy = 1),
      expand = ggplot2::expansion(mult = c(0.01, 0.14))
    ) +
    ggplot2::labs(x = "Single-bid rate", y = NULL) +
    .rel_theme(base_size = 11) +
    ggplot2::theme(
      axis.text.y     = ggplot2::element_text(size = 9),
      legend.position = "none",
      plot.margin     = ggplot2::margin(20, 10, 6, 6)
    )
}

# kept for pipeline compatibility (used by top-market facet removed above)
plot_single_bid_market_procedure_price_top <- function(df,
                                                       top_n_markets = 5,
                                                       proc_col = "tender_proceduretype") {
  plot_single_bid_by_market(df)
}

# ── Plot 6: Top buyers by single-bid incidence ────────────────────────────
plot_top_buyers_single_bid <- function(df,
                                       buyer_id_col    = "buyer_masterid",
                                       buyer_name_col  = "buyer_name",
                                       buyer_type_col  = "buyer_buyertype",
                                       top_n           = 20,
                                       min_tenders     = 30,
                                       label_max_chars = 35) {
  if (!buyer_id_col %in% names(df)) stop("Missing required column: ", buyer_id_col)
  
  overall <- mean(df$single_bid, na.rm = TRUE)
  
  # Detect a contract value column (prefer USD)
  value_col <- intersect(
    c("bid_priceusd", "bid_price", "lot_estimatedpriceusd", "lot_estimatedprice"),
    names(df)
  )[1]
  has_value <- length(value_col) > 0 && !is.na(value_col)
  
  dat <- df %>%
    dplyr::filter(!is.na(single_bid), !is.na(.data[[buyer_id_col]])) %>%
    dplyr::group_by(.data[[buyer_id_col]]) %>%
    dplyr::summarise(
      share_single_bid    = mean(single_bid, na.rm = TRUE),
      n_contracts         = dplyr::n(),
      total_contract_value = if (has_value) sum(.data[[value_col]], na.rm = TRUE) else NA_real_,
      .groups             = "drop"
    ) %>%
    dplyr::filter(n_contracts >= min_tenders) %>%
    dplyr::arrange(dplyr::desc(share_single_bid)) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::rename(buyer_id = 1)
  
  # Attach buyer names
  if (!is.null(buyer_name_col) && buyer_name_col %in% names(df)) {
    nms <- df %>%
      dplyr::filter(!is.na(.data[[buyer_id_col]])) %>%
      dplyr::distinct(.data[[buyer_id_col]], .data[[buyer_name_col]]) %>%
      dplyr::rename(buyer_id = 1, buyer_name = 2) %>%
      dplyr::group_by(buyer_id) %>% dplyr::slice_head(n = 1) %>% dplyr::ungroup()
    dat <- dat %>% dplyr::left_join(nms, by = "buyer_id") %>%
      dplyr::mutate(buyer_full_name = buyer_name)
  } else {
    dat$buyer_name      <- as.character(dat$buyer_id)
    dat$buyer_full_name <- dat$buyer_name
  }
  
  if (nrow(dat) == 0)
    return(ggplot2::ggplot() + ggplot2::labs(
      subtitle = "Try lowering the minimum contracts threshold"))
  
  dat <- dat %>%
    dplyr::arrange(share_single_bid) %>%
    dplyr::mutate(
      # Axis label: just the truncated name, no brackets
      buyer_short  = stringr::str_trunc(buyer_name, label_max_chars, "right", "\u2026"),
      buyer_short  = make.unique(buyer_short),
      buyer_label  = factor(buyer_short, levels = buyer_short),
      dot_col      = .sb_col(share_single_bid, overall),
      hover_text   = paste0(
        "Buyer: ",             buyer_full_name, "<br>",
        "Single-bid rate: ",   scales::percent(share_single_bid, accuracy = 0.1), "<br>",
        "N contracts: ",       scales::comma(n_contracts),
        if (has_value && !is.na(total_contract_value[1]))
          paste0("<br>Total contract value: $",
                 scales::label_number(scale_cut = scales::cut_short_scale(),
                                      accuracy  = 0.1)(total_contract_value))
        else ""
      )
    )
  
  max_x     <- max(dat$share_single_bid, na.rm = TRUE)
  y_top_lbl <- levels(dat$buyer_label)[nlevels(dat$buyer_label)]
  
  ggplot2::ggplot(dat, ggplot2::aes(y = buyer_label)) +
    # overall avg dashed — hover-only
    ggplot2::geom_vline(xintercept = overall, linetype = "dashed",
                        linewidth = 0.6, colour = "#555555") +
    ggplot2::geom_point(data = dat %>% dplyr::mutate(.vx = overall),
                        ggplot2::aes(x = .vx, y = buyer_label,
                                     text = paste0("Overall avg: ",
                                                   scales::percent(overall, accuracy = 0.1))),
                        size = 0, alpha = 0) +
    ggplot2::geom_segment(ggplot2::aes(x = 0, xend = share_single_bid, yend = buyer_label),
                          linewidth = 0.5, colour = "#dddddd") +
    ggplot2::geom_point(ggplot2::aes(x = share_single_bid, colour = dot_col,
                                     size = log1p(n_contracts), text = hover_text),
                        shape = 16, alpha = 0.9) +
    ggplot2::geom_text(ggplot2::aes(x = share_single_bid + max_x * 0.02,
                                    label = scales::percent(share_single_bid, accuracy = 1)),
                       hjust = 0, size = 2.9, colour = "#444444") +
    ggplot2::scale_colour_identity() +
    ggplot2::scale_size_continuous(range = c(3, 7), guide = "none") +
    ggplot2::scale_x_continuous(
      labels = scales::percent_format(accuracy = 1),
      limits = c(0, NA),
      expand = ggplot2::expansion(mult = c(0, 0.18))
    ) +
    ggplot2::labs(
      x        = "Single-bid rate",
      y        = NULL
    ) +
    .comp_theme() +
    ggplot2::theme(axis.text.y = ggplot2::element_text(size = 9))
}

# ------------------------------------------------------------------------
# 9) Pipeline: run_economic_efficiency_pipeline
# ------------------------------------------------------------------------

run_economic_efficiency_pipeline <- function(df,
                                             country_code = "GEN",
                                             output_dir,
                                             cpv_lookup = NULL,
                                             cpv_digits = 2,
                                             save_outputs = TRUE,
                                             network_cpv_clusters = character(0),
                                             network_top_buyers = 15,
                                             network_ncol = 2) {
  message("Running economic efficiency pipeline for ", country_code, " ...")
  
  # Keep behavior stable across fread/data.table vs tibble
  df <- tibble::as_tibble(df)
  
  # ----------------------------------------------------------------------
  # A) Standard fields
  # ----------------------------------------------------------------------
  df <- df %>%
    add_tender_year() %>%
    add_cpv_cluster(digits = cpv_digits) %>%
    add_single_bid_flag() %>%
    add_price_bins_usd()
  
  # Attach CPV category labels if lookup provided
  if (!is.null(cpv_lookup) && is.list(cpv_lookup) && "cpv_2d" %in% names(cpv_lookup)) {
    df <- attach_cpv_labels(df, cpv_lookup_2d = cpv_lookup$cpv_2d)
    
  } else {
    if (!"cpv_category" %in% names(df)) df$cpv_category <- NA_character_
  }
  
  # ----------------------------------------------------------------------
  # SUMMARY STATS BLOCK (returned, not printed)
  # ----------------------------------------------------------------------
  n_obs_per_year <- df %>% dplyr::count(tender_year, name = "n_observations")
  
  n_unique_buyers <- if ("buyer_masterid" %in% names(df)) {
    dplyr::n_distinct(df$buyer_masterid, na.rm = TRUE)
  } else NA_integer_
  
  n_unique_bidders <- if ("bidder_masterid" %in% names(df)) {
    dplyr::n_distinct(df$bidder_masterid, na.rm = TRUE)
  } else NA_integer_
  
  tender_year_tenders <- if ("tender_id" %in% names(df)) {
    df %>%
      dplyr::group_by(tender_year) %>%
      dplyr::summarise(n_unique_tender_id = dplyr::n_distinct(tender_id), .groups = "drop")
  } else NULL
  
  vars_present <- names(df)
  vars_present <- vars_present[!startsWith(vars_present, "ind_")]
  
  summary_stats <- list(
    n_obs_per_year      = n_obs_per_year,
    n_unique_buyers     = n_unique_buyers,
    tender_year_tenders = tender_year_tenders,
    n_unique_bidders    = n_unique_bidders,
    vars_present        = vars_present
  )
  
  # ----------------------------------------------------------------------
  # B) Market sizing
  # ----------------------------------------------------------------------
  # Determine which price variable to use (priority order)
  .PRICE_COLS <- c("bid_priceusd","lot_estimatedpriceusd","tender_finalprice","lot_estimatedprice","bid_price")
  price_var   <- .PRICE_COLS[.PRICE_COLS %in% names(df)][1L]
  if (is.na(price_var)) price_var <- NULL
  
  if (!is.null(price_var))
    message("Using price variable: ", price_var)
  else
    message("No price variable found. Market sizing will show contract counts only.")
  
  market_summary <- summarise_market_size(df, value_col = price_var)
  
  # CPV cluster -> category legend (for the Rmd)
  cpv_cluster_legend <- NULL
  if (!is.null(market_summary) && all(c("cpv_cluster","cpv_category") %in% names(market_summary))) {
    cpv_cluster_legend <- make_cpv_cluster_legend(market_summary)
  }
  
  market_size_n  <- plot_market_contract_counts(market_summary)
  market_size_v  <- plot_market_total_value(market_summary)
  market_size_av <- plot_market_bubble(market_summary)
  
  
  # ----------------------------------------------------------------------
  # C) Supplier entry
  # ----------------------------------------------------------------------
  supplier_stats <- NULL
  suppliers_entrance <- NULL
  unique_supp <- NULL
  
  if (all(c("bidder_masterid", "tender_year", "cpv_cluster") %in% names(df))) {
    
    # Create a shared filtered dataset for consistency
    df_supplier_analysis <- df %>%
      dplyr::filter(
        !is.na(bidder_masterid),
        !is.na(tender_year),
        !is.na(cpv_cluster)
      )
    
    # Both calculations now use the same filtered data
    supplier_stats <- compute_supplier_entry(df_supplier_analysis)
    suppliers_entrance <- plot_supplier_shares_heatmap(supplier_stats)
    unique_supp <- plot_unique_suppliers_heatmap(df_supplier_analysis)
    
    # Verify they have the same clusters
    clusters_in_stats <- sort(unique(supplier_stats$cpv_cluster))
    clusters_in_plot <- sort(unique(df_supplier_analysis$cpv_cluster))
    if (!identical(clusters_in_stats, clusters_in_plot)) {
      warning("CPV clusters differ between supplier_stats and plot data! ",
              "supplier_stats: ", paste(clusters_in_stats, collapse=", "),
              " | plot data: ",   paste(clusters_in_plot,  collapse=", "))
    }
  }
  
  # ----------------------------------------------------------------------
  # D) Networks (optional)
  # ----------------------------------------------------------------------
  network_plots <- list()
  if (length(network_cpv_clusters) > 0) {
    for (ccpv in network_cpv_clusters) {
      nm <- paste0("network_cpv_", ccpv)
      network_plots[[nm]] <- tryCatch(
        plot_buyer_supplier_networks(
          df,
          cpv_focus     = ccpv,
          n_top_buyers  = network_top_buyers,
          ncol          = network_ncol,
          country_code  = country_code   # <-- add this
        ),
        error = function(e) NULL
      )
    }
  }
  
  # ----------------------------------------------------------------------
  # E) Relative prices (only if columns exist)
  # ----------------------------------------------------------------------
  rel_tot <- rel_year <- rel_10 <- rel_buy <- NULL
  relative_price_data <- NULL
  top_markets <- NULL
  top_buyers_rel <- NULL
  market_proc_price_plot <- NULL
  
  
  if (all(c("bid_price", "lot_estimatedprice") %in% names(df))) {
    relative_price_data <- df %>% add_relative_price()
    
    rel_tot  <- plot_relative_price_density(relative_price_data)
    
    if ("tender_year" %in% names(relative_price_data)) {
      rel_year <- plot_relative_price_by_year(relative_price_data)
    }
    
    if ("cpv_category" %in% names(relative_price_data)) {
      top_markets <- top_markets_by_relative_price(relative_price_data, n = 10)
      rel_10 <- plot_top_markets_relative_price(relative_price_data, top_markets)
    }
    
    if ("buyer_name" %in% names(relative_price_data)) {
      top_buyers_rel <- top_buyers_by_relative_price(relative_price_data)
      if (nrow(top_buyers_rel) > 0) rel_buy <- plot_top_buyers_relative_price(top_buyers_rel)
    }
  }
  
  # ----------------------------------------------------------------------
  # F) Competition (single-bid)
  # ----------------------------------------------------------------------
  sb_overall_plot <- proc_plot <- price_plot <- buyer_group_plot <-
    market_plot <- top_buyers_plot <- NULL
  
  if ("single_bid" %in% names(df) && any(!is.na(df$single_bid))) {
    
    # 1) overall trend by year
    if ("tender_year" %in% names(df))
      sb_overall_plot <- tryCatch(plot_single_bid_overall(df), error = function(e) NULL)
    
    # 2) by procedure
    if ("tender_proceduretype" %in% names(df))
      proc_plot <- tryCatch(plot_single_bid_by_procedure(df), error = function(e) NULL)
    
    # 3) by contract value
    if ("price_bin" %in% names(df))
      price_plot <- tryCatch(plot_single_bid_by_price(df), error = function(e) NULL)
    
    # 4) by buyer group
    if ("buyer_buyertype" %in% names(df))
      buyer_group_plot <- tryCatch(plot_single_bid_by_buyer_group(df), error = function(e) NULL)
    
    # 5) by CPV market
    market_plot <- tryCatch(plot_single_bid_by_market(df), error = function(e) NULL)
    
    # 6) top buyers
    if ("buyer_masterid" %in% names(df))
      top_buyers_plot <- tryCatch(
        plot_top_buyers_single_bid(df, buyer_id_col = "buyer_masterid",
                                   top_n = 20, min_tenders = 30),
        error = function(e) NULL
      )
  }
  # ----------------------------------------------------------------------
  # G) Save standard outputs (optional)
  # ----------------------------------------------------------------------
  if (isTRUE(save_outputs)) {
    dir_ensure(output_dir)
    
    save_plot(market_size_n,  output_dir, "market_size_n.png",  width = 10, height = 6)
    save_plot(market_size_v,  output_dir, "market_size_v.png",  width = 10, height = 6)
    save_plot(market_size_av, output_dir, "market_size_av.png", width = 10, height = 7)
    
    if (!is.null(suppliers_entrance)) save_plot(suppliers_entrance, output_dir, "suppliers_entrance.png", width = 10, height = 7)
    if (!is.null(unique_supp))        save_plot(unique_supp,        output_dir, "unique_supp.png",        width = 10, height = 7)
    
    if (!is.null(rel_tot))  save_plot(rel_tot,  output_dir, "rel_tot.png",  width = 10, height = 7)
    if (!is.null(rel_year)) save_plot(rel_year, output_dir, "rel_year.png", width = 10, height = 7)
    if (!is.null(rel_10))   save_plot(rel_10,   output_dir, "rel_10.png",   width = 10, height = 7)
    if (!is.null(rel_buy))  save_plot(rel_buy,  output_dir, "rel_buy.png",  width = 10, height = 7)
    
    if (!is.null(sb_overall_plot))  save_plot(sb_overall_plot,  output_dir, "single_bid_overall.png",       width = 10, height = 6)
    if (!is.null(proc_plot))        save_plot(proc_plot,        output_dir, "single_bid_by_procedure.png",  width = 10, height = 7)
    if (!is.null(price_plot))       save_plot(price_plot,       output_dir, "single_bid_by_price.png",      width = 10, height = 7)
    if (!is.null(buyer_group_plot)) save_plot(buyer_group_plot, output_dir, "single_bid_by_buyer_group.png",width = 10, height = 7)
    if (!is.null(market_plot))      save_plot(market_plot,      output_dir, "single_bid_by_market.png",     width = 10, height = 9)
    if (!is.null(top_buyers_plot))  save_plot(top_buyers_plot,  output_dir, "top_buyers_single_bid.png",    width = 10, height = 7)
    
    if (length(network_plots) > 0) {
      for (nm in names(network_plots)) {
        p <- network_plots[[nm]]
        if (!is.null(p)) save_plot(p, output_dir, paste0(nm, ".png"), width = 10, height = 7)
      }
    }
  }
  
  # ----------------------------------------------------------------------
  # H) Collect outputs (like your admin pipeline)
  # ----------------------------------------------------------------------
  results <- list(
    country_code = country_code,
    summary_stats = summary_stats,
    
    # cleaned/enriched data
    df = df,
    
    # market sizing
    market_summary = market_summary,
    market_size_n  = market_size_n,
    market_size_v  = market_size_v,
    market_size_av = market_size_av,
    
    # supplier dynamics
    supplier_stats     = supplier_stats,
    suppliers_entrance = suppliers_entrance,
    unique_supp        = unique_supp,
    
    # networks
    network_plots = network_plots,
    
    # relative prices
    relative_price_data = relative_price_data,
    top_markets_relative_price = top_markets,
    top_buyers_relative_price  = top_buyers_rel,
    rel_tot  = rel_tot,
    rel_year = rel_year,
    rel_10   = rel_10,
    rel_buy  = rel_buy,
    single_bid_market_procedure_price_top = market_proc_price_plot,
    
    # competition
    single_bid_overall            = sb_overall_plot,
    single_bid_by_procedure       = proc_plot,
    single_bid_by_price           = price_plot,
    single_bid_by_buyer_group     = buyer_group_plot,
    single_bid_by_market          = market_plot,
    top_buyers_single_bid         = top_buyers_plot,
    
    #cpv clusters definition
    cpv_cluster_legend = cpv_cluster_legend
    
    
  )
  
  invisible(results)
}