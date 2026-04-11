suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(purrr)
  library(lubridate)
  library(tidyr)
  library(tibble)
  library(janitor)
})

source("scripts/utils_topic_grouping.R")

dir.create("data/processed/fortnightly", recursive = TRUE, showWarnings = FALSE)

today_date <- Sys.Date()
window_start <- today_date - days(13)
period_label <- paste0(window_start, "_to_", today_date)

rules <- read_csv("config/topic_rules.csv", show_col_types = FALSE)

message("Fortnight window: ", window_start, " to ", today_date)

# -------------------------
# File lists
# -------------------------
reddit_files <- list.files(
  "data/raw/reddit",
  pattern = "^reddit_rss_\\d{4}-\\d{2}-\\d{2}\\.csv$",
  full.names = TRUE
)

google_region_files <- list.files(
  "data/raw/google",
  pattern = "^google_trends_region_\\d{4}-\\d{2}-\\d{2}\\.csv$",
  full.names = TRUE
)

google_overtime_files <- list.files(
  "data/raw/google",
  pattern = "^google_trends_overtime_\\d{4}-\\d{2}-\\d{2}\\.csv$",
  full.names = TRUE
)

# -------------------------
# Load data
# -------------------------
reddit_df <- if (length(reddit_files) > 0) {
  map_dfr(reddit_files, ~ read_csv(.x, show_col_types = FALSE))
} else {
  tibble()
}

google_region_df <- if (length(google_region_files) > 0) {
  map_dfr(google_region_files, ~ read_csv(.x, show_col_types = FALSE))
} else {
  tibble()
}

google_overtime_df <- if (length(google_overtime_files) > 0) {
  map_dfr(google_overtime_files, ~ read_csv(.x, show_col_types = FALSE))
} else {
  tibble()
}

# -------------------------
# REDDIT
# Daily snapshots may repeat posts across days.
# Deduplicate by post id (fallback to link if id missing).
# -------------------------
if (nrow(reddit_df) > 0) {
  reddit_df <- reddit_df %>%
    mutate(
      scrape_date = as.Date(scrape_date),
      dedup_key = coalesce(id, link)
    ) %>%
    filter(scrape_date >= window_start, scrape_date <= today_date)

  reddit_raw_window <- reddit_df

  reddit_dedup <- reddit_df %>%
    arrange(dedup_key, desc(scrape_date)) %>%
    distinct(dedup_key, .keep_all = TRUE) %>%
    mutate(
      topic_text = title,
      topic_group = assign_topic_group(topic_text, rules)
    )

  reddit_summary <- reddit_dedup %>%
    group_by(subreddit, topic_group) %>%
    summarise(
      unique_posts = n(),
      .groups = "drop"
    ) %>%
    arrange(desc(unique_posts), subreddit, topic_group)

  reddit_daily_summary <- reddit_dedup %>%
    group_by(scrape_date, subreddit, topic_group) %>%
    summarise(
      unique_posts = n(),
      .groups = "drop"
    ) %>%
    arrange(scrape_date, subreddit, desc(unique_posts))

  reddit_keywords <- extract_keywords_simple(
    reddit_dedup$title,
    min_nchar = 3,
    stopwords = c("the", "and", "for", "with", "this", "that", "from", "what", "how")
  ) %>%
    mutate(source = "reddit")

  write_csv(
    reddit_raw_window,
    file.path("data/processed/fortnightly", paste0("reddit_raw_window_", period_label, ".csv"))
  )

  write_csv(
    reddit_dedup,
    file.path("data/processed/fortnightly", paste0("reddit_dedup_", period_label, ".csv"))
  )

  write_csv(
    reddit_summary,
    file.path("data/processed/fortnightly", paste0("reddit_topic_summary_", period_label, ".csv"))
  )

  write_csv(
    reddit_daily_summary,
    file.path("data/processed/fortnightly", paste0("reddit_daily_topic_summary_", period_label, ".csv"))
  )

  write_csv(
    reddit_keywords,
    file.path("data/processed/fortnightly", paste0("reddit_keywords_", period_label, ".csv"))
  )
} else {
  reddit_dedup <- tibble()
  reddit_summary <- tibble()
  reddit_daily_summary <- tibble()
  reddit_keywords <- tibble()
}

# -------------------------
# GOOGLE REGION SNAPSHOTS
# Treat as snapshots, not unique raw events.
# Keep the latest scrape for each country/region/query combination.
# -------------------------
if (nrow(google_region_df) > 0) {
  google_region_df <- google_region_df %>%
    mutate(
      scrape_date = as.Date(scrape_date),
      interest_score = suppressWarnings(as.numeric(interest_score))
    ) %>%
    filter(scrape_date >= window_start, scrape_date <= today_date)

  google_region_latest <- google_region_df %>%
    arrange(country_code, region, query_term, desc(scrape_date)) %>%
    distinct(country_code, region, query_term, .keep_all = TRUE) %>%
    mutate(
      topic_text = query_term,
      topic_group = assign_topic_group(topic_text, rules)
    )

  google_region_summary <- google_region_latest %>%
    group_by(country_code, country_name, region, topic_group) %>%
    summarise(
      avg_interest = mean(interest_score, na.rm = TRUE),
      n_queries = n(),
      .groups = "drop"
    ) %>%
    arrange(country_code, region, desc(avg_interest))

  google_country_summary <- google_region_latest %>%
    group_by(country_code, country_name, topic_group) %>%
    summarise(
      avg_interest = mean(interest_score, na.rm = TRUE),
      n_queries = n(),
      .groups = "drop"
    ) %>%
    arrange(country_code, desc(avg_interest))

  write_csv(
    google_region_df,
    file.path("data/processed/fortnightly", paste0("google_region_window_", period_label, ".csv"))
  )

  write_csv(
    google_region_latest,
    file.path("data/processed/fortnightly", paste0("google_region_latest_", period_label, ".csv"))
  )

  write_csv(
    google_region_summary,
    file.path("data/processed/fortnightly", paste0("google_region_topic_summary_", period_label, ".csv"))
  )

  write_csv(
    google_country_summary,
    file.path("data/processed/fortnightly", paste0("google_country_topic_summary_", period_label, ".csv"))
  )
} else {
  google_region_latest <- tibble()
  google_region_summary <- tibble()
  google_country_summary <- tibble()
}

# -------------------------
# GOOGLE OVER-TIME SNAPSHOTS
# Keep the latest scrape for each country/query/date combination.
# This avoids repeating the same historical point from multiple daily snapshots.
# -------------------------
if (nrow(google_overtime_df) > 0) {
  google_overtime_df <- google_overtime_df %>%
    mutate(
      scrape_date = as.Date(scrape_date),
      date = as.Date(date),
      interest_score = suppressWarnings(as.numeric(interest_score))
    ) %>%
    filter(scrape_date >= window_start, scrape_date <= today_date)

  google_overtime_latest <- google_overtime_df %>%
    arrange(country_code, keyword, date, desc(scrape_date)) %>%
    distinct(country_code, keyword, date, .keep_all = TRUE) %>%
    mutate(
      topic_text = keyword,
      topic_group = assign_topic_group(topic_text, rules)
    )

  google_overtime_summary <- google_overtime_latest %>%
    group_by(country_code, country_name, date, topic_group) %>%
    summarise(
      avg_interest = mean(interest_score, na.rm = TRUE),
      n_rows = n(),
      .groups = "drop"
    ) %>%
    arrange(country_code, date, desc(avg_interest))

  write_csv(
    google_overtime_df,
    file.path("data/processed/fortnightly", paste0("google_overtime_window_", period_label, ".csv"))
  )

  write_csv(
    google_overtime_latest,
    file.path("data/processed/fortnightly", paste0("google_overtime_latest_", period_label, ".csv"))
  )

  write_csv(
    google_overtime_summary,
    file.path("data/processed/fortnightly", paste0("google_overtime_topic_summary_", period_label, ".csv"))
  )
} else {
  google_overtime_latest <- tibble()
  google_overtime_summary <- tibble()
}

# -------------------------
# COMBINED OVERVIEW
# Reddit = unique post counts
# Google = average relative interest
# -------------------------
combined_parts <- list()

if (nrow(reddit_summary) > 0) {
  combined_parts[[length(combined_parts) + 1]] <- reddit_summary %>%
    transmute(
      source = "reddit",
      geography = subreddit,
      topic_group = topic_group,
      metric = unique_posts
    )
}

if (nrow(google_country_summary) > 0) {
  combined_parts[[length(combined_parts) + 1]] <- google_country_summary %>%
    transmute(
      source = "google_country",
      geography = country_code,
      topic_group = topic_group,
      metric = avg_interest
    )
}

combined_overview <- if (length(combined_parts) > 0) {
  bind_rows(combined_parts)
} else {
  tibble(
    source = character(),
    geography = character(),
    topic_group = character(),
    metric = numeric()
  )
}

write_csv(
  combined_overview,
  file.path("data/processed/fortnightly", paste0("combined_overview_", period_label, ".csv"))
)

message("Rows loaded:")
message("  reddit_df: ", nrow(reddit_df))
message("  google_region_df: ", nrow(google_region_df))
message("  google_overtime_df: ", nrow(google_overtime_df))

message("Rows after processing:")
message("  reddit_dedup: ", nrow(reddit_dedup))
message("  google_region_latest: ", nrow(google_region_latest))
message("  google_overtime_latest: ", nrow(google_overtime_latest))

message("Fortnightly merge complete.")