suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(purrr)
  library(lubridate)
  library(tidyr)
  library(tibble)
})

source("scripts/utils_topic_grouping.R")

dir.create("data/processed/backfill", recursive = TRUE, showWarnings = FALSE)

rules <- read_csv("config/topic_rules.csv", show_col_types = FALSE)

start_date <- as.Date("2026-04-01")
end_date   <- as.Date("2026-04-30")

period_label <- paste0(start_date, "_to_", end_date)

reddit_files <- list.files(
  "data/raw/reddit",
  pattern = "^reddit_rss_\\d{4}-\\d{2}-\\d{2}\\.csv$",
  full.names = TRUE
)

google_files <- list.files(
  "data/raw/google",
  pattern = "^google_trends_\\d{4}-\\d{2}-\\d{2}\\.csv$",
  full.names = TRUE
)

reddit_df <- if (length(reddit_files) > 0) {
  map_dfr(reddit_files, ~ read_csv(.x, show_col_types = FALSE))
} else {
  tibble()
}

google_df <- if (length(google_files) > 0) {
  map_dfr(google_files, ~ read_csv(.x, show_col_types = FALSE))
} else {
  tibble()
}

if (nrow(reddit_df) > 0) {
  reddit_backfill <- reddit_df %>%
    mutate(scrape_date = as.Date(scrape_date)) %>%
    filter(scrape_date >= start_date, scrape_date <= end_date) %>%
    mutate(topic_group = assign_topic_group(title, rules)) %>%
    count(scrape_date, subreddit, topic_group, sort = TRUE, name = "post_count")
  
  write_csv(
    reddit_backfill,
    file.path("data/processed/backfill", paste0("reddit_backfill_", period_label, ".csv"))
  )
}

if (nrow(google_df) > 0) {
  google_backfill <- google_df %>%
    mutate(scrape_date = as.Date(scrape_date)) %>%
    filter(scrape_date >= start_date, scrape_date <= end_date) %>%
    mutate(topic_group = assign_topic_group(query_term, rules)) %>%
    group_by(scrape_date, country_code, country_name, region, topic_group) %>%
    summarise(
      avg_interest = mean(suppressWarnings(as.numeric(interest_score)), na.rm = TRUE),
      n_rows = n(),
      .groups = "drop"
    )
  
  write_csv(
    google_backfill,
    file.path("data/processed/backfill", paste0("google_backfill_", period_label, ".csv"))
  )
}

message("Backfill complete for ", period_label)