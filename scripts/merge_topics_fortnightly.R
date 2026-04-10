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

reddit_files <- list.files(
  "data/raw/reddit",
  pattern = "^reddit_rss_\\d{4}-\\d{2}-\\d{2}\\.csv$",
  full.names = TRUE
)

google_files <- list.files(
  "data/raw/google",
  pattern = "^google_trends_region_\\d{4}-\\d{2}-\\d{2}\\.csv$",
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
  reddit_df <- reddit_df %>%
    mutate(scrape_date = as.Date(scrape_date)) %>%
    filter(scrape_date >= window_start, scrape_date <= today_date) %>%
    mutate(
      topic_text = title,
      topic_group = assign_topic_group(topic_text, rules)
    )
  
 reddit_summary <- reddit_df %>%
  group_by(scrape_date, subreddit, topic_group) %>%
  summarise(post_count = n(), .groups = "drop")
  
  reddit_keywords <- extract_keywords_simple(
    reddit_df$title,
    min_nchar = 3,
    stopwords = c("the", "and", "for", "with", "this", "that", "from", "what", "how")
  ) %>%
    mutate(source = "reddit")
  
  write_csv(
    reddit_summary,
    file.path("data/processed/fortnightly", paste0("reddit_topic_summary_", period_label, ".csv"))
  )
  
  write_csv(
    reddit_keywords,
    file.path("data/processed/fortnightly", paste0("reddit_keywords_", period_label, ".csv"))
  )
} else {
  reddit_summary <- tibble()
}

if (nrow(google_df) > 0) {
  google_df <- google_df %>%
    mutate(scrape_date = as.Date(scrape_date)) %>%
    filter(scrape_date >= window_start, scrape_date <= today_date) %>%
    mutate(
      topic_text = query_term,
      topic_group = assign_topic_group(topic_text, rules)
    )
  
  google_summary <- google_df %>%
    group_by(scrape_date, country_code, country_name, region, topic_group) %>%
    summarise(
      avg_interest = mean(suppressWarnings(as.numeric(interest_score)), na.rm = TRUE),
      n_rows = n(),
      .groups = "drop"
    )
  
  google_country_summary <- google_df %>%
    group_by(scrape_date, country_code, country_name, topic_group) %>%
    summarise(
      avg_interest = mean(suppressWarnings(as.numeric(interest_score)), na.rm = TRUE),
      n_rows = n(),
      .groups = "drop"
    )
  
  write_csv(
    google_summary,
    file.path("data/processed/fortnightly", paste0("google_region_topic_summary_", period_label, ".csv"))
  )
  
  write_csv(
    google_country_summary,
    file.path("data/processed/fortnightly", paste0("google_country_topic_summary_", period_label, ".csv"))
  )
} else {
  google_summary <- tibble()
  google_country_summary <- tibble()
}

combined_parts <- list()

if (exists("reddit_summary") && nrow(reddit_summary) > 0) {
  if ("subreddit" %in% names(reddit_summary)) {
    combined_parts[[length(combined_parts) + 1]] <- reddit_summary %>%
      mutate(
        source = "reddit",
        geography = subreddit,
        metric = post_count
      ) %>%
      select(source, scrape_date, geography, topic_group, metric)
  }
}

if (exists("google_country_summary") && nrow(google_country_summary) > 0) {
  combined_parts[[length(combined_parts) + 1]] <- google_country_summary %>%
    mutate(
      source = "google_country",
      geography = country_code,
      metric = avg_interest
    ) %>%
    select(source, scrape_date, geography, topic_group, metric)
}

combined_overview <- if (length(combined_parts) > 0) {
  bind_rows(combined_parts)
} else {
  tibble(
    source = character(),
    scrape_date = as.Date(character()),
    geography = character(),
    topic_group = character(),
    metric = numeric()
  )
}

write_csv(
  combined_overview,
  file.path("data/processed/fortnightly", paste0("combined_overview_", period_label, ".csv"))
)
message("Fortnightly merge complete.")