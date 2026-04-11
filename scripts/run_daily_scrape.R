suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(purrr)
  library(lubridate)
  library(httr2)
  library(xml2)
  library(tidyr)
  library(tibble)
  library(janitor)
  library(gtrendsR)
})

dir.create("data/raw/google", recursive = TRUE, showWarnings = FALSE)
dir.create("data/raw/reddit", recursive = TRUE, showWarnings = FALSE)
dir.create("data/processed/logs", recursive = TRUE, showWarnings = FALSE)

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0 || (length(x) == 1 && is.na(x))) y else x
}

run_date <- Sys.Date()
run_date_chr <- as.character(run_date)

message("Running daily scrape for: ", run_date_chr)

broad_terms <- c(
  "spanking",
  "spanko",
  "impact play",
  "bdsm"
)

niche_terms <- c(
  "domestic discipline",
  "otk",
  "adult corporal punishment",
  "spanking only"
)

search_terms <- c(broad_terms, niche_terms)

country_map <- c(
  AU = "Australia",
  GB = "United Kingdom",
  US = "United States",
  NZ = "New Zealand"
)

subreddits <- c(
  "BDSMcommunity",
  "BDSMAdvice",
  "spanking",
  "domesticdiscipline"
)

parse_trends_hits <- function(x) {
  x_chr <- as.character(x)

  dplyr::case_when(
    is.na(x_chr) ~ NA_real_,
    x_chr == "" ~ NA_real_,
    x_chr == "<1" ~ 0.5,
    TRUE ~ suppressWarnings(as.numeric(x_chr))
  )
}

safe_gtrends_region <- function(keyword, geo_code, timeframe = "today 12-m") {
  tryCatch({
    res <- gtrendsR::gtrends(
      keyword = keyword,
      geo = geo_code,
      time = timeframe
    )

    region_df <- res$interest_by_region

    if (is.null(region_df) || nrow(region_df) == 0) {
      message("Google Trends returned no regional rows for ", keyword, " / ", geo_code)
      return(tibble())
    }

    region_df %>%
      janitor::clean_names() %>%
      mutate(
        hits = as.character(hits)
      ) %>%
      mutate(
        scrape_date = run_date_chr,
        source = "google_trends_region",
        country_code = geo_code,
        country_name = country_map[[geo_code]] %||% geo_code,
        query_term = keyword,
        timeframe = timeframe,
        interest_score_raw = hits,
        interest_score = parse_trends_hits(hits)
      ) %>%
      rename(region = location) %>%
      select(
        scrape_date,
        source,
        country_code,
        country_name,
        region,
        query_term,
        timeframe,
        interest_score,
        interest_score_raw,
        everything()
      )
  }, error = function(e) {
    message("Google Trends failed for ", keyword, " / ", geo_code, ": ", e$message)
    tibble()
  })
}

safe_gtrends_over_time <- function(keyword, geo_code, timeframe = "today 12-m") {
  tryCatch({
    res <- gtrendsR::gtrends(
      keyword = keyword,
      geo = geo_code,
      time = timeframe
    )

    over_time_df <- res$interest_over_time

    if (is.null(over_time_df) || nrow(over_time_df) == 0) {
      message("Google Trends returned no over-time rows for ", keyword, " / ", geo_code)
      return(tibble())
    }

    over_time_df %>%
      janitor::clean_names() %>%
      mutate(
        hits = as.character(hits)
      ) %>%
      mutate(
        scrape_date = run_date_chr,
        source = "google_trends_over_time",
        country_code = geo_code,
        country_name = country_map[[geo_code]] %||% geo_code,
        timeframe = timeframe,
        interest_score_raw = hits,
        interest_score = parse_trends_hits(hits)
      ) %>%
      select(
        scrape_date,
        source,
        country_code,
        country_name,
        keyword,
        date,
        timeframe,
        interest_score,
        interest_score_raw,
        everything()
      )
  }, error = function(e) {
    message("Google Trends over-time failed for ", keyword, " / ", geo_code, ": ", e$message)
    tibble()
  })
}

google_region_list <- map(names(country_map), function(one_country) {
  map(search_terms, function(one_term) {
    safe_gtrends_region(
      keyword = one_term,
      geo_code = one_country,
      timeframe = "today 12-m"
    )
  })
}) %>%
  flatten()

google_region_df <- bind_rows(google_region_list)

google_overtime_list <- map(names(country_map), function(one_country) {
  map(search_terms, function(one_term) {
    safe_gtrends_over_time(
      keyword = one_term,
      geo_code = one_country,
      timeframe = "today 12-m"
    )
  })
}) %>%
  flatten()

google_overtime_df <- bind_rows(google_overtime_list)

google_region_outfile <- file.path(
  "data/raw/google",
  paste0("google_trends_region_", run_date_chr, ".csv")
)

google_overtime_outfile <- file.path(
  "data/raw/google",
  paste0("google_trends_overtime_", run_date_chr, ".csv")
)

write_csv(google_region_df, google_region_outfile)
write_csv(google_overtime_df, google_overtime_outfile)

get_reddit_rss <- function(subreddit) {
  url <- paste0("https://www.reddit.com/r/", subreddit, "/.rss")

  resp <- request(url) |>
    req_user_agent("trend-research-bot/1.0") |>
    req_perform()

  rss_text <- resp_body_string(resp)
  rss_xml <- read_xml(rss_text)

  items <- xml_find_all(rss_xml, ".//entry")

  if (length(items) == 0) {
    message("No Reddit RSS entries found for /r/", subreddit)
    return(tibble())
  }

  tibble(
    scrape_date = run_date_chr,
    source = "reddit_rss",
    subreddit = subreddit,
    title = map_chr(items, ~ xml_text(xml_find_first(.x, "./title")) %||% NA_character_),
    id = map_chr(items, ~ xml_text(xml_find_first(.x, "./id")) %||% NA_character_),
    updated = map_chr(items, ~ xml_text(xml_find_first(.x, "./updated")) %||% NA_character_),
    author = map_chr(items, ~ xml_text(xml_find_first(.x, ".//name")) %||% NA_character_),
    link = map_chr(items, ~ xml_attr(xml_find_first(.x, "./link"), "href") %||% NA_character_)
  )
}

reddit_df <- map_dfr(
  subreddits,
  possibly(get_reddit_rss, otherwise = tibble())
)

reddit_outfile <- file.path(
  "data/raw/reddit",
  paste0("reddit_rss_", run_date_chr, ".csv")
)

write_csv(reddit_df, reddit_outfile)

for (sr in subreddits) {
  try({
    url <- paste0("https://www.reddit.com/r/", sr, "/.rss")
    resp <- request(url) |>
      req_user_agent("trend-research-bot/1.0") |>
      req_perform()

    xml_text_value <- resp_body_string(resp)
    xml_file <- file.path("data/raw/reddit", paste0("reddit_rss_", sr, "_", run_date_chr, ".xml"))
    writeLines(xml_text_value, xml_file, useBytes = TRUE)
  }, silent = TRUE)
}

log_df <- tibble(
  run_datetime_utc = format(with_tz(now("UTC"), "UTC"), "%Y-%m-%d %H:%M:%S"),
  run_date = run_date_chr,
  google_region_rows = nrow(google_region_df),
  google_overtime_rows = nrow(google_overtime_df),
  reddit_rows = nrow(reddit_df),
  google_terms = length(search_terms),
  google_countries = length(country_map),
  reddit_subreddits = length(subreddits)
)

log_outfile <- file.path(
  "data/processed/logs",
  paste0("daily_scrape_log_", run_date_chr, ".csv")
)

write_csv(log_df, log_outfile)

message("Saved Google region data to: ", google_region_outfile)
message("Saved Google over-time data to: ", google_overtime_outfile)
message("Saved Reddit data to: ", reddit_outfile)
message("Saved log to: ", log_outfile)
message("Done.")