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

search_terms <- c(
  "spanking",
  "domestic discipline",
  "impact play",
  "otk",
  "adult corporal punishment",
  "spanko",
  "spanking only",
  "bdsm"
)

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

safe_gtrends_region <- function(keyword, geo_code, timeframe = "now 7-d") {
  tryCatch({
    res <- gtrendsR::gtrends(
      keyword = keyword,
      geo = geo_code,
      time = timeframe
    )
    
    region_df <- res$interest_by_region
    
    if (is.null(region_df) || nrow(region_df) == 0) {
      return(tibble())
    }
    
    region_df %>%
      clean_names() %>%
      mutate(
        scrape_date = run_date_chr,
        source = "google_trends",
        country_code = geo_code,
        country_name = country_map[[geo_code]] %||% geo_code,
        query_term = keyword
      ) %>%
      rename(
        region = location,
        interest_score = hits
      ) %>%
      select(
        scrape_date,
        source,
        country_code,
        country_name,
        region,
        query_term,
        interest_score,
        everything()
      )
  }, error = function(e) {
    message("Google Trends failed for ", keyword, " / ", geo_code, ": ", e$message)
    tibble()
  })
}

google_df <- purrr::map_dfr(names(country_map), function(one_country) {
  purrr::map_dfr(search_terms, function(one_term) {
    safe_gtrends_region(keyword = one_term, geo_code = one_country, timeframe = "now 7-d")
  })
})

google_outfile <- file.path(
  "data/raw/google",
  paste0("google_trends_", run_date_chr, ".csv")
)

write_csv(google_df, google_outfile)

get_reddit_rss <- function(subreddit) {
  url <- paste0("https://www.reddit.com/r/", subreddit, "/.rss")
  
  resp <- request(url) |>
    req_user_agent("trend-research-bot/1.0") |>
    req_perform()
  
  rss_text <- resp_body_string(resp)
  rss_xml <- read_xml(rss_text)
  
  items <- xml_find_all(rss_xml, ".//entry")
  
  if (length(items) == 0) {
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

reddit_df <- purrr::map_dfr(
  subreddits,
  purrr::possibly(get_reddit_rss, otherwise = tibble())
)

reddit_outfile <- file.path(
  "data/raw/reddit",
  paste0("reddit_rss_", run_date_chr, ".csv")
)

write_csv(reddit_df, reddit_outfile)

log_df <- tibble(
  run_datetime_utc = format(with_tz(now("UTC"), "UTC"), "%Y-%m-%d %H:%M:%S"),
  run_date = run_date_chr,
  google_rows = nrow(google_df),
  reddit_rows = nrow(reddit_df),
  google_terms = length(search_terms),
  google_countries = length(country_map),
  reddit_subreddits = length(subreddits)
)

write_csv(
  log_df,
  file.path("data/processed/logs", paste0("daily_scrape_log_", run_date_chr, ".csv"))
)

message("Saved Google data to: ", google_outfile)
message("Saved Reddit data to: ", reddit_outfile)
message("Done.")