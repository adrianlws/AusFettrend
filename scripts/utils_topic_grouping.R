suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(purrr)
  library(tidyr)
  library(tibble)
})

clean_topic_text <- function(x) {
  x %>%
    str_to_lower() %>%
    str_replace_all("[\r\n\t]", " ") %>%
    str_replace_all("[^[:alnum:]\\s]", " ") %>%
    str_squish()
}

assign_topic_group <- function(text_vector, rules_df) {
  cleaned <- clean_topic_text(text_vector)
  
  map_chr(cleaned, function(one_text) {
    hits <- rules_df %>%
      mutate(is_match = str_detect(one_text, regex(pattern, ignore_case = TRUE))) %>%
      filter(is_match) %>%
      arrange(desc(priority))
    
    if (nrow(hits) == 0) {
      return("unclassified")
    }
    
    hits$topic_group[[1]]
  })
}

extract_keywords_simple <- function(text_vector, min_nchar = 3L, stopwords = character()) {
  tibble(raw_text = text_vector) %>%
    mutate(raw_text = clean_topic_text(raw_text)) %>%
    separate_rows(raw_text, sep = "\\s+") %>%
    rename(keyword = raw_text) %>%
    filter(
      !is.na(keyword),
      keyword != "",
      nchar(keyword) >= min_nchar,
      !keyword %in% stopwords
    ) %>%
    count(keyword, sort = TRUE, name = "n")
}