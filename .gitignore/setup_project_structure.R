suppressPackageStartupMessages({
  library(fs)
  library(glue)
})

message("Setting up project structure...")

# -------------------------
# Define folder structure
# -------------------------
dirs <- c(
  "data",
  "data/raw",
  "data/raw/google",
  "data/raw/reddit",
  "data/processed",
  "data/processed/fortnightly",
  "data/processed/logs",
  "data/processed/backfill",
  "config",
  "scripts",
  ".github",
  ".github/workflows"
)

# -------------------------
# Create directories
# -------------------------
walk(dirs, function(d) {
  if (!dir_exists(d)) {
    dir_create(d, recurse = TRUE)
    message("Created: ", d)
  } else {
    message("Exists: ", d)
  }
})

# -------------------------
# Add .gitkeep files
# -------------------------
add_gitkeep <- function(dir) {
  gitkeep_path <- file.path(dir, ".gitkeep")
  if (!file_exists(gitkeep_path)) {
    file_create(gitkeep_path)
    message("Added .gitkeep to: ", dir)
  }
}

walk(dirs, add_gitkeep)

# -------------------------
# Create .gitignore (optional)
# -------------------------
gitignore_path <- ".gitignore"

gitignore_lines <- c(
  "# R temp files",
  ".Rhistory",
  ".RData",
  ".Rproj.user",
  "",
  "# Local-only scripts",
  "scripts/local_*.R",
  "",
  "# Optional: large raw backups (uncomment if needed)",
  "# data/raw/**/*.xml",
  "",
  "# Logs if you want to ignore them",
  "# data/processed/logs/*.csv"
)

if (!file_exists(gitignore_path)) {
  writeLines(gitignore_lines, gitignore_path)
  message("Created .gitignore")
} else {
  message(".gitignore already exists — not overwriting")
}

message("Project structure setup complete.")