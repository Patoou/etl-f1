#!/usr/bin/env Rscript
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Uso: Rscript scripts/load_gold.R contracts/r_contract.json", call. = FALSE)
}
contract_path <- args[1]

suppressPackageStartupMessages({
  library(jsonlite)
  library(arrow)
  library(dplyr)
  library(purrr)
  library(readr)
})

ct <- jsonlite::fromJSON(contract_path)
stopifnot(!is.null(ct$train_glob), !is.null(ct$test_glob))

# Helper para leer muchos parquet
read_many <- function(glob) {
  files <- Sys.glob(glob)
  if (length(files) == 0) return(dplyr::tibble())
  purrr::map_dfr(files, ~arrow::read_parquet(.x))
}

train <- read_many(ct$train_glob)
test  <- read_many(ct$test_glob)

# Checks mínimos
required <- ct$required_columns
miss_train <- setdiff(required, names(train))
miss_test  <- setdiff(required, names(test))
if (length(miss_train) > 0) stop(paste("Faltan columnas en train:", paste(miss_train, collapse=", ")))
if (length(miss_test)  > 0) stop(paste("Faltan columnas en test:",  paste(miss_test, collapse=", ")))

# Guardar dataset combinado (opcional)
dir.create("data/gold", showWarnings = FALSE, recursive = TRUE)
saveRDS(train, "data/gold/train.rds")
saveRDS(test,  "data/gold/test.rds")

# CSVs livianos (si hace falta compartir)
readr::write_csv(train, "data/gold/train_head.csv", na = "")
readr::write_csv(test,  "data/gold/test_head.csv",  na = "")

cat("Listo. train/test cargados y guardados en data/gold/*.rds\n")
