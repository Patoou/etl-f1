# ETL F1 (FastF1 → Bronze/Silver/Gold → R)

## Requisitos
- Python 3.11+ (pip/venv)
- R 4.3+ (paquetes: `arrow`, `dplyr`, `purrr`, `readr`)
- SO: Windows/Linux/macOS

## Estructura (clave)
- `src/etl/{bronze,silver,gold}`: pipelines por etapa.
- `data/{bronze,silver,gold}`: salidas por `race_id` (p.ej. `2022_Silverstone`).
- `scripts/`: utilidades (materializar rango, checksums, carga R).
- `contracts/`: contrato para R.
- `Makefile`: comandos rápidos.

## Flujo
1. **Bronze**: extracción cruda (laps, weather, results, pitstops, meta).
2. **Silver**: normalización (tiempos→ms, compuestos S/M/H/I/W, stints, `rain_flag`).
3. **Gold**: unión final por piloto → `driver_strategy.parquet` + `train/test`.

## Comandos mínimos (ver más abajo “Qué correr”)
- `make setup`
- `make one SEASON=2022 EVENT=Silverstone`
- `make all_range START=2018 END=2024`
- `make checksums`
- `make r-load`
