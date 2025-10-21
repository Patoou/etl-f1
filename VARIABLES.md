# Diccionario de variables (Gold: driver_strategy.parquet)

| Columna            | Tipo         | Unidad | Descripción |
|--------------------|--------------|--------|-------------|
| season             | int          | –      | Año calendario. |
| race_id            | str          | –      | `YYYY_Location` (slug). |
| driver             | str          | –      | Abreviatura FIA (p.ej., VER). |
| driver_number      | int          | –      | Número del piloto (nullable). |
| team               | str          | –      | Equipo. |
| grid               | int          | –      | Posición de largada (nullable). |
| final_pos          | int          | –      | Posición final (nullable: DNF/NC). |
| rain_flag          | int {0,1}    | –      | 1 si hubo lluvia (I/W o lluvia>umbral). |
| pit_count          | int ≥0       | –      | Cantidad de paradas. |
| stint_count        | int ≥1       | –      | Cantidad de stints. |
| strategy_key       | str          | –      | Secuencia de compuestos `S-M-H-…` (sin tiempos). |
| stint_laps         | list<int>    | laps   | Laps por stint, en orden. |
| compounds          | list<str>    | –      | Conjunto ordenado único de compuestos usados. |
| first_compound     | str|null     | –      | Primer compuesto de la estrategia. |
| last_compound      | str|null     | –      | Último compuesto de la estrategia. |
| unique_comp_count  | int ≥0       | –      | Nº de compuestos distintos. |
| avg_stint_len      | float        | laps   | Promedio de longitud de stint. |
| uses_wet           | int {0,1}    | –      | 1 si aparece I o W. |
| order_kendall_tau  | float [-1..1]| –      | Orden relativo vs. S→M→H→I→W. |
| total_laps         | int ≥1       | laps   | Máxima vuelta completada en carrera. |
