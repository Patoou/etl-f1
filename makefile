PY := python
RSCRIPT := Rscript
START ?= 2018
END ?= 2024
SEASON ?=
EVENT ?=

.PHONY: setup one bronze silver gold all_range checksums r-load

setup:
	$(PY) -m pip install -r requirements.txt
	@echo "OK"

one: bronze silver gold

bronze:
	@test -n "$(SEASON)" && test -n "$(EVENT)" || (echo "Falta SEASON y EVENT"; exit 1)
	$(PY) -m src.etl.bronze.run --season $(SEASON) --event $(EVENT)

silver:
	@test -n "$(SEASON)" && test -n "$(EVENT)" || (echo "Falta SEASON y EVENT"; exit 1)
	$(PY) -m src.etl.silver.run --season $(SEASON) --event $(EVENT)

gold:
	@test -n "$(SEASON)" && test -n "$(EVENT)" || (echo "Falta SEASON y EVENT"; exit 1)
	$(PY) -m src.etl.gold.run --season $(SEASON) --event $(EVENT)

all_range:
	$(PY) scripts/materialize_range.py --start $(START) --end $(END)

checksums:
	$(PY) scripts/checksums.py

r-load:
	$(RSCRIPT) scripts/load_gold.R contracts/r_contract.json
