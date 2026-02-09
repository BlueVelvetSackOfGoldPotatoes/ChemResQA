PYTHON ?= python3

.PHONY: install smoke

install:
	$(PYTHON) -m pip install --user --break-system-packages -r requirements.txt

smoke:
	$(PYTHON) -m py_compile app/app.py filter.py scripts/*.py
	$(PYTHON) filter.py --help > /dev/null
	$(PYTHON) filter.py filter --data_file data/chem_mqa_dataset.json --include_keywords catalyst --max_results 1 --output_format json > /dev/null
	$(PYTHON) scripts/benchmark_gpt_3.py --dry_run > /dev/null
	$(PYTHON) scripts/benchmark_gpt_4.py --dry_run > /dev/null
	$(PYTHON) scripts/benchmark_huggingface_MCQ.py --dry_run --limit 1 > /dev/null
	$(PYTHON) scripts/benchmark_huggingface_binary.py --dry_run --limit 1 > /dev/null
	$(PYTHON) scripts/benchmark_google_cloud.py --dry_run > /dev/null
	$(PYTHON) scripts/smoke_check.py
