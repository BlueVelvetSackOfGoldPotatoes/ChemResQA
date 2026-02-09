# ChemResQA

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: CC BY 4.0 (dataset)](https://img.shields.io/badge/Dataset%20License-CC%20BY%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)
[![Open Source](https://img.shields.io/badge/Open%20Access-Yes-brightgreen.svg)](#data-provenance-and-access)

ChemResQA is an open-access chemistry QA workflow and dataset project for building, filtering, and benchmarking multiple-choice questions generated from scientific papers.

It combines:
- data collection from open-access publishers,
- PDF-to-structured-data extraction,
- GPT-based question generation,
- model benchmarking (OpenAI and Hugging Face),
- and a lightweight Flask app for human evaluation.

## Why This Repository

- Build chemistry QA datasets from literature with reproducible scripts.
- Benchmark LLMs and QA systems on domain-specific MCQs.
- Run human-in-the-loop validation through a simple web app.
- Reuse existing outputs in `data/` and `results/` without rebuilding everything from scratch.

## Highlights

- End-to-end path from open-access papers to MCQ dataset artifacts.
- Includes both machine benchmarking and human evaluation workflows.
- Ships with ready-to-use dataset files under `data/`.
- Supports filtering and exploratory analysis via `filter.py`.

## What Is Included

- **Dataset and metadata**
  - `data/chem_mqa_dataset.json`
  - `data/chem_mqa_metadata.json`
- **Scraping and preprocessing pipeline**
  - `scripts/main.py`, `scripts/scrapers.py`, `scripts/pdf_utils.py`, `scripts/pdf_utils_openai.py`
- **Question generation**
  - `scripts/q_a_3_5.py`, `scripts/q_a_4.py`
- **Benchmarking**
  - `scripts/benchmark_gpt_3.py`, `scripts/benchmark_gpt_4.py`
  - `scripts/benchmark_huggingface_MCQ.py`, `scripts/benchmark_huggingface_binary.py`
  - `scripts/benchmark_google_cloud.py`
- **Filtering and analysis utilities**
  - `filter.py`, `scripts/plot_results.py`, `scripts/augment_rsc_data.py`
- **Human evaluation app**
  - `app/app.py` and templates in `app/templates/`

## Repository Layout

```text
ChemResQA/
├── app/                       # Flask app for human QA evaluation
│   ├── app.py
│   ├── data/
│   └── templates/
├── data/                      # Datasets and generated/intermediate artifacts
├── scripts/                   # Scraping, processing, generation, benchmarking
├── results/                   # Model evaluation outputs
├── docs/images/               # README assets
├── api_keys/
│   └── api_keys.py
└── filter.py                  # CLI for filtering and word clouds
```

## Quickstart

### 1) Clone and create an environment

```bash
git clone https://github.com/BlueVelvetSackOfGoldPotatoes/ChemResQA.git
cd ChemResQA
python3 -m venv .venv
source .venv/bin/activate
```

If `python3 -m venv` fails on Ubuntu/Debian, install `python3-venv` first.

### 2) Install dependencies

```bash
pip install -r requirements.txt
```

Or use:

```bash
make install
```

### 3) Configure API keys

This project uses two key-loading patterns:
- `api_keys/api_keys.py` (used by some scripts)
- `OPENAI_KEY` environment variable (required by `scripts/pdf_utils_openai.py`)

Set both for compatibility with current scripts:

```bash
export OPENAI_KEY="your_openai_key_here"
```

Then set `key_openai` in `api_keys/api_keys.py`.

For `wordcloud` mode in `filter.py`, download NLTK stopwords once:

```bash
python3 -c "import nltk; nltk.download('stopwords')"
```

## Main Usage Paths

### A) Run the human evaluation app

```bash
cd app
python app.py
```

Open `http://127.0.0.1:5000` in your browser.

### B) Filter the chemistry QA dataset

```bash
python filter.py filter \
  --data_file data/chem_mqa_dataset.json \
  --include_keywords nanoparticle uptake \
  --exclude_keywords PLGA \
  --fields Context Question \
  --max_results 10 \
  --output_format json
```

Generate a word cloud:

```bash
python filter.py wordcloud \
  --data_file data/chem_mqa_dataset.json \
  --output_file chemresqa_wordcloud \
  --format png
```

### C) Scrape and process papers

```bash
cd scripts
python main.py
```

### D) Generate MCQs with GPT-4 pipeline

```bash
cd scripts
python q_a_4.py
```

### E) Benchmark models

```bash
python scripts/benchmark_gpt_3.py
python scripts/benchmark_gpt_4.py
python scripts/benchmark_huggingface_MCQ.py
python scripts/benchmark_huggingface_binary.py
python scripts/benchmark_google_cloud.py --dry_run
```

Quick benchmark smoke-checks without external API calls:

```bash
python scripts/benchmark_gpt_3.py --dry_run
python scripts/benchmark_gpt_4.py --dry_run
python scripts/benchmark_huggingface_MCQ.py --dry_run --limit 1
python scripts/benchmark_huggingface_binary.py --dry_run --limit 1
python scripts/benchmark_google_cloud.py --dry_run
```

Run one command for a repository smoke check:

```bash
make smoke
```

## Data Provenance and Access

- The dataset metadata in `data/chem_mqa_metadata.json` describes sources across publishers including Springer, RSC, ACS, Nature, PeerJ, AIChE, and Wiley.
- The metadata currently references a Creative Commons Attribution 4.0 license for dataset distribution.
- Generated artifacts and benchmark outputs are available under `data/` and `results/`.

## Reproducibility Notes

- Some scripts rely on relative paths and are intended to be run from specific directories (`scripts/` or `app/`).
- `scripts/scrapers.py` assumes a Chrome binary at `/usr/bin/google-chrome`.
- API-dependent scripts can incur costs and hit rate limits.
- Hugging Face benchmarking may require additional backend installs depending on your system (`torch` or equivalent runtime).
- `scripts/benchmark_google_cloud.py` currently supports a dry-run scaffold in this repository and requires backend integration before live API benchmarking.

## Current Limitations

- No formal automated test suite in the repository at this time.
- Key configuration is split between environment variables and `api_keys/api_keys.py`.

## Contributing

Contributions are welcome and appreciated. Helpful contributions include:
- dependency pinning and environment setup improvements,
- test coverage for dataset tooling,
- stronger path/config handling,
- new benchmark adapters and evaluation metrics.

Please open an issue or pull request with a clear problem statement and reproduction steps.

## Citation

If you use this repository in academic or applied work, cite the dataset metadata in `data/chem_mqa_metadata.json` and link back to this repository.
