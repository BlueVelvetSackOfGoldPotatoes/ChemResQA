import argparse
import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DATASET_PATH = ROOT_DIR / "data" / "chem_mqa_dataset.json"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "results" / "GoogleCloud"
DEFAULT_MODELS = [
    "text-bison@002",
    "codechat-bison@002",
    "meta/llama3",
    "mistralai/Mistral-7B-v0.1",
    "microsoft/biogpt",
    "lmsys/vicuna-7b-v1.5",
    "claude-3-opus@20240229",
]


def parse_arguments():
    """
    Parse command line arguments for Google Cloud benchmark workflow.

    Args:
        None

    Returns:
        argparse.Namespace: Parsed command line values.
    """
    parser = argparse.ArgumentParser(description="Experimental Google Cloud benchmark scaffold.")
    parser.add_argument("--dataset", default=str(DEFAULT_DATASET_PATH), help="Path to dataset JSON file.")
    parser.add_argument("--output_dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory to store output files.")
    parser.add_argument("--models", nargs="+", default=DEFAULT_MODELS, help="Model identifiers to include in report.")
    parser.add_argument("--dry_run", action="store_true", help="Create output placeholders without API calls.")
    return parser.parse_args()


def load_dataset(dataset_path):
    """
    Load benchmark dataset from disk.

    Args:
        dataset_path (Path): Dataset location.

    Returns:
        list: Dataset entries.
    """
    with open(dataset_path, "r", encoding="utf-8") as file:
        return json.load(file)


def write_dry_run_reports(dataset, model_ids, output_dir):
    """
    Write placeholder reports so pipeline paths can be validated.

    Args:
        dataset (list): Question dataset entries.
        model_ids (list[str]): Requested model identifiers.
        output_dir (Path): Output directory path.

    Returns:
        None
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    for model_id in model_ids:
        output_file = output_dir / f"{model_id.replace('/', '-')}.json"
        payload = {
            "mode": "dry_run",
            "model": model_id,
            "total_questions": len(dataset),
            "results": [],
        }
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(payload, file, indent=4)


def main():
    """
    Run experimental Google Cloud benchmark workflow.

    Args:
        None

    Returns:
        None
    """
    args = parse_arguments()
    dataset = load_dataset(Path(args.dataset))
    if args.dry_run:
        write_dry_run_reports(dataset, args.models, Path(args.output_dir))
        print(f"Dry run complete. Placeholder files written to {args.output_dir}.")
        return
    raise SystemExit(
        "Google Cloud benchmark backend is not configured in this repository. "
        "Run with --dry_run to validate dataset/output paths."
    )


if __name__ == "__main__":
    main()
