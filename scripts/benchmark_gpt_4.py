import os
import json
import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from api_keys.api_keys import key_openai
from openai import OpenAI

DEFAULT_DATASET_PATH = ROOT_DIR / "data" / "all_questions_gpt_4.json"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "results" / "GPT4_Answers"


def load_dataset(dataset_path):
    """
    Load benchmark questions from disk.

    Args:
        dataset_path (Path): Absolute or relative path to the dataset JSON.

    Returns:
        list: Parsed list of question objects.
    """
    with open(dataset_path, "r", encoding="utf-8") as file:
        return json.load(file)


def build_client():
    """
    Build an OpenAI client using available key sources.

    Args:
        None

    Returns:
        OpenAI: Configured client instance.
    """
    api_key = os.environ.get("OPENAI_KEY") or key_openai
    if not api_key:
        raise SystemExit("Missing OpenAI API key. Set OPENAI_KEY or api_keys.api_keys.key_openai.")
    return OpenAI(api_key=api_key)

def evaluate_questions_with_gpt4(client, questions, output_dir, output_filename):
    """
    Evaluate multiple-choice questions with GPT-4 and save results.

    Args:
        client (OpenAI): Configured OpenAI client.
        questions (list): List of question objects.
        output_dir (Path): Directory for output artifacts.
        output_filename (str): Output JSON filename.

    Returns:
        tuple[int, int]: Correct count and total count.
    """
    results = []
    correct_count = 0
    os.makedirs(output_dir, exist_ok=True)

    for question_data in questions:
        for question_id, details in question_data.items():
            if question_id.startswith("Question"):
                prompt = f"You can only respond to this prompt with one letter, nothing else. This is a multiple-choice question. You must answer the following question by simply printing one of the following letters (A, B, C, or D). You shall not write anything else except the letter in your following response, no text whatsoever except for the letter. {details['Context']} {details['Question']} Choices: A: {details['A']}, B: {details['B']}, C: {details['C']}, D: {details['D']}."
                
                messages = [
                    {"role": "system", "content": "You are a multiple-choice question answering machine - you only answer with a letter out of A, B, C, and D, nothing else is outputted by you."},
                    {"role": "user", "content": prompt}
                ]

                try:
                    completion = client.chat.completions.create(
                        model="gpt-4-1106-preview",
                        messages=messages,
                        temperature=0
                    )

                    generated_answer = completion.choices[0].message.content
                    
                    print(f"{details['Context']} {details['Question']} Choices: A: {details['A']}, B: {details['B']}, C: {details['C']}, D: {details['D']} \n {generated_answer}")

                    is_correct = generated_answer.upper() == details['Answer'].upper()
                    if is_correct:
                        correct_count += 1

                    results.append({
                        'question_id': question_id,
                        'prompt': prompt,
                        'generated_answer': generated_answer,
                        'is_correct': is_correct
                    })

                except Exception as e:
                    print(f"Error with question {question_id}: {e}")

    with open(output_dir / output_filename, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4)

    print(f"Correct Answers: {correct_count}/{len(questions)}")
    return correct_count, len(questions)

def parse_arguments():
    """
    Parse command line arguments for GPT-4 benchmarking.

    Args:
        None

    Returns:
        argparse.Namespace: Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(description="Benchmark ChemResQA MCQs with GPT-4.")
    parser.add_argument("--dataset", default=str(DEFAULT_DATASET_PATH), help="Path to dataset JSON file.")
    parser.add_argument("--output_dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory to store output JSON.")
    parser.add_argument("--output_filename", default="gpt4_evaluation_results.json", help="Output JSON filename.")
    parser.add_argument("--dry_run", action="store_true", help="Validate paths without calling the API.")
    return parser.parse_args()


def main():
    """
    Run GPT-4 benchmark end-to-end.

    Args:
        None

    Returns:
        None
    """
    args = parse_arguments()
    dataset = load_dataset(Path(args.dataset))
    if args.dry_run:
        print(f"Dry run complete. Loaded {len(dataset)} questions from {args.dataset}.")
        return
    client = build_client()
    evaluate_questions_with_gpt4(client, dataset, Path(args.output_dir), args.output_filename)

if __name__ == '__main__':
    main()

""" RESULTS: 4003/4590 correct answers
"""