import json
import csv
from tqdm import tqdm
from transformers import pipeline
import torch
import os
import logging

from api_keys.api_keys import key_openai
from openai import OpenAI

# Setting up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

client = OpenAI(api_key=key_openai)

device = 0 if torch.cuda.is_available() else -1
logging.debug(f"Using device: {device}")

model_types = {
    "zero-shot-classification": [
        "facebook/bart-large-mnli",
        "cross-encoder/nli-roberta-base",
        "MoritzLaurer/DeBERTa-v3-base-mnli-fever-anli",
        "MoritzLaurer/mDeBERTa-v3-base-xnli-multilingual-nli-2mil7",
        "cointegrated/rubert-base-cased-nli-threeway",
        "cross-encoder/nli-deberta-v3-large",
        "sileod/deberta-v3-small-tasksource-nli",
        "cross-encoder/nli-deberta-base",
        "cross-encoder/nli-distilroberta-base",
        "MoritzLaurer/deberta-v3-base-zeroshot-v1.1-all-33"
    ],
    "text-generation": [
        "CHE-72-ZLab/Alibaba-Qwen2-7B-Instruct-GGUF",
        "TheBloke/Mistral-7B-Claude-Chat-GGUF",
        "TheBloke/Llama-2-70B-Chat-GGUF",
        "TheBloke/WizardLM-1.0-Uncensored-CodeLlama-34B-GGUF",
        "TheBloke/Wizard-Vicuna-7B-Uncensored-GPTQ",
        "TheBloke/Wizard-Vicuna-13B-Uncensored-GPTQ",
        "mistralai/Mistral-7B-Instruct-v0.2",
        "google/gemma-2-9b",
        "google/gemma-2-27b",
        "facebook/opt-125m",
        "meta-llama/Meta-Llama-3-8B-Instruct",
        "meta-llama/Llama-2-13b-chat-hf",
        "Qwen/Qwen-VL-Chat",
        "petals-team/StableBeluga2",
        "tiiuae/falcon-7b-instruct",
        "meta-llama/Llama-2-7b-hf",
        "microsoft/Phi-3-mini-128k-instruct",
        "meta-llama/Meta-Llama-3-8B"
    ]
}

def gpt_evaluate(question, model_answer):
    full_prompt = [
        {"role": "system", "content": "Classify the response as 'True' or 'False'."},
        {"role": "user", "content": f"Question: {question}. Model's response: {model_answer}"},
    ]
    try:
        completion = client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        prompt=full_prompt,
                        max_tokens=5,
                        temperature=0.0
                    )
        return completion.choices[0].message.content.strip().lower()
    
    except Exception as e:
        logging.error(f"GPT evaluation failed: {str(e)}")
        return None

def evaluate_model(model_name, modality, questions):
    logging.debug(f"Starting evaluation for model {model_name} with modality {modality}")
    try:
        classifier = pipeline(modality, model=model_name, device=device)
    except Exception as e:
        logging.error(f"Error initializing model {model_name} with modality {modality}: {e}")
        return 0, 0, 0, [], []

    results = []
    gpt_results = []
    correct_count = 0
    wrong_count = 0
    unparsable_count = 0

    for question_data in tqdm(questions, desc="Processing Questions"):
        for question_id, details in question_data.items():
            if question_id.startswith("Question"):
                logging.debug(f"The question: \n {details}")
                logging.debug("="*50)
                context = details['Context']
                question = details['Question']
                expected_answer = details['Answer']

                for option_label, choice in zip(['A', 'B', 'C', 'D'], [details['A'], details['B'], details['C'], details['D']]):
                    prompt = f"{context} {question}. Is the following statement true or false?"
                    choices = ["true", "false"]

                    try:                        
                        if modality == "zero-shot-classification":
                            result = classifier(prompt, candidate_labels=choices)
                            max_score_index = result['scores'].index(max(result['scores']))
                            generated_answer = choices[max_score_index]

                        elif modality == "text-generation":
                            result = classifier(prompt, num_return_sequences=1)
                            generated_answer = result[0]['generated_text'][0]
                            gpt_classification = gpt_evaluate(details, choice + " is " + generated_answer)
                            logging.debug(f"Model {model_name}: GPT-4 Classification for Question {question_id}, Choice {option_label}: {gpt_classification}")


                            gpt_results.append({
                                'question_id': question_id,
                                'choice_label': option_label,
                                'prompt': prompt,
                                'generated_answer': generated_answer,
                                'gpt_classification': gpt_classification
                            })
                        
                        print(f"Result: {result}")

                        correct_answer = "true" if option_label == expected_answer else "false"
                        is_correct = generated_answer == correct_answer

                        logging.debug(f"Model {model_name}: Question {question_id}, Choice {option_label}, Generated Answer: {generated_answer}, Is Correct: {is_correct}")

                        if generated_answer not in ["true", "false"]:
                            generated_answer = 'Unparsable'
                            unparsable_count += 1
                        elif is_correct:
                            correct_count += 1
                        else:
                            wrong_count += 1

                        results.append({
                            'question_id': question_id,
                            'choice_label': option_label,
                            'prompt': prompt,
                            'generated_answer': generated_answer,
                            'is_correct': is_correct
                        })

                    except Exception as e:
                        logging.error(f"Error processing question {question_id} in model {model_name}: {e}")

    return correct_count, wrong_count, unparsable_count, results, gpt_results

def main(save_files=1):
    logging.debug("Loading dataset")
    with open("./data/chem_mqa_dataset.json", "r") as f:
        dataset = json.load(f)

    overall_stats = {}
    if save_files:
        os.makedirs('./results/Binary', exist_ok=True)

    for modality, models in model_types.items():
        modality_results = []
        for model_name in tqdm(models, desc=f"Evaluating models for {modality}"):
            try:
                correct_count, wrong_count, unparsable_count, results, gpt_results = evaluate_model(model_name, modality, dataset)
                modality_results.append({
                    'model_name': model_name,
                    'correct': correct_count,
                    'wrong': wrong_count,
                    'unparsable': unparsable_count
                })

                if save_files:
                    csv_filename = f"./results/Binary/{model_name.replace('/', '_')}_results.csv"
                    gpt_csv_filename = f"./results/Binary/{model_name.replace('/', '_')}_gpt4_results.csv"
                    save_results(csv_filename, results)
                    save_results(gpt_csv_filename, gpt_results)

            except Exception as e:
                logging.error(f"Failed to evaluate model {model_name} due to: {e}")

        overall_stats[modality] = modality_results

    if save_files:
        with open('./results/Binary/overall_stats.json', 'w') as f:
            json.dump(overall_stats, f, indent=4)

def save_results(filename, data):
    logging.debug(f"Saving results to {filename}")
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in data:
            writer.writerow(result)

if __name__ == '__main__':
    main(1)