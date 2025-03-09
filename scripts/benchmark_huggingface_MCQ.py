import os
import json
import matplotlib.pyplot as plt
import numpy as np
import csv
from transformers import pipeline
from tqdm import tqdm
import torch

"""TODO
- We need to save the model's answer.
- Read how they are parsing their answers / how they query the models with multiple choice questions.
- Look into how bigbench is handeling multiple choice questions.
- Make a google form for catalysis with a sample of questions (different questions per person - do a python program that evalutes answers - does this question make sense? - Yes / No) - Flask application self hosted and let people answer (random questions)
- For every question just ask if it is true or false - this might make it parsable.
"""

device = 0 if torch.cuda.is_available() else -1
model_types = {
    "question-answering": [
        "deepset/roberta-base-squad2",
        "deepset/bert-large-uncased-whole-word-masking-squad2",
        "distilbert/distilbert-base-cased-distilled-squad",
        "google-bert/bert-large-uncased-whole-word-masking-finetuned-squad",
        "monologg/koelectra-small-v2-distilled-korquad-384",
        "valhalla/bart-large-finetuned-squadv1",
        "trod/electra_large_discriminator_squad2_512",
        "distilbert/distilbert-base-uncased-distilled-squad",
        "deepset/minilm-uncased-squad2",
        "deepset/tinyroberta-squad2"
    ],
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

def evaluate_model(model_name, modality, questions):
    classifier = pipeline(modality, model=model_name, device=device)
    results = []
    correct_count = 0
    wrong_count = 0
    unparsable_count = 0

    for question_data in tqdm(questions, desc="Processing Questions"):
        for question_id, details in question_data.items():
            if question_id.startswith("Question"):
                prompt = f"{details['Context']} {details['Question']}"
                choices = [details['A'], details['B'], details['C'], details['D']]
                expected_answer = details['Answer']

                try:
                    if modality == "question-answering":
                        result = classifier(question=prompt, context=" ".join(choices))
                        generated_answer = max(choices, key=lambda choice: result['answer'] in choice)
                    
                    elif modality == "zero-shot-classification":
                        result = classifier(prompt,
                            candidate_labels=choices,
                        )
                        max_score_index = result['scores'].index(max(result['scores']))
                        generated_answer = ['A', 'B', 'C', 'D'][max_score_index]

                    elif modality == "text-generation":
                        options = ['A', 'B', 'C', 'D']
                        completion_prompt = prompt + "".join([f"{options[i]}) {choices[i]}\n" for i in range(len(options))])

                        max_choice_length = max(len(choice) for choice in choices)
                        max_length = max_choice_length + 10

                        result = classifier(completion_prompt, max_length=max_length, num_return_sequences=1)
                        generated_answer = result[0]['generated_text'][0]

                    if generated_answer not in ['A', 'B', 'C', 'D']:
                        generated_answer = 'Unparsable'
                        unparsable_count += 1
                    elif generated_answer == expected_answer:
                        correct_count += 1
                    else:
                        wrong_count += 1

                    results.append({
                        'question_id': question_id,
                        'prompt': prompt,
                        'generated_answer': generated_answer,
                        'is_correct': generated_answer == expected_answer,
                        'is_unparsable': generated_answer == 'Unparsable'
                    })

                except Exception as e:
                    print(f"Error with question {question_id}: {e}")

    return correct_count, wrong_count, unparsable_count, results

def main():
    with open("./data/chem_mqa_dataset.json", "r") as f:
        dataset = json.load(f)

    overall_stats = {}
    os.makedirs('./results/HuggingFace', exist_ok=True)
    unavailable_models_file = "./results/HuggingFace/unavailable_models.txt"

    for modality, models in model_types.items():
        modality_results = []
        for model_name in tqdm(models, desc=f"Evaluating models for {modality}"):
            try:
                print(f"Evaluating {model_name} with modality {modality}")
                correct_count, wrong_count, unparsable_count, results = evaluate_model(model_name, modality, dataset)
                modality_results.append({
                    'model_name': model_name,
                    'correct': correct_count,
                    'wrong': wrong_count,
                    'unparsable': unparsable_count
                })

                # Save individual model results to CSV
                csv_filename = f"./results/HuggingFace/{model_name.replace('/', '_')}_results.csv"
                with open(csv_filename, 'w', newline='') as csvfile:
                    fieldnames = ['question_id', 'prompt', 'generated_answer', 'is_correct', 'is_unparsable']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                    writer.writeheader()
                    for result in results:
                        writer.writerow(result)
            except Exception as e:
                print(f"Failed to evaluate model {model_name} due to: {e}")
                with open(unavailable_models_file, "a") as log_file:
                    log_file.write(f"{model_name} could not be loaded.\n")

        overall_stats[modality] = modality_results

        # Plotting and saving results with standard deviation
        labels = [res['model_name'].split('/')[-1] for res in modality_results]
        corrects = np.array([res['correct'] for res in modality_results])
        wrongs = np.array([res['wrong'] for res in modality_results])
        unparsables = np.array([res['unparsable'] for res in modality_results])

        mean_correct = np.mean(corrects)
        mean_wrongs = np.mean(wrongs)
        mean_unparsables = np.mean(unparsables)

        std_dev_correct = np.std(corrects)
        std_dev_wrongs = np.std(wrongs)
        std_dev_unparsables = np.std(unparsables)

        x = np.arange(len(labels))  # the label locations
        width = 0.25  # the width of the bars

        fig, ax = plt.subplots()
        rects1 = ax.bar(x - width, corrects, width, label='Correct', color='green', yerr=std_dev_correct)
        rects2 = ax.bar(x, wrongs, width, label='Wrong', color='red', yerr=std_dev_wrongs)
        rects3 = ax.bar(x + width, unparsables, width, label='Unparsable', color='blue', yerr=std_dev_unparsables)

        ax.set_ylabel('Counts')
        ax.set_title(f'Performance of {modality} Models')
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=45, ha="right")
        ax.legend()

        plot_filename = f"./results/HuggingFace/{modality}_performance.png"
        plt.tight_layout()
        plt.savefig(plot_filename)
        plt.close()

    # Save overall statistics
    with open('./results/HuggingFace/overall_stats.json', 'w') as f:
        json.dump(overall_stats, f, indent=4)

if __name__ == '__main__':
    main()
