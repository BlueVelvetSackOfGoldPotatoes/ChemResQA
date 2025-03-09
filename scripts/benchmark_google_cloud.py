import json
import os
import time

# Load the dataset
with open("./chem_mqa_dataset.json", "r") as f:
    dataset = json.load(f)

models = [
    "text-bison@002",  # PaLM2
    "codechat-bison@002",  # Codey
    "meta/llama3",  # llama3
    "mistralai/Mistral-7B-v0.1",  # mistral
    "microsoft/biogpt",  # biogpt
    "lmsys/vicuna-7b-v1.5",  # vicuna
    "claude-3-opus@20240229"  # claude
]

quota_limit = 200  # Number of requests allowed per minute
wait_time = 60     # Time to wait in seconds when quota limit is hit
requests_count = 0
start_time = time.time()

# Function to generate content and handle quota errors
def generate_content_with_retry(prompt, model_id):
    global requests_count, start_time
    while True:
        try:
            # Simulate the model API call
            generated_answer = model.generate_content(model_id, prompt)
            requests_count += 1
            return generated_answer.text.strip()
        except Exception as e:
            if "Quota exceeded" in str(e):
                elapsed_time = time.time() - start_time
                if elapsed_time < wait_time:
                    time.sleep(wait_time - elapsed_time)
                requests_count = 0
                start_time = time.time()
            else:
                raise e

for model_id in models:
    results = []
    correct_count = 0

    # Process each question in the dataset
    for question_data in dataset:
        for question_id, details in question_data.items():
            if question_id.startswith("Question"):
                prompt = f"You are a multiple-choice question answering machine - you only answer with a letter out of A, B, C, and D, nothing else is outputted by you. You can only respond to this prompt with one letter, nothing else. This is a multiple-choice question. You must answer the following question by simply printing one of the following letters (A, B, C, or D). You shall not write anything else except the letter in your following response, no text whatsoever except for the letter. {details['Context']} {details['Question']} Choices: A: {details['A']}, B: {details['B']}, C: {details['C']}, D: {details['D']}."

                try:
                    generated_answer = generate_content_with_retry(prompt, model_id)

                    # Check if the answer is correct
                    is_correct = generated_answer.upper() == details['Answer'].upper()
                    if is_correct:
                        correct_count += 1

                    # Append the result to the results list
                    result = {
                        'question_id': question_id,
                        'prompt': prompt,
                        'generated_answer': generated_answer,
                        'correct_answer': details['Answer'],
                        'is_correct': is_correct
                    }
                    results.append(result)
                    print("="*25)

                except Exception as e:
                    print(f"Error with question {question_id}: {e}")

    # Write the current results to the output file
    output_file = f"./results_{model_id.replace('/', '-')}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=4)

    # Print results summary for each model
    print(f"Model: {model_id} - Correct Answers: {correct_count}/{len(dataset)}")
