import os
import json

from api_keys.api_keys import key_openai
from openai import OpenAI

client = OpenAI(api_key=key_openai)

output_dir = "../results/GPT4_Answers"
os.makedirs(output_dir, exist_ok=True)

with open("../data/all_questions_gpt_4.json", "r") as f:
    dataset = json.load(f)

def evaluate_questions_with_gpt4(questions, output_filename):
    results = []
    correct_count = 0

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

    with open(os.path.join(output_dir, output_filename), 'w') as f:
        json.dump(results, f, indent=4)

    print(f"Correct Answers: {correct_count}/{len(questions)}")
    return correct_count, len(questions)

def main():
    evaluate_questions_with_gpt4(dataset, "../data/gpt4_evaluation_results.json")

if __name__ == '__main__':
    main()

""" RESULTS: 4003/4590 correct answers
"""