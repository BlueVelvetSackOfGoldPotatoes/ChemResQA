import os
import json
import openai

from api_keys.api_keys import key_openai
from openai import OpenAI

client = OpenAI(api_key=key_openai)

def load_total_cost():
    try:
        with open("total_cost_4_gpt.py", "r") as f:
            globals = {}
            exec(f.read(), globals)
            return globals['TOTAL_COST']
    except FileNotFoundError:
        return 0  # Default to 0 if the file does not exist

def save_total_cost(total_cost):
    with open("total_cost_4_gpt.py", "w") as f:
        f.write(f"TOTAL_COST = {total_cost}\n")

def generate_questions(text, paper_name, path_q):
    """
    Generates multiple-choice questions from a given text by dividing the text into parts and
    processing each part separately to manage token limits.

    :param client: OpenAI GPT client.
    :param text: Text to generate questions from.
    :return: A list containing all generated questions.
    """

    if os.path.exists(path_q + paper_name + ".json"):
        print(f"Questions for {paper_name} already exist. Skipping generation.")
        print("="*50)
        return False
    
    print("*"*50)
    print("Preparing Q&A for: ", paper_name)
    print("*"*50)

    openai.api_key = key_openai

    prompt = f"""Given the following text, extract structured information in JSON format so as to create 10 multiple-choice questions with 4 options each and the correct answer and source of the answer based on the provided chemistry-related content.
    Focus solely on the concepts in the paper without addressing the paper explicitly - address only the content.
    
    Translate all questions into a json format. Base the questions on the content:
    {text}
    
    Only output the json, so brackets and whatever is inside, and commas between question entries. Nothing else. Don't forget to number the questions. You have to include sufficient information regarding an experiment or whatever your question is about. Assume the person has not read the paper.
    Avoid any questions about the authors or their opinion, the title, or study-specific data, or of the type:
    "<(...) based on the study's results/study/research/In the study's context/used in the study?>". Essentially, the questions must be abstracted from the study's results/study/research/used in the study.

    Format each question as follows:
    {{
        "Question_1": {{
        "Context":"<Explain the theory without referring to the paper or the study>",
        "Question": "<question text>",
        "A": "<option 1>",
        "B": "<option 2>",
        "C": "<option 3>",
        "D": "<option 4>",
        "Answer": "<correct option>",
        "Source": "<a literal transcription of at least 3 sentences from the text that justify your answer>"
        }}
    }}

    Dont include ''' 
    Only produce questions in the context of Organic Chemistry.
    """
    
    output_dir = "../data/Q&A_jsons_gpt_4"
    os.makedirs(output_dir, exist_ok=True)

    attempts = 0
    max_attempts = 3
    partial_output = ""

    while attempts < max_attempts:
        print("Attempts: ", attempts)

        if partial_output != "":
            text = text[int(len(text)/2):]
            prompt = f"""Given the following text, extract structured information in JSON format so as to create 10 multiple-choice questions with 4 options each and the correct answer and source of the answer based on the provided chemistry-related content.
            Focus solely on the concepts in the paper without addressing the paper explicitly - address only the content.
            
            Translate all questions into a json format. Base the questions on the content:
            {text}
            
            Continue from: {last_response} but print 10 questions again, or rather, the whole json. Don't forget to number the questions.
            Only output the json, so brackets and whatever is inside, and commas between question entries. Nothing else. You have to include sufficient information regarding an experiment or whatever your question is about. Assume the person has not read the paper.
            Avoid any questions about the authors or their opinion, the title, or study-specific data, or of the type:
            "<(...) based on the study's results/study/research/In the study's context/used in the study?>". Essentially, the questions must be abstracted from the study's results/study/research/used in the study.

            {{
                "Question_1": {{
                "Context":"<Explain the theory without referring to the paper or the study>",
                "Question": "<question text>",
                "A": "<option 1>",
                "B": "<option 2>",
                "C": "<option 3>",
                "D": "<option 4>",
                "Answer": "<correct option>",
                "Source": "<a literal transcription of at least 3 sentences from the text that justify your answer>"
                }}
            }}
            Dont include '''. 
            Only produce questions in the context of Organic Chemistry.
            """
        
        messages = [
            {"role": "system", "content": "You are a helpful assistant, skilled in extracting structured information from research papers and outputting it in JSON format."},
            {"role": "user", "content": prompt}
        ]

        try:
            TOTAL_COST = load_total_cost()
            completion = client.chat.completions.create(
                model="gpt-4-1106-preview",
                messages=messages,
                temperature=0.2
            )

            last_response = completion.choices[0].message.content
            partial_output = last_response

            print(last_response)
            print("="*10)
            print("Model")
            print(completion.model)
            print("="*10)
            print("Token count")
            print(completion.usage.completion_tokens + int(completion.usage.prompt_tokens))
            print("="*10)
            total_cost_paper = (int(completion.usage.completion_tokens) * 0.01 / 1000) + (int(completion.usage.prompt_tokens) * 0.03 / 1000) # https://openai.com/pricing
            TOTAL_COST += total_cost_paper
            print(f"Total cost for paper: {total_cost_paper}")
            print(f"Incremented cost: {TOTAL_COST}")
            print("="*10)
            save_total_cost(TOTAL_COST)
            
            # Check if we have 10 questions now by counting occurrences
            question_count = partial_output.count('"Question":')

            if question_count >= 10:
                parsed_response = json.loads(last_response)
                output_filename = os.path.join(output_dir, f"{paper_name}.json")
                with open(output_filename, "w") as json_file:
                    json.dump(parsed_response, json_file, indent=4)                 
                return parsed_response

        except json.JSONDecodeError as e:
            print(f"JSON decoding error: {e}. Retrying...")
            attempts += 1
        except TypeError as e:
            # This catches non-iterable responses
            print(f"Received a non-iterable response: {e}. Retrying...")
            attempts += 1
        except Exception as e:
            print(f"An unexpected error occurred: {e}. Retrying...")
            attempts += 1

    print(f"Failed to generate questions after {max_attempts} attempts.")
    exit()

def merge_and_reindex_questions(folder_path, output_file):
    all_questions = []
    question_index = 1
    count = 0
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.json'):
            file_path = os.path.join(folder_path, file_name)
            
            with open(file_path, 'r') as file:
                data = json.load(file)
                
                if isinstance(data, list):
                    for item in data:
                        for key, question in item.items():
                            count += 1
                            new_question = {
                                f"Question_{question_index}": question,
                                "doi": os.path.splitext(file_name)[0]
                            }
                            all_questions.append(new_question)
                            question_index += 1
                else:
                    for key, question in data.items():
                        count += 1
                        new_question = {
                            f"Question_{question_index}": question,
                            "doi": os.path.splitext(file_name)[0]
                        }
                        all_questions.append(new_question)
                        question_index += 1
    print(count)
    with open(output_file, 'w') as output_file:
        json.dump(all_questions, output_file, indent=4)

def main():

    all_questions = {}
    question_counter = 0

    for paper in os.listdir("../data/all_output"):
        if paper.endswith(".txt"):
            with open(os.path.join("../data/all_output", paper), "r") as f:
                text = f.read()

            paper_name = paper[:-4]

            qa = generate_questions(text, paper_name, "../data/Q&A_jsons_gpt_4/")

            if qa:
                for question in qa:
                    all_questions[f"question_{question_counter}"] = question
                    question_counter += 1

    with open('../data/all_questions_gpt_4.json', 'w') as file:
        json.dump(all_questions, file, indent=4)

    folder_path = '../data/Q&A_jsons_gpt_4'
    output_file = '../data/all_questions_gpt_4.json'
    merge_and_reindex_questions(folder_path, output_file)

if __name__ == "__main__":
    main()