import argparse
import json
import csv
import matplotlib.pyplot as plt
import json

from nltk.corpus import stopwords
from wordcloud import WordCloud
from io import StringIO

def load_dataset(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def collect_text(data):
    text_data = []
    for item in data:
        text = ""
        if "Question_" in item:
            question_data = item["Question_"]
            context = question_data.get('Context', '')
            question = question_data.get('Question', '')
            answer = question_data.get('Answer', '')
            source = question_data.get('Source', '')
            text += f"{context} {question} {answer} {source} "
        
        if "related_data" in item:
            related_data = item["related_data"]
            keywords = ' '.join(related_data.get('Keywords', []))
            abstract = related_data.get('Abstract', '')
            methods = related_data.get('Methods', '')
            results = related_data.get('Results', '')
            exp_details = related_data.get('Experiment details', '')
            text += f"{keywords} {abstract} {methods} {results} {exp_details} "
        
        text_data.append(text.strip())  # Strip to remove any extra spaces from the start and end of the text
    return text_data

def generate_word_cloud(data, file_name, format_type='svg'):
    text_data = collect_text(data)
    nltk_stopwords = set(stopwords.words('english'))
    
    manual_remove = [
        "certain", "involved", "response", "surface", "particular", "study", "show", "results", "found", "result", "used", "using", "studies", "provide", "provide", "discuss", "investigate", "investigation", "provide", "demonstrate", "evaluate", "evaluate", "propose", "novel", "investigated", "found", "proposed", "discussed", "demonstrated", "reported", "shown", "research", "investigate", "investigating", "evaluated", "evaluate", "proposed", "proposes", "novel", "investigated", "demonstrated", "reported", "showed", "demonstrated", "investigate", "investigating", "proposed", "proposes", "novel", "investigated", "reported", "shown", "discuss", "discusses", "provide", "provides", "demonstrate", "demonstrates", "evaluate", "evaluates", "investigate", "investigates", "propose", "proposes", "novel", "novels", "investigated", "investigations", "demonstrated", "demonstrates", "reported", "reports", "shown", "shows", "discussing", "proposing", "investigated", "demonstrated", "reported", "showed", "discussed", "proposed", "novel", "investigated", "demonstrated", "reported", "shown", "discuss", "investigate", "evaluate", "propose", "novel", "research", "role", "including", "known", "context", "content", "use", "important", "different", "production", "effect", "affect", "method", "application", "compound", "structure", "activity", "form", "also", "role", "including", "known", "context", "content", "use", "important", "different", "production", "effect", "affect", "method", "application", "compound", "structure", "activity", "form", "also", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "process activities", "presence",
        "function", "involve", "compared", "component", "level", "increased", "highest", "methods", "higher", "influence", "identified", "formation", "refer", "due", "product", "analysis", "technique", "concentration", "within", "substance", "purpose", "various", "chemical", "properties", "play", "specific", "increase", "compounds", "process", "involves", "include", "seconday", "dose", "time", "indicating", "lower", "observed", "target", "respectively", "value", "material",
        "et al", "factor", "potential", "following", "produce", "system", "measure", "uptake", "term", "associated", "total", "change", "low", "ga", "one", "two", "three", "produced", "significantly", "components", "development", "characterized",
        "often", "expression", "effective", "mechanism", "growth", "interaction", "yield", "detection", "ability", "levels", "amount", "essential", "enhance", "chemistry", "part", "activities", "control", "model", "imaging", "crucial",
        "heme", "ratio", "changes", "commonly", "primary", "significant", "processes", "refers", "size", "stress", "defense", "induce", "materials", "therapeutic", "group", "high", "assay", "distribution", "treatment", "environmental", "like",
        "element", "molecule", "condition", "validation", "parameter", "substances", "however", "host plant", "heat", "resistance", "agent", "disease", "non", "may", "health", "parameters", "conducted", "experiment", "focused", "effects", "review", "importance", "work", "developed", "temperature", "analyzed", "employed", "applications", "performed", "day", "conditions", "present", "biological", "red", "impact", "days", "secondary"
    ]

    all_stopwords = nltk_stopwords.union(manual_remove) # .union(common_terms)
    
    wordcloud = WordCloud(stopwords=all_stopwords, background_color="black", mode="RGBA", width=800, height=400).generate(' '.join(text_data))
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    
    if format_type == 'png':
        plt.savefig(f"{file_name}.png", format='png')
    elif format_type == 'jpeg':
        plt.savefig(f"{file_name}.jpeg", format='jpeg')
    elif format_type == 'svg':
        plt.savefig(f"{file_name}.svg", format='svg')
    else:
        raise ValueError("Unsupported image format specified.")
    
    plt.close()

def check_correct_answers(result_data, question_data):
    """
    Dynamically checks the correctness of answers from multiple models.

    :param result_data: A dictionary or list of dictionaries containing model results.
    :param question_data: A list of dictionaries containing the original questions and correct answers.
    :return: None
    """
    model_results = {}
    
    for model_result in result_data:
        model_name = model_result.get('model_name', 'Unknown Model')
        correct_answers = 0
        for question in question_data:
            question_id = question.get('id')
            correct_answer = question.get('Answer')
            model_answer = model_result.get(question_id, {}).get('generated_answer')
            
            if model_answer == correct_answer:
                correct_answers += 1
        
        total_questions = len(question_data)
        accuracy = correct_answers / total_questions * 100
        model_results[model_name] = {'correct': correct_answers, 'total': total_questions, 'accuracy': f"{accuracy:.2f}%"}
    
    for model, results in model_results.items():
        print(f"Model: {model}")
        print(f"Correct Answers: {results['correct']}/{results['total']} ({results['accuracy']})")
        print("-" * 50)
        
def filter_questions(data, include_keywords=None, exclude_keywords=None, fields=None, max_results=None, case_sensitive=False, output_format='json'):
    """
    Filter questions based on inclusion or exclusion of keywords in specified fields and limit the number of results.

    Parameters:
    - data (list): List of dictionaries containing the dataset.
    - include_keywords (list): List of keywords to include in the results.
    - exclude_keywords (list): List of keywords to exclude from the results.
    - fields (list or None): List of fields to search in. If None, searches all text fields.
    - max_results (int or None): Maximum number of questions to return. If None, returns all matching questions.
    - case_sensitive (bool): Boolean indicating if the search should be case sensitive.
    - output_format (str): The format of the output file ('json', 'csv', 'txt').

    Returns:
    - filtered_data (str): Filtered data in the specified output format.

    Use Case:
        Load dataset from URL:
        >>> data = load_dataset('https://raw.githubusercontent.com/BlueVelvetSackOfGoldPotatoes/chem-aca-q-a/main/data/chem_mqa_dataset.json')

        Filter questions and limit results:
        >>> filtered_data = filter_questions(
        ...     data,
        ...     include_keywords=['nanoparticle', 'uptake'],
        ...     exclude_keywords=['PLGA'],
        ...     fields=['Context', 'Question'],
        ...     max_results=10,  # Limit the results to 10 questions
        ...     case_sensitive=False,
        ...     output_format='csv'
        ... )

        Print or use filtered data:
        >>> print(filtered_data)
    """
    filtered_data = []
    for item in data:
        if max_results is not None and len(filtered_data) >= max_results:
            break  # Stop searching once the maximum number of results is reached
        for question_key, question_value in item.items():
            if isinstance(question_value, dict):
                content_to_search = {field: question_value.get(field, '') for field in (fields if fields else question_value.keys())}
                content_string = ' '.join(content_to_search.values())
                content_string = content_string if case_sensitive else content_string.lower()

                if include_keywords and not any((keyword if case_sensitive else keyword.lower()) in content_string for keyword in include_keywords):
                    continue  # Skip this item if no include keywords are found
                if exclude_keywords and any((keyword if case_sensitive else keyword.lower()) in content_string for keyword in exclude_keywords):
                    continue  # Skip this item if any exclude keywords are found

                filtered_data.append(item)
                break  # Break to avoid duplicating the same item if multiple fields match

    return format_output(filtered_data, output_format)

def format_output(data, format_type):
    if format_type == 'json':
        return json.dumps(data, indent=4)
    elif format_type == 'csv':
        output = StringIO()
        if data:
            keys = data[0].keys()
            writer = csv.DictWriter(output, fieldnames=keys)
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        return output.getvalue()
    elif format_type == 'txt':
        return '\n'.join(json.dumps(item, indent=4) for item in data)
    else:
        raise ValueError("Unsupported format specified.")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Process chemistry questions from datasets.")
    subparsers = parser.add_subparsers(dest='command')

    # Subparser for filtering
    filter_parser = subparsers.add_parser('filter', help='Filter questions from the dataset')
    filter_parser.add_argument('--data_file', type=str, required=True, help='Path to the JSON file containing the dataset')
    filter_parser.add_argument('--include_keywords', type=str, nargs='+', help='Keywords to include in the results')
    filter_parser.add_argument('--exclude_keywords', type=str, nargs='+', help='Keywords to exclude from the results')
    filter_parser.add_argument('--fields', type=str, nargs='+', help='Fields to search in the dataset')
    filter_parser.add_argument('--max_results', type=int, help='Maximum number of questions to return')
    filter_parser.add_argument('--case_sensitive', action='store_true', help='Enable case-sensitive search')
    filter_parser.add_argument('--output_format', type=str, choices=['json', 'csv', 'txt'], default='json', help='Output format of the filtered data')

    # Subparser for generating word cloud
    wc_parser = subparsers.add_parser('wordcloud', help='Generate a word cloud from the dataset')
    wc_parser.add_argument('--data_file', type=str, required=True, help='Path to the JSON file containing the dataset')
    wc_parser.add_argument('--output_file', type=str, required=True, help='File name for the output image')
    wc_parser.add_argument('--format', type=str, choices=['png', 'jpeg', 'svg'], default='png', help='Image format for the word cloud')

    # Subparser for checking correctness
    check_parser = subparsers.add_parser('check', help='Check the correctness of answers')
    check_parser.add_argument('--result_file', type=str, required=True, help='Path to the JSON file containing evaluation results')
    check_parser.add_argument('--data_file', type=str, required=True, help='Path to the JSON file containing the original questions')
    
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    
    if args.command == 'filter':
        data = load_dataset(args.data_file)
        filtered_data = filter_questions(data, include_keywords=args.include_keywords, exclude_keywords=args.exclude_keywords, fields=args.fields, max_results=args.max_results, case_sensitive=args.case_sensitive)
        output = format_output(filtered_data, args.output_format)
        print(output)
    elif args.command == 'wordcloud':
        data = load_dataset(args.data_file)
        generate_word_cloud(data, args.output_file, args.format)
    elif args.command == 'check':
        result_data = load_dataset(args.result_file)
        question_data = load_dataset(args.data_file)
        check_correct_answers(result_data, question_data)
    else:
        print("Invalid command. Please use 'filter', 'wordcloud', or 'check'.")