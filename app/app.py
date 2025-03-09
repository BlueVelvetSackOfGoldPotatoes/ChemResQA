from flask import Flask, render_template, request, session, redirect, url_for
import pandas as pd
import os
import json
import random
import uuid

app = Flask(__name__)
app.secret_key = str(os.urandom(16).hex())

with open("data/chem_mqa_dataset.json", "r") as f:
    questions = json.load(f)

def initialize_dataframe(path):
    pd.DataFrame(columns=['Question', 'Selected', 'Correct', 'QuestionKey', 'QuestionIndex', 'QuestionQuality']).to_csv(path, index=False)

@app.route('/')
def home():
    session.clear()
    session['user_id'] = str(uuid.uuid4())  # Generate a unique session ID for each user
    csv_path = f'data/human_answers_{session["user_id"]}.csv'
    initialize_dataframe(csv_path)  # Initialize a new CSV file for this user
    
    random_indices = random.sample(range(len(questions)), 10)
    session['question_indices'] = random_indices
    session['current_index'] = 0
    return render_template('index.html')

@app.route('/question', methods=['GET', 'POST'])
def question():
    csv_path = f'data/human_answers_{session["user_id"]}.csv'
    
    def find_question_key(data):
        for key in data:
            if key.startswith('Question'):
                return key
        return None

    question_indices = session.get('question_indices', [])
    current_index = session.get('current_index', 0)
    question_index = question_indices[current_index]
    question_key = find_question_key(questions[question_index])

    if request.method == 'POST':
        selected_option = request.form.get('option')
        question_quality = request.form.get('question_quality', 'Not Answered')
        
        if selected_option:
            df = pd.read_csv(csv_path)
            update_dict = {
                'Selected': selected_option,
                'Correct': selected_option == questions[question_index][question_key]['Answer'],
                'QuestionQuality': question_quality
            }

            if question_index in df['QuestionIndex'].values:
                df.loc[df['QuestionIndex'] == question_index, list(update_dict.keys())] = list(update_dict.values())
            else:
                new_row = pd.DataFrame([{
                    'Question': question_key,
                    'Selected': selected_option,
                    'Correct': selected_option == questions[question_index][question_key]['Answer'],
                    'QuestionQuality': question_quality,
                    'QuestionIndex': question_index
                }])
                df = pd.concat([df, new_row], ignore_index=True)
            df.to_csv(csv_path, index=False)

            if current_index + 1 < len(question_indices):
                session['current_index'] += 1
                return redirect(url_for('question'))
            else:
                return redirect(url_for('finish'))
        else:
            return render_template('question.html', question=questions[question_index][question_key],
                                   question_index=question_index, error="Please select an option and provide feedback on the question.")

    return render_template('question.html', question=questions[question_index][question_key], question_index=question_index)

@app.route('/finish', methods=['GET'])
def finish():
    csv_path = f'data/human_answers_{session["user_id"]}.csv'
    df = pd.read_csv(csv_path)
    correct_count = df['Correct'].sum()
    return render_template('finish.html', score=correct_count)

if __name__ == '__main__':
    app.run(debug=True)
