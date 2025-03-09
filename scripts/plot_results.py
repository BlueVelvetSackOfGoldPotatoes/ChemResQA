import os
import pandas as pd

path = './results/HuggingFace'  # Updated path for clarity
files = os.listdir(path)
dataframes = []
model_labels = {}
label_count = 65  # ASCII for 'A'

for file in files:
    if file.endswith('.csv'):
        df = pd.read_csv(os.path.join(path, file))
        model_label = 'Model ' + chr(label_count)
        model_labels[file.split('.')[0]] = model_label
        df['model'] = model_label
        dataframes.append(df)
        label_count += 1

data = pd.concat(dataframes, ignore_index=True)

# Step 2: Data preparation
data['is_incorrect'] = ~data['is_correct'] & ~data['is_unparsable']

# Step 3: Create summary table for counts
summary_counts = data.groupby('model')[['is_correct', 'is_incorrect', 'is_unparsable']].sum()
summary_counts.columns = ['Correct', 'Incorrect', 'Unparsable']

# Step 4: Calculate standard deviation
std_dev = data.groupby('model')[['is_correct', 'is_incorrect', 'is_unparsable']].std()
std_dev.columns = ['Std Dev Correct', 'Std Dev Incorrect', 'Std Dev Unparsable']

# Step 5: Merge tables and print
summary_table = pd.concat([summary_counts, std_dev], axis=1)
print("Summary Table of Responses per Model:")
print(summary_table)

# Optionally, save to CSV
summary_table.to_csv('model_summary_table.csv', index=True)