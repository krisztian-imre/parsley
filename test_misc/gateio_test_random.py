# File: gateio_test_random.py

import pandas as pd

# Load the TSV file into a DataFrame
filename = 'gateio_article_list.tsv'
df = pd.read_csv(filename, sep='\t')

# Randomly select 90% of rows where 'processed' is 'No'
no_rows = df[df['processed'] == 'No']  # Select all rows where 'processed' is 'No'
sampled_rows = no_rows.sample(frac=0.85, random_state=1)  # Randomly select frac % of those rows

# Set 'processed' to 'Yes' for the selected rows
df.loc[sampled_rows.index, 'processed'] = 'Yes'

# Save the updated DataFrame back to the TSV file
df.to_csv(filename, sep='\t', index=False)

print(f"90% of 'No' values have been updated to 'Yes' in the 'processed' column.")
