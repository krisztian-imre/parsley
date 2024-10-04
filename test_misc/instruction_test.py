# File: gateio_test.py

import pandas as pd

# Load the TSV file into a DataFrame
filename = 'gateio_articles.tsv'
df = pd.read_csv(filename, sep='\t')

# Change all 'Yes' to 'No' under the 'llm_processed' column
df['llm_processed'] = df['llm_processed'].replace('Yes', 'No')

# Save the updated DataFrame back to the TSV file
df.to_csv(filename, sep='\t', index=False)

print(f"All 'Yes' values have been updated to 'No' in the 'llm_processed' column.")
