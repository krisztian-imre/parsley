# File: gateio_test.py

import pandas as pd

# Load the TSV file into a DataFrame
filename = 'gateio_article_list.tsv'
df = pd.read_csv(filename, sep='\t')

# Change all 'No' to 'Yes' under the 'processed' column
df['processed'] = df['processed'].replace('Yes', 'No')

# Save the updated DataFrame back to the TSV file
df.to_csv(filename, sep='\t', index=False)

print(f"All 'No' values have been updated to 'Yes' in the 'processed' column.")
