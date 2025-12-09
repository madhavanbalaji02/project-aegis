"""
Quick test script to generate sample corrupted data for testing
"""
import csv
import random
import os

# Ensure directory exists
os.makedirs("data/raw", exist_ok=True)

output_path = "data/raw/test_sample.csv"

print(f"Generating test CSV file: {output_path}")

# Generate a simple corrupted dataset
with open(output_path, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    
    # Header
    writer.writerow(['id', 'value_A', 'value_B', 'category', 'score'])
    
    # 1000 rows with intentional issues
    for i in range(1000):
        row = [
            i,
            # value_A: 60% missing
            '' if random.random() < 0.6 else random.randint(1, 100),
            # value_B: 2% missing (good for imputation)
            '' if random.random() < 0.02 else random.gauss(50, 10),
            # category: normal
            random.choice(['A', 'B', 'C']),
            # score: 30% missing (flagged)
            '' if random.random() < 0.3 else random.uniform(0, 100)
        ]
        writer.writerow(row)

print(f"✅ Created {output_path} with 1000 rows")
print("Expected behavior:")
print("  - value_A: >50% nulls → will be DROPPED")
print("  - value_B: <5% nulls → will be IMPUTED")
print("  - score: 30% nulls → will be FLAGGED")
