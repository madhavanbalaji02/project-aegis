"""
🐒 Chaos Monkey - Data Corruption Generator
Generates large CSV files with intentional quality issues for testing Project Aegis.
"""
import csv
import random
import os
from datetime import datetime, timedelta

def generate_chaos_data(
    output_path: str = "data/raw/chaos_test_1gb.csv",
    num_rows: int = 10_000_000,  # ~1GB file
    num_columns: int = 20
):
    """
    Generate a large CSV file with intentional data quality issues.
    
    Issues injected:
    - Random missing values (0-80% per column)
    - Mixed data types in numeric columns
    - Outliers and anomalies
    - Duplicate rows
    - Inconsistent formats
    """
    
    print(f"🐒 Chaos Monkey activated!")
    print(f"📊 Generating {num_rows:,} rows × {num_columns} columns")
    print(f"💾 Target: {output_path}")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Define column types and corruption strategies
    columns = []
    corruption_levels = []
    
    for i in range(num_columns):
        col_type = random.choice(['numeric', 'categorical', 'datetime', 'text'])
        null_rate = random.uniform(0, 0.8)  # 0-80% missing
        
        columns.append({
            'name': f'col_{i}_{col_type}',
            'type': col_type,
            'null_rate': null_rate
        })
        corruption_levels.append(null_rate)
    
    print("\n📋 Column Corruption Profile:")
    for col in columns:
        print(f"  • {col['name']}: {col['null_rate']:.1%} nulls")
    
    # Generate data
    start_time = datetime.now()
    
    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        header = [col['name'] for col in columns]
        writer.writerow(header)
        
        # Write rows with corruption
        for row_num in range(num_rows):
            row = []
            
            for col in columns:
                # Random nulls based on corruption level
                if random.random() < col['null_rate']:
                    row.append('')
                else:
                    # Generate value based on type
                    if col['type'] == 'numeric':
                        # Sometimes inject text into numeric columns (type mismatch)
                        if random.random() < 0.05:
                            row.append('ERROR')
                        else:
                            # Occasionally add outliers
                            if random.random() < 0.02:
                                value = random.uniform(1e6, 1e9)  # Extreme outlier
                            else:
                                value = random.gauss(100, 25)  # Normal distribution
                            row.append(f"{value:.2f}")
                    
                    elif col['type'] == 'categorical':
                        # Limited set of categories with occasional typos
                        categories = ['A', 'B', 'C', 'D', 'E']
                        if random.random() < 0.03:
                            row.append('UNKNOWN')  # Rare category
                        else:
                            row.append(random.choice(categories))
                    
                    elif col['type'] == 'datetime':
                        # Random dates with occasional format inconsistencies
                        days_ago = random.randint(0, 365)
                        date = datetime.now() - timedelta(days=days_ago)
                        
                        if random.random() < 0.05:
                            # Wrong format
                            row.append(date.strftime('%d/%m/%Y'))
                        else:
                            # Standard format
                            row.append(date.strftime('%Y-%m-%d'))
                    
                    else:  # text
                        # Random text with occasional duplicates
                        if random.random() < 0.1:
                            row.append('DUPLICATE_VALUE')
                        else:
                            row.append(f'text_{random.randint(1, 1000)}')
            
            writer.writerow(row)
            
            # Progress indicator
            if (row_num + 1) % 1_000_000 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = (row_num + 1) / elapsed
                remaining = (num_rows - row_num - 1) / rate
                print(f"  ⏳ Progress: {row_num + 1:,}/{num_rows:,} rows "
                      f"({(row_num + 1) / num_rows * 100:.1f}%) - "
                      f"ETA: {remaining:.0f}s")
    
    # Final statistics
    file_size = os.path.getsize(output_path)
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print(f"\n✅ Chaos data generated successfully!")
    print(f"📁 File: {output_path}")
    print(f"💾 Size: {file_size / (1024**3):.2f} GB")
    print(f"⏱️  Time: {elapsed:.1f} seconds")
    print(f"🚀 Rate: {num_rows / elapsed:,.0f} rows/sec")
    
    # Print corruption summary
    avg_corruption = sum(corruption_levels) / len(corruption_levels)
    print(f"\n🔥 Corruption Summary:")
    print(f"  • Average null rate: {avg_corruption:.1%}")
    print(f"  • Columns with >50% nulls: {sum(1 for c in corruption_levels if c > 0.5)}")
    print(f"  • Columns with <5% nulls: {sum(1 for c in corruption_levels if c < 0.05)}")
    
    return output_path


def test_aegis_engine(chaos_file: str):
    """
    Test the AegisEngine on the generated chaos data.
    """
    print(f"\n{'='*60}")
    print("🛡 Testing Project Aegis on Chaos Data")
    print(f"{'='*60}\n")
    
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        
        from src.engine import AegisEngine
        
        engine = AegisEngine()
        
        # Scan for drift
        print("🔬 Scanning for drift...")
        drift_result = engine.scan_for_drift(chaos_file)
        
        print(f"\n📊 Drift Analysis Results:")
        print(f"  • Drift Score: {drift_result['drift_score']:.2%}")
        print(f"  • Rows Analyzed: {drift_result['num_rows']:,}")
        print(f"  • Columns: {drift_result['num_columns']}")
        print(f"  • Total Missing Values: {drift_result['missing_stats']['total_missing']:,}")
        
        # Heal data
        print(f"\n🔧 Activating self-healing protocols...")
        healing_result = engine.heal_data(chaos_file)
        
        print(f"\n✅ Healing Complete:")
        print(f"  • Rows Processed: {healing_result['rows_processed']:,}")
        print(f"  • Columns Dropped: {len(healing_result['columns_dropped'])}")
        print(f"  • Columns Imputed: {len(healing_result['columns_imputed'])}")
        print(f"  • Columns Flagged: {len(healing_result['columns_flagged'])}")
        print(f"  • Output: {healing_result['output_path']}")
        
        print(f"\n🎉 SUCCESS! Aegis handled {healing_result['rows_processed']:,} rows without crashing!")
        
    except Exception as e:
        print(f"\n❌ Error during Aegis test: {str(e)}")
        raise


if __name__ == "__main__":
    print("🐒 Chaos Monkey - Data Quality Destroyer\n")
    
    # Generate chaos data
    chaos_file = generate_chaos_data(
        output_path="data/raw/chaos_test_1gb.csv",
        num_rows=10_000_000,  # ~1GB
        num_columns=20
    )
    
    # Test Aegis engine
    test_aegis_engine(chaos_file)
    
    print(f"\n{'='*60}")
    print("🛡 Project Aegis: Mission Complete!")
    print(f"{'='*60}")
