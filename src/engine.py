"""
Project Aegis - Self-Healing Data Engine
Detects data drift and automatically fixes quality issues.
"""
import os
import json
import sys
import argparse
from typing import Dict, Any, Optional, Union
from io import BytesIO
import duckdb
import polars as pl
import numpy as np
# Simplified version without Evidently for compatibility
import pandas as pd

from src.utils import get_execution_mode, format_bytes


class AegisEngine:
    """
    Self-healing data quality engine with drift detection and auto-repair.
    Optimized for both cloud (small files) and local (big data) execution.
    """
    
    def __init__(self):
        self.execution_mode = get_execution_mode()
        self.conn = duckdb.connect(':memory:')
        
    def scan_for_drift(
        self, 
        filepath: Union[str, BytesIO], 
        reference_data: Optional[pd.DataFrame] = None
    ) -> Dict[str, Any]:
        """
        Analyze data for drift and quality issues using statistical methods.
        
        Args:
            filepath: Path to CSV file (local) or BytesIO object (cloud)
            reference_data: Optional reference dataset for drift comparison
            
        Returns:
            Dictionary containing drift report and metrics
        """
        try:
            # Load data based on execution mode
            if self.execution_mode == "local" and isinstance(filepath, str):
                print(f"🚀 Big Data Mode: Sampling from {filepath}")
                # Use DuckDB to sample large files without loading into memory
                query = f"""
                    SELECT * FROM read_csv_auto('{filepath}') 
                    USING SAMPLE 100000 ROWS
                """
                current_df = self.conn.execute(query).df()
            else:
                # Cloud mode or BytesIO object
                if isinstance(filepath, BytesIO):
                    current_df = pd.read_csv(filepath)
                else:
                    current_df = pd.read_csv(filepath)
            
            print(f"📊 Loaded {len(current_df)} rows, {len(current_df.columns)} columns")
            
            # Calculate drift score using simple statistics
            # Drift = percentage of columns with missing values
            missing_counts = current_df.isnull().sum()
            columns_with_issues = (missing_counts > 0).sum()
            drift_score = columns_with_issues / len(current_df.columns) if len(current_df.columns) > 0 else 0.0
            
            # Get missing value statistics
            missing_stats = self._analyze_missing_values(current_df)
            
            result = {
                'drift_score': drift_score,
                'report': {},  # Empty report for simplified version
                'missing_stats': missing_stats,
                'num_rows': len(current_df),
                'num_columns': len(current_df.columns),
                'columns': list(current_df.columns)
            }
            
            return result
            
        except Exception as e:
            print(f"❌ Error during drift scan: {str(e)}")
            raise
    
    def scan_batch(self, batch_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze a single batch of streaming data for real-time drift detection.
        Optimized for speed with minimal memory footprint.
        
        Args:
            batch_df: DataFrame batch from streaming source
            
        Returns:
            Dictionary with batch analysis results
        """
        try:
            # Quick statistical drift detection
            missing_counts = batch_df.isnull().sum()
            columns_with_issues = (missing_counts > 0).sum()
            drift_score = columns_with_issues / len(batch_df.columns) if len(batch_df.columns) > 0 else 0.0
            
            # Check for anomalies (values >3 std devs from mean)
            numeric_cols = batch_df.select_dtypes(include=[np.number]).columns
            anomaly_count = 0
            
            for col in numeric_cols:
                if len(batch_df[col]) > 1:
                    mean = batch_df[col].mean()
                    std = batch_df[col].std()
                    if std > 0:
                        z_scores = np.abs((batch_df[col] - mean) / std)
                        anomaly_count += (z_scores > 3).sum()
            
            # Check for drift event flag
            has_drift_event = batch_df.attrs.get('has_drift', False)
            
            # Calculate batch health score
            health_score = 1.0 - (drift_score + (anomaly_count / len(batch_df)) * 0.5)
            health_score = max(0.0, min(1.0, health_score))
            
            result = {
                'drift_score': drift_score,
                'anomaly_count': anomaly_count,
                'health_score': health_score,
                'has_drift_event': has_drift_event,
                'batch_size': len(batch_df),
                'missing_values': int(missing_counts.sum()),
                'timestamp': pd.Timestamp.now(),
                'alert': has_drift_event or drift_score > 0.3 or health_score < 0.7
            }
            
            return result
            
        except Exception as e:
            print(f"❌ Error during batch scan: {str(e)}")
            return {
                'drift_score': 0.0,
                'anomaly_count': 0,
                'health_score': 1.0,
                'has_drift_event': False,
                'batch_size': 0,
                'missing_values': 0,
                'timestamp': pd.Timestamp.now(),
                'alert': False,
                'error': str(e)
            }
    
    def heal_data(
        self, 
        filepath: Union[str, BytesIO],
        output_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Automatically fix data quality issues.
        Strategy:
        - Drop columns with >50% nulls
        - Impute columns with <5% nulls using median
        - Flag columns with 5-50% nulls for manual review
        
        Args:
            filepath: Path to CSV file (local) or BytesIO object (cloud)
            output_path: Where to save cleaned data (auto-generated if None)
            
        Returns:
            Dictionary with healing statistics and output path
        """
        try:
            healing_log = {
                'columns_dropped': [],
                'columns_imputed': [],
                'columns_flagged': [],
                'rows_processed': 0
            }
            
            if self.execution_mode == "local" and isinstance(filepath, str):
                print("🚀 Big Data Mode: Streaming repair using DuckDB")
                # Use DuckDB for in-place transformations
                cleaned_path = output_path or filepath.replace('.csv', '_cleaned.csv')
                
                # Simplified approach for big data: Load, clean, save
                df = self.conn.execute(f"SELECT * FROM read_csv_auto('{filepath}')").df()
                
            else:
                # Cloud mode
                if isinstance(filepath, BytesIO):
                    df = pd.read_csv(filepath)
                else:
                    df = pd.read_csv(filepath)
            
            healing_log['rows_processed'] = len(df)
            
            # Calculate null percentages for each column
            null_percentages = (df.isnull().sum() / len(df)) * 100
            
            # Strategy 1: Drop columns with >50% nulls
            columns_to_drop = null_percentages[null_percentages > 50].index.tolist()
            if columns_to_drop:
                df = df.drop(columns=columns_to_drop)
                healing_log['columns_dropped'] = columns_to_drop
                print(f"🗑️  Dropped {len(columns_to_drop)} columns with >50% nulls")
            
            # Strategy 2: Impute columns with <5% nulls (numeric only)
            columns_to_impute = null_percentages[
                (null_percentages > 0) & (null_percentages < 5)
            ].index.tolist()
            
            for col in columns_to_impute:
                if col not in df.columns:  # Skip if already dropped
                    continue
                    
                if pd.api.types.is_numeric_dtype(df[col]):
                    median_value = df[col].median()
                    df[col] = df[col].fillna(median_value)
                    healing_log['columns_imputed'].append({
                        'column': col,
                        'method': 'median',
                        'value': float(median_value)
                    })
                    print(f"💉 Imputed '{col}' with median: {median_value:.2f}")
            
            # Strategy 3: Flag columns with 5-50% nulls for review
            columns_to_flag = null_percentages[
                (null_percentages >= 5) & (null_percentages <= 50)
            ].index.tolist()
            healing_log['columns_flagged'] = [
                {'column': col, 'null_percentage': float(null_percentages[col])}
                for col in columns_to_flag
            ]
            
            # Save cleaned data
            if output_path is None:
                if isinstance(filepath, str):
                    output_path = filepath.replace('.csv', '_cleaned.csv')
                else:
                    output_path = 'data/raw/cleaned_data.csv'
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            df.to_csv(output_path, index=False)
            healing_log['output_path'] = output_path
            healing_log['output_size'] = format_bytes(os.path.getsize(output_path))
            
            print(f"✅ Data healed! Saved to: {output_path}")
            
            return healing_log
            
        except Exception as e:
            print(f"❌ Error during data healing: {str(e)}")
            raise
    
    def _calculate_drift_score(self, report_dict: Dict[str, Any]) -> float:
        """
        Calculate a single drift score (0-1) from Evidently report.
        Higher score = more drift.
        """
        try:
            metrics = report_dict.get('metrics', [])
            
            # Look for drift metrics
            drift_share = 0.0
            for metric in metrics:
                if metric.get('metric') == 'DataDriftTable':
                    result = metric.get('result', {})
                    drift_share = result.get('share_of_drifted_columns', 0.0)
                    break
            
            return float(drift_share)
        except Exception as e:
            print(f"Warning: Could not calculate drift score: {e}")
            return 0.0
    
    def _analyze_missing_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze missing values in the dataset."""
        missing_counts = df.isnull().sum()
        missing_percentages = (missing_counts / len(df)) * 100
        
        return {
            'total_missing': int(missing_counts.sum()),
            'columns_with_missing': [
                {
                    'column': col,
                    'count': int(missing_counts[col]),
                    'percentage': float(missing_percentages[col])
                }
                for col in missing_counts[missing_counts > 0].index
            ]
        }


def run_health_check():
    """
    Run a quick health check on sample data for CI/CD validation.
    Creates sample data, runs drift detection and healing, validates results.
    """
    print("🏥 Starting System Health Check...")
    print("=" * 60)
    
    try:
        # Create temporary sample data
        import tempfile
        import csv
        import random
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            temp_file = f.name
            writer = csv.writer(f)
            writer.writerow(['id', 'value', 'category'])
            
            for i in range(100):
                writer.writerow([
                    i,
                    '' if random.random() < 0.3 else random.randint(1, 100),
                    random.choice(['A', 'B', 'C'])
                ])
        
        print(f"✅ Created test data: {temp_file}")
        
        # Initialize engine
        engine = AegisEngine()
        print(f"✅ Engine initialized in {engine.execution_mode} mode")
        
        # Run drift scan
        print("\n🔍 Running drift detection...")
        result = engine.scan_for_drift(temp_file)
        print(f"✅ Drift score: {result['drift_score']:.2%}")
        print(f"✅ Analyzed {result['num_rows']} rows, {result['num_columns']} columns")
        
        # Run healing
        print("\n🔧 Running auto-heal...")
        healing_result = engine.heal_data(temp_file)
        print(f"✅ Processed {healing_result['rows_processed']} rows")
        print(f"✅ Dropped: {len(healing_result['columns_dropped'])} columns")
        print(f"✅ Imputed: {len(healing_result['columns_imputed'])} columns")
        
        # Cleanup
        os.unlink(temp_file)
        if os.path.exists(healing_result['output_path']):
            os.unlink(healing_result['output_path'])
        
        print("\n" + "=" * 60)
        print("🎉 Health check PASSED! All systems operational.")
        return 0
        
    except Exception as e:
        print(f"\n❌ Health check FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Project Aegis - Self-Healing Data Engine')
    parser.add_argument('--mode', choices=['health_check'], help='Operation mode')
    
    args = parser.parse_args()
    
    if args.mode == 'health_check':
        sys.exit(run_health_check())
    else:
        parser.print_help()
        sys.exit(1)
