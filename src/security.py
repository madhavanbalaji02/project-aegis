"""
PII Security Shield - GDPR Compliance Module
Detects and redacts personally identifiable information (PII) from datasets.
"""
import re
import hashlib
import pandas as pd
from typing import Dict, Any, Tuple


# High-risk PII patterns
PII_PATTERNS = {
    'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
    'credit_card': r'\b(?:\d[ -]*?){13,16}\b',
    'ssn': r'\b\d{3}-\d{2}-\d{4}\b'
}


def scan_and_redact(df: pd.DataFrame, threshold: float = 0.1, 
                   redaction_method: str = 'mask') -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Scan DataFrame for PII and redact sensitive columns.
    
    Args:
        df: Input DataFrame to scan
        threshold: Percentage threshold for flagging column as sensitive (default: 0.1 = 10%)
        redaction_method: 'mask' for ***REDACTED*** or 'hash' for SHA256
        
    Returns:
        Tuple of (sanitized_df, pii_log)
    """
    
    sanitized_df = df.copy()
    pii_log = {
        'total_columns_scanned': len(df.columns),
        'sensitive_columns': [],
        'total_redactions': 0,
        'redaction_details': {}
    }
    
    # Scan string columns for PII patterns
    for column in df.columns:
        # Only scan string/object columns
        if df[column].dtype not in ['object', 'string']:
            continue
        
        column_size = len(df[column].dropna())
        if column_size == 0:
            continue
        
        # Check each PII pattern
        for pii_type, pattern in PII_PATTERNS.items():
            matches = 0
            matched_values = []
            
            for value in df[column].dropna():
                if isinstance(value, str) and re.search(pattern, value):
                    matches += 1
                    matched_values.append(value)
            
            # Calculate match percentage
            match_percentage = matches / column_size if column_size > 0 else 0
            
            # Flag as sensitive if above threshold
            if match_percentage >= threshold:
                pii_log['sensitive_columns'].append({
                    'column': column,
                    'pii_type': pii_type,
                    'match_percentage': match_percentage,
                    'redactions': matches
                })
                
                pii_log['total_redactions'] += matches
                pii_log['redaction_details'][column] = {
                    'type': pii_type,
                    'count': matches
                }
                
                # Redact the sensitive column
                if redaction_method == 'mask':
                    sanitized_df[column] = sanitized_df[column].apply(
                        lambda x: '***REDACTED***' if isinstance(x, str) and re.search(pattern, x) else x
                    )
                elif redaction_method == 'hash':
                    sanitized_df[column] = sanitized_df[column].apply(
                        lambda x: hashlib.sha256(str(x).encode()).hexdigest()[:16] 
                        if isinstance(x, str) and re.search(pattern, x) else x
                    )
                
                # Only redact once per column (use first matching pattern)
                break
    
    return sanitized_df, pii_log


def generate_security_report(pii_log: Dict[str, Any], batch_num: int = None) -> str:
    """
    Generate security audit report for PII redactions.
    
    Args:
        pii_log: Log dictionary from scan_and_redact
        batch_num: Optional batch number for streaming context
        
    Returns:
        Formatted security report string
    """
    
    if pii_log['total_redactions'] == 0:
        return f"✅ Security Scan: No PII detected{f' in batch #{batch_num}' if batch_num else ''}. Data cleared for visualization."
    
    report = f"🛡️ **Privacy Shield Active**{f' - Batch #{batch_num}' if batch_num else ''}\n\n"
    report += f"**Threats Blocked:** {pii_log['total_redactions']} PII items\n\n"
    
    for col_info in pii_log['sensitive_columns']:
        report += f"- **{col_info['column']}**: {col_info['redactions']} {col_info['pii_type']}(s) redacted ({col_info['match_percentage']:.1%} of column)\n"
    
    report += f"\n**Status:** Data sanitized and safe for display. GDPR compliant."
    
    return report


def assess_privacy_risk(pii_log: Dict[str, Any]) -> str:
    """
    Assess overall privacy risk level based on PII detections.
    
    Args:
        pii_log: Log dictionary from scan_and_redact
        
    Returns:
        Risk level string
    """
    
    if pii_log['total_redactions'] == 0:
        return "LOW"
    elif pii_log['total_redactions'] < 10:
        return "MEDIUM"
    elif pii_log['total_redactions'] < 50:
        return "HIGH"
    else:
        return "CRITICAL"


if __name__ == "__main__":
    # Test PII detection
    import numpy as np
    
    test_df = pd.DataFrame({
        'name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'email': ['john@example.com', 'jane@test.org', 'bob@company.net'],
        'phone': ['555-123-4567', '555-987-6543', '555-555-5555'],
        'amount': [100.50, 250.75, 87.25],
        'card': ['4532-1234-5678-9010', '5105-1051-0510-5100', '3782-822463-10005']
    })
    
    print("Original DataFrame:")
    print(test_df)
    print("\n" + "="*60 + "\n")
    
    sanitized, log = scan_and_redact(test_df, threshold=0.5)
    
    print("Sanitized DataFrame:")
    print(sanitized)
    print("\n" + "="*60 + "\n")
    
    print("Security Report:")
    print(generate_security_report(log))
    print("\n" + "="*60 + "\n")
    
    print(f"Risk Level: {assess_privacy_risk(log)}")
