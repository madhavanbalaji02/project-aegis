"""
Stream Simulator - Simulated Kafka Data Generator
Generates real-time transaction data batches for Project Aegis streaming mode.
"""
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def generate_live_batch(batch_size: int = 50, drift_probability: float = 0.1) -> pd.DataFrame:
    """
    Generate a micro-batch of simulated transaction data with geospatial coordinates.
    
    Args:
        batch_size: Number of rows to generate (default: 50)
        drift_probability: Probability of injecting a drift event (default: 0.1)
        
    Returns:
        DataFrame with transaction data including NYC coordinates
    """
    
    # NYC center coordinates
    NYC_LAT = 40.7128
    NYC_LON = -74.0060
    COORD_STD = 0.01  # ~1km scatter
    
    # Determine if this batch has a drift event
    has_drift = random.random() < drift_probability
    
    # Base data generation
    transactions = []
    base_time = datetime.now()
    
    for i in range(batch_size):
        # Normal transaction values
        amount = np.random.normal(100, 25)
        fee = amount * 0.02
        
        # Generate NYC coordinates with realistic scatter
        lat = np.random.normal(NYC_LAT, COORD_STD)
        lon = np.random.normal(NYC_LON, COORD_STD)
        
        # Inject drift event (spike by 500%)
        if has_drift and i > batch_size // 2:  # Drift in second half of batch
            amount *= 6.0  # 500% spike
            fee *= 6.0
        
        transaction = {
            'timestamp': base_time + timedelta(milliseconds=i * 20),
            'transaction_id': f'TXN_{int(datetime.now().timestamp() * 1000) + i}',
            'amount': max(0, amount),
            'fee': max(0, fee),
            'latitude': lat,
            'longitude': lon,
            'status': random.choice(['completed', 'pending', 'failed'] if not has_drift else ['completed', 'pending']),
            'category': random.choice(['payment', 'transfer', 'withdrawal', 'deposit']),
            'risk_score': np.random.uniform(0, 1) if not has_drift else np.random.uniform(0.7, 1.0)
        }
        
        # Add occasional missing values
        if random.random() < 0.05:
            transaction['fee'] = None
        
        transactions.append(transaction)
    
    df = pd.DataFrame(transactions)
    
    # Add metadata flag
    df.attrs['has_drift'] = has_drift
    
    # User Review Column - Semantic Drift Detection
    # Normal positive reviews
    normal_reviews = [
        "Great service, highly recommend!",
        "Fast and efficient app",
        "Excellent customer support",
        "Very satisfied with the transaction",
        "Quick processing time",
        "User-friendly interface",
        "Reliable and secure platform",
        "Best payment app I've used",
        "Smooth transaction experience",
        "Professional and trustworthy"
    ]
    
    # Poisoned text (SQL injection patterns, spam)
    poisoned_reviews = [
        "Buy crypto now! Limited offer!",
        "'; DROP TABLE users; --",
        "Click here for free money!!!",
        "<script>alert('XSS')</script>",
        "URGENT: Your account has been compromised",
        "1=1 OR 1=1; DELETE FROM transactions",
        "*** SPAM *** Earn $1000 today!",
        "admin'--",
        "Contact us at fake-phishing-site.com",
        "Investment opportunity! Act now!"
    ]
    
    if has_drift:
        # Mix normal and poisoned reviews during drift
        df['user_review'] = [
            random.choice(poisoned_reviews if random.random() < 0.6 else normal_reviews)
            for _ in range(len(df))
        ]
        df.attrs['has_semantic_drift'] = True
    else:
        # Only normal reviews
        df['user_review'] = [random.choice(normal_reviews) for _ in range(len(df))]
        df.attrs['has_semantic_drift'] = False
    
    # PII Leak Events (5% chance each)
    pii_leak_email = random.random() < 0.05
    pii_leak_card = random.random() < 0.05
    
    if pii_leak_email:
        # Generate fake email addresses
        domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'company.com', 'test.org']
        names = ['john.doe', 'jane.smith', 'bob.johnson', 'alice.wilson', 'mike.brown', 
                 'sarah.davis', 'tom.miller', 'emma.garcia', 'chris.martinez', 'lisa.anderson']
        
        df['user_email'] = [
            f"{random.choice(names)}{random.randint(1, 999)}@{random.choice(domains)}"
            for _ in range(len(df))
        ]
        df.attrs['has_pii_leak'] = True
    
    if pii_leak_card:
        # Generate fake credit card numbers (Luhn algorithm valid format)
        def generate_fake_card():
            # Common card prefixes
            prefixes = ['4532', '5105', '3782', '6011']
            prefix = random.choice(prefixes)
            # Generate remaining digits
            remaining = ''.join([str(random.randint(0, 9)) for _ in range(12)])
            card = prefix + remaining
            # Format with dashes
            return f"{card[0:4]}-{card[4:8]}-{card[8:12]}-{card[12:16]}"
        
        df['credit_card'] = [generate_fake_card() for _ in range(len(df))]
        df.attrs['has_pii_leak'] = True
    
    return df


def get_batch_stats(df: pd.DataFrame) -> dict:
    """
    Calculate statistics for a batch.
    
    Args:
        df: DataFrame batch
        
    Returns:
        Dictionary with batch statistics
    """
    return {
        'batch_size': len(df),
        'avg_amount': df['amount'].mean(),
        'max_amount': df['amount'].max(),
        'min_amount': df['amount'].min(),
        'null_count': df.isnull().sum().sum(),
        'has_drift': df.attrs.get('has_drift', False),
        'risk_score_avg': df['risk_score'].mean(),
        'failed_txns': (df['status'] == 'failed').sum()
    }


if __name__ == "__main__":
    # Test the generator
    print("🔴 Testing Live Data Generator...\n")
    
    for i in range(5):
        batch = generate_live_batch()
        stats = get_batch_stats(batch)
        
        print(f"Batch {i+1}:")
        print(f"  Size: {stats['batch_size']}")
        print(f"  Avg Amount: ${stats['avg_amount']:.2f}")
        print(f"  Drift Event: {'🚨 YES' if stats['has_drift'] else '✅ No'}")
        print(f"  Risk Score: {stats['risk_score_avg']:.3f}")
        print()
        
        import time
        time.sleep(1)
