"""
Vector Embedding Engine - Semantic Drift Detection for LLMOps
Monitors unstructured text data for semantic drift using sentence embeddings.
"""
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Tuple
from sklearn.decomposition import PCA
from sklearn.metrics.pairwise import cosine_similarity
import warnings
warnings.filterwarnings('ignore')

# Lazy import for sentence transformers (heavy dependency)
_model = None

def get_model():
    """Lazy load the sentence transformer model."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        print("🤖 Loading SentenceTransformer model...")
        _model = SentenceTransformer('all-MiniLM-L6-v2')
        print("✅ Model loaded successfully!")
    return _model


def compute_embeddings(texts: List[str]) -> np.ndarray:
    """
    Convert text to vector embeddings.
    
    Args:
        texts: List of text strings
        
    Returns:
        Array of 384-dimensional embeddings
    """
    model = get_model()
    embeddings = model.encode(texts, show_progress_bar=False)
    return embeddings


def compute_embedding_drift(batch_text: List[str], 
                           reference_text: List[str] = None) -> Dict[str, Any]:
    """
    Detect semantic drift in text data using vector embeddings.
    
    Args:
        batch_text: Current batch of text samples
        reference_text: Optional reference (normal) text samples
        
    Returns:
        Dictionary with semantic drift metrics and 2D PCA coordinates
    """
    
    # Default reference text (normal positive reviews)
    if reference_text is None:
        reference_text = [
            "Great service, highly recommend!",
            "Fast and efficient app",
            "Excellent customer support",
            "Very satisfied with the transaction",
            "Quick processing time"
        ] * 5  # Repeat for better centroid
    
    # Compute embeddings
    batch_embeddings = compute_embeddings(batch_text)
    ref_embeddings = compute_embeddings(reference_text)
    
    # Calculate centroids
    batch_centroid = np.mean(batch_embeddings, axis=0).reshape(1, -1)
    ref_centroid = np.mean(ref_embeddings, axis=0).reshape(1, -1)
    
    # Compute cosine distance between centroids
    cosine_sim = cosine_similarity(batch_centroid, ref_centroid)[0][0]
    embedding_distance = 1 - cosine_sim  # Convert to distance
    
    # Apply PCA for 2D visualization
    all_embeddings = np.vstack([batch_embeddings, ref_embeddings])
    
    # Only apply PCA if we have more than 2 dimensions
    if all_embeddings.shape[1] > 2:
        pca = PCA(n_components=2)
        embeddings_2d = pca.fit_transform(all_embeddings)
        explained_variance = pca.explained_variance_ratio_.sum()
    else:
        embeddings_2d = all_embeddings
        explained_variance = 1.0
    
    # Split back into batch and reference
    batch_2d = embeddings_2d[:len(batch_text)]
    ref_2d = embeddings_2d[len(batch_text):]
    
    # Determine semantic drift score (0-1 scale)
    # Distance > 0.3 indicates significant semantic shift
    semantic_drift_score = min(embedding_distance / 0.3, 1.0)
    
    # Classify individual points as normal or anomalous
    # Points far from reference centroid are anomalies
    ref_centroid_2d = np.mean(ref_2d, axis=0)
    distances_from_ref = np.linalg.norm(batch_2d - ref_centroid_2d, axis=1)
    threshold = np.percentile(distances_from_ref, 75)  # Top 25% are anomalies
    is_anomalous = distances_from_ref > threshold
    
    result = {
        'embedding_distance': float(embedding_distance),
        'semantic_drift_score': float(semantic_drift_score),
        'batch_2d_coords': batch_2d,
        'reference_2d_coords': ref_2d,
        'is_anomalous': is_anomalous,
        'anomaly_count': int(is_anomalous.sum()),
        'explained_variance': float(explained_variance),
        'batch_size': len(batch_text)
    }
    
    return result


def create_vector_space_df(semantic_result: Dict[str, Any], 
                           batch_text: List[str]) -> pd.DataFrame:
    """
    Create DataFrame for visualization of vector space.
    
    Args:
        semantic_result: Result from compute_embedding_drift
        batch_text: Original text samples
        
    Returns:
        DataFrame with x, y coordinates and labels
    """
    
    batch_coords = semantic_result['batch_2d_coords']
    is_anomalous = semantic_result['is_anomalous']
    
    viz_df = pd.DataFrame({
        'x': batch_coords[:, 0],
        'y': batch_coords[:, 1],
        'text': batch_text[:len(batch_coords)],
        'category': ['Anomalous' if anom else 'Normal' for anom in is_anomalous],
        'color': ['#FF4444' if anom else '#4444FF' for anom in is_anomalous]
    })
    
    return viz_df


if __name__ == "__main__":
    # Test semantic drift detection
    print("Testing Semantic Drift Detection...\n")
    
    normal_reviews = [
        "Great service!",
        "Fast app",
        "Excellent support",
        "Very satisfied",
        "Quick processing"
    ]
    
    poisoned_reviews = [
        "Buy crypto now!",
        "'; DROP TABLE users; --",
        "Click here for free money!!!",
        "URGENT: Your account compromised",
        "*** SPAM *** Earn $1000!"
    ]
    
    mixed_batch = normal_reviews[:3] + poisoned_reviews[:2]
    
    result = compute_embedding_drift(mixed_batch)
    
    print(f"Embedding Distance: {result['embedding_distance']:.4f}")
    print(f"Semantic Drift Score: {result['semantic_drift_score']:.2%}")
    print(f"Anomaly Count: {result['anomaly_count']}/{result['batch_size']}")
    print(f"PCA Explained Variance: {result['explained_variance']:.2%}")
    
    viz_df = create_vector_space_df(result, mixed_batch)
    print("\nVisualization DataFrame:")
    print(viz_df)
