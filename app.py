"""
🛡 Project Aegis: Autonomous Data Immunity
Mission Control Dashboard for Self-Healing Data Pipeline
"""
import streamlit as st
import os
import json
import time
import psutil
from io import BytesIO, StringIO
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.engine import AegisEngine
from src.utils import get_execution_mode, format_bytes

# Page configuration
st.set_page_config(
    page_title="Project Aegis - Mission Control",
    page_icon="🛡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for premium styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1rem 0;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 1rem;
        color: white;
        text-align: center;
        box-shadow: 0 10px 30px rgba(0,0,0,0.2);
    }
    
    .perf-metric {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        padding: 1.5rem;
        border-radius: 0.8rem;
        color: white;
        text-align: center;
        box-shadow: 0 5px 15px rgba(0,0,0,0.15);
    }
    
    .alert-critical {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        color: white;
        font-weight: bold;
        text-align: center;
        animation: pulse 2s infinite;
    }
    
    .big-data-badge {
        background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
        padding: 1rem;
        border-radius: 0.8rem;
        color: #333;
        font-weight: bold;
        text-align: center;
        margin: 1rem 0;
        box-shadow: 0 5px 15px rgba(0,0,0,0.2);
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }
    
    .info-box {
        background: #f8f9fa;
        border-left: 4px solid #667eea;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<h1 class="main-header">🛡 Project Aegis: Autonomous Data Immunity</h1>', unsafe_allow_html=True)
st.markdown("---")

# Initialize session state
if 'engine' not in st.session_state:
    st.session_state.engine = AegisEngine()
if 'drift_result' not in st.session_state:
    st.session_state.drift_result = None
if 'healing_result' not in st.session_state:
    st.session_state.healing_result = None
if 'performance_metrics' not in st.session_state:
    st.session_state.performance_metrics = None
if 'raw_dataframe' not in st.session_state:
    st.session_state.raw_dataframe = None

# Sidebar Configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    execution_mode = get_execution_mode()
    
    # Display execution mode
    if execution_mode == "cloud":
        st.info("☁️ **Cloud Mode**: Running on Render (512MB RAM limit)")
    else:
        st.success("💻 **Local Mode**: Big Data processing enabled")
    
    st.markdown("---")
    
    # Data source selection
    st.subheader("📂 Data Source")
    data_source = st.radio(
        "Choose input method:",
        ["Upload File", "Local Path"],
        key="data_source"
    )
    
    data_file = None
    data_path = None
    file_size = 0
    
    if data_source == "Upload File":
        uploaded_file = st.file_uploader(
            "Upload CSV File",
            type=['csv'],
            help="Max 200MB for cloud deployment"
        )
        if uploaded_file:
            file_size = uploaded_file.size
            st.info(f"📊 File size: {format_bytes(file_size)}")
            
            # Big Data Badge
            if file_size > 100 * 1024 * 1024:  # > 100MB
                st.markdown(
                    '<div class="big-data-badge">🚀 BIG DATA MODE ACTIVE<br/>Streaming via DuckDB Zero-Copy</div>',
                    unsafe_allow_html=True
                )
            
            if execution_mode == "cloud" and file_size > 200 * 1024 * 1024:
                st.error("⚠️ File too large for cloud deployment (max 200MB)")
            else:
                data_file = uploaded_file
    else:
        st.markdown('<div class="info-box">🚀 <b>Big Data Mode Active</b>: Leveraging DuckDB for Zero-Copy Processing.</div>', unsafe_allow_html=True)
        
        data_path = st.text_input(
            "Enter file path:",
            placeholder="/path/to/large_dataset.csv",
            help="Full path to CSV file on local disk"
        )
        
        if data_path and os.path.exists(data_path):
            file_size = os.path.getsize(data_path)
            st.success(f"✅ File found: {format_bytes(file_size)}")
            
            # Big Data Badge for local files
            if file_size > 100 * 1024 * 1024:  # > 100MB
                st.markdown(
                    '<div class="big-data-badge">🚀 BIG DATA MODE ACTIVE<br/>Streaming via DuckDB Zero-Copy</div>',
                    unsafe_allow_html=True
                )
        elif data_path:
            st.error("❌ File not found")

# Main content area
col1, col2 = st.columns([2, 1])

with col1:
    st.header("🔍 Data Quality Analysis")
    
    # Scan button
    if st.button("🚀 Scan for Drift", type="primary", use_container_width=True):
        if data_file or (data_path and os.path.exists(data_path)):
            with st.spinner("🔬 Analyzing data quality..."):
                try:
                    input_source = data_file if data_file else data_path
                    
                    # Performance telemetry
                    process = psutil.Process()
                    mem_before = process.memory_info().rss / 1024 / 1024  # MB
                    start_time = time.time()
                    
                    # Run drift detection
                    result = st.session_state.engine.scan_for_drift(input_source)
                    
                    # Calculate metrics
                    end_time = time.time()
                    processing_time = end_time - start_time
                    mem_after = process.memory_info().rss / 1024 / 1024  # MB
                    memory_delta = mem_after - mem_before
                    throughput = result['num_rows'] / processing_time if processing_time > 0 else 0
                    
                    st.session_state.drift_result = result
                    st.session_state.performance_metrics = {
                        'processing_time': processing_time,
                        'memory_delta': memory_delta,
                        'throughput': throughput
                    }
                    
                    # Store raw dataframe for inspection
                    import pandas as pd
                    if isinstance(input_source, str):
                        st.session_state.raw_dataframe = pd.read_csv(input_source, nrows=50)
                    else:
                        input_source.seek(0)  # Reset file pointer
                        st.session_state.raw_dataframe = pd.read_csv(input_source, nrows=50)
                    
                    st.success("✅ Scan complete!")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
        else:
            st.warning("⚠️ Please select a data source first")
    
    # Performance Telemetry Display
    if st.session_state.performance_metrics:
        st.markdown("### ⚡ Performance Telemetry")
        
        perf_col1, perf_col2, perf_col3 = st.columns(3)
        
        perf = st.session_state.performance_metrics
        
        with perf_col1:
            st.markdown(
                f"""
                <div class="perf-metric">
                    <h4>⏱️ Processing Time</h4>
                    <h2>{perf['processing_time']:.3f}s</h2>
                    <p>DuckDB Speed</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        with perf_col2:
            mem_sign = "+" if perf['memory_delta'] >= 0 else ""
            st.markdown(
                f"""
                <div class="perf-metric">
                    <h4>💾 Memory Delta</h4>
                    <h2>{mem_sign}{perf['memory_delta']:.1f}MB</h2>
                    <p>RAM Footprint</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        with perf_col3:
            st.markdown(
                f"""
                <div class="perf-metric">
                    <h4>⚡ Throughput</h4>
                    <h2>{perf['throughput']:,.0f}</h2>
                    <p>rows/sec</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        st.markdown("<br>", unsafe_allow_html=True)
    
    # X-Ray Vision Tabs
    if st.session_state.drift_result:
        result = st.session_state.drift_result
        drift_score = result['drift_score']
        
        tab1, tab2, tab3 = st.tabs(["📊 Drift Visuals", "🔬 Raw Data Inspector", "🛠️ System Logs"])
        
        with tab1:
            st.markdown("### 📊 Drift Analysis")
            
            # Drift score metrics
            metric_col1, metric_col2, metric_col3 = st.columns(3)
            
            with metric_col1:
                st.metric("Drift Score", f"{drift_score:.2%}")
            
            with metric_col2:
                st.metric("Rows Analyzed", f"{result['num_rows']:,}")
            
            with metric_col3:
                st.metric("Columns", result['num_columns'])
            
            # Critical alert for high drift
            if drift_score > 0.3:
                st.markdown(
                    f'<div class="alert-critical">🚨 CRITICAL DRIFT DETECTED: {drift_score:.1%} of columns show significant drift!</div>',
                    unsafe_allow_html=True
                )
                
                # Auto-fix button
                if st.button("🔧 AUTO-FIX DATA", type="primary", use_container_width=True):
                    with st.spinner("🛡 Activating self-healing protocols..."):
                        try:
                            input_source = data_file if data_file else data_path
                            healing_result = st.session_state.engine.heal_data(input_source)
                            st.session_state.healing_result = healing_result
                            st.success("✅ Data healing complete!")
                            st.balloons()
                        except Exception as e:
                            st.error(f"❌ Healing error: {str(e)}")
            
            # Missing values analysis
            if result['missing_stats']['total_missing'] > 0:
                st.markdown("### 🔎 Missing Values")
                missing_data = result['missing_stats']['columns_with_missing']
                
                for col_info in missing_data[:10]:  # Show top 10
                    col_name = col_info['column']
                    pct = col_info['percentage']
                    
                    # Color code based on severity
                    if pct > 50:
                        st.error(f"❌ **{col_name}**: {pct:.1f}% missing (will be dropped)")
                    elif pct > 5:
                        st.warning(f"⚠️ **{col_name}**: {pct:.1f}% missing (flagged for review)")
                    else:
                        st.info(f"💉 **{col_name}**: {pct:.1f}% missing (auto-impute)")
        
        with tab2:
            st.markdown("### 🔬 Raw Data Inspector")
            st.caption("First 50 rows of the dataset")
            
            if st.session_state.raw_dataframe is not None:
                st.dataframe(
                    st.session_state.raw_dataframe,
                    use_container_width=True,
                    height=400
                )
                st.info(f"📊 Showing {len(st.session_state.raw_dataframe)} of {result['num_rows']:,} total rows")
            else:
                st.warning("No data preview available")
        
        with tab3:
            st.markdown("### 🛠️ System Logs")
            st.caption("Raw drift report structure (JSON)")
            
            # Display raw report
            if result['report']:
                st.json(result['report'])
            else:
                st.json({
                    "engine": "Statistical Drift Detection",
                    "version": "1.0",
                    "drift_score": drift_score,
                    "columns_analyzed": result['num_columns'],
                    "rows_sampled": result['num_rows'],
                    "missing_value_stats": result['missing_stats'],
                    "columns": result['columns']
                })
            

with col2:
    st.header("📋 System Status")
    
    # Execution mode card
    mode_emoji = "☁️" if execution_mode == "cloud" else "💻"
    st.markdown(
        f"""
        <div class="metric-card">
            <h2>{mode_emoji}</h2>
            <h3>{execution_mode.upper()} MODE</h3>
            <p>{"Render Deployment" if execution_mode == "cloud" else "Local Processing"}</p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Healing results
    if st.session_state.healing_result:
        st.success("### ✅ Healing Complete")
        
        healing = st.session_state.healing_result
        
        st.metric("Rows Processed", f"{healing['rows_processed']:,}")
        
        if healing['columns_dropped']:
            st.error(f"🗑️ Dropped {len(healing['columns_dropped'])} columns")
            with st.expander("View dropped columns"):
                for col in healing['columns_dropped']:
                    st.text(f"• {col}")
        
        if healing['columns_imputed']:
            st.info(f"💉 Imputed {len(healing['columns_imputed'])} columns")
            with st.expander("View imputed columns"):
                for col_info in healing['columns_imputed']:
                    st.text(f"• {col_info['column']}: {col_info['method']} = {col_info['value']:.2f}")
        
        if healing['columns_flagged']:
            st.warning(f"⚠️ Flagged {len(healing['columns_flagged'])} columns")
        
        st.markdown("---")
        
        # Download or path display
        if execution_mode == "cloud" or data_file:
            # Provide download button
            if os.path.exists(healing['output_path']):
                with open(healing['output_path'], 'rb') as f:
                    st.download_button(
                        label="📥 Download Clean Data",
                        data=f,
                        file_name=os.path.basename(healing['output_path']),
                        mime="text/csv",
                        use_container_width=True
                    )
        else:
            # Show local path
            st.success("**Clean Data Path:**")
            st.code(healing['output_path'])
            st.caption(f"Size: {healing['output_size']}")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p>🛡 <b>Project Aegis</b> - Autonomous Data Quality System</p>
    <p>Powered by DuckDB, Evidently AI & Streamlit</p>
</div>
""", unsafe_allow_html=True)
