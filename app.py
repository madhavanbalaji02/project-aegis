"""
🛡 Project Aegis: Autonomous Data Immunity
Mission Control Dashboard for Self-Healing Data Pipeline
"""
import streamlit as st
import os
import json
import time
import psutil
from datetime import datetime
from io import BytesIO
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
    
    # Mode selection
    st.subheader("📂 Operation Mode")
    operation_mode = st.radio(
        "Choose mode:",
        ["📁 File Analysis", "🔴 Live Feed"],
        key="operation_mode"
    )
    
    st.markdown("---")
    
    data_file = None
    data_path = None
    file_size = 0
    
    if operation_mode == "📁 File Analysis":
        # Data source selection
        st.subheader("Data Source")
        data_source = st.radio(
            "Choose input method:",
            ["Upload File", "Local Path"],
            key="data_source"
        )
    
    if operation_mode == "📁 File Analysis":
        # Data source selection (continued)
        if 'data_source' in st.session_state and st.session_state.data_source == "Upload File":
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
        elif 'data_source' in st.session_state and st.session_state.data_source == "Local Path":
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
# Check operation mode from sidebar
if 'operation_mode' in st.session_state and st.session_state.operation_mode == "🔴 Live Feed":
    # LIVE FEED MODE
    st.header("🔴 LIVE FEED - Real-Time Data Stream")
    
    # Initialize live feed session state
    if 'live_feed_active' not in st.session_state:
        st.session_state.live_feed_active = False
    if 'stream_history' not in st.session_state:
        st.session_state.stream_history = []
    if 'batch_count' not in st.session_state:
        st.session_state.batch_count = 0
    
    # Import stream simulator
    from src.stream_sim import generate_live_batch, get_batch_stats
    
    # Control buttons
    col_start, col_stop = st.columns(2)
    
    with col_start:
        start_button = st.button("▶️ Start Stream", type="primary", use_container_width=True, disabled=st.session_state.live_feed_active)
    
    with col_stop:
        stop_button = st.button("⏹️ Stop Stream", type="secondary", use_container_width=True, disabled=not st.session_state.live_feed_active)
    
    if start_button:
        st.session_state.live_feed_active = True
        st.session_state.stream_history = []
        st.session_state.batch_count = 0
        st.rerun()
    
    if stop_button:
        st.session_state.live_feed_active = False
        st.rerun()
    
    if st.session_state.live_feed_active:
        st.markdown("### 📊 Live Stream Analytics")
        
        # Create placeholders for dynamic content
        metrics_placeholder = st.empty()
        chart_placeholder = st.empty()
        matrix_placeholder = st.empty()
        
        import time
        
        # Streaming loop
        for _ in range(20):  # Run for 20 batches then stop
            if not st.session_state.live_feed_active:
                break
            
            # Generate batch
            batch_df = generate_live_batch()
            stats = get_batch_stats(batch_df)
            
            # Analyze batch
            analysis = st.session_state.engine.scan_batch(batch_df)
            
            # Update history
            st.session_state.batch_count += 1
            st.session_state.stream_history.append({
                'batch_num': st.session_state.batch_count,
                'health_score': analysis['health_score'],
                'drift_score': analysis['drift_score'],
                'anomaly_count': analysis['anomaly_count'],
                'timestamp': analysis['timestamp']
            })
            
            # Keep only last 50 batches
            if len(st.session_state.stream_history) > 50:
                st.session_state.stream_history.pop(0)
            
            # Show alert if drift detected
            if analysis['alert']:
                st.toast("⚠️ Anomaly Detected in Stream!", icon="🚨")
            
            # Update metrics
            with metrics_placeholder.container():
                met_col1, met_col2, met_col3, met_col4 = st.columns(4)
                
                with met_col1:
                    st.metric("Batch #", st.session_state.batch_count)
                
                with met_col2:
                    st.metric("Health Score", f"{analysis['health_score']:.2%}", 
                             delta=f"{analysis['health_score']-1.0:.2%}" if analysis['health_score'] < 1.0 else None)
                
                with met_col3:
                    st.metric("Anomalies", analysis['anomaly_count'])
                
                with met_col4:
                    status_emoji = "🚨" if analysis['alert'] else "✅"
                    st.metric("Status", f"{status_emoji} {'ALERT' if analysis['alert'] else 'OK'}")
            
            # Update live chart
            if len(st.session_state.stream_history) > 1:
                import pandas as pd
                chart_data = pd.DataFrame(st.session_state.stream_history)
                
                with chart_placeholder.container():
                    st.line_chart(
                        chart_data.set_index('batch_num')[['health_score', 'drift_score']],
                        use_container_width=True,
                        height=300
                    )
            
            # 3D Hexagon Map - Geospatial Command Center
            with matrix_placeholder.container():
                st.markdown("### 🗺️ Geospatial Command Center - NYC Transactions")
                
                # Prepare map data
                import pydeck as pdk
                
                # Build color based on health score/drift
                map_df = batch_df[['latitude', 'longitude', 'amount', 'risk_score']].copy()
                
                # Create 3D Hexagon Layer
                layer = pdk.Layer(
                    "HexagonLayer",
                    data=map_df,
                    get_position='[longitude, latitude]',
                    get_elevation_weight='amount',
                    elevation_scale=50,
                    elevation_range=[0, 1000],
                    extruded=True,
                    coverage=0.8,
                    radius=100,
                    # Color range: Green (low) to Red (high)
                    color_range=[
                        [0, 255, 0, 200],      # Green
                        [255, 255, 0, 200],    # Yellow
                        [255, 165, 0, 200],    # Orange
                        [255, 0, 0, 200]       # Red
                    ],
                    pickable=True,
                    auto_highlight=True
                )
                
                # Set initial view state centered on NYC
                view_state = pdk.ViewState(
                    latitude=40.7128,
                    longitude=-74.0060,
                    zoom=12,
                    pitch=45,
                    bearing=0
                )
                
                # Render the deck
                st.pydeck_chart(pdk.Deck(
                    layers=[layer],
                    initial_view_state=view_state,
                    tooltip={
                        "text": "Transactions\nElevation based on amount"
                    }
                ))
                
                st.caption(f"🌆 Live NYC Transaction Heat Map | Batch #{st.session_state.batch_count} | {len(batch_df)} transactions")
            
            # Sleep for streaming effect
            time.sleep(0.5)
        
        # Auto-stop after 20 batches
        if st.session_state.batch_count >= 20:
            st.session_state.live_feed_active = False
            st.success("✅ Stream completed (20 batches processed)")
            st.rerun()
    
    elif st.session_state.stream_history:
        # Show summary when stopped
        st.info("ℹ️ Stream paused. Click 'Start Stream' to resume.")
        
        st.markdown("### 📊 Stream Summary")
        import pandas as pd
        summary_df = pd.DataFrame(st.session_state.stream_history)
        
        sum_col1, sum_col2, sum_col3 = st.columns(3)
        
        with sum_col1:
            st.metric("Total Batches", len(summary_df))
        
        with sum_col2:
            st.metric("Avg Health Score", f"{summary_df['health_score'].mean():.2%}")
        
        with sum_col3:
            st.metric("Total Anomalies", int(summary_df['anomaly_count'].sum()))
        
        st.line_chart(
            summary_df.set_index('batch_num')[['health_score', 'drift_score']],
            use_container_width=True
        )

else:
    # FILE ANALYSIS MODE (existing code)
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
                    
                    # Enterprise Alerting Simulation
                    if result['drift_score'] > 0.3:
                        # Simulate PagerDuty alert
                        st.toast("📟 Sending PagerDuty Alert...", icon="🚨")
                        
                        # Log simulated Slack webhook event
                        import json
                        slack_webhook_payload = {
                            "channel": "#data-quality",
                            "username": "Project Aegis Alert Bot",
                            "icon_emoji": ":shield:",
                            "attachments": [{
                                "color": "danger" if result['drift_score'] > 0.5 else "warning",
                                "title": f"🚨 Data Quality Alert - Drift Score: {result['drift_score']:.1%}",
                                "text": f"High drift detected in data pipeline. {int(result['drift_score'] * result['num_columns'])} out of {result['num_columns']} columns affected.",
                                "fields": [
                                    {"title": "Severity", "value": "CRITICAL" if result['drift_score'] > 0.5 else "HIGH", "short": True},
                                    {"title": "Rows Analyzed", "value": f"{result['num_rows']:,}", "short": True},
                                    {"title": "Action Required", "value": "Investigate immediately", "short": False}
                                ],
                                "footer": "Project Aegis - Autonomous Data Immunity",
                                "ts": int(datetime.now().timestamp())
                            }]
                        }
                        
                        # Store webhook payload in session (will be shown in System Logs tab)
                        if 'alert_logs' not in st.session_state:
                            st.session_state.alert_logs = []
                        st.session_state.alert_logs.append({
                            'timestamp': datetime.now().isoformat(),
                            'type': 'slack_webhook',
                            'payload': slack_webhook_payload
                        })
                        
                        st.toast("✅ Alert sent successfully!", icon="📧")
                    
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
        
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Drift Visuals", "🔬 Raw Data Inspector", "🛠️ System Logs", "📐 Blueprint"])
        
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
            
            # AI Assistant Analysis
            st.markdown("---")
            with st.expander("🤖 Ask AI Assistant", expanded=drift_score > 0.3):
                st.markdown("### AI-Powered Analysis")
                
                # Import reporter for AI insights
                from src.reporter import generate_ai_insight
                
                # Generate AI insight
                ai_insight = generate_ai_insight(
                    drift_score,
                    result['num_rows'],
                    result['num_columns'],
                    result['missing_stats']
                )
                
                # Simulate streaming effect with markdown
                st.markdown(ai_insight)
                
                # Download Executive Report Button
                st.markdown("---")
                if st.button("📄 Generate Executive Report", type="secondary", use_container_width=True):
                    from src.reporter import generate_executive_report
                    
                    # Generate report
                    report_content = generate_executive_report(
                        result,
                        st.session_state.performance_metrics if st.session_state.performance_metrics else None,
                        ai_insight
                    )
                    
                    # Provide download
                    st.download_button(
                        label="📥 Download Data Quality Report",
                        data=report_content,
                        file_name=f"Data_Quality_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                        mime="text/plain",
                        use_container_width=True
                    )
                    st.success("✅ Executive report generated!")

        
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
            
            # Display enterprise alert logs if any
            if 'alert_logs' in st.session_state and st.session_state.alert_logs:
                st.markdown("#### 📡 Enterprise Alert Logs")
                st.caption(f"{len(st.session_state.alert_logs)} alert(s) triggered")
                
                for i, alert_log in enumerate(reversed(st.session_state.alert_logs[-5:])):  # Show last 5
                    with st.expander(f"🚨 Alert #{len(st.session_state.alert_logs) - i} - {alert_log['timestamp']}"):
                        st.json(alert_log)
                
                st.markdown("---")
            
            # Display raw report
            if result['report']:
                st.markdown("#### 📊 Drift Detection Report")
                st.json(result['report'])
            else:
                st.markdown("#### 📊 Drift Detection Report")
                st.json({
                    "engine": "Statistical Drift Detection",
                    "version": "1.0",
                    "drift_score": drift_score,
                    "columns_analyzed": result['num_columns'],
                    "rows_sampled": result['num_rows'],
                    "missing_value_stats": result['missing_stats'],
                    "columns": result['columns']
                })
            
        with tab4:
            st.markdown("### 📐 System Architecture Blueprint")
            st.caption("Technical infrastructure showing hybrid cloud/local architecture")
            
            # Create graphviz diagram
            architecture_diagram = '''
            digraph ProjectAegis {
                rankdir=LR;
                node [shape=box, style="rounded,filled", fontname="Arial"];
                
                // Data Sources
                subgraph cluster_sources {
                    label="Data Sources";
                    style=filled;
                    color=lightgrey;
                    upload [label="File Upload\n(Cloud)", fillcolor="#FFE6E6"];
                    local [label="Local Path\n(Big Data)", fillcolor="#E6F3FF"];
                    stream [label="Live Stream\n(Kafka Sim)", fillcolor="#E6FFE6"];
                }
                
                // Processing Layer
                subgraph cluster_processing {
                    label="Processing Engine";
                    style=filled;
                    color=lightblue;
                    duckdb [label="DuckDB\n(Zero-Copy)", fillcolor="#FFFACD"];
                    engine [label="AegisEngine\n(Drift + Heal)", fillcolor="#FFD700"];
                }
                
                // Analytics
                subgraph cluster_analytics {
                    label="Analytics";
                    style=filled;
                    color=lightgreen;
                    drift [label="Drift Detection\n(Statistical)", fillcolor="#98FB98"];
                    heal [label="Auto-Healing\n(3 Strategies)", fillcolor="#90EE90"];
                }
                
                // Visualization
                subgraph cluster_ui {
                    label="User Interface";
                    style=filled;
                    color=lavender;
                    streamlit [label="Streamlit\nDashboard", fillcolor="#E6E6FA"];
                    pydeck [label="PyDeck 3D\nGeospatial", fillcolor="#DDA0DD"];
                }
                
                // CI/CD
                subgraph cluster_cicd {
                    label="CI/CD Pipeline";
                    style=filled;
                    color=lightyellow;
                    github [label="GitHub Actions\n(Gatekeeper)", fillcolor="#FFE4B5"];
                    health [label="Daily Health\n(Night Watchman)", fillcolor="#FFDAB9"];
                }
                
                // Connections
                upload -> duckdb;
                local -> duckdb;
                stream -> engine;
                duckdb -> engine;
                engine -> drift;
                engine -> heal;
                drift -> streamlit;
                heal -> streamlit;
                streamlit -> pydeck;
                engine -> github [style=dashed];
                engine -> health [style=dashed];
            }
            '''
            
            st.graphviz_chart(architecture_diagram)
            
            st.markdown("""
            **Key Components:**
            - **DuckDB**: Zero-copy streaming for 50GB+ files
            - **AegisEngine**: Statistical drift detection + 3-strategy healing
            - **Live Stream**: Simulated Kafka with NYC geospatial data
            - **PyDeck 3D**: Real-time hexagon visualization
            - **CI/CD**: Automated testing + daily health checks
            """)


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
