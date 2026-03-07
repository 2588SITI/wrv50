import os
import re
import io
import collections
import zipfile
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# =========================================================
#         STREAMLIT PAGE SETUP - V45 (PREMIUM RAILWAY THEME)
# =========================================================
st.set_page_config(page_title="Loco-Speed Safety Audit", layout="wide", page_icon="🚄")

# --- Constants & Colors ---
NAVY = "#1A237E"
GOLD = "#FFD700"
BG_MAP = {
    "Green": "#E6FFFA",
    "Yellow": "#FFFFE0",
    "Double Yellow": "#FFF5E6",
    "Red": "#F2F2F2"
}

# --- CSS Styling & Beautiful Bullet Train Header ---
st.markdown(f"""
    <style>
    /* Full Length Bullet Train Image Background */
    .train-bg {{
        width: 100%;
        height: 250px;
        background: url('https://images.unsplash.com/photo-1474487548417-781cb71495f3?q=80&w=2000&auto=format&fit=crop') no-repeat center center;
        background-size: cover;
        border-radius: 12px;
        position: relative;
        margin-bottom: 25px;
        box-shadow: 0 8px 20px rgba(0,0,0,0.5);
        border: 3px solid {NAVY};
    }}
    /* Dark Gradient Overlay for the Upper Space */
    .text-overlay {{
        position: absolute;
        top: 0; left: 0; width: 100%; height: 100%;
        background: linear-gradient(180deg, rgba(26,35,126,0.9) 0%, rgba(26,35,126,0.5) 45%, transparent 100%);
        display: flex; flex-direction: column; align-items: center; justify-content: flex-start; padding-top: 25px;
        border-radius: 9px;
    }}
    .title-text {{ 
        color: {GOLD}; font-size: 46px; font-weight: 900; margin: 0; 
        text-transform: uppercase; letter-spacing: 3px; 
        text-shadow: 3px 3px 6px rgba(0,0,0,0.9); font-family: 'Arial Black', sans-serif;
    }}
    .subtitle-text {{ 
        color: #FFFFFF; font-size: 20px; font-weight: bold; margin-top: 5px; 
        letter-spacing: 6px; text-shadow: 2px 2px 4px rgba(0,0,0,0.9);
    }}
    /* Adjusting container widths */
    .stDataFrame {{ border: 2px solid #ddd; border-radius: 8px; }}
    </style>
    
    <div class="train-bg">
        <div class="text-overlay">
            <div class="title-text">Loco-Speed Safety Audit Tool</div>
            <div class="subtitle-text">🛤️ INDIAN RAILWAYS DASHBOARD 🚦</div>
        </div>
    </div>
""", unsafe_allow_html=True)

# --- Session State ---
if 'events' not in st.session_state: st.session_state.events =[]
if 'rtis' not in st.session_state: st.session_state.rtis = None
if 'processed' not in st.session_state: st.session_state.processed = False

# =========================================================
#                     HELPER FUNCTIONS
# =========================================================
def clean_id(s):
    m = re.search(r'([AS])?-?(\d+)', str(s).upper())
    return f"{m.group(1) or 'S'}{m.group(2)}" if m else None

def base_station(s):
    return str(s).split('_')[0].split('-')[0].split(' ')[0].upper()

def relay_type(name):
    name = str(name).upper()
    if any(x in name for x in['DECR','DECPR_K','DECPR', 'DGCR']): return 'Green'
    if any(x in name for x in['HHECR','HHECPR2_K', 'HHGCR']): return 'Double Yellow'
    if any(x in name for x in ['HECR', 'HGCR']): return 'Yellow'
    if any(x in name for x in ['RECR', 'RGCR']): return 'Red'
    return None

@st.cache_data(show_spinner=False)
def load_file(file_name, file_bytes):
    file_obj = io.BytesIO(file_bytes)
    if file_name.endswith(('.xlsx', '.xls')):
        return pd.read_excel(file_obj, engine='openpyxl')
    else:
        try:
            return pd.read_csv(file_obj, engine='pyarrow')
        except:
            file_obj.seek(0)
            return pd.read_csv(file_obj, encoding='latin1', on_bad_lines='skip', low_memory=False)

# =========================================================
#                     CORE PROCESSING
# =========================================================
def process_data(rtis_up, dlog_up, sig_up):
    with st.spinner("⏳ Analyzing Data (High-Speed Engine)... Please Wait."):
        try:
            sig_map = load_file(sig_up.name, sig_up.getvalue())
            up_signals = {clean_id(s) for s in sig_map.iloc[:, 6].dropna().astype(str) if clean_id(s)}

            rtis = load_file(rtis_up.name, rtis_up.getvalue())
            rtis.columns = rtis.columns.str.strip()
            rtis['Logging Time'] = pd.to_datetime(rtis['Logging Time'], format='mixed', errors='coerce')
            rtis = rtis.dropna(subset=['Logging Time']).sort_values('Logging Time')
            rtis['CumDist'] = pd.to_numeric(rtis.get('distFromSpeed', 0), errors='coerce').fillna(0).cumsum()
            rtis['BASE_STN'] = rtis['STATION NAME'].astype(str).apply(base_station)
            st.session_state.rtis = rtis

            dlog = load_file(dlog_up.name, dlog_up.getvalue())
            dlog.columns = dlog.columns.str.strip()
            dlog = dlog.rename(columns={'STATION NAME': 'STATION_NAME', 'SIGNAL NAME': 'SIGNAL_NAME', 'SIGNAL STATUS': 'SIGNAL_STATUS', 'SIGNAL TIME': 'SIGNAL_TIME'})
            
            time_series = dlog['SIGNAL_TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True)
            dlog['dt'] = pd.to_datetime(time_series, format='mixed', dayfirst=True, errors='coerce')
            dlog = dlog.dropna(subset=['dt']).sort_values('dt')

            latch_aspect = collections.defaultdict(lambda: "Red")
            last_down_event = {}
            raw_events =[]

            for row in dlog.itertuples(index=False):
                stn = base_station(row.STATION_NAME)
                sig_full = str(row.SIGNAL_NAME).strip().upper()
                sig = clean_id(sig_full)
                if sig not in up_signals: continue
                
                status = str(row.SIGNAL_STATUS).upper()
                key = (stn, sig)
                is_up = any(x in status for x in['UP', 'ON', 'PICKUP', 'CLOSED', 'OCCURRED'])
                rtype = relay_type(sig_full)
                if not rtype: continue

                if rtype == 'Red' and is_up:
                    ev_time = row.dt
                    final_asp = latch_aspect[key]
                    if final_asp == "Red" and key in last_down_event:
                        down_asp, down_time = last_down_event[key]
                        if 0 <= (ev_time - down_time).total_seconds() <= 5:
                            final_asp = down_asp
                    
                    diffs = (rtis['Logging Time'] - ev_time).abs()
                    idx = diffs.idxmin()
                    pt = rtis.loc[idx]
                    if pt['Speed'] > 1 and diffs[idx].total_seconds() <= 15 and pt['BASE_STN'] == stn:
                        raw_events.append({
                            'Stn': stn, 'Sig': sig, 'Time': ev_time,
                            'Aspect': final_asp, 'Speed': pt['Speed'],
                            'CumDist': pt['CumDist'], 'RTIS_Stn': pt['BASE_STN']
                        })
                    latch_aspect[key] = "Red"
                elif rtype in ['Green', 'Double Yellow', 'Yellow']:
                    if is_up: latch_aspect[key] = rtype
                    else: last_down_event[key] = (rtype, row.dt)

            final_events =[]
            if raw_events:
                df_ev = pd.DataFrame(raw_events).sort_values(['Stn', 'Sig', 'Time'])
                for _, grp in df_ev.groupby(['Stn', 'Sig']):
                    dt_check = grp['Time'].diff().shift(-1).dt.total_seconds()
                    valid = grp[dt_check.isna() | (dt_check > 15)]
                    final_events.extend(valid.to_dict('records'))

            st.session_state.events = sorted(final_events, key=lambda x: x['Time'])
            st.session_state.processed = True
            st.success(f"✅ Processing Complete! Found {len(st.session_state.events)} aspect events.")
        except Exception as e:
            st.error(f"❌ Error during processing: {str(e)}")

# =========================================================
#                     EXPORT GENERATORS
# =========================================================
def generate_excel(data):
    output = io.BytesIO()
    export_df = pd.DataFrame([{
        'Station': ev['Stn'], 'Signal': ev['Sig'], 
        'RECR Up Time (ms)': ev['Time'].strftime('%d/%m/%Y %H:%M:%S.%f')[:-3],
        'Aspect Before Red': ev['Aspect'], 'Speed (km/h)': ev['Speed'], 'RTIS Stn': ev['RTIS_Stn']
    } for ev in data])
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='Signal Aspects')
    return output.getvalue()

def generate_zip_graphs(data, rtis_df):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for i, ev in enumerate(data):
            fig, ax = plt.subplots(figsize=(10, 6))
            sub = rtis_df[(rtis_df['CumDist'] >= ev['CumDist'] - 1000) & (rtis_df['CumDist'] <= ev['CumDist'] + 1000)]
            
            ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
            ax.plot(sub['Logging Time'], sub['Speed'], color=NAVY, lw=2.5)
            ax.axvline(x=ev['Time'], color='red', linestyle='--', linewidth=2)
            
            time_ms = ev['Time'].strftime('%H:%M:%S.%f')[:-3]
            box_text = f"STN: {ev['Stn']}\nSIG: {ev['Sig']}\nTIME: {time_ms}\nSPEED: {ev['Speed']} km/h"
            
            ax.annotate(box_text, xy=(ev['Time'], ev['Speed']), xytext=(20, 20), textcoords='offset points',
                        bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="red", lw=2, alpha=0.9),
                        arrowprops=dict(arrowstyle="-|>", connectionstyle="arc3,rad=0.3", color="red"),
                        fontweight='bold', fontsize=10)

            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            ax.set_title(f"DETAILED ANALYSIS: {ev['Stn']} | {ev['Sig']} | {ev['Aspect']} -> RED", fontweight='bold')
            ax.set_ylabel("Speed (km/h)", fontweight='bold')
            ax.grid(True, alpha=0.3)
            
            img_buffer = io.BytesIO()
            fig.savefig(img_buffer, format="png", bbox_inches='tight')
            plt.close(fig)
            zip_file.writestr(f"Graph_{i+1}_{ev['Sig']}.png", img_buffer.getvalue())
            
    return zip_buffer.getvalue()

# =========================================================
#                       UI LAYOUT
# =========================================================
with st.sidebar:
    st.header("📁 1. Load Data Files")
    rtis_f = st.file_uploader("RTIS File", type=['csv', 'xlsx'])
    dlog_f = st.file_uploader("Datalogger File", type=['csv', 'xlsx'])
    sig_f = st.file_uploader("Signal Mapping File", type=['csv', 'xlsx'])
    
    if st.button("🚀 PROCESS DATA", use_container_width=True, type="primary"):
        if rtis_f and dlog_f and sig_f:
            process_data(rtis_f, dlog_f, sig_f)
        else:
            st.warning("Please upload all 3 files first.")

if st.session_state.processed and st.session_state.events:
    # --- Top Control Bar ---
    col1, col2, col3, col4 = st.columns([1.5, 2, 1.2, 1.5])
    
    with col1:
        st.markdown("**🚥 Aspect Filters:**")
        filter_opt = st.radio("Aspect:", ["All", "Yellow", "Double Yellow"], horizontal=True, label_visibility="collapsed")
    
    # Apply Filter
    filtered_events = st.session_state.events
    if filter_opt != "All":
        filtered_events = [e for e in st.session_state.events if e['Aspect'] == filter_opt]
    
    with col3:
        st.download_button("📥 Excel Report", data=generate_excel(filtered_events), 
                           file_name="Signal_Aspects_Report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with col4:
        st.download_button("🖼 Download All Graphs (ZIP)", data=generate_zip_graphs(filtered_events, st.session_state.rtis), 
                           file_name="Annotated_Graphs.zip", mime="application/zip")

    st.divider()

    # --- Split Screen Layout (Table & Graph Side-by-Side) ---
    c_table, c_graph = st.columns([1.2, 1.8])

    with c_table:
        st.markdown("### 🚦 SIGNAL ASPECT")
        st.caption("🖱️ **Tip:** Click any row below, then press **Up (🔼) / Down (🔽) arrow keys** to change graphs instantly!")
        
        display_df = pd.DataFrame(filtered_events)
        if not display_df.empty:
            display_df['Time (ms)'] = display_df['Time'].dt.strftime('%H:%M:%S.%f').str[:-3]
            display_cols =['Stn', 'Sig', 'Time (ms)', 'Aspect', 'Speed', 'RTIS_Stn']
            
            # Interactive Streamlit Dataframe (Arrow Keys Work Here!)
            selected_row = st.dataframe(
                display_df[display_cols],
                on_select="rerun",           # Triggers rerun on click/arrow keys
                selection_mode="single-row", # Single row selection allowed
                hide_index=True,
                use_container_width=True,
                height=550                   # Fixed height so scroll bar appears
            )
        else:
            st.info("No events match the selected filter.")

    with c_graph:
        st.markdown("### 🛤️ Precision Speed Profile")
        if not display_df.empty:
            # Check which row is selected, default to 0
            if len(selected_row.selection.rows) > 0:
                idx = selected_row.selection.rows[0]
            else:
                idx = 0
                
            ev = filtered_events[idx]
            rtis_df = st.session_state.rtis
            sub = rtis_df[(rtis_df['CumDist'] >= ev['CumDist'] - 1000) & (rtis_df['CumDist'] <= ev['CumDist'] + 1000)]
            
            fig, ax = plt.subplots(figsize=(10, 6.5)) # Adjusted aspect ratio
            ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
            
            # Plot Speed Line
            ax.plot(sub['Logging Time'], sub['Speed'], color=NAVY, lw=3)
            
            # Plot Red Vertical Line
            ax.axvline(x=ev['Time'], color='red', linestyle='--', linewidth=2.5)
            
            # Annotation Box
            time_ms = ev['Time'].strftime('%H:%M:%S.%f')[:-3]
            box_text = f"STN: {ev['Stn']}\nSIG: {ev['Sig']}\nTIME: {time_ms}\nSPEED: {ev['Speed']} km/h"
            
            ax.annotate(box_text, xy=(ev['Time'], ev['Speed']), xytext=(20, 20), textcoords='offset points',
                        bbox=dict(boxstyle="round,pad=0.6", fc="white", ec="red", lw=2, alpha=0.9),
                        arrowprops=dict(arrowstyle="-|>", connectionstyle="arc3,rad=0.3", color="red", lw=1.5),
                        fontweight='bold', fontsize=11)

            # Beautify Graph
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            ax.set_title(f"Detailed Analysis: {ev['Stn']} | Signal: {ev['Sig']} | {ev['Aspect']} -> RED", fontweight='bold', fontsize=14, color=NAVY)
            ax.set_ylabel("Speed (km/h)", fontweight='bold', fontsize=12)
            ax.set_xlabel("Time", fontweight='bold', fontsize=12)
            
            # Grid styling
            ax.grid(True, which='major', linestyle='-', alpha=0.4)
            ax.grid(True, which='minor', linestyle=':', alpha=0.2)
            ax.minorticks_on()
            
            # Remove top and right borders for a cleaner look
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            
            st.pyplot(fig)

elif st.session_state.processed:
    st.info("No safety violations found in the given dataset.")
else:
    st.info("👈 Please upload the 3 data files in the sidebar and click 'PROCESS DATA'.")
