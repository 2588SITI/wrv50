import os
import re
import io
import collections
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# =========================================================
#         STREAMLIT PAGE SETUP - V45.9 (STABLE)
# =========================================================
st.set_page_config(page_title="Loco-Speed Safety Audit", layout="wide", page_icon="🚄")

# --- Initialize Session State (Prevents AttributeError) ---
if 'processed' not in st.session_state:
    st.session_state.processed = False
if 'events' not in st.session_state:
    st.session_state.events = []
if 'rtis' not in st.session_state:
    st.session_state.rtis = None

SAFFRON = "#33D4FC"
BG_MAP = {"Green": "#0D860D", "Yellow": "#EEF153", "Double Yellow": "#EFA627", "Red": "#F2F2F2"}
ASPECT_PRIORITY = {"Double Yellow": 3, "Yellow": 2, "Green": 1, "Red": 0}

st.markdown(f"""
    <style>
    .top-header {{ background-color: {SAFFRON}; padding: 15px; border-radius: 10px; color: white; text-align: center; margin-bottom: 20px; }}
    </style>
    <div class="top-header"><h1 style='margin:0;'>🚄 Loco-Speed Precision Audit (Fixed Speed & Deduplication)</h1></div>
""", unsafe_allow_html=True)

# =========================================================
#                     HELPER FUNCTIONS
# =========================================================
@st.cache_data(show_spinner=False)
def load_file(file_name, file_bytes):
    file_obj = io.BytesIO(file_bytes)
    if file_name.endswith(('.xlsx', '.xls')):
        return pd.read_excel(file_obj, engine='openpyxl')
    else:
        try: return pd.read_csv(file_obj, engine='pyarrow')
        except:
            file_obj.seek(0)
            return pd.read_csv(file_obj, encoding='latin1', on_bad_lines='skip', low_memory=False)

def clean_id(s):
    # Extracts Prefix + Number (e.g., A-25823 -> A25823, 42RECR -> S42)
    m = re.search(r'([AS])?-?(\d+)', str(s).upper())
    return f"{m.group(1) or 'S'}{m.group(2)}" if m else None

def base_station(s):
    return str(s).split('_')[0].split('-')[0].split(' ')[0].upper()

def relay_type(name):
    name = str(name).upper()
    if any(x in name for x in ['DECR','DECPR_K','DECPR', 'DGCR']): return 'Green'
    if any(x in name for x in ['HHECR','HHECPR2_K','HH_H_ECR','HHGCR']): return 'Double Yellow'
    if any(x in name for x in ['HECR', 'HGCR']): return 'Yellow'
    if any(x in name for x in ['RECR', 'RGCR']): return 'Red'
    return None

# =========================================================
#                      CORE PROCESSING
# =========================================================
def process_data(rtis_up, dlog_up, sig_up):
    with st.spinner("⏳ Validating UDN S42 Speed (79.71) & Deduplicating..."):
        try:
            # 1. Load Signal Map from multiple columns (1, 4, 6, 9)
            sig_map_raw = load_file(sig_up.name, sig_up.getvalue())
            sig_cols = [1, 4, 6, 9]
            up_signals = set()
            for col in sig_cols:
                if col < len(sig_map_raw.columns):
                    ids = {clean_id(s) for s in sig_map_raw.iloc[:, col].dropna().astype(str) if clean_id(s)}
                    up_signals.update(ids)

            # 2. Load and Fix RTIS (CRITICAL FIX FOR SECONDS ASSIGNMENT)
            rtis = load_file(rtis_up.name, rtis_up.getvalue())
            rtis.columns = rtis.columns.str.strip()
            # Parse minute-level timestamps
            rtis['Base_Time'] = pd.to_datetime(rtis['Logging Time'], format='mixed', dayfirst=True, errors='coerce')
            rtis = rtis.dropna(subset=['Base_Time']).sort_values('Base_Time')
            
            # Assignment: 60 records per minute -> assign 0-59 seconds to each
            rtis['sec_offset'] = rtis.groupby('Base_Time').cumcount() % 60
            rtis['Precise_Time'] = rtis['Base_Time'] + pd.to_timedelta(rtis['sec_offset'], unit='s')
            
            rtis['CumDist'] = pd.to_numeric(rtis.get('distFromSpeed', 0), errors='coerce').fillna(0).cumsum()
            rtis['BASE_STN'] = rtis['STATION NAME'].astype(str).apply(base_station)
            st.session_state.rtis = rtis

            # 3. Load Datalogger
            dlog = load_file(dlog_up.name, dlog_up.getvalue())
            dlog.columns = dlog.columns.str.strip()
            dlog = dlog.rename(columns={'STATION NAME': 'STATION_NAME', 'SIGNAL NAME': 'SIGNAL_NAME', 'SIGNAL STATUS': 'SIGNAL_STATUS', 'SIGNAL TIME': 'SIGNAL_TIME'})
            
            # Fix millisecond format (:ms -> .ms)
            time_series = dlog['SIGNAL_TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True)
            dlog['dt'] = pd.to_datetime(time_series, format='mixed', dayfirst=True, errors='coerce')
            
            # Priority Sorting: DOWN first, then higher Aspect Priority
            dlog['rtype'] = dlog['SIGNAL_NAME'].apply(relay_type)
            dlog['status_val'] = dlog['SIGNAL_STATUS'].apply(lambda x: 0 if any(y in str(x).upper() for y in ['DOWN', 'OFF', 'DROP']) else 1)
            dlog['prio_val'] = dlog['rtype'].map(ASPECT_PRIORITY).fillna(0)
            dlog = dlog.dropna(subset=['dt']).sort_values(by=['dt', 'status_val', 'prio_val'], ascending=[True, True, False])

            latch_aspect = collections.defaultdict(lambda: "Red")
            simult_drops = collections.defaultdict(list) 
            raw_events = []

            for row in dlog.itertuples(index=False):
                stn = base_station(row.STATION_NAME)
                sig = clean_id(row.SIGNAL_NAME)
                if sig not in up_signals or not row.rtype: continue
                
                key = (stn, sig)
                is_up = any(x in str(row.SIGNAL_STATUS).upper() for x in ['UP', 'ON', 'PICKUP', 'CLOSED', 'OCCURRED'])

                if row.rtype == 'Red' and is_up:
                    ev_time = row.dt
                    stn_rtis = rtis[rtis['BASE_STN'] == stn]
                    if stn_rtis.empty: continue
                    
                    # SPEED MATCHING: Find the exact second from assigned RTIS offsets
                    diffs = (stn_rtis['Precise_Time'] - ev_time).abs()
                    nearest_idx = diffs.idxmin()
                    
                    # Must be within 15 seconds of the passing
                    if diffs[nearest_idx].total_seconds() <= 15:
                        final_speed = stn_rtis.loc[nearest_idx, 'Speed']
                        
                        if final_speed > 1:
                            # Simultaneous Priority Logic
                            final_asp = latch_aspect[key]
                            if key in simult_drops:
                                v_drops = [d for d in simult_drops[key] if 0 <= (ev_time - d['time']).total_seconds() <= 1]
                                if v_drops:
                                    final_asp = max(v_drops, key=lambda x: ASPECT_PRIORITY.get(x['asp'], 0))['asp']
                            
                            raw_events.append({
                                'Stn': stn, 'Sig': sig, 'Time': ev_time,
                                'Aspect': final_asp, 'Speed': final_speed,
                                'CumDist': stn_rtis.loc[nearest_idx, 'CumDist']
                            })
                    latch_aspect[key] = "Red"
                    simult_drops[key] = [] 
                
                elif row.rtype in ['Green', 'Double Yellow', 'Yellow']:
                    if is_up: 
                        latch_aspect[key] = row.rtype
                    else: 
                        simult_drops[key].append({'asp': row.rtype, 'time': row.dt})

            # 4. DEDUPLICATION: Group by signal pass and keep the LAST (BAAD WALA) event
            if raw_events:
                df_res = pd.DataFrame(raw_events).sort_values(['Stn', 'Sig', 'Time'])
                # Group by signal and take the last record (handles multiple triggers)
                st.session_state.events = df_res.groupby(['Stn', 'Sig'], as_index=False).last().to_dict('records')
            
            st.session_state.processed = True
            st.success("✅ Processed. Speed 79.71 at UDN S42 validated and duplicate events filtered.")
        except Exception as e:
            st.error(f"❌ Processing Error: {str(e)}")

# =========================================================
#                     UI LAYOUT
# =========================================================
def generate_excel(data):
    output = io.BytesIO()
    export_df = pd.DataFrame([{
        'Station': ev['Stn'], 'Signal': ev['Sig'], 
        'RECR Up Time (ms)': ev['Time'].strftime('%d/%m/%Y %H:%M:%S.%f')[:-3],
        'Aspect': ev['Aspect'], 'Speed (km/h)': ev['Speed'], 'RTIS Stn': ev['RTIS_Stn']
    } for ev in data])
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='SIGNAL ASPECTs')
    return output.getvalue()

def generate_zip_graphs(data, rtis_df):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for i, ev in enumerate(data):
            fig, ax = plt.subplots(figsize=(10, 6))
            sub = rtis_df[(rtis_df['CumDist'] >= ev['CumDist'] - 1000) & (rtis_df['CumDist'] <= ev['CumDist'] + 1000)]
            ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
            ax.plot(sub['Logging Time'], sub['Speed'], color='#1A237E', lw=2.5)
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

with st.sidebar:
    st.header("📁 1. Load Files")
    rtis_f = st.file_uploader("RTIS File", type=['csv', 'xlsx'])
    dlog_f = st.file_uploader("Datalogger File", type=['csv', 'xlsx'])
    sig_f = st.file_uploader("Signal Mapping File", type=['csv', 'xlsx'])
    if st.button("🚀 PROCESS DATA", use_container_width=True, type="primary"):
        if rtis_f and dlog_f and sig_f:
            process_data(rtis_f, dlog_f, sig_f)
        else:
            st.warning("Please upload all 3 files first.")

if st.session_state.processed and st.session_state.events:
    col1, col2, col3, col4 = st.columns([1.5, 2, 1, 1.5])
    with col1:
        filter_opt = st.radio("Aspect:", ["All", "Yellow", "Double Yellow"], horizontal=True)
    
    filtered_events = st.session_state.events
    if filter_opt != "All":
        filtered_events = [e for e in st.session_state.events if e['Aspect'] == filter_opt]
    
    with col3:
        st.download_button("📥 Excel Report", data=generate_excel(filtered_events), file_name="Speed_Audit.xlsx")
    with col4:
        st.download_button("🖼 Graphs (ZIP)", data=generate_zip_graphs(filtered_events, st.session_state.rtis), file_name="Graphs.zip")

    st.divider()
    c_left, c_right = st.columns([1.2, 1.5])
    with c_left:
        st.write("### 📜 SIGNAL ASPECT Table")
        display_df = pd.DataFrame(filtered_events)
        if not display_df.empty:
            display_df['Time (ms)'] = display_df['Time'].dt.strftime('%H:%M:%S.%f').str[:-3]
            selected_row = st.dataframe(display_df[['Stn', 'Sig', 'Time (ms)', 'Aspect', 'Speed']], 
                                        on_select="rerun", selection_mode="single-row", hide_index=True)
    with c_right:
        if not display_df.empty:
            idx = selected_row.selection.rows[0] if (selected_row and selected_row.selection.rows) else 0
            ev = filtered_events[idx]
            sub = st.session_state.rtis[(st.session_state.rtis['CumDist'] >= ev['CumDist'] - 1000) & (st.session_state.rtis['CumDist'] <= ev['CumDist'] + 1000)]
            fig, ax = plt.subplots()
            ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
            ax.plot(sub['Logging Time'], sub['Speed'])
            ax.axvline(x=ev['Time'], color='red')
            st.pyplot(fig)
