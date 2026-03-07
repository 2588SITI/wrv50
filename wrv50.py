import os
import re
import collections
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from io import BytesIO

# --- Page Configuration ---
st.set_page_config(page_title="Loco-Speed Safety Audit Tool", layout="wide")

# --- Constants & Styling ---
SAFFRON = "#FF9933"
NAVY = "#1A237E"
BG_MAP = {
    "Green": "#E6FFFA",
    "Yellow": "#FFFFE0",
    "Double Yellow": "#FFF5E6",
    "Red": "#F2F2F2"
}

# --- Session State Initialization ---
if 'events' not in st.session_state:
    st.session_state.events = []
if 'rtis' not in st.session_state:
    st.session_state.rtis = None

# --- Intelligence: Auto-Column Finder ---
def get_best_column(actual_cols, target_keywords):
    for col in actual_cols:
        col_clean = str(col).upper().replace(" ", "").replace("_", "")
        if any(key in col_clean for key in target_keywords):
            return col
    return None

def load_data_smart(file, is_dlog=False, is_sig_map=False):
    try:
        if file is None: return None
        
        if file.name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file, engine='openpyxl')
        else:
            df = pd.read_csv(file, encoding='latin1', on_bad_lines='skip', low_memory=False)
            
        if df.empty: return None
        actual_cols = df.columns.tolist()

        if is_dlog:
            stn_col = get_best_column(actual_cols, ['STATION', 'STN'])
            sig_col = get_best_column(actual_cols, ['SIGNALNAME', 'SIGNAM', 'SIGNAME'])
            sts_col = get_best_column(actual_cols, ['STATUS', 'STATE'])
            tim_col = get_best_column(actual_cols, ['TIME', 'DATETIME'])
            needed = [c for c in [stn_col, sig_col, sts_col, tim_col] if c]
            df = df[needed]
            mapping = {stn_col: 'STATION NAME', sig_col: 'SIGNAL NAME', sts_col: 'SIGNAL STATUS', tim_col: 'SIGNAL TIME'}
            return df.rename(columns=mapping)
        
        if is_sig_map:
            sig_id_col = get_best_column(actual_cols, ['SIGNAL', 'SIGID', 'UPSIGNAL', 'ID'])
            if sig_id_col:
                return df.rename(columns={sig_id_col: 'SIGNAL_ID'})
        
        return df
    except Exception as e:
        st.error(f"Error reading {file.name}: {e}")
        return None

# --- Logic Helpers ---
def clean_id(s):
    if pd.isna(s): return None
    m = re.search(r'([AS])?-?(\d+)', str(s).upper())
    return f"{m.group(1) or 'S'}{m.group(2)}" if m else None

def base_station(s):
    return str(s).split('_')[0].split('-')[0].split(' ')[0].upper()

def relay_type(name):
    name = str(name).upper()
    if any(x in name for x in ['DECR','DECPR', 'DGCR']): return 'Green'
    if any(x in name for x in ['HHECR','HHECPR', 'HHGCR']): return 'Double Yellow'
    if any(x in name for x in ['HECR', 'HGCR']): return 'Yellow'
    if any(x in name for x in ['RECR', 'RGCR']): return 'Red'
    return None

# --- Processing Engine ---
def process_files(rtis_file, dlog_file, sig_file):
    with st.spinner("Analyzing Safety Parameters..."):
        try:
            sig_map = load_data_smart(sig_file, is_sig_map=True)
            if sig_map is None: return
            
            # Smart Signal Extraction
            if 'SIGNAL_ID' in sig_map.columns:
                up_signals = {clean_id(s) for s in sig_map['SIGNAL_ID'].dropna() if clean_id(s)}
            else:
                up_signals = {clean_id(s) for s in sig_map.iloc[:, 6].dropna() if clean_id(s)}

            rtis = load_data_smart(rtis_file)
            if rtis is None: return
            rtis.columns = rtis.columns.str.strip()
            rtis['Logging Time'] = pd.to_datetime(rtis['Logging Time'], errors='coerce')
            rtis = rtis.dropna(subset=['Logging Time']).sort_values('Logging Time')
            rtis['CumDist'] = pd.to_numeric(rtis['distFromSpeed'], errors='coerce').fillna(0).cumsum()
            rtis['BASE_STN'] = rtis['STATION NAME'].apply(base_station)
            st.session_state.rtis = rtis

            dlog = load_data_smart(dlog_file, is_dlog=True)
            if dlog is None: return
            dlog['dt'] = pd.to_datetime(dlog['SIGNAL TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True), errors='coerce')
            dlog = dlog.dropna(subset=['dt']).sort_values('dt')

            latch_aspect, last_down_event, raw_events = collections.defaultdict(lambda: "Red"), {}, []

            for _, row in dlog.iterrows():
                stn = base_station(row['STATION NAME'])
                sig_full = str(row['SIGNAL NAME']).strip().upper()
                sig = clean_id(sig_full)
                if sig not in up_signals: continue
                
                status = str(row['SIGNAL STATUS']).upper()
                rtype = relay_type(sig_full)
                if not rtype: continue
                key = (stn, sig)
                is_up = any(x in status for x in ['UP', 'ON', 'PICKUP', 'CLOSED', 'OCCURRED'])

                if rtype == 'Red' and is_up:
                    ev_time = row['dt']
                    final_asp = latch_aspect[key]
                    if final_asp == "Red" and key in last_down_event:
                        down_asp, down_time = last_down_event[key]
                        if 0 <= (ev_time - down_time).total_seconds() <= 10:
                            final_asp = down_asp
                    
                    diffs = (rtis['Logging Time'] - ev_time).abs()
                    idx = diffs.idxmin()
                    pt = rtis.loc[idx]
                    
                    if pt['Speed'] > 1 and diffs[idx].total_seconds() <= 20: 
                        raw_events.append({
                            'Stn': stn, 'Sig': sig, 'Time': ev_time,
                            'Aspect': final_asp, 'Speed': pt['Speed'],
                            'RTIS_Stn': pt['BASE_STN'], 'CumDist': pt['CumDist']
                        })
                    latch_aspect[key] = "Red"
                elif is_up:
                    latch_aspect[key] = rtype
                else:
                    last_down_event[key] = (rtype, row['dt'])

            st.session_state.events = sorted(raw_events, key=lambda x: x['Time'])
            if not st.session_state.events:
                st.warning("No violations found. Please check if Signal Names in Datalogger match the Mapping file.")
            else:
                st.success(f"Safety Audit Complete: {len(st.session_state.events)} events found.")
        except Exception as e:
            st.error(f"Error: {e}")

# --- UI Layout ---
st.markdown(f"""
    <div style='text-align: center; background-color: {NAVY}; padding: 15px; border-radius: 10px; margin-bottom: 20px;'>
        <h1 style='color: white; margin-bottom: 0;'>Loco-Speed Safety Audit Tool</h1>
        <p style='color: {SAFFRON}; font-weight: bold; font-size: 18px; margin-top: 5px;'>Western Railway | ADEE TRO BL</p>
    </div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.header("📂 Data Import")
    rtis_up = st.file_uploader("1. RTIS File", type=['csv', 'xlsx'])
    dlog_up = st.file_uploader("2. Datalogger File", type=['csv', 'xlsx'])
    sig_up = st.file_uploader("3. Signal Mapping", type=['csv', 'xlsx'])
    if st.button("🚀 START SAFETY AUDIT", use_container_width=True):
        if rtis_up and dlog_up and sig_up:
            process_files(rtis_up, dlog_up, sig_up)
        else:
            st.error("Missing files!")

if st.session_state.events:
    df = pd.DataFrame(st.session_state.events)
    col1, col2 = st.columns([1.2, 1])
    with col1:
        st.write("### Violation List")
        selected = st.dataframe(df[['Stn', 'Sig', 'Time', 'Aspect', 'Speed', 'RTIS_Stn']],
                                on_select="rerun", selection_mode="single-row", hide_index=True)
    with col2:
        st.write("### Speed Profile Analysis")
        if selected and len(selected.selection.rows) > 0:
            ev = df.iloc[selected.selection.rows[0]]
            fig, ax = plt.subplots(figsize=(10, 6))
            rtis = st.session_state.rtis
            sub = rtis[(rtis['CumDist'] >= ev['CumDist'] - 1000) & (rtis['CumDist'] <= ev['CumDist'] + 1000)]
            ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFF"))
            ax.plot(sub['Logging Time'], sub['Speed'], color=NAVY, lw=2.5)
            ax.axvline(x=ev['Time'], color='red', linestyle='--', linewidth=2)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            st.pyplot(fig)

    st.divider()
    st.download_button("📄 Download Report", data=df.to_csv(index=False).encode('utf-8'), file_name="Safety_Audit.csv")

st.markdown("---")
st.markdown("<div style='text-align: center; color: grey;'><b>ADEE TRO BL</b><br>Indian Railways | Operational Safety Audit Tool</div>", unsafe_allow_html=True)
