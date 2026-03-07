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
BG_MAP = {"Green": "#E6FFFA", "Yellow": "#FFFFE0", "Double Yellow": "#FFF5E6", "Red": "#F2F2F2"}

if 'events' not in st.session_state: st.session_state.events = []
if 'rtis' not in st.session_state: st.session_state.rtis = None

# --- Logic Helpers ---
def clean_id(s):
    """Enhanced Cleaning: Removes all non-alphanumeric characters for matching"""
    if pd.isna(s): return None
    s_clean = re.sub(r'[^A-Z0-9]', '', str(s).upper())
    # Ensure it starts with S or A (Common Railway Prefixes)
    if not (s_clean.startswith('S') or s_clean.startswith('A')):
        s_clean = 'S' + s_clean
    return s_clean

def base_station(s):
    return str(s).split('_')[0].split('-')[0].split(' ')[0].upper()

def relay_type(name):
    name = str(name).upper()
    if any(x in name for x in ['DECR','DECPR', 'DGCR']): return 'Green'
    if any(x in name for x in ['HHECR','HHECPR', 'HHGCR']): return 'Double Yellow'
    if any(x in name for x in ['HECR', 'HGCR']): return 'Yellow'
    if any(x in name for x in ['RECR', 'RGCR']): return 'Red'
    return None

def load_data_smart(file, is_dlog=False):
    try:
        if file.name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file, engine='openpyxl')
        else:
            df = pd.read_csv(file, encoding='latin1', on_bad_lines='skip', low_memory=False)
        
        if is_dlog:
            # Flexible column detection for Datalogger
            actual_cols = df.columns.tolist()
            stn_col = next((c for c in actual_cols if any(k in c.upper() for k in ['STATION', 'STN'])), None)
            sig_col = next((c for c in actual_cols if any(k in c.upper() for k in ['SIGNALNAME', 'SIGNAME', 'SIGNAL'])), None)
            sts_col = next((c for c in actual_cols if any(k in c.upper() for k in ['STATUS', 'STATE'])), None)
            tim_col = next((c for c in actual_cols if any(k in c.upper() for k in ['TIME', 'DATETIME'])), None)
            
            df = df[[stn_col, sig_col, sts_col, tim_col]].dropna()
            df.columns = ['STATION NAME', 'SIGNAL NAME', 'SIGNAL STATUS', 'SIGNAL TIME']
        return df
    except Exception as e:
        st.error(f"Error reading {file.name}: {e}")
        return None

# --- Processing Engine ---
def process_files(rtis_file, dlog_file, sig_file):
    with st.spinner("Correlating Data Streams..."):
        try:
            # 1. Load Mapping (Try Column 7 first, then search)
            sig_map = load_data_smart(sig_file)
            if sig_map is None: return
            
            # Extract signal IDs using fuzzy cleaning
            up_signals = set()
            col_to_use = sig_map.columns[6] if len(sig_map.columns) > 6 else sig_map.columns[0]
            for s in sig_map[col_to_use].dropna():
                up_signals.add(clean_id(s))
            
            # DEBUG: Show signals found in Sidebar
            with st.sidebar.expander("🔍 View Detected Signals"):
                st.write(list(up_signals)[:20], "...Total:", len(up_signals))

            # 2. Load RTIS
            rtis = load_data_smart(rtis_file)
            rtis['Logging Time'] = pd.to_datetime(rtis['Logging Time'], errors='coerce')
            rtis = rtis.dropna(subset=['Logging Time']).sort_values('Logging Time')
            rtis['CumDist'] = pd.to_numeric(rtis['distFromSpeed'], errors='coerce').fillna(0).cumsum()
            rtis['BASE_STN'] = rtis['STATION NAME'].apply(base_station)
            st.session_state.rtis = rtis

            # 3. Load Datalogger
            dlog = load_data_smart(dlog_file, is_dlog=True)
            dlog['dt'] = pd.to_datetime(dlog['SIGNAL TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True), errors='coerce')
            dlog = dlog.dropna(subset=['dt']).sort_values('dt')

            latch_aspect, last_down_event, raw_events = collections.defaultdict(lambda: "Red"), {}, []

            for _, row in dlog.iterrows():
                stn = base_station(row['STATION NAME'])
                sig_full = str(row['SIGNAL NAME']).strip().upper()
                sig_clean = clean_id(sig_full)
                
                if sig_clean not in up_signals: continue
                
                rtype = relay_type(sig_full)
                if not rtype: continue
                
                status = str(row['SIGNAL STATUS']).upper()
                is_up = any(x in status for x in ['UP', 'ON', 'PICKUP', 'CLOSED', 'OCCURRED'])
                key = (stn, sig_clean)

                if rtype == 'Red' and is_up:
                    ev_time = row['dt']
                    final_asp = latch_aspect[key]
                    if final_asp == "Red" and key in last_down_event:
                        down_asp, down_time = last_down_event[key]
                        if 0 <= (ev_time - down_time).total_seconds() <= 15:
                            final_asp = down_asp
                    
                    diffs = (rtis['Logging Time'] - ev_time).abs()
                    idx = diffs.idxmin()
                    pt = rtis.loc[idx]
                    
                    if pt['Speed'] > 1 and diffs[idx].total_seconds() <= 30: 
                        raw_events.append({
                            'Stn': stn, 'Sig': sig_clean, 'Time': ev_time,
                            'Aspect': final_asp, 'Speed': pt['Speed'],
                            'RTIS_Stn': pt['BASE_STN'], 'CumDist': pt['CumDist']
                        })
                    latch_aspect[key] = "Red"
                elif is_up: latch_aspect[key] = rtype
                else: last_down_event[key] = (rtype, row['dt'])

            st.session_state.events = sorted(raw_events, key=lambda x: x['Time'])
            if not st.session_state.events:
                st.warning("⚠️ Zero matches! Check if Signal IDs match between Mapping and Datalogger.")
            else:
                st.success(f"Audit Complete: {len(st.session_state.events)} events found.")
        except Exception as e: st.error(f"Processing Error: {e}")

# --- UI Header ---
st.markdown(f"<div style='text-align: center; background-color: {NAVY}; padding: 15px; border-radius: 10px; margin-bottom: 20px;'><h1 style='color: white; margin-bottom: 0;'>Loco-Speed Safety Audit Tool</h1><p style='color: {SAFFRON}; font-weight: bold; font-size: 18px; margin-top: 5px;'>Western Railway | ADEE TRO BL</p></div>", unsafe_allow_html=True)

with st.sidebar:
    st.header("📂 Data Import")
    rtis_up = st.file_uploader("1. RTIS File", type=['csv', 'xlsx'])
    dlog_up = st.file_uploader("2. Datalogger File", type=['csv', 'xlsx'])
    sig_up = st.file_uploader("3. Signal Mapping", type=['csv', 'xlsx'])
    if st.button("🚀 START SAFETY AUDIT", use_container_width=True):
        if rtis_up and dlog_up and sig_up: process_files(rtis_up, dlog_up, sig_up)
        else: st.error("Please upload all files.")

# --- Dashboard Display ---
if st.session_state.events:
    df = pd.DataFrame(st.session_state.events)
    col1, col2 = st.columns([1.2, 1])
    with col1:
        st.write("### Violation List")
        sel = st.dataframe(df[['Stn', 'Sig', 'Time', 'Aspect', 'Speed', 'RTIS_Stn']], on_select="rerun", selection_mode="single-row", hide_index=True)
    with col2:
        st.write("### Speed Profile Analysis")
        if sel and len(sel.selection.rows) > 0:
            ev = df.iloc[sel.selection.rows[0]]
            fig, ax = plt.subplots(figsize=(10, 6))
            sub = st.session_state.rtis[(st.session_state.rtis['CumDist'] >= ev['CumDist'] - 1000) & (st.session_state.rtis['CumDist'] <= ev['CumDist'] + 1000)]
            ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFF"))
            ax.plot(sub['Logging Time'], sub['Speed'], color=NAVY, lw=2.5)
            ax.axvline(x=ev['Time'], color='red', ls='--')
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            st.pyplot(fig)

st.markdown("---")
st.markdown("<div style='text-align: center; color: grey;'><b>ADEE TRO BL</b> | Indian Railways</div>", unsafe_allow_html=True)
