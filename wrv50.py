import os
import re
import collections
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from io import BytesIO

# --- Page Config ---
st.set_page_config(page_title="Railway Dashboard V44.6", layout="wide")

# --- Styling & Constants ---
SAFFRON = "#FF9933"
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

# --- Helper Functions ---
def clean_id(s):
    m = re.search(r'([AS])?-?(\d+)', str(s).upper())
    return f"{m.group(1) or 'S'}{m.group(2)}" if m else None

def base_station(s):
    return str(s).split('_')[0].split('-')[0].split(' ')[0].upper()

def relay_type(name):
    name = name.upper()
    if any(x in name for x in ['DECR','DECPR_K','DECPR', 'DGCR']): return 'Green'
    if any(x in name for x in ['HHECR','HHECPR2_K', 'HHGCR']): return 'Double Yellow'
    if any(x in name for x in ['HECR', 'HGCR']): return 'Yellow'
    if any(x in name for x in ['RECR', 'RGCR']): return 'Red'
    return None

def load_data(file):
    if file.name.endswith(('.xlsx', '.xls')):
        return pd.read_excel(file, engine='openpyxl')
    return pd.read_csv(file, encoding='latin1', on_bad_lines='skip', low_memory=False)

# --- Core Logic ---
def process_files(rtis_file, dlog_file, sig_file):
    try:
        sig_map = load_data(sig_file)
        up_signals = {clean_id(s) for s in sig_map.iloc[:, 6].dropna().astype(str) if clean_id(s)}

        rtis = load_data(rtis_file)
        rtis.columns = rtis.columns.str.strip()
        rtis['Logging Time'] = pd.to_datetime(rtis['Logging Time'], errors='coerce')
        rtis = rtis.dropna(subset=['Logging Time']).sort_values('Logging Time')
        rtis['CumDist'] = pd.to_numeric(rtis['distFromSpeed'], errors='coerce').fillna(0).cumsum()
        rtis['BASE_STN'] = rtis['STATION NAME'].apply(base_station)
        st.session_state.rtis = rtis

        dlog = load_data(dlog_file)
        dlog.columns = dlog.columns.str.strip()
        dlog['dt'] = pd.to_datetime(dlog['SIGNAL TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True),
                                     format='%d/%m/%Y %H:%M:%S.%f', errors='coerce')
        dlog = dlog.dropna(subset=['dt']).sort_values('dt')

        latch_aspect = collections.defaultdict(lambda: "Red")
        last_down_event = {}
        raw_events = []

        for _, row in dlog.iterrows():
            stn = base_station(row['STATION NAME'])
            sig_full = str(row['SIGNAL NAME']).strip().upper()
            sig = clean_id(sig_full)
            if sig not in up_signals: continue
            status = str(row['SIGNAL STATUS']).upper()
            key = (stn, sig)
            is_up = any(x in status for x in ['UP', 'ON', 'PICKUP', 'CLOSED', 'OCCURRED'])
            rtype = relay_type(sig_full)
            if not rtype: continue

            if rtype == 'Red' and is_up:
                ev_time = row['dt']
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
                        'RTIS_Idx': idx, 'RTIS_Stn': pt['BASE_STN'],
                        'CumDist': pt['CumDist']
                    })
                latch_aspect[key] = "Red"
            elif rtype in ['Green', 'Double Yellow', 'Yellow']:
                if is_up: latch_aspect[key] = rtype
                else: last_down_event[key] = (rtype, row['dt'])

        final_events = []
        if raw_events:
            df = pd.DataFrame(raw_events).sort_values(['Stn', 'Sig', 'Time'])
            for _, grp in df.groupby(['Stn', 'Sig']):
                dt_check = grp['Time'].diff().shift(-1).dt.total_seconds()
                final_events.extend(grp[dt_check.isna() | (dt_check > 15)].to_dict('records'))

        st.session_state.events = sorted(final_events, key=lambda x: x['Time'])
        st.success(f"Processed {len(st.session_state.events)} events!")
    except Exception as e:
        st.error(f"Error processing files: {e}")

# --- UI Layout ---
st.title("WR YY & Y Speed Analyzer - V44.6")

# Sidebar for file uploads
with st.sidebar:
    st.header("1. Upload Data")
    rtis_up = st.file_uploader("RTIS File", type=['csv', 'xlsx'])
    dlog_up = st.file_uploader("Datalogger File", type=['csv', 'xlsx'])
    sig_up = st.file_uploader("Signal Mapping File", type=['csv', 'xlsx'])
    
    if st.button("🚀 PROCESS DATA"):
        if rtis_up and dlog_up and sig_up:
            process_files(rtis_up, dlog_up, sig_up)
        else:
            st.warning("Please upload all 3 files.")

# Main Dashboard
if st.session_state.events:
    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("Event List")
        filter_opt = st.radio("Filters:", ["All", "Yellow", "Double Yellow"], horizontal=True)
        
        # Apply Filter
        display_df = pd.DataFrame(st.session_state.events)
        if filter_opt != "All":
            display_df = display_df[display_df['Aspect'] == filter_opt]
        
        # Selection
        selected_indices = st.dataframe(
            display_df[['Stn', 'Sig', 'Time', 'Aspect', 'Speed', 'RTIS_Stn']],
            on_select="rerun",
            selection_mode="single-row"
        )

    with col2:
        st.subheader("Analysis Graph")
        if selected_indices and len(selected_indices.selection.rows) > 0:
            idx = selected_indices.selection.rows[0]
            ev = display_df.iloc[idx]
            
            # Plotting
            fig, ax = plt.subplots(figsize=(10, 6))
            rtis = st.session_state.rtis
            sub = rtis[(rtis['CumDist'] >= ev['CumDist'] - 1000) & (rtis['CumDist'] <= ev['CumDist'] + 1000)]
            
            ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
            ax.plot(sub['Logging Time'], sub['Speed'], color='#1A237E', lw=2.5, label='Speed')
            ax.axvline(x=ev['Time'], color='red', linestyle='--', linewidth=2)
            
            time_ms = ev['Time'].strftime('%H:%M:%S.%f')[:-3]
            info_box = f"STN: {ev['Stn']}\nSIG: {ev['Sig']}\nTIME: {time_ms}\nSPEED: {ev['Speed']} km/h"
            
            ax.annotate(info_box, xy=(ev['Time'], ev['Speed']), xytext=(20, 20), textcoords='offset points',
                         bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="red", lw=2, alpha=0.9),
                         arrowprops=dict(arrowstyle="-|>", connectionstyle="arc3,rad=0.3", color="red"),
                         fontweight='bold', fontsize=10)

            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            ax.set_title(f"{ev['Stn']} | {ev['Sig']} | {ev['Aspect']} -> RED", fontweight='bold')
            ax.set_ylabel("Speed (km/h)")
            ax.grid(True, alpha=0.3)
            st.pyplot(fig)
        else:
            st.info("Select a row in the table to view the graph.")

    # Export Section
    st.divider()
    st.subheader("📥 Downloads")
    csv = display_df.to_csv(index=False).encode('utf-8')
    st.download_button("Download Report as CSV", data=csv, file_name="railway_report.csv", mime="text/csv")
