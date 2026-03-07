import os
import re
import collections
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import zipfile
from io import BytesIO

# --- Page Configuration ---
st.set_page_config(page_title="Railway Dashboard V44.6", layout="wide")

# --- Constants & Styling ---
SAFFRON = "#FF9933"
BG_MAP = {
    "Green": "#E6FFFA",
    "Yellow": "#FFFFE0",
    "Double Yellow": "#FFF5E6",
    "Red": "#F2F2F2"
}

# --- Session State Initialization ---
# This keeps data in memory when you click buttons
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

# --- Core Processing Logic ---
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
        st.success(f"Successfully processed {len(st.session_state.events)} events!")
    except Exception as e:
        st.error(f"Error during processing: {e}")

# --- User Interface ---
st.markdown(f"<h1 style='text-align: center; color: {SAFFRON};'>WR YY & Y Speed Analyzer - V44.6</h1>", unsafe_allow_html=True)

# Sidebar for Inputs
with st.sidebar:
    st.header("📂 Data Input")
    rtis_up = st.file_uploader("Upload RTIS File", type=['csv', 'xlsx'])
    dlog_up = st.file_uploader("Upload Datalogger File", type=['csv', 'xlsx'])
    sig_up = st.file_uploader("Upload Signal Mapping File", type=['csv', 'xlsx'])
    
    if st.button("🚀 PROCESS ALL DATA", use_container_width=True):
        if rtis_up and dlog_up and sig_up:
            process_files(rtis_up, dlog_up, sig_up)
        else:
            st.error("Please upload all three files first!")

# Main Dashboard View
if st.session_state.events:
    # 1. Filters
    st.subheader("📊 Event Analysis")
    filter_opt = st.radio("Show Aspects:", ["All", "Yellow", "Double Yellow"], horizontal=True)
    
    full_df = pd.DataFrame(st.session_state.events)
    display_df = full_df if filter_opt == "All" else full_df[full_df['Aspect'] == filter_opt]

    # 2. Main Layout (Table on left, Graph on right)
    col1, col2 = st.columns([1, 1])

    with col1:
        st.write("Click a row to view the detailed speed graph:")
        # Display table and capture selection
        event_selection = st.dataframe(
            display_df[['Stn', 'Sig', 'Time', 'Aspect', 'Speed', 'RTIS_Stn']],
            on_select="rerun",
            selection_mode="single-row",
            hide_index=True,
            use_container_width=True
        )

    with col2:
        if event_selection and len(event_selection.selection.rows) > 0:
            row_idx = event_selection.selection.rows[0]
            ev = display_df.iloc[row_idx]
            
            # Plot Individual Graph
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
            ax.set_title(f"ANALYSIS: {ev['Stn']} | {ev['Sig']} | {ev['Aspect']} -> RED", fontweight='bold')
            ax.set_ylabel("Speed (km/h)")
            ax.grid(True, alpha=0.3)
            st.pyplot(fig)
        else:
            st.info("👈 Select a record from the table to visualize the data.")

    # 3. Export & Download Section
    st.divider()
    st.subheader("📥 Export Center")
    dl_col1, dl_col2 = st.columns(2)

    with dl_col1:
        # Download Excel Report
        csv_data = display_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📄 Download Excel Report (CSV)",
            data=csv_data,
            file_name=f"Railway_Report_{filter_opt}.csv",
            mime="text/csv",
            use_container_width=True
        )

    with dl_col2:
        # ZIP Download for all graphs
        if st.button("📦 Generate All Graphs (ZIP)", use_container_width=True):
            with st.spinner("Creating high-precision graphs... please wait."):
                zip_buffer = BytesIO()
                with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                    for i, ev in enumerate(display_df.to_dict('records')):
                        # Recreate plot for each event
                        fig, ax = plt.subplots(figsize=(10, 6))
                        rtis = st.session_state.rtis
                        sub = rtis[(rtis['CumDist'] >= ev['CumDist'] - 1000) & (rtis['CumDist'] <= ev['CumDist'] + 1000)]
                        ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
                        ax.plot(sub['Logging Time'], sub['Speed'], color='#1A237E')
                        ax.axvline(x=ev['Time'], color='red', linestyle='--')
                        
                        time_ms = ev['Time'].strftime('%H:%M:%S.%f')[:-3]
                        ax.annotate(f"STN: {ev['Stn']}\nSIG: {ev['Sig']}\nSPEED: {ev['Speed']}", 
                                    xy=(ev['Time'], ev['Speed']), xytext=(15, 15), textcoords='offset points',
                                    bbox=dict(boxstyle="round", fc="white", ec="red", alpha=0.8))
                        
                        # Save to buffer
                        img_io = BytesIO()
                        fig.savefig(img_io, format='png')
                        plt.close(fig) # Prevent memory leak
                        
                        zip_file.writestr(f"Graph_{i+1}_{ev['Sig']}.png", img_io.getvalue())
                
                st.download_button(
                    label="✅ Download All Graphs (.ZIP)",
                    data=zip_buffer.getvalue(),
                    file_name="Annotated_Graphs.zip",
                    mime="application/zip",
                    use_container_width=True
                )
