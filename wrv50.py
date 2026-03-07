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

# --- Fast Loading Helper ---
def load_data_fast(file, is_dlog=False):
    """Optimized loading for large Railway Datalogger files"""
    try:
        if is_dlog:
            # Only load essential columns to save RAM and Time
            cols_needed = ['STATION NAME', 'SIGNAL NAME', 'SIGNAL STATUS', 'SIGNAL TIME']
            if file.name.endswith(('.xlsx', '.xls')):
                return pd.read_excel(file, engine='openpyxl', usecols=cols_needed)
            return pd.read_csv(file, encoding='latin1', on_bad_lines='skip', 
                               low_memory=False, usecols=cols_needed)
        
        if file.name.endswith(('.xlsx', '.xls')):
            return pd.read_excel(file, engine='openpyxl')
        return pd.read_csv(file, encoding='latin1', on_bad_lines='skip', low_memory=False)
    except Exception as e:
        st.error(f"Error reading file {file.name}: {e}")
        return None

# --- Processing Logic Helpers ---
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

# --- Main Processing Function ---
def process_files(rtis_file, dlog_file, sig_file):
    with st.spinner("Processing Large Datalogger Data... Please Wait"):
        try:
            sig_map = load_data_fast(sig_file)
            up_signals = {clean_id(s) for s in sig_map.iloc[:, 6].dropna().astype(str) if clean_id(s)}

            # Load RTIS
            rtis = load_data_fast(rtis_file)
            rtis.columns = rtis.columns.str.strip()
            rtis['Logging Time'] = pd.to_datetime(rtis['Logging Time'], errors='coerce')
            rtis = rtis.dropna(subset=['Logging Time']).sort_values('Logging Time')
            rtis['CumDist'] = pd.to_numeric(rtis['distFromSpeed'], errors='coerce').fillna(0).cumsum()
            rtis['BASE_STN'] = rtis['STATION NAME'].apply(base_station)
            st.session_state.rtis = rtis

            # Load Datalogger (Optimized)
            dlog = load_data_fast(dlog_file, is_dlog=True)
            dlog.columns = dlog.columns.str.strip()
            # Fast Date Conversion
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
            st.success(f"Analysis Complete: {len(st.session_state.events)} Safety Events Found.")
        except Exception as e:
            st.error(f"Critical Processing Error: {e}")

# --- Header Section ---
st.markdown(f"""
    <div style='text-align: center; background-color: {NAVY}; padding: 10px; border-radius: 10px;'>
        <h1 style='color: white; margin-bottom: 0;'>Loco-Speed Safety Audit Tool</h1>
        <p style='color: {SAFFRON}; font-weight: bold;'>Western Railway | ADEE TRO BL</p>
    </div>
""", unsafe_allow_html=True)

# --- Sidebar Inputs ---
with st.sidebar:
    st.header("📂 Data Import")
    rtis_up = st.file_uploader("1. RTIS File", type=['csv', 'xlsx'])
    dlog_up = st.file_uploader("2. Datalogger File", type=['csv', 'xlsx'])
    sig_up = st.file_uploader("3. Signal Mapping", type=['csv', 'xlsx'])
    
    if st.button("🚀 START SAFETY AUDIT", use_container_width=True):
        if rtis_up and dlog_up and sig_up:
            process_files(rtis_up, dlog_up, sig_up)
        else:
            st.warning("Please upload all required files.")

# --- Main Dashboard ---
if st.session_state.events:
    st.subheader("📊 Detected Signal-Speed Violations")
    filter_opt = st.radio("Filter Aspect before RED:", ["All", "Yellow", "Double Yellow"], horizontal=True)
    
    full_df = pd.DataFrame(st.session_state.events)
    display_df = full_df if filter_opt == "All" else full_df[full_df['Aspect'] == filter_opt]

    col1, col2 = st.columns([1.2, 1])

    with col1:
        st.write("### Violation List")
        selected = st.dataframe(
            display_df[['Stn', 'Sig', 'Time', 'Aspect', 'Speed', 'RTIS_Stn']],
            on_select="rerun",
            selection_mode="single-row",
            hide_index=True,
            use_container_width=True
        )

    with col2:
        st.write("### Speed Profile Analysis")
        if selected and len(selected.selection.rows) > 0:
            row_idx = selected.selection.rows[0]
            ev = display_df.iloc[row_idx]
            
            fig, ax = plt.subplots(figsize=(10, 6))
            rtis = st.session_state.rtis
            sub = rtis[(rtis['CumDist'] >= ev['CumDist'] - 1000) & (rtis['CumDist'] <= ev['CumDist'] + 1000)]
            
            ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
            ax.plot(sub['Logging Time'], sub['Speed'], color=NAVY, lw=2.5, label='Speed (km/h)')
            ax.axvline(x=ev['Time'], color='red', linestyle='--', linewidth=2, label='Signal Transition')
            
            time_ms = ev['Time'].strftime('%H:%M:%S.%f')[:-3]
            ax.annotate(f"STN: {ev['Stn']}\nSIG: {ev['Sig']}\nSPEED: {ev['Speed']} km/h", 
                        xy=(ev['Time'], ev['Speed']), xytext=(30, 30), textcoords='offset points',
                        bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="red", lw=2),
                        arrowprops=dict(arrowstyle="-|>", color="red"))

            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            ax.set_title(f"Detailed Analysis: {ev['Stn']} | Signal {ev['Sig']}")
            ax.set_ylabel("Speed (km/h)")
            ax.grid(True, alpha=0.3)
            st.pyplot(fig)
        else:
            st.info("Select a violation record from the left table to view the graph.")

    # --- Export Section ---
    st.divider()
    st.subheader("📥 Audit Export")
    dl_col1, dl_col2 = st.columns(2)

    with dl_col1:
        csv_report = display_df.to_csv(index=False).encode('utf-8')
        st.download_button("📄 Download Excel Report", data=csv_report, 
                           file_name=f"Safety_Audit_{filter_opt}.csv", mime="text/csv", use_container_width=True)

    with dl_col2:
        if st.button("📦 Generate All Annotated Graphs (ZIP)", use_container_width=True):
            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                for i, ev in enumerate(display_df.to_dict('records')):
                    fig, ax = plt.subplots(figsize=(10, 6))
                    rtis = st.session_state.rtis
                    sub = rtis[(rtis['CumDist'] >= ev['CumDist'] - 1000) & (rtis['CumDist'] <= ev['CumDist'] + 1000)]
                    ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
                    ax.plot(sub['Logging Time'], sub['Speed'], color=NAVY)
                    ax.axvline(x=ev['Time'], color='red', linestyle='--')
                    img_io = BytesIO()
                    fig.savefig(img_io, format='png')
                    plt.close(fig)
                    zip_file.writestr(f"Graph_{i+1}_{ev['Sig']}.png", img_io.getvalue())
            
            st.download_button("✅ Download ZIP Archive", data=zip_buffer.getvalue(), 
                               file_name="Loco_Speed_Graphs.zip", mime="application/zip", use_container_width=True)

