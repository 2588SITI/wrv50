import os
import re
import io
import collections
import zipfile
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg
import matplotlib.dates as mdates

# =========================================================
#          STREAMLIT PAGE SETUP - V46.0 (PRIORITY LOGIC)
# =========================================================
st.set_page_config(page_title="Loco-Speed Safety Audit", layout="wide", page_icon="🚄")

SAFFRON = "#33D4FC"
BG_MAP = {
    "Green": "#0D860D",
    "Yellow": "#EEF153",
    "Double Yellow": "#EFA627",
    "Red": "#F2F2F2"
}

st.markdown(f"""
    <style>
    .top-header {{ background-color: {SAFFRON}; padding: 15px; border-radius: 10px; color: white; text-align: center; margin-bottom: 20px; }}
    </style>
    <div class="top-header">
        <h1 style='margin:0;'>🚄 Loco-Speed Safety Audit </h1>
    </div>
""", unsafe_allow_html=True)

if 'events' not in st.session_state: st.session_state.events = []
if 'rtis' not in st.session_state: st.session_state.rtis = None
if 'processed' not in st.session_state: st.session_state.processed = False

# =========================================================
#                     HELPER FUNCTIONS
# =========================================================
def clean_id(s):
    # Modified to capture alphanumeric signal IDs like A25823
    m = re.search(r'([AS])?-?(\d+)', str(s).upper())
    if m:
        prefix = m.group(1) or 'S'
        return f"{prefix}{m.group(2)}"
    return None

def base_station(s):
    return str(s).split('_')[0].split('-')[0].split(' ')[0].upper()

def relay_type(name):
    name = str(name).upper()
    # Priority Keywords Matching
    if re.search(r'\b(HHECR|HHECPR|HHGCR|H_HH_ECR)\b', name) or 'HHECPR2_K' in name: 
        return 'Double Yellow'
    if re.search(r'\b(DECR|DECPR|DGCR)\b', name) or 'DECPR_K' in name: 
        return 'Green'
    if re.search(r'\b(HECR|HGCR|HECPR)\b', name): 
        return 'Yellow'
    if re.search(r'\b(RECR|RGCR)\b', name): 
        return 'Red'
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
#           CORE PROCESSING - PRIORITY & PASSING LOGIC
# =========================================================
def process_data(rtis_up, dlog_up, sig_up):
    with st.spinner("⏳ Processing Signals with Priority Logic..."):
        try:
            # 1. Load Files
            sig_map = load_file(sig_up.name, sig_up.getvalue())
            # Capture all possible signal IDs from mapping
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
            dlog = dlog.rename(columns={'STATION NAME': 'STATION_NAME', 'SIGNAL NAME': 'SIGNAL_NAME', 
                                      'SIGNAL STATUS': 'SIGNAL_STATUS', 'SIGNAL TIME': 'SIGNAL_TIME'})
            
            time_series = dlog['SIGNAL_TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True)
            dlog['dt'] = pd.to_datetime(time_series, format='mixed', dayfirst=True, errors='coerce')
            dlog = dlog.dropna(subset=['dt']).sort_values(['dt', 'SIGNAL_NAME']) # Sort by time then name

            # --- Logic: Priority & Latching ---
            # Priority Level: Green(3) > Double Yellow(2) > Yellow(1) > Red(0)
            priority = {"Green": 3, "Double Yellow": 2, "Yellow": 1, "Red": 0}
            
            latch_aspect = collections.defaultdict(lambda: "Red")
            raw_events = []

            for row in dlog.itertuples(index=False):
                stn = base_station(row.STATION_NAME)
                sig_full = str(row.SIGNAL_NAME).strip().upper()
                sig = clean_id(sig_full)
                if not sig or sig not in up_signals: continue
                
                rtype = relay_type(sig_full)
                if not rtype: continue

                status = str(row.SIGNAL_STATUS).upper()
                is_up = any(x in status for x in ['UP', 'ON', 'PICKUP', 'CLOSED', 'OCCURRED'])
                key = (stn, sig)

                if rtype == 'Red' and is_up:
                    # PASSING POINT: Get aspect stored JUST BEFORE Red
                    aspect_before_red = latch_aspect[key]
                    
                    if aspect_before_red != "Red":
                        ev_time = row.dt
                        diffs = (rtis['Logging Time'] - ev_time).abs()
                        idx = diffs.idxmin()
                        pt = rtis.loc[idx]
                        
                        if pt['Speed'] > 1 and diffs[idx].total_seconds() <= 15:
                            raw_events.append({
                                'Stn': stn, 'Sig': sig, 'Time': ev_time,
                                'Aspect': aspect_before_red, 
                                'Speed': pt['Speed'],
                                'CumDist': pt['CumDist'], 'RTIS_Stn': pt['BASE_STN']
                            })
                    latch_aspect[key] = "Red" 

                elif is_up:
                    # Update Latch with Priority Check
                    # Agar current memory mein Yellow hai aur naya relay Double Yellow hai, toh update karein.
                    current_stored = latch_aspect[key]
                    if priority.get(rtype, 0) >= priority.get(current_stored, 0):
                        latch_aspect[key] = rtype

            # 4. Filter Duplicates
            final_events = []
            if raw_events:
                df_ev = pd.DataFrame(raw_events).sort_values(['Stn', 'Sig', 'Time'])
                for _, grp in df_ev.groupby(['Stn', 'Sig']):
                    dt_check = grp['Time'].diff().shift(-1).dt.total_seconds()
                    valid = grp[dt_check.isna() | (dt_check > 15)]
                    final_events.extend(valid.to_dict('records'))

            st.session_state.events = sorted(final_events, key=lambda x: x['Time'])
            st.session_state.processed = True
            st.success(f"✅ Processed! Found {len(st.session_state.events)} pass-through events.")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

# =========================================================
#                     UI & DISPLAY
# =========================================================
def generate_excel(data):
    output = io.BytesIO()
    export_df = pd.DataFrame([{
        'Station': ev['Stn'], 'Signal': ev['Sig'], 
        'Passing Time': ev['Time'].strftime('%d/%m/%Y %H:%M:%S.%f')[:-3],
        'Aspect': ev['Aspect'], 'Speed (km/h)': ev['Speed']
    } for ev in data])
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='Audit')
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
            box_text = f"STN: {ev['Stn']}\nSIG: {ev['Sig']}\nASPECT: {ev['Aspect']}\nSPEED: {ev['Speed']} km/h"
            ax.annotate(box_text, xy=(ev['Time'], ev['Speed']), xytext=(20, 20), textcoords='offset points',
                        bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="red", lw=2, alpha=0.9),
                        arrowprops=dict(arrowstyle="-|>", connectionstyle="arc3,rad=0.3", color="red"),
                        fontweight='bold', fontsize=10)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            img_buffer = io.BytesIO()
            fig.savefig(img_buffer, format="png", bbox_inches='tight')
            plt.close(fig)
            zip_file.writestr(f"Graph_{ev['Stn']}_{ev['Sig']}.png", img_buffer.getvalue())
    return zip_buffer.getvalue()

with st.sidebar:
    st.header("📁 Load Files")
    rtis_f = st.file_uploader("RTIS", type=['csv', 'xlsx'])
    dlog_f = st.file_uploader("Datalogger", type=['csv', 'xlsx'])
    sig_f = st.file_uploader("Signal Map", type=['csv', 'xlsx'])
    if st.button("🚀 PROCESS", use_container_width=True, type="primary"):
        if rtis_f and dlog_f and sig_f: process_data(rtis_f, dlog_f, sig_f)

if st.session_state.processed and st.session_state.events:
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        f_opt = st.radio("Filter Aspect:", ["All", "Yellow", "Double Yellow", "Green"], horizontal=True)
    filtered = st.session_state.events
    if f_opt != "All": filtered = [e for e in st.session_state.events if e['Aspect'] == f_opt]
    
    st.download_button("📥 Excel", data=generate_excel(filtered), file_name="Report.xlsx")
    st.download_button("🖼 ZIP", data=generate_zip_graphs(filtered, st.session_state.rtis), file_name="Graphs.zip")

    st.divider()
    c_l, c_r = st.columns([1, 1.5])
    with c_l:
        df_disp = pd.DataFrame(filtered)
        if not df_disp.empty:
            df_disp['Time'] = df_disp['Time'].dt.strftime('%H:%M:%S.%f').str[:-3]
            sel = st.dataframe(df_disp[['Stn', 'Sig', 'Time', 'Aspect', 'Speed']], 
                             on_select="rerun", selection_mode="single-row", hide_index=True)
    with c_r:
        if not df_disp.empty:
            idx = sel.selection.rows[0] if (sel and len(sel.selection.rows) > 0) else 0
            ev = filtered[idx]
            fig, ax = plt.subplots(figsize=(10, 6))
            sub = st.session_state.rtis[(st.session_state.rtis['CumDist'] >= ev['CumDist'] - 1000) & (st.session_state.rtis['CumDist'] <= ev['CumDist'] + 1000)]
            ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
            ax.plot(sub['Logging Time'], sub['Speed'], color='#1A237E', lw=2)
            ax.axvline(x=ev['Time'], color='red', ls='--')
            ax.set_title(f"Passing: {ev['Stn']} {ev['Sig']} ({ev['Aspect']})")
            st.pyplot(fig)
