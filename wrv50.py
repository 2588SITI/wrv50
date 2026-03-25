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
#          Loco-Speed Safety Audit - V51.0 (FINAL)
# =========================================================
st.set_page_config(page_title="Loco-Speed Safety Audit", layout="wide", page_icon="🚄")

SAFFRON = "#33D4FC"
BG_MAP = {"Green": "#0D860D", "Yellow": "#EEF153", "Double Yellow": "#EFA627", "Red": "#F2F2F2"}

st.markdown(f"""
    <div style="background-color:{SAFFRON}; padding:15px; border-radius:10px; color:white; text-align:center;">
        <h1 style='margin:0;'>🚄 Loco-Speed Safety Audit (Precision Mode) </h1>
    </div>
""", unsafe_allow_html=True)

if 'events' not in st.session_state: st.session_state.events = []
if 'rtis' not in st.session_state: st.session_state.rtis = None
if 'processed' not in st.session_state: st.session_state.processed = False

def clean_id(s):
    s = str(s).strip().upper()
    m = re.search(r'([AS])?-?(\d+)', s)
    if m:
        prefix = m.group(1) or 'S'
        return f"{prefix}{m.group(2)}"
    return None

def base_station(s):
    return str(s).split('_')[0].split('-')[0].split(' ')[0].upper()

def relay_type(name):
    name = str(name).upper()
    # Word boundary removed to match A25823RECR style names
    if re.search(r'HHECR|HHECPR|HHGCR|H_HH_ECR|HHECPR2_K', name): return 'Double Yellow'
    if re.search(r'DECR|DECPR|DGCR|DECPR_K', name): return 'Green'
    if re.search(r'HECR|HGCR|HECPR', name): return 'Yellow'
    if re.search(r'RECR|RGCR', name): return 'Red'
    return None

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

def process_data(rtis_up, dlog_up, sig_up):
    with st.spinner("⏳ Analyzing Pass-through Events..."):
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
            dlog = dlog.rename(columns={'STATION NAME': 'STATION_NAME', 'SIGNAL NAME': 'SIGNAL_NAME', 
                                      'SIGNAL STATUS': 'SIGNAL_STATUS', 'SIGNAL TIME': 'SIGNAL_TIME'})
            
            time_series = dlog['SIGNAL_TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True)
            dlog['dt'] = pd.to_datetime(time_series, format='mixed', dayfirst=True, errors='coerce')
            
            priority_map = {"Red": 0, "Yellow": 1, "Double Yellow": 2, "Green": 3}
            dlog['rtype'] = dlog['SIGNAL_NAME'].apply(relay_type)
            dlog['prio'] = dlog['rtype'].map(priority_map).fillna(-1)
            dlog = dlog.dropna(subset=['dt']).sort_values(['dt', 'prio'])

            latch_aspect = collections.defaultdict(lambda: "Red")
            raw_events = []

            for row in dlog.itertuples(index=False):
                stn = base_station(row.STATION_NAME)
                sig = clean_id(row.SIGNAL_NAME)
                if not sig or sig not in up_signals: continue
                
                rtype = row.rtype
                if not rtype: continue
                status = str(row.SIGNAL_STATUS).upper()
                is_up = any(x in status for x in ['UP', 'ON', 'PICKUP', 'CLOSED', 'OCCURRED'])
                key = (stn, sig)

                if rtype == 'Red' and is_up:
                    aspect_before_red = latch_aspect[key]
                    if aspect_before_red != "Red":
                        ev_time = row.dt
                        diffs = (rtis['Logging Time'] - ev_time).abs()
                        idx = diffs.idxmin()
                        pt = rtis.loc[idx]
                        
                        # PHYSICAL PASSING CHECK:
                        # 1. Speed MUST be > 0
                        # 2. For Auto signals ('A'), we allow station mismatch as they are between stations
                        is_auto = sig.startswith('A')
                        stn_match = (pt['BASE_STN'] == stn) or is_auto
                        
                        if pt['Speed'] > 0 and diffs[idx].total_seconds() <= 15 and stn_match:
                            raw_events.append({
                                'Stn': stn, 'Sig': sig, 'Time': ev_time,
                                'Aspect': aspect_before_red, 'Speed': pt['Speed'],
                                'CumDist': pt['CumDist'], 'RTIS_Stn': pt['BASE_STN']
                            })
                    latch_aspect[key] = "Red" 

                elif is_up and rtype in ['Green', 'Double Yellow', 'Yellow']:
                    if priority_map.get(rtype, 0) >= priority_map.get(latch_aspect[key], 0):
                        latch_aspect[key] = rtype

            final_events = []
            if raw_events:
                df_ev = pd.DataFrame(raw_events).sort_values(['Stn', 'Sig', 'Time'])
                for _, grp in df_ev.groupby(['Stn', 'Sig']):
                    dt_check = grp['Time'].diff().shift(-1).dt.total_seconds()
                    valid = grp[dt_check.isna() | (dt_check > 15)]
                    final_events.extend(valid.to_dict('records'))

            st.session_state.events = sorted(final_events, key=lambda x: x['Time'])
            st.session_state.processed = True
            st.success(f"✅ Processed {len(st.session_state.events)} events.")
        except Exception as e: st.error(f"❌ Error: {str(e)}")

def generate_excel(data):
    output = io.BytesIO()
    export_df = pd.DataFrame([{
        'Station': ev['Stn'], 'Signal': ev['Sig'], 
        'Passing Time': ev['Time'].strftime('%d/%m/%Y %H:%M:%S.%f')[:-3],
        'Aspect': ev['Aspect'], 'Speed (km/h)': ev['Speed'], 'RTIS Location': ev['RTIS_Stn']
    } for ev in data])
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='Audit')
    return output.getvalue()

with st.sidebar:
    rtis_f = st.file_uploader("RTIS", type=['csv', 'xlsx'])
    dlog_f = st.file_uploader("Datalogger", type=['csv', 'xlsx'])
    sig_f = st.file_uploader("Signal Map", type=['csv', 'xlsx'])
    if st.button("🚀 PROCESS"): process_data(rtis_f, dlog_f, sig_f)

if st.session_state.processed:
    st.download_button("📥 Excel Audit", data=generate_excel(st.session_state.events), file_name="Safety_Audit.xlsx")
    st.dataframe(pd.DataFrame(st.session_state.events), use_container_width=True)
