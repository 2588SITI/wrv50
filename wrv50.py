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
#         STREAMLIT PAGE SETUP - V45.6 (STABLE)
# =========================================================
st.set_page_config(page_title="Loco-Speed Safety Audit", layout="wide", page_icon="🚄")

SAFFRON = "#33D4FC"
BG_MAP = {"Green": "#0D860D", "Yellow": "#EEF153", "Double Yellow": "#EFA627", "Red": "#F2F2F2"}
ASPECT_PRIORITY = {"Double Yellow": 3, "Yellow": 2, "Green": 1, "Red": 0}

st.markdown(f"""
    <style>
    .top-header {{ background-color: {SAFFRON}; padding: 15px; border-radius: 10px; color: white; text-align: center; margin-bottom: 20px; }}
    </style>
    <div class="top-header"><h1 style='margin:0;'>🚄 Loco-Speed Audit (Precise Signal Analysis)</h1></div>
""", unsafe_allow_html=True)

if 'events' not in st.session_state: st.session_state.events = []
if 'rtis' not in st.session_state: st.session_state.rtis = None
if 'processed' not in st.session_state: st.session_state.processed = False

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
    with st.spinner("⏳ Analyzing RTIS Seconds and Signal Relay States..."):
        try:
            # 1. Load Mapping
            sig_map = load_file(sig_up.name, sig_up.getvalue())
            up_signals = {clean_id(s) for s in sig_map.iloc[:, 6].dropna().astype(str) if clean_id(s)}

            # 2. Load and Fix RTIS (Handle seconds assignment)
            rtis = load_file(rtis_up.name, rtis_up.getvalue())
            rtis.columns = rtis.columns.str.strip()
            rtis['Minute_Time'] = pd.to_datetime(rtis['Logging Time'], format='mixed', dayfirst=True, errors='coerce')
            rtis = rtis.dropna(subset=['Minute_Time']).sort_values('Minute_Time')
            
            # Assignment of seconds 0-59 for records within the same minute
            rtis['sec_offset'] = rtis.groupby('Minute_Time').cumcount() % 60
            rtis['Logging Time'] = rtis['Minute_Time'] + pd.to_timedelta(rtis['sec_offset'], unit='s')
            
            rtis['CumDist'] = pd.to_numeric(rtis.get('distFromSpeed', 0), errors='coerce').fillna(0).cumsum()
            rtis['BASE_STN'] = rtis['STATION NAME'].astype(str).apply(base_station)
            st.session_state.rtis = rtis

            # 3. Load Datalogger
            dlog = load_file(dlog_up.name, dlog_up.getvalue())
            dlog.columns = dlog.columns.str.strip()
            dlog = dlog.rename(columns={'STATION NAME': 'STATION_NAME', 'SIGNAL NAME': 'SIGNAL_NAME', 'SIGNAL STATUS': 'SIGNAL_STATUS', 'SIGNAL TIME': 'SIGNAL_TIME'})
            
            time_series = dlog['SIGNAL_TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True)
            dlog['dt'] = pd.to_datetime(time_series, format='mixed', dayfirst=True, errors='coerce')
            
            dlog['rtype'] = dlog['SIGNAL_NAME'].apply(relay_type)
            dlog['status_val'] = dlog['SIGNAL_STATUS'].apply(lambda x: 0 if any(y in str(x).upper() for y in ['DOWN', 'OFF', 'DROP']) else 1)
            dlog['prio_val'] = dlog['rtype'].map(ASPECT_PRIORITY).fillna(0)
            
            # Sort: Time -> Drops before Picks -> High Aspect Priority (HHECR) before Low
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
                    
                    # Speed logic: Find nearest RTIS second
                    diffs = (stn_rtis['Logging Time'] - ev_time).abs()
                    nearest_idx = diffs.idxmin()
                    
                    if diffs[nearest_idx].total_seconds() <= 15:
                        precise_speed = stn_rtis.loc[nearest_idx, 'Speed']
                        
                        if precise_speed > 1:
                            final_asp = latch_aspect[key]
                            if key in simult_drops:
                                valid_drops = [d for d in simult_drops[key] if 0 <= (ev_time - d['time']).total_seconds() <= 5]
                                if valid_drops:
                                    final_asp = max(valid_drops, key=lambda x: ASPECT_PRIORITY.get(x['asp'], 0))['asp']
                            
                            raw_events.append({
                                'Stn': stn, 'Sig': sig, 'Time': ev_time,
                                'Aspect': final_asp, 'Speed': precise_speed,
                                'CumDist': stn_rtis.loc[nearest_idx, 'CumDist']
                            })
                    latch_aspect[key] = "Red"
                    simult_drops[key] = [] 
                
                elif row.rtype in ['Green', 'Double Yellow', 'Yellow']:
                    if is_up: latch_aspect[key] = row.rtype
                    else: simult_drops[key].append({'asp': row.rtype, 'time': row.dt})

            # 4. DEDUPLICATION (Flicker Handling): Merge events within 60s for the same signal pass
            if raw_events:
                df_res = pd.DataFrame(raw_events).sort_values(['Stn', 'Sig', 'Time'])
                # Group by station/signal and keep the last one of every pass (diff > 60s)
                df_res['pass_grp'] = (df_res.groupby(['Stn', 'Sig'])['Time'].diff().dt.total_seconds() > 60).cumsum()
                st.session_state.events = df_res.groupby(['Stn', 'Sig', 'pass_grp'], as_index=False).last().to_dict('records')
            
            st.session_state.processed = True
            st.success(f"✅ Processed {len(st.session_state.events)} events. UDN S46 speed aligned.")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

# --- UI Layout (Graphing & Tables) ---
def draw_plot(ev, rtis_df):
    sub = rtis_df[(rtis_df['CumDist'] >= ev['CumDist'] - 1200) & (rtis_df['CumDist'] <= ev['CumDist'] + 1200)]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
    ax.plot(sub['Logging Time'], sub['Speed'], marker='o', color='#1A237E', lw=2)
    ax.axvline(x=ev['Time'], color='red', ls='--')
    ms_time = ev['Time'].strftime('%H:%M:%S.%f')[:-3]
    label = f"STN: {ev['Stn']} | SIG: {ev['Sig']}\nCROSSING: {ms_time}\nSPEED: {ev['Speed']} km/h\n{ev['Aspect']} ➔ RED"
    ax.annotate(label, xy=(ev['Time'], ev['Speed']), xytext=(30, 30), textcoords='offset points', fontweight='bold', bbox=dict(boxstyle="round", fc="white", ec="red"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    return fig

with st.sidebar:
    st.header("📁 Load Files")
    rtis_f = st.file_uploader("RTIS", type=['csv', 'xlsx'])
    dlog_f = st.file_uploader("Datalogger", type=['csv', 'xlsx'])
    sig_f = st.file_uploader("Signal Map", type=['csv', 'xlsx'])
    if st.button("🚀 PROCESS", use_container_width=True, type="primary"):
        if rtis_f and dlog_f and sig_f: process_data(rtis_f, dlog_f, sig_f)

if st.session_state.processed and st.session_state.events:
    df_disp = pd.DataFrame(st.session_state.events)
    df_disp['Time_ms'] = df_disp['Time'].dt.strftime('%H:%M:%S.%f').str[:-3]
    c1, c2 = st.columns([1, 1.5])
    with c1:
        sel = st.dataframe(df_disp[['Stn', 'Sig', 'Time_ms', 'Aspect', 'Speed']], on_select="rerun", selection_mode="single-row", hide_index=True)
    with c2:
        idx = sel.selection.rows[0] if (sel and sel.selection.rows) else 0
        st.pyplot(draw_plot(st.session_state.events[idx], st.session_state.rtis))
