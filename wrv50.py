import os
import re
import io
import collections
import zipfile
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

# =========================================================
#         STREAMLIT PAGE SETUP - V45.1 (PHYSICAL PASS)
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
    <div class="top-header"><h1 style='margin:0;'>🚄 Loco-Speed Audit (Physical Signal Crossing Mode)</h1></div>
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

ASPECT_PRIORITY = {"Double Yellow": 3, "Yellow": 2, "Green": 1, "Red": 0}

def get_interpolated_speed(target_time, rtis_df):
    """Calculates exact speed by interpolating between RTIS points."""
    before = rtis_df[rtis_df['Logging Time'] <= target_time].tail(1)
    after = rtis_df[rtis_df['Logging Time'] >= target_time].head(1)
    
    if before.empty or after.empty:
        if not rtis_df.empty:
            idx = (rtis_df['Logging Time'] - target_time).abs().idxmin()
            return rtis_df.loc[idx, 'Speed']
        return 0.0
    
    t1, v1 = before.iloc[0]['Logging Time'].timestamp(), before.iloc[0]['Speed']
    t2, v2 = after.iloc[0]['Logging Time'].timestamp(), after.iloc[0]['Speed']
    tx = target_time.timestamp()
    
    if t1 == t2: return v1
    return round(v1 + (v2 - v1) * (tx - t1) / (t2 - t1), 2)

# =========================================================
#                      CORE PROCESSING
# =========================================================
def process_data(rtis_up, dlog_up, sig_up):
    with st.spinner("⏳ Validating Physical Train Movement..."):
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
                    
                    # --- PHYSICAL VALIDATION CHECK ---
                    # 1. Check if RTIS records exist for this station at this time
                    stn_rtis = rtis[rtis['BASE_STN'] == stn]
                    if stn_rtis.empty: continue
                    
                    # 2. Check if the train was actually near this station (Time window check)
                    time_diff = (stn_rtis['Logging Time'] - ev_time).abs().min().total_seconds()
                    if time_diff > 30: continue # Agar train 30s se zyada door hai, toh ignore.

                    # 3. Get Interpolated Speed
                    precise_speed = get_interpolated_speed(ev_time, stn_rtis)
                    
                    # 4. Only capture if train is moving (Passing the signal)
                    if precise_speed < 1: continue 

                    # Determine Aspect
                    final_asp = latch_aspect[key]
                    if key in simult_drops:
                        valid_drops = [d for d in simult_drops[key] if 0 <= (ev_time - d['time']).total_seconds() <= 5]
                        if valid_drops:
                            best_drop = max(valid_drops, key=lambda x: ASPECT_PRIORITY.get(x['asp'], 0))
                            final_asp = best_drop['asp']
                    
                    # Store event
                    idx_closest = (rtis['Logging Time'] - ev_time).abs().idxmin()
                    raw_events.append({
                        'Stn': stn, 'Sig': sig, 'Time': ev_time,
                        'Aspect': final_asp, 'Speed': precise_speed,
                        'CumDist': rtis.loc[idx_closest, 'CumDist'], 'RTIS_Stn': stn
                    })
                    
                    latch_aspect[key] = "Red"
                    simult_drops[key] = [] 
                
                elif row.rtype in ['Green', 'Double Yellow', 'Yellow']:
                    if is_up: latch_aspect[key] = row.rtype
                    else: simult_drops[key].append({'asp': row.rtype, 'time': row.dt})

            # Final Cleanup: Filter duplicates (same signal passing within 2 mins)
            st.session_state.events = sorted(raw_events, key=lambda x: x['Time'])
            st.session_state.processed = True
            st.success(f"✅ Found {len(st.session_state.events)} physical signal crossing events.")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

# =========================================================
#                     UI & GRAPHING
# =========================================================
def draw_detailed_plot(ev, rtis_df):
    # Zoom in on the crossing event
    sub = rtis_df[(rtis_df['CumDist'] >= ev['CumDist'] - 1500) & (rtis_df['CumDist'] <= ev['CumDist'] + 1500)]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
    
    ax.plot(sub['Logging Time'], sub['Speed'], color='#1A237E', lw=2, marker='o', label="Train Path")
    ax.axvline(x=ev['Time'], color='red', linestyle='--', lw=2.5)
    
    ms_time = ev['Time'].strftime('%H:%M:%S.%f')[:-3]
    label = (f"STATION: {ev['Stn']}\nSIGNAL: {ev['Sig']}\n"
             f"CROSSING TIME: {ms_time}\nSPEED: {ev['Speed']} km/h\n"
             f"ASPECT: {ev['Aspect']} ➔ RED")
    
    ax.annotate(label, xy=(ev['Time'], ev['Speed']), xytext=(40, 40),
                textcoords='offset points', fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="red", alpha=0.9),
                arrowprops=dict(arrowstyle="->", color="black"))
    
    ax.set_title(f"PHYSICAL PASS ANALYSIS: {ev['Stn']} {ev['Sig']}", fontweight='bold')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax.grid(True, alpha=0.3)
    return fig

# --- Sidebar & Main ---
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
    
    col_t, col_g = st.columns([1, 1.5])
    with col_t:
        st.write("### 📜 Physical Pass Table")
        sel = st.dataframe(df_disp[['Stn', 'Sig', 'Time_ms', 'Aspect', 'Speed']], 
                           on_select="rerun", selection_mode="single-row", hide_index=True)
    with col_g:
        idx = sel.selection.rows[0] if (sel and sel.selection.rows) else 0
        st.pyplot(draw_detailed_plot(st.session_state.events[idx], st.session_state.rtis))
