import os
import re
import io
import collections
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# =========================================================
#         STREAMLIT PAGE SETUP - V45.3 (STABLE)
# =========================================================
st.set_page_config(page_title="Loco-Speed Safety Audit", layout="wide", page_icon="🚄")

# Colors & Config
SAFFRON = "#33D4FC"
BG_MAP = {"Green": "#0D860D", "Yellow": "#EEF153", "Double Yellow": "#EFA627", "Red": "#F2F2F2"}
ASPECT_PRIORITY = {"Double Yellow": 3, "Yellow": 2, "Green": 1, "Red": 0}

st.markdown(f"""
    <style>
    .top-header {{ background-color: {SAFFRON}; padding: 15px; border-radius: 10px; color: white; text-align: center; margin-bottom: 20px; }}
    </style>
    <div class="top-header"><h1 style='margin:0;'>🚄 Loco-Speed Audit (Precision v45.3)</h1></div>
""", unsafe_allow_html=True)

if 'events' not in st.session_state: st.session_state.events = []
if 'rtis' not in st.session_state: st.session_state.rtis = None

# =========================================================
#                     HELPER FUNCTIONS
# =========================================================
def load_file_robust(file):
    """Loads file and strips column spaces automatically."""
    try:
        if file.name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file, engine='openpyxl')
        else:
            df = pd.read_csv(file, encoding='latin1', on_bad_lines='skip')
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        st.error(f"Error loading {file.name}: {e}")
        return None

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

def get_interpolated_speed(target_time, rtis_df):
    """Calculates exact speed by looking at neighbors in RTIS."""
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
    # Linear Interpolation (v1 + slope * time_diff)
    return round(v1 + (v2 - v1) * (tx - t1) / (t2 - t1), 2)

# =========================================================
#                      CORE PROCESSING
# =========================================================
def process_all_data(rtis_file, dlog_file, sig_file):
    with st.spinner("⏳ Processing... Please wait."):
        # 1. Load Data
        df_rtis = load_file_robust(rtis_file)
        df_dlog = load_file_robust(dlog_file)
        df_sig = load_file_robust(sig_file)

        if df_rtis is None or df_dlog is None or df_sig is None:
            return

        try:
            # 2. Setup RTIS
            df_rtis['Logging Time'] = pd.to_datetime(df_rtis['Logging Time'], format='mixed', errors='coerce')
            df_rtis = df_rtis.dropna(subset=['Logging Time']).sort_values('Logging Time')
            df_rtis['BASE_STN'] = df_rtis['STATION NAME'].astype(str).apply(base_station)
            df_rtis['CumDist'] = pd.to_numeric(df_rtis.get('distFromSpeed', 0), errors='coerce').fillna(0).cumsum()
            st.session_state.rtis = df_rtis

            # 3. Setup Signal Map
            valid_signals = {clean_id(s) for s in df_sig.iloc[:, 6].dropna().astype(str) if clean_id(s)}

            # 4. Setup Datalogger
            # Normalize column names to avoid "Column not found" errors
            df_dlog = df_dlog.rename(columns={
                'STATION NAME': 'STN', 'SIGNAL NAME': 'SIG', 
                'SIGNAL STATUS': 'STAT', 'SIGNAL TIME': 'TIME'
            })
            
            # Handle millisecond colon format (12:00:00:123 -> 12:00:00.123)
            time_fixed = df_dlog['TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True)
            df_dlog['dt'] = pd.to_datetime(time_fixed, format='mixed', dayfirst=True, errors='coerce')
            df_dlog = df_dlog.dropna(subset=['dt'])

            # Sorting: Important for priority
            df_dlog['rtype'] = df_dlog['SIG'].apply(relay_type)
            df_dlog['status_val'] = df_dlog['STAT'].apply(lambda x: 0 if any(y in str(x).upper() for y in ['DOWN', 'OFF', 'DROP']) else 1)
            df_dlog['prio_val'] = df_dlog['rtype'].map(ASPECT_PRIORITY).fillna(0)
            df_dlog = df_dlog.sort_values(by=['dt', 'status_val', 'prio_val'], ascending=[True, True, False])

            latch_aspect = collections.defaultdict(lambda: "Red")
            simult_drops = collections.defaultdict(list) 
            raw_events = []

            # 5. Iteration Logic
            for row in df_dlog.itertuples():
                stn = base_station(row.STN)
                sig = clean_id(row.SIG)
                if sig not in valid_signals or not row.rtype: continue
                
                key = (stn, sig)
                is_up = any(x in str(row.STAT).upper() for x in ['UP', 'ON', 'PICKUP', 'CLOSED', 'OCCURRED'])

                if row.rtype == 'Red' and is_up:
                    ev_time = row.dt
                    stn_rtis = df_rtis[df_rtis['BASE_STN'] == stn]
                    
                    if not stn_rtis.empty:
                        # Only take if train is actually at station (within 45s window)
                        if (stn_rtis['Logging Time'] - ev_time).abs().min().total_seconds() <= 45:
                            
                            speed = get_interpolated_speed(ev_time, stn_rtis)
                            
                            if speed > 1: # Physically moving
                                aspect = latch_aspect[key]
                                if key in simult_drops:
                                    drops = [d for d in simult_drops[key] if 0 <= (ev_time - d['time']).total_seconds() <= 5]
                                    if drops:
                                        aspect = max(drops, key=lambda x: ASPECT_PRIORITY.get(x['asp'], 0))['asp']
                                
                                idx_c = (df_rtis['Logging Time'] - ev_time).abs().idxmin()
                                raw_events.append({
                                    'Stn': stn, 'Sig': sig, 'Time': ev_time,
                                    'Aspect': aspect, 'Speed': speed,
                                    'CumDist': df_rtis.loc[idx_c, 'CumDist']
                                })
                    latch_aspect[key] = "Red"
                    simult_drops[key] = []
                
                elif row.rtype in ['Green', 'Double Yellow', 'Yellow']:
                    if is_up: latch_aspect[key] = row.rtype
                    else: simult_drops[key].append({'asp': row.rtype, 'time': row.dt})

            # 6. Final Filter: Deduplication (Keep only the LAST event for a signal pass)
            if raw_events:
                df_res = pd.DataFrame(raw_events).sort_values(['Stn', 'Sig', 'Time'])
                # Group and take the last record (handles flickering/multiple triggers)
                st.session_state.events = df_res.groupby(['Stn', 'Sig'], as_index=False).last().to_dict('records')
                st.success(f"✅ Found {len(st.session_state.events)} valid crossing events.")
            else:
                st.warning("No physical passing events found. Check your Signal Map or Date range.")

        except Exception as e:
            st.error(f"Processing Error: {str(e)}")

# =========================================================
#                     UI LAYOUT
# =========================================================
with st.sidebar:
    st.header("📁 Step 1: Upload Files")
    rtis_f = st.file_uploader("RTIS File", type=['csv', 'xlsx'])
    dlog_f = st.file_uploader("Datalogger File", type=['csv', 'xlsx'])
    sig_f = st.file_uploader("Signal Map File", type=['csv', 'xlsx'])
    
    if st.button("🚀 START PROCESSING", use_container_width=True, type="primary"):
        if rtis_f and dlog_f and sig_f:
            process_all_data(rtis_f, dlog_f, sig_f)
        else:
            st.error("Please upload all 3 files.")

if st.session_state.rtis is not None and st.session_state.events:
    df_final = pd.DataFrame(st.session_state.events)
    df_final['DisplayTime'] = df_final['Time'].dt.strftime('%d-%m-%Y %H:%M:%S.%f').str[:-3]

    col1, col2 = st.columns([1, 1.5])
    
    with col1:
        st.write("### 📜 Passing Events")
        sel = st.dataframe(df_final[['Stn', 'Sig', 'DisplayTime', 'Aspect', 'Speed']], 
                           on_select="rerun", selection_mode="single-row", hide_index=True)
    
    with col2:
        st.write("### 📈 Precision Graph")
        idx = sel.selection.rows[0] if (sel and sel.selection.rows) else 0
        ev = st.session_state.events[idx]
        
        # Plotting
        rtis = st.session_state.rtis
        sub = rtis[(rtis['CumDist'] >= ev['CumDist'] - 1200) & (rtis['CumDist'] <= ev['CumDist'] + 1200)]
        
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
        ax.plot(sub['Logging Time'], sub['Speed'], marker='o', color='#1A237E', label="Train Path")
        ax.axvline(x=ev['Time'], color='red', linestyle='--', label="Crossing Point")
        
        # Details Box
        label = f"STN: {ev['Stn']} | SIG: {ev['Sig']}\nTIME: {ev['Time'].strftime('%H:%M:%S.%f')[:-3]}\nSPEED: {ev['Speed']} km/h\n{ev['Aspect']} ➔ RED"
        ax.annotate(label, xy=(ev['Time'], ev['Speed']), xytext=(30, 30), textcoords='offset points',
                    bbox=dict(boxstyle="round", fc="white", ec="red", alpha=0.9), fontweight='bold')
        
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        st.pyplot(fig)
