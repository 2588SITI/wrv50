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
#         STREAMLIT PAGE SETUP - V45.5 (STABLE)
# =========================================================
st.set_page_config(page_title="Loco-Speed Safety Audit", layout="wide", page_icon="🚄")

SAFFRON = "#33D4FC"
BG_MAP = {"Green": "#0D860D", "Yellow": "#EEF153", "Double Yellow": "#EFA627", "Red": "#F2F2F2"}
ASPECT_PRIORITY = {"Double Yellow": 3, "Yellow": 2, "Green": 1, "Red": 0}

st.markdown(f"""
    <style>
    .top-header {{ background-color: {SAFFRON}; padding: 15px; border-radius: 10px; color: white; text-align: center; margin-bottom: 20px; }}
    </style>
    <div class="top-header"><h1 style='margin:0;'>🚄 Loco-Speed Safety Audit (Precision Mode)</h1></div>
""", unsafe_allow_html=True)

if 'events' not in st.session_state: st.session_state.events = []
if 'rtis' not in st.session_state: st.session_state.rtis = None
if 'processed' not in st.session_state: st.session_state.processed = False

# =========================================================
#                     HELPER FUNCTIONS
# =========================================================
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
#             FIXED SPEED VALIDATION LOGIC
# =========================================================
def get_nearest_rtis_speed(target_time, rtis_df):
    """
    Finds the speed from the nearest recorded RTIS second.
    Handles multiple events per minute by finding the minimum absolute time difference.
    """
    if rtis_df.empty:
        return 0.0
    
    # Calculate absolute time difference between Datalogger time and all RTIS entries
    time_diffs = (rtis_df['Logging Time'] - target_time).abs()
    nearest_idx = time_diffs.idxmin()
    
    # Validation: Nearest RTIS point must be within 5 seconds to be considered valid
    if time_diffs[nearest_idx].total_seconds() <= 5:
        return rtis_df.loc[nearest_idx, 'Speed']
    return 0.0

# =========================================================
#                      CORE PROCESSING
# =========================================================
def process_data(rtis_up, dlog_up, sig_up):
    with st.spinner("⏳ Analyzing speed at nearest second and filtering duplicates..."):
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
            
            # Sort: Priority given to DOWN events so simultaneous state is captured correctly
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
                    
                    # USE FIXED SPEED LOGIC
                    precise_speed = get_nearest_rtis_speed(ev_time, stn_rtis)
                    
                    if precise_speed > 1:
                        final_asp = latch_aspect[key]
                        if key in simult_drops:
                            valid_drops = [d for d in simult_drops[key] if 0 <= (ev_time - d['time']).total_seconds() <= 5]
                            if valid_drops:
                                final_asp = max(valid_drops, key=lambda x: ASPECT_PRIORITY.get(x['asp'], 0))['asp']
                        
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

            # ---------------------------------------------------------
            # DEDUPLICATION: Group by Signal and Station, keep only the LAST event
            # ---------------------------------------------------------
            final_events = []
            if raw_events:
                df_res = pd.DataFrame(raw_events).sort_values(['Stn', 'Sig', 'Time'])
                # Grouping by Stn and Sig, taking the last entry ensures only the final pass is recorded
                deduped_df = df_res.groupby(['Stn', 'Sig'], as_index=False).last()
                final_events = deduped_df.to_dict('records')

            st.session_state.events = sorted(final_events, key=lambda x: x['Time'])
            st.session_state.processed = True
            st.success(f"✅ Processed successfully. Found {len(st.session_state.events)} unique passing events.")
        except Exception as e:
            st.error(f"❌ Error during processing: {str(e)}")

# =========================================================
#           EXPORT & UI (Remain unchanged)
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

def generate_excel(data):
    output = io.BytesIO()
    export_df = pd.DataFrame([{
        'Station': ev['Stn'], 'Signal': ev['Sig'], 
        'RECR Up Time (ms)': ev['Time'].strftime('%d/%m/%Y %H:%M:%S.%f')[:-3],
        'Aspect': ev['Aspect'], 'Speed (km/h)': ev['Speed'], 'RTIS Stn': ev['RTIS_Stn']
    } for ev in data])
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='Safety_Audit')
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
            time_ms = ev['Time'].strftime('%H:%M:%S.%f')[:-3]
            box_text = f"STN: {ev['Stn']}\nSIG: {ev['Sig']}\nTIME: {time_ms}\nSPEED: {ev['Speed']} km/h"
            ax.annotate(box_text, xy=(ev['Time'], ev['Speed']), xytext=(20, 20), textcoords='offset points',
                        bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="red", lw=2, alpha=0.9),
                        arrowprops=dict(arrowstyle="-|>", connectionstyle="arc3,rad=0.3", color="red"),
                        fontweight='bold', fontsize=10)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            ax.set_title(f"DETAILED ANALYSIS: {ev['Stn']} | {ev['Sig']} | {ev['Aspect']} -> RED", fontweight='bold')
            ax.grid(True, alpha=0.3)
            img_buffer = io.BytesIO()
            fig.savefig(img_buffer, format="png", bbox_inches='tight')
            plt.close(fig)
            zip_file.writestr(f"Graph_{i+1}_{ev['Sig']}.png", img_buffer.getvalue())
    return zip_buffer.getvalue()

with st.sidebar:
    st.header("📁 Load Files")
    rtis_f = st.file_uploader("RTIS", type=['csv', 'xlsx'])
    dlog_f = st.file_uploader("Datalogger", type=['csv', 'xlsx'])
    sig_f = st.file_uploader("Signal Map", type=['csv', 'xlsx'])
    if st.button("🚀 PROCESS", use_container_width=True, type="primary"):
        if rtis_f and dlog_f and sig_f: process_data(rtis_f, dlog_f, sig_f)

if st.session_state.processed and st.session_state.events:
    col1, col2, col3, col4 = st.columns([1.5, 2, 1, 1.5])
    with col1: filter_opt = st.radio("Aspect:", ["All", "Yellow", "Double Yellow"], horizontal=True)
    filtered = [e for e in st.session_state.events if filter_opt == "All" or e['Aspect'] == filter_opt]
    
    with col3: st.download_button("📥 Excel", data=generate_excel(filtered), file_name="Speed_Audit.xlsx")
    with col4: st.download_button("🖼 Graphs", data=generate_zip_graphs(filtered, st.session_state.rtis), file_name="Graphs.zip")

    st.divider()
    c_left, c_right = st.columns([1.2, 1.5])
    with c_left:
        st.write("### 📜 Event Table")
        df_disp = pd.DataFrame(filtered)
        if not df_disp.empty:
            df_disp['Time (ms)'] = df_disp['Time'].dt.strftime('%H:%M:%S.%f').str[:-3]
            sel = st.dataframe(df_disp[['Stn', 'Sig', 'Time (ms)', 'Aspect', 'Speed']], on_select="rerun", selection_mode="single-row", hide_index=True)
    with c_right:
        if not df_disp.empty:
            idx = sel.selection.rows[0] if (sel and sel.selection.rows) else 0
            ev = filtered[idx]
            sub = st.session_state.rtis[(st.session_state.rtis['CumDist'] >= ev['CumDist'] - 1000) & (st.session_state.rtis['CumDist'] <= ev['CumDist'] + 1000)]
            fig, ax = plt.subplots(); ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF")); ax.plot(sub['Logging Time'], sub['Speed']); ax.axvline(x=ev['Time'], color='red'); st.pyplot(fig)
