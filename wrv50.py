import os
import re
import io
import collections
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# =========================================================
#         STREAMLIT PAGE SETUP - V46.1 (FINAL STABLE)
# =========================================================
st.set_page_config(page_title="Loco-Speed Safety Audit", layout="wide", page_icon="🚄")

if 'processed' not in st.session_state: st.session_state.processed = False
if 'events' not in st.session_state: st.session_state.events = []
if 'rtis' not in st.session_state: st.session_state.rtis = None

SAFFRON = "#33D4FC"
BG_MAP = {"Green": "#0D860D", "Yellow": "#EEF153", "Double Yellow": "#EFA627", "Red": "#F2F2F2"}
ASPECT_PRIORITY = {"Green": 3, "Double Yellow": 2, "Yellow": 1,  "Red": 0}

st.markdown(f"""
    <style>
    .top-header {{ background-color: {SAFFRON}; padding: 15px; border-radius: 10px; color: white; text-align: center; margin-bottom: 20px; }}
    </style>
    <div class="top-header"><h1 style='margin:0;'>🚄 Loco-Speed Precision Audit (v46.1)</h1></div>
""", unsafe_allow_html=True)

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
    if any(x in name for x in ['DECR','DECPR_K','DECPR','GP1R3','DGCR']): return 'Green'
    if any(x in name for x in ['HHECR','HHECPR2_K','HH_H_ECR','GP1R4','HHECP1R','HHGCR']): return 'Double Yellow'
    if any(x in name for x in ['HECR','GP1R2','HGCR']): return 'Yellow'
    if any(x in name for x in ['RECR', 'RGCR']): return 'Red'
    return None

# =========================================================
#                      CORE PROCESSING
# =========================================================
def process_data(rtis_up, dlog_up, sig_up):
    with st.spinner("⏳ Validating Speed & Deduplicating Events..."):
        try:
            sig_map_raw = load_file(sig_up.name, sig_up.getvalue())
            sig_cols = [1, 4, 6, 9]
            up_signals = set()
            for col in sig_cols:
                if col < len(sig_map_raw.columns):
                    ids = {clean_id(s) for s in sig_map_raw.iloc[:, col].dropna().astype(str) if clean_id(s)}
                    up_signals.update(ids)

            rtis = load_file(rtis_up.name, rtis_up.getvalue())
            rtis.columns = rtis.columns.str.strip()
            rtis['Base_Time'] = pd.to_datetime(rtis['Logging Time'], format='mixed', dayfirst=True, errors='coerce')
            rtis = rtis.dropna(subset=['Base_Time']).sort_values('Base_Time')
            
            rtis['sec_offset'] = rtis.groupby('Base_Time').cumcount() % 60
            rtis['Precise_Time'] = rtis['Base_Time'] + pd.to_timedelta(rtis['sec_offset'], unit='s')
            rtis['CumDist'] = pd.to_numeric(rtis.get('distFromSpeed', 0), errors='coerce').fillna(0).cumsum()
            rtis['BASE_STN'] = rtis['STATION NAME'].astype(str).apply(base_station)
            st.session_state.rtis = rtis

            dlog = load_file(dlog_up.name, dlog_up.getvalue())
            dlog.columns = dlog.columns.str.strip()
            dlog = dlog.rename(columns={'STATION NAME': 'STATION_NAME', 'SIGNAL NAME': 'SIGNAL_NAME', 'SIGNAL STATUS': 'SIGNAL_STATUS', 'SIGNAL TIME': 'SIGNAL_TIME'})
            
            time_fixed = dlog['SIGNAL_TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True)
            dlog['dt'] = pd.to_datetime(time_fixed, format='mixed', dayfirst=True, errors='coerce')
            
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
                    stn_rtis = rtis[rtis['BASE_STN'] == stn]
                    if stn_rtis.empty: continue
                    
                    diffs = (stn_rtis['Precise_Time'] - ev_time).abs()
                    nearest_idx = diffs.idxmin()
                    
                    if diffs[nearest_idx].total_seconds() <= 15:
                        final_speed = stn_rtis.loc[nearest_idx, 'Speed']
                        if final_speed > 1:
                            final_asp = latch_aspect[key]
                            if key in simult_drops:
                                v_drops = [d for d in simult_drops[key] if 0 <= (ev_time - d['time']).total_seconds() <= 5]
                                if v_drops:
                                    final_asp = max(v_drops, key=lambda x: ASPECT_PRIORITY.get(x['asp'], 0))['asp']
                            
                            raw_events.append({
                                'Stn': stn, 'Sig': sig, 'Time': ev_time,
                                'Aspect': final_asp, 'Speed': final_speed,
                                'CumDist': stn_rtis.loc[nearest_idx, 'CumDist']
                            })
                    latch_aspect[key] = "Red"
                    simult_drops[key] = [] 
                
                elif row.rtype in ['Green', 'Double Yellow', 'Yellow']:
                    if is_up: latch_aspect[key] = row.rtype
                    else: simult_drops[key].append({'asp': row.rtype, 'time': row.dt})

            if raw_events:
                df_res = pd.DataFrame(raw_events).sort_values(['Stn', 'Sig', 'Time'])
                st.session_state.events = df_res.groupby(['Stn', 'Sig'], as_index=False).last().to_dict('records')
            
            st.session_state.processed = True
            st.success("✅ Analysis Complete.")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

# =========================================================
#                     UI & DOWNLOADS
# =========================================================
def generate_excel(data):
    output = io.BytesIO()
    export_df = pd.DataFrame([{
        'Station': ev['Stn'], 'Signal': ev['Sig'], 
        'Time': ev['Time'].strftime('%d/%m/%Y %H:%M:%S.%f')[:-3],
        'Mode': f"{ev['Aspect']} -> Red", 'Speed (km/h)': ev['Speed']
    } for ev in data])
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='Speed_Audit')
    return output.getvalue()

with st.sidebar:
    st.header("📁 Load Files")
    rtis_f = st.file_uploader("RTIS", type=['csv', 'xlsx'])
    dlog_f = st.file_uploader("Datalogger", type=['csv', 'xlsx'])
    sig_f = st.file_uploader("Signal Map", type=['csv', 'xlsx'])
    if st.button("🚀 PROCESS", use_container_width=True, type="primary"):
        if rtis_f and dlog_f and sig_f: process_data(rtis_f, dlog_f, sig_f)

if st.session_state.get('processed'):
    # --- Filter UI ---
    st.write("### 🔍 Filters")
    f_col1, f_col2, f_col3 = st.columns(3)
    with f_col1: filter_opt = st.radio("Select Aspect Filter:", ["All Signals", "Double Yellow Only", "Yellow Only"], horizontal=True)
    
    # Apply Filter
    filtered_events = st.session_state.events
    if "Double Yellow" in filter_opt:
        filtered_events = [e for e in st.session_state.events if e['Aspect'] == "Double Yellow"]
    elif "Yellow" in filter_opt:
        filtered_events = [e for e in st.session_state.events if e['Aspect'] == "Yellow"]

    df_disp = pd.DataFrame(filtered_events)
    if not df_disp.empty:
        df_disp['Time_ms'] = df_disp['Time'].dt.strftime('%H:%M:%S.%f').str[:-3]
        
        c1, c2 = st.columns([1, 1.5])
        with c1:
            st.write("### 📜 Event Results")
            st.download_button("📥 Download Excel Report", data=generate_excel(filtered_events), file_name="Speed_Audit.xlsx")
            sel = st.dataframe(df_disp[['Stn', 'Sig', 'Time_ms', 'Aspect', 'Speed']], on_select="rerun", selection_mode="single-row", hide_index=True)
        
        with c2:
            st.write("### 📈 Precision Graph")
            idx = sel.selection.rows[0] if (sel and sel.selection.rows) else 0
            ev = filtered_events[idx]
            
            rtis_df = st.session_state.rtis
            sub = rtis_df[(rtis_df['CumDist'] >= ev['CumDist'] - 1000) & (rtis_df['CumDist'] <= ev['CumDist'] + 1000)]
            
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFFFFF"))
            ax.plot(sub['Precise_Time'], sub['Speed'], marker='o', color='#1A237E', label="Train Path")
            ax.axvline(x=ev['Time'], color='red', ls='--')
            
            # Detailed Annotation for Graph
            mode_text = f"{ev['Aspect']} ➔ Red"
            ms_time = ev['Time'].strftime('%H:%M:%S.%f')[:-3]
            label = (f"STATION: {ev['Stn']}\nSIGNAL: {ev['Sig']}\n"
                     f"TIME: {ms_time}\nSPEED: {ev['Speed']} km/h\n"
                     f"MODE: {mode_text}")
            
            ax.annotate(label, xy=(ev['Time'], ev['Speed']), xytext=(35, 35),
                        textcoords='offset points', fontweight='bold',
                        bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="red", alpha=0.9),
                        arrowprops=dict(arrowstyle="->", color="black"))
            
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            ax.set_title(f"PRECISION ANALYSIS: {ev['Stn']} | {ev['Sig']}", fontweight='bold')
            ax.grid(True, alpha=0.3)
            
            st.pyplot(fig)
            
            # Individual Graph Download
            img_buf = io.BytesIO()
            fig.savefig(img_buf, format="png", bbox_inches='tight')
            st.download_button(f"🖼 Download Graph ({ev['Sig']})", data=img_buf.getvalue(), file_name=f"Graph_{ev['Sig']}_{ms_time.replace(':','-')}.png", mime="image/png")
    else:
        st.info("Selected filter has no events.")
