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
#         STREAMLIT PAGE SETUP - V44.6 (PRECISE)
# =========================================================
st.set_page_config(page_title="Loco-Speed Safety Audit", layout="wide", page_icon="🚄")

# --- Constants & Colors ---
SAFFRON = "#33D4FC"
NAVY = "#1A237E"
BG_MAP = {
    "Green": "#0D860D",
    "Yellow": "#EEF153",
    "Double Yellow": "#EFA627",
    "Red": "#F2F2F2"
}

# --- CSS Styling ---
st.markdown(f"""
    <style>
    .top-header {{ background-color: {SAFFRON}; padding: 15px; border-radius: 10px; color: white; text-align: center; margin-bottom: 20px; }}
    </style>
    <div class="top-header">
        <h1 style='margin:0;'>🚄 Loco-Speed Safety Audit </h1>
    </div>
""", unsafe_allow_html=True)

# --- Session State ---
if 'events' not in st.session_state: st.session_state.events =[]
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
    if any(x in name for x in['DECR','DECPR_K','DECPR', 'DGCR']): return 'Green'
    if any(x in name for x in ['HHECR','HHECPR2_K', 'HHGCR']): return 'Double Yellow'
    if any(x in name for x in ['HECR', 'HGCR']): return 'Yellow'
    if any(x in name for x in ['RECR', 'RGCR']): return 'Red'
    return None

@st.cache_data(show_spinner=False)
def load_file(file_name, file_bytes):
    file_obj = io.BytesIO(file_bytes)
    if file_name.endswith(('.xlsx', '.xls')):
        return pd.read_excel(file_obj, engine='openpyxl')
    else:
        # Use pyarrow for ultra-fast CSV reading if possible
        try:
            return pd.read_csv(file_obj, engine='pyarrow')
        except:
            file_obj.seek(0)
            return pd.read_csv(file_obj, encoding='latin1', on_bad_lines='skip', low_memory=False)

# =========================================================
#                     CORE PROCESSING
# =========================================================
def process_data(rtis_up, dlog_up, sig_up):
    with st.spinner("⏳ Processing Data (SIGNAL ASPECT-Speed Mode)... Please Wait."):
        try:
            # 1. Load Signals
            sig_map = load_file(sig_up.name, sig_up.getvalue())
            up_signals = {clean_id(s) for s in sig_map.iloc[:, 6].dropna().astype(str) if clean_id(s)}

            # 2. Load RTIS
            rtis = load_file(rtis_up.name, rtis_up.getvalue())
            rtis.columns = rtis.columns.str.strip()
            rtis['Logging Time'] = pd.to_datetime(rtis['Logging Time'], format='mixed', errors='coerce')
            rtis = rtis.dropna(subset=['Logging Time']).sort_values('Logging Time')
            rtis['CumDist'] = pd.to_numeric(rtis.get('distFromSpeed', 0), errors='coerce').fillna(0).cumsum()
            rtis['BASE_STN'] = rtis['STATION NAME'].astype(str).apply(base_station)
            st.session_state.rtis = rtis

            # 3. Load Datalogger
            dlog = load_file(dlog_up.name, dlog_up.getvalue())
            dlog.columns = dlog.columns.str.strip()
            
            # Format DataLogger columns to avoid space issues in itertuples
            dlog = dlog.rename(columns={'STATION NAME': 'STATION_NAME', 'SIGNAL NAME': 'SIGNAL_NAME', 'SIGNAL STATUS': 'SIGNAL_STATUS', 'SIGNAL TIME': 'SIGNAL_TIME'})
            
            time_series = dlog['SIGNAL_TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True)
            dlog['dt'] = pd.to_datetime(time_series, format='mixed', dayfirst=True, errors='coerce')
            dlog = dlog.dropna(subset=['dt']).sort_values('dt')

            latch_aspect = collections.defaultdict(lambda: "Red")
            last_down_event = {}
            raw_events =[]

            # FAST LOOP using itertuples
            for row in dlog.itertuples(index=False):
                stn = base_station(row.STATION_NAME)
                sig_full = str(row.SIGNAL_NAME).strip().upper()
                sig = clean_id(sig_full)
                if sig not in up_signals: continue
                
                status = str(row.SIGNAL_STATUS).upper()
                key = (stn, sig)
                is_up = any(x in status for x in ['UP', 'ON', 'PICKUP', 'CLOSED', 'OCCURRED'])
                rtype = relay_type(sig_full)
                if not rtype: continue

                if rtype == 'Red' and is_up:
                    ev_time = row.dt
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
                            'CumDist': pt['CumDist'], 'RTIS_Stn': pt['BASE_STN']
                        })
                    latch_aspect[key] = "Red"
                elif rtype in['Green', 'Double Yellow', 'Yellow']:
                    if is_up: latch_aspect[key] = rtype
                    else: last_down_event[key] = (rtype, row.dt)

            # 4. Filter Duplicates (15s Window)
            final_events =[]
            if raw_events:
                df_ev = pd.DataFrame(raw_events).sort_values(['Stn', 'Sig', 'Time'])
                for _, grp in df_ev.groupby(['Stn', 'Sig']):
                    dt_check = grp['Time'].diff().shift(-1).dt.total_seconds()
                    valid = grp[dt_check.isna() | (dt_check > 15)]
                    final_events.extend(valid.to_dict('records'))

            st.session_state.events = sorted(final_events, key=lambda x: x['Time'])
            st.session_state.processed = True
            st.success(f"✅ Processed successfully! Found {len(st.session_state.events)} events.")
        except Exception as e:
            st.error(f"❌ Error during processing: {str(e)}")

# =========================================================
#                     EXPORT GENERATORS
# =========================================================
def generate_excel(data):
    output = io.BytesIO()
    export_df = pd.DataFrame([{
        'Station': ev['Stn'], 'Signal': ev['Sig'], 
        'RECR Up Time (ms)': ev['Time'].strftime('%d/%m/%Y %H:%M:%S.%f')[:-3],
        'Aspect': ev['Aspect'], 'Speed (km/h)': ev['Speed'], 'RTIS Stn': ev['RTIS_Stn']
    } for ev in data])
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='SIGNAL ASPECTs')
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
            ax.set_ylabel("Speed (km/h)", fontweight='bold')
            ax.grid(True, alpha=0.3)
            
            img_buffer = io.BytesIO()
            fig.savefig(img_buffer, format="png", bbox_inches='tight')
            plt.close(fig)
            zip_file.writestr(f"Graph_{i+1}_{ev['Sig']}.png", img_buffer.getvalue())
            
    return zip_buffer.getvalue()

# =========================================================
#                       UI LAYOUT
# =========================================================
with st.sidebar:
    st.header("📁 1. Load Files")
    rtis_f = st.file_uploader("RTIS File", type=['csv', 'xlsx'])
    dlog_f = st.file_uploader("Datalogger File", type=['csv', 'xlsx'])
    sig_f = st.file_uploader("Signal Mapping File", type=['csv', 'xlsx'])
    
    if st.button("🚀 PROCESS DATA", use_container_width=True, type="primary"):
        if rtis_f and dlog_f and sig_f:
            process_data(rtis_f, dlog_f, sig_f)
        else:
            st.warning("Please upload all 3 files first.")

if st.session_state.processed and st.session_state.events:
    # --- Top Control Bar ---
    col1, col2, col3, col4 = st.columns([1.5, 2, 1, 1.5])
    
    with col1:
        st.markdown("**Filters:**")
        filter_opt = st.radio("Aspect:", ["All", "Yellow", "Double Yellow"], horizontal=True, label_visibility="collapsed")
    
    # Apply Filter
    filtered_events = st.session_state.events
    if filter_opt != "All":
        filtered_events =[e for e in st.session_state.events if e['Aspect'] == filter_opt]
    
    with col3:
        st.download_button("📥 Excel Report", data=generate_excel(filtered_events), 
                           file_name="Speed_SIGNAL ASPECTs.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with col4:
        st.download_button("🖼 Download All Graphs (ZIP)", data=generate_zip_graphs(filtered_events, st.session_state.rtis), 
                           file_name="Annotated_Graphs.zip", mime="application/zip")

    st.divider()

    # --- Main Display (Table + Graph) ---
    c_left, c_right = st.columns([1.2, 1.5])

    with c_left:
        st.write("### 📜 SIGNAL ASPECT Table")
        # Format for display
        display_df = pd.DataFrame(filtered_events)
        if not display_df.empty:
            display_df['Time (ms)'] = display_df['Time'].dt.strftime('%H:%M:%S.%f').str[:-3]
            display_cols =['Stn', 'Sig', 'Time (ms)', 'Aspect', 'Speed', 'RTIS_Stn']
            
            selected_row = st.dataframe(
                display_df[display_cols], 
                on_select="rerun", selection_mode="single-row",
                hide_index=True, use_container_width=True, height=500
            )
        else:
            st.info("No events match the selected filter.")

    with c_right:
        st.write("### 📈 Precision Graph Analysis")
        if not display_df.empty:
            if selected_row and len(selected_row.selection.rows) > 0:
                idx = selected_row.selection.rows[0]
            else:
                idx = 0 # Default to first row
            
            ev = filtered_events[idx]
            rtis_df = st.session_state.rtis
            sub = rtis_df[(rtis_df['CumDist'] >= ev['CumDist'] - 1000) & (rtis_df['CumDist'] <= ev['CumDist'] + 1000)]
            
            fig, ax = plt.subplots(figsize=(10, 6))
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
            ax.set_ylabel("Speed (km/h)", fontweight='bold')
            ax.grid(True, alpha=0.3)
            
            st.pyplot(fig)
elif st.session_state.processed:
    st.info("No safety SIGNAL ASPECTs found in the given dataset.")
else:
    st.info("👈 Please upload the 3 files in the sidebar and click 'PROCESS DATA'.")
