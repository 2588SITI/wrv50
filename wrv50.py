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
#         STREAMLIT PAGE SETUP - V44.8 (PREMIUM UI)
# =========================================================
st.set_page_config(page_title="Loco-Speed Safety Audit", layout="wide", page_icon="🚄")

# --- Constants & Colors ---
SAFFRON = "#FF9933"
NAVY = "#1A237E"
BG_MAP = {
    "Green": "#E6FFFA",
    "Yellow": "#FFFFE0",
    "Double Yellow": "#FFF5E6",
    "Red": "#F2F2F2"
}

# --- Premium Bullet Train Header ---
st.markdown("""
    <style>
    .train-container {
        width: 100%;
        height: 300px;
        background: url('https://images.unsplash.com/photo-1532105956690-da2bc44b825d?q=80&w=1600&auto=format&fit=crop') no-repeat center center;
        background-size: cover;
        border-radius: 15px;
        position: relative;
        margin-bottom: 25px;
        box-shadow: 0 8px 16px rgba(0,0,0,0.4);
        display: flex;
        align-items: center;
        justify-content: center;
        overflow: hidden;
    }
    .overlay {
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(0, 0, 0, 0.4); /* Subtle darkening to make text pop */
    }
    .header-content {
        position: relative;
        text-align: center;
        z-index: 10;
    }
    .main-title {
        color: #FFFFFF;
        font-size: 50px;
        font-weight: 900;
        text-transform: uppercase;
        letter-spacing: 3px;
        margin-bottom: 0px;
        text-shadow: 3px 3px 6px rgba(0,0,0,0.9);
    }
    .sub-designation {
        color: #FF9933;
        font-size: 28px;
        font-weight: bold;
        letter-spacing: 5px;
        margin-top: -5px;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.9);
    }
    </style>
    
    <div class="train-container">
        <div class="overlay"></div>
        <div class="header-content">
            <div class="main-title">Loco-Speed Safety Audit Tool</div>
            <div class="sub-designation">ADEE TRO BL</div>
        </div>
    </div>
""", unsafe_allow_html=True)

# --- Session State ---
if 'events' not in st.session_state: st.session_state.events = []
if 'rtis' not in st.session_state: st.session_state.rtis = None
if 'processed' not in st.session_state: st.session_state.processed = False
if 'graph_idx' not in st.session_state: st.session_state.graph_idx = 0

# --- Helper Functions ---
def clean_id(s):
    m = re.search(r'([AS])?-?(\d+)', str(s).upper())
    return f"{m.group(1) or 'S'}{m.group(2)}" if m else None

def base_station(s):
    return str(s).split('_')[0].split('-')[0].split(' ')[0].upper()

def relay_type(name):
    name = str(name).upper()
    if any(x in name for x in ['DECR','DECPR', 'DGCR']): return 'Green'
    if any(x in name for x in ['HHECR','HHECPR', 'HHGCR']): return 'Double Yellow'
    if any(x in name for x in ['HECR', 'HGCR']): return 'Yellow'
    if any(x in name for x in ['RECR', 'RGCR']): return 'Red'
    return None

@st.cache_data(show_spinner=False)
def load_file(file_name, file_bytes):
    file_obj = io.BytesIO(file_bytes)
    if file_name.endswith(('.xlsx', '.xls')):
        return pd.read_excel(file_obj, engine='openpyxl')
    else:
        return pd.read_csv(file_obj, encoding='latin1', on_bad_lines='skip', low_memory=False)

# --- Core Processing ---
def process_data(rtis_up, dlog_up, sig_up):
    with st.spinner("⚡ Synchronizing Datalogger with RTIS..."):
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
            dlog = dlog.rename(columns={'STATION NAME': 'STN', 'SIGNAL NAME': 'SIG', 'SIGNAL STATUS': 'STS', 'SIGNAL TIME': 'TIME'})
            
            dlog['dt'] = pd.to_datetime(dlog['TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True), format='mixed', errors='coerce')
            dlog = dlog.dropna(subset=['dt']).sort_values('dt')

            latch_aspect = collections.defaultdict(lambda: "Red")
            last_down_event = {}
            raw_events = []

            for row in dlog.itertuples():
                stn = base_station(row.STN)
                sig = clean_id(row.SIG)
                if sig not in up_signals: continue
                
                status = str(row.STS).upper()
                rtype = relay_type(row.SIG)
                if not rtype: continue
                is_up = any(x in status for x in ['UP', 'ON', 'PICKUP', 'CLOSED'])

                if rtype == 'Red' and is_up:
                    ev_time = row.dt
                    final_asp = latch_aspect[(stn, sig)]
                    if final_asp == "Red" and (stn, sig) in last_down_event:
                        d_asp, d_time = last_down_event[(stn, sig)]
                        if 0 <= (ev_time - d_time).total_seconds() <= 5: final_asp = d_asp
                    
                    diffs = (rtis['Logging Time'] - ev_time).abs()
                    idx = diffs.idxmin()
                    pt = rtis.loc[idx]
                    if pt['Speed'] > 1 and diffs[idx].total_seconds() <= 15 and pt['BASE_STN'] == stn:
                        raw_events.append({'Stn': stn, 'Sig': sig, 'Time': ev_time, 'Aspect': final_asp, 'Speed': pt['Speed'], 'CumDist': pt['CumDist'], 'RTIS_Stn': pt['BASE_STN']})
                    latch_aspect[(stn, sig)] = "Red"
                elif is_up: latch_aspect[(stn, sig)] = rtype
                else: last_down_event[(stn, sig)] = (rtype, row.dt)

            st.session_state.events = sorted(raw_events, key=lambda x: x['Time'])
            st.session_state.processed = True
            st.success("✅ Analysis Complete.")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

# --- UI Layout ---
with st.sidebar:
    st.header("📂 1. Import Data")
    rt_f = st.file_uploader("RTIS", type=['csv', 'xlsx'])
    dl_f = st.file_uploader("Datalogger", type=['csv', 'xlsx'])
    sg_f = st.file_uploader("Mapping", type=['csv', 'xlsx'])
    if st.button("🚀 RUN AUDIT", use_container_width=True, type="primary"):
        if rt_f and dl_f and sg_f: process_data(rt_f, dl_f, sg_f)

if st.session_state.processed and st.session_state.events:
    col_a, col_b = st.columns([2, 1])
    with col_b:
        st.write("### 📥 Export Options")
        exp_df = pd.DataFrame(st.session_state.events)
        st.download_button("📊 Download Report (CSV)", data=exp_df.to_csv(index=False).encode('utf-8'), file_name="Safety_Audit.csv", use_container_width=True)

    st.write("### 📜 Audit Log")
    st.dataframe(exp_df[['Stn', 'Sig', 'Time', 'Aspect', 'Speed', 'RTIS_Stn']], use_container_width=True, hide_index=True)

    st.divider()
    st.write("### 📈 Visual Safety Analysis")
    
    # Navigation
    nav1, nav2, nav3 = st.columns([1, 2, 1])
    with nav1:
        if st.button("◀ Previous", use_container_width=True): st.session_state.graph_idx -= 1
    with nav3:
        if st.button("Next ▶", use_container_width=True): st.session_state.graph_idx += 1
    
    st.session_state.graph_idx %= len(st.session_state.events)
    ev = st.session_state.events[st.session_state.graph_idx]
    
    with nav2:
        st.markdown(f"<h4 style='text-align:center;'>Graph {st.session_state.graph_idx + 1} of {len(st.session_state.events)}</h4>", unsafe_allow_html=True)

    # Plot
    fig, ax = plt.subplots(figsize=(12, 6))
    rt = st.session_state.rtis
    sub = rt[(rt['CumDist'] >= ev['CumDist'] - 1000) & (rt['CumDist'] <= ev['CumDist'] + 1000)]
    ax.set_facecolor(BG_MAP.get(ev['Aspect'], "#FFF"))
    ax.plot(sub['Logging Time'], sub['Speed'], color=NAVY, lw=2.5)
    ax.axvline(x=ev['Time'], color='red', ls='--')
    ax.annotate(f"SPEED: {ev['Speed']} km/h\nSIGNAL: {ev['Sig']}", xy=(ev['Time'], ev['Speed']), xytext=(20, 20), textcoords='offset points', bbox=dict(boxstyle="round", fc="white", ec="red"), arrowprops=dict(arrowstyle="->", color="red"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    st.pyplot(fig)

elif not st.session_state.processed:
    st.info("👈 Please upload files and click 'RUN AUDIT' to begin.")
