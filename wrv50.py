# Import at the top (add this if not present)
import io

# --- 1. OPTIMIZATION: Added @st.cache_data so it doesn't reload on every click ---
@st.cache_data(show_spinner="Reading File... Please wait")
def load_data_smart(file_name, file_bytes, is_dlog=False):
    try:
        # Create a mock file object from bytes to support hashing in cache
        file = io.BytesIO(file_bytes)
        file.name = file_name
        
        if file.name.endswith(('.xlsx', '.xls')):
            df_preview = pd.read_excel(file, nrows=0)
        else:
            df_preview = pd.read_csv(file, encoding='latin1', nrows=0)
            
        actual_cols = df_preview.columns.tolist()

        if is_dlog:
            stn_col = get_best_column(actual_cols, ['STATION', 'STN'])
            sig_col = get_best_column(actual_cols,['SIGNALNAME', 'SIGNAM', 'SIGNAME'])
            sts_col = get_best_column(actual_cols, ['STATUS', 'STATE'])
            tim_col = get_best_column(actual_cols, ['TIME', 'DATETIME'])
            
            needed = [c for c in[stn_col, sig_col, sts_col, tim_col] if c]
            
            file.seek(0) # Reset pointer
            if file.name.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file, engine='openpyxl', usecols=needed)
            else:
                # Using PyArrow engine makes CSV loading lightning fast
                try:
                    df = pd.read_csv(file, engine='pyarrow', usecols=needed)
                except:
                    file.seek(0)
                    df = pd.read_csv(file, encoding='latin1', on_bad_lines='skip', low_memory=False, usecols=needed)
            
            mapping = {stn_col: 'STATION_NAME', sig_col: 'SIGNAL_NAME', sts_col: 'SIGNAL_STATUS', tim_col: 'SIGNAL_TIME'}
            return df.rename(columns=mapping)
        
        file.seek(0)
        if file.name.endswith(('.xlsx', '.xls')):
            return pd.read_excel(file, engine='openpyxl')
        return pd.read_csv(file, encoding='latin1', on_bad_lines='skip', low_memory=False)
    except Exception as e:
        st.error(f"Error reading {file_name}: {e}")
        return None

# --- 2. OPTIMIZATION: Changed iterrows to itertuples & Fast DateTime Parse ---
def process_files(rtis_file, dlog_file, sig_file):
    with st.spinner("Synchronizing RTIS & Datalogger Tracks... (Optimized Mode)"):
        try:
            # Load using Cached bytes
            sig_map = load_data_smart(sig_file.name, sig_file.getvalue())
            up_signals = {clean_id(s) for s in sig_map.iloc[:, 6].dropna().astype(str) if clean_id(s)}

            rtis = load_data_smart(rtis_file.name, rtis_file.getvalue())
            rtis.columns = rtis.columns.str.strip()
            # Fast parsing RTIS time
            rtis['Logging Time'] = pd.to_datetime(rtis['Logging Time'], format='mixed', errors='coerce')
            rtis = rtis.dropna(subset=['Logging Time']).sort_values('Logging Time')
            rtis['CumDist'] = pd.to_numeric(rtis['distFromSpeed'], errors='coerce').fillna(0).cumsum()
            rtis['BASE_STN'] = rtis['STATION NAME'].apply(base_station)
            st.session_state.rtis = rtis

            # Load Datalogger
            dlog = load_data_smart(dlog_file.name, dlog_file.getvalue(), is_dlog=True)
            
            # Fast Date-Time Regex replacement & parsing
            time_series = dlog['SIGNAL_TIME'].astype(str)
            dlog['dt'] = pd.to_datetime(time_series.str.replace(r':(\d{3})$', r'.\1', regex=True), errors='coerce')
            dlog = dlog.dropna(subset=['dt']).sort_values('dt')

            latch_aspect, last_down_event, raw_events = collections.defaultdict(lambda: "Red"), {},[]

            # 🔥 GAME CHANGER: Using itertuples() instead of iterrows()
            for row in dlog.itertuples(index=False):
                stn = base_station(row.STATION_NAME)
                sig_full = str(row.SIGNAL_NAME).strip().upper()
                sig = clean_id(sig_full)
                if sig not in up_signals: continue
                
                status = str(row.SIGNAL_STATUS).upper()
                rtype = relay_type(sig_full)
                if not rtype: continue
                key = (stn, sig)
                is_up = any(x in status for x in['UP', 'ON', 'PICKUP', 'CLOSED', 'OCCURRED'])

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
                            'RTIS_Idx': idx, 'RTIS_Stn': pt['BASE_STN'],
                            'CumDist': pt['CumDist']
                        })
                    latch_aspect[key] = "Red"
                elif is_up:
                    latch_aspect[key] = rtype
                else:
                    last_down_event[key] = (rtype, row.dt)

            st.session_state.events = sorted(raw_events, key=lambda x: x['Time'])
            st.success(f"Audit Complete: {len(st.session_state.events)} Safety Events Found.")
        except Exception as e:
            st.error(f"Processing Error: {e}")
