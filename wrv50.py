import os
import re
import collections
import threading
import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.dates as mdates

plt.rcParams['figure.dpi'] = 100

# =========================================================
#             RAILWAY DASHBOARD V44.6 (PRECISE)
# =========================================================

class RailwayDashboardV43:

    def __init__(self, root):
        self.root = root
        self.root.title("WR YY & Y Speed Analyzer - V44.6 (High Precision)")
        self.root.geometry("1550x900")

        self.saffron = "#FF9933"
        self.white = "#FFFFFF"
        self.bg_light = "#FFF5E6"
        
        self.bg_map = {
            "Green": "#E6FFFA",
            "Yellow": "#FFFFE0",
            "Double Yellow": "#FFF5E6",
            "Red": "#F2F2F2"
        }

        self.root.configure(bg=self.bg_light)
        self.rtis = None
        self.events = []
        self.filtered_events = []
        self.setup_ui()

    def setup_ui(self):
        # --- Top Bar ---
        top_bar = tk.Frame(self.root, bg=self.saffron, height=70)
        top_bar.pack(side=tk.TOP, fill=tk.X)

        self.load_btn = tk.Button(top_bar, text="📁 PROCESS DATA", command=self.start_thread,
                                  bg=self.white, fg=self.saffron, font=('Segoe UI', 10, 'bold'), padx=15)
        self.load_btn.pack(side=tk.LEFT, padx=20, pady=10)

        self.status_var = tk.StringVar(value="READY: High Precision Annotation Active")
        tk.Label(top_bar, textvariable=self.status_var, bg=self.saffron, fg="white", font=('Segoe UI', 11, 'bold')).pack(side=tk.LEFT)

        # --- Control Bar ---
        control_bar = tk.Frame(self.root, bg=self.white, pady=5)
        control_bar.pack(side=tk.TOP, fill=tk.X)

        tk.Label(control_bar, text="Filters:", font=('Segoe UI', 10, 'bold'), bg=self.white).pack(side=tk.LEFT, padx=10)
        tk.Button(control_bar, text="ALL", command=lambda: self.apply_filter("All")).pack(side=tk.LEFT, padx=5)
        tk.Button(control_bar, text="Y->R", bg="#FFFFE0", command=lambda: self.apply_filter("Yellow")).pack(side=tk.LEFT, padx=5)
        tk.Button(control_bar, text="YY->R", bg="#FFF5E6", command=lambda: self.apply_filter("Double Yellow")).pack(side=tk.LEFT, padx=5)

        tk.Label(control_bar, text=" | Downloads:", font=('Segoe UI', 10, 'bold'), bg=self.white).pack(side=tk.LEFT, padx=10)
        tk.Button(control_bar, text="📥 DOWNLOAD REPORT", bg="#D4EDDA", command=self.download_report).pack(side=tk.LEFT, padx=5)
        tk.Button(control_bar, text="🖼 DOWNLOAD ALL GRAPHS", bg="#CCE5FF", command=self.download_graphs).pack(side=tk.LEFT, padx=5)

        paned = tk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.sidebar = tk.Frame(paned, bg=self.white)
        paned.add(self.sidebar, width=800)

        cols = ('Sr', 'Station', 'Signal', 'RECR Up Time (ms)', 'Aspect', 'Speed', 'RTIS Stn')
        self.tree = ttk.Treeview(self.sidebar, columns=cols, show='headings', height=30)
        
        for col in cols:
            self.tree.heading(col, text=col)
            width = 180 if "Time" in col else 100
            self.tree.column(col, width=width, anchor=tk.CENTER)

        self.tree.pack(fill=tk.BOTH, expand=True)
        self.tree.bind('<<TreeviewSelect>>', self.on_event_select)

        self.graph_container = tk.Frame(paned, bg=self.white)
        paned.add(self.graph_container)

        self.fig, self.ax = plt.subplots(figsize=(10, 6))
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.graph_container)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    def download_report(self):
        if not self.filtered_events:
            messagebox.showwarning("Warning", "Pehle data process karein!")
            return
        
        path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel Files", "*.xlsx")])
        if path:
            try:
                clean_data = []
                for ev in self.filtered_events:
                    clean_data.append({
                        'Station': ev['Stn'],
                        'Signal': ev['Sig'],
                        'RECR Time': ev['Time'].strftime('%d/%m/%Y %H:%M:%S.%f')[:-3],
                        'Aspect Before Red': ev['Aspect'],
                        'Speed (km/h)': ev['Speed'],
                        'RTIS Station': ev['RTIS_Stn']
                    })
                df_export = pd.DataFrame(clean_data)
                df_export.to_excel(path, index=False)
                messagebox.showinfo("Success", "Excel Report Saved!")
            except Exception as e:
                messagebox.showerror("Error", f"Save Error: {str(e)}")

    def download_graphs(self):
        if not self.filtered_events: return
        folder = filedialog.askdirectory()
        if not folder: return
        
        self.status_var.set("Generating Annotated Graphs... Please Wait")
        def run_export():
            try:
                for i, ev in enumerate(self.filtered_events):
                    f = plt.Figure(figsize=(10, 6))
                    a = f.add_subplot(111)
                    rt_row = ev['RTIS']
                    sub = self.rtis[(self.rtis['CumDist'] >= rt_row['CumDist'] - 1000) & (self.rtis['CumDist'] <= rt_row['CumDist'] + 1000)]
                    
                    a.set_facecolor(self.bg_map.get(ev['Aspect'], "#FFFFFF"))
                    a.plot(sub['Logging Time'], sub['Speed'], color='#1A237E', lw=2)
                    a.axvline(x=ev['Time'], color='red', linestyle='--', lw=2)
                    
                    time_ms = ev['Time'].strftime('%H:%M:%S.%f')[:-3]
                    box_text = f"STN: {ev['Stn']}\nSIG: {ev['Sig']}\nTIME: {time_ms}\nSPEED: {ev['Speed']} km/h"
                    
                    a.annotate(box_text, xy=(ev['Time'], ev['Speed']), xytext=(15, 15), textcoords='offset points',
                               bbox=dict(boxstyle="round", fc="white", ec="red", alpha=0.9),
                               arrowprops=dict(arrowstyle="->", color="red"), fontweight='bold')
                    
                    a.set_title(f"{ev['Stn']} | {ev['Sig']} | {ev['Aspect']} -> RED")
                    f.savefig(os.path.join(folder, f"Graph_{i+1}_{ev['Sig']}.png"))
                self.root.after(0, lambda: messagebox.showinfo("Success", "Annotated Graphs Saved!"))
                self.root.after(0, lambda: self.status_var.set("Ready"))
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Export Error", str(e)))
        
        threading.Thread(target=run_export, daemon=True).start()

    def apply_filter(self, aspect):
        if not self.events: return
        self.filtered_events = self.events if aspect == "All" else [e for e in self.events if e['Aspect'] == aspect]
        self.update_tree_display()

    def load_file(self, path):
        ext = os.path.splitext(path)[1].lower()
        if ext in ['.xlsx', '.xls']: return pd.read_excel(path, engine='openpyxl')
        return pd.read_csv(path, encoding='latin1', on_bad_lines='skip', low_memory=False)

    def start_thread(self):
        rtis_p = filedialog.askopenfilename(title="Select RTIS File")
        dlog_p = filedialog.askopenfilename(title="Select Datalogger File")
        sig_p = filedialog.askopenfilename(title="Select Signal Mapping File")
        if all([rtis_p, dlog_p, sig_p]):
            self.load_btn.config(state=tk.DISABLED)
            threading.Thread(target=self.process_files, args=(rtis_p, dlog_p, sig_p), daemon=True).start()

    def process_files(self, rtis_p, dlog_p, sig_p):
        try:
            def clean_id(s):
                m = re.search(r'([AS])?-?(\d+)', str(s).upper())
                return f"{m.group(1) or 'S'}{m.group(2)}" if m else None

            def base_station(s):
                return str(s).split('_')[0].split('-')[0].split(' ')[0].upper()

            def relay_type(name):
                name = name.upper()
                if any(x in name for x in ['DECR','DECPR_K','DECPR', 'DGCR']): return 'Green'
                if any(x in name for x in ['HHECR','HHECPR2_K', 'HHGCR']): return 'Double Yellow'
                if any(x in name for x in ['HECR', 'HGCR']): return 'Yellow'
                if any(x in name for x in ['RECR', 'RGCR']): return 'Red'
                return None

            sig_map = self.load_file(sig_p)
            up_signals = {clean_id(s) for s in sig_map.iloc[:, 6].dropna().astype(str) if clean_id(s)}

            rtis = self.load_file(rtis_p)
            rtis.columns = rtis.columns.str.strip()
            rtis['Logging Time'] = pd.to_datetime(rtis['Logging Time'], errors='coerce')
            rtis = rtis.dropna(subset=['Logging Time']).sort_values('Logging Time')
            rtis['CumDist'] = pd.to_numeric(rtis['distFromSpeed'], errors='coerce').fillna(0).cumsum()
            rtis['BASE_STN'] = rtis['STATION NAME'].apply(base_station)
            self.rtis = rtis

            dlog = self.load_file(dlog_p)
            dlog.columns = dlog.columns.str.strip()
            dlog['dt'] = pd.to_datetime(dlog['SIGNAL TIME'].astype(str).str.replace(r':(\d{3})$', r'.\1', regex=True),
                                       format='%d/%m/%Y %H:%M:%S.%f', errors='coerce')
            dlog = dlog.dropna(subset=['dt']).sort_values('dt')

            latch_aspect = collections.defaultdict(lambda: "Red")
            last_down_event = {}
            raw_events = []

            for _, row in dlog.iterrows():
                stn = base_station(row['STATION NAME'])
                sig_full = str(row['SIGNAL NAME']).strip().upper()
                sig = clean_id(sig_full)
                if sig not in up_signals: continue
                status = str(row['SIGNAL STATUS']).upper()
                key = (stn, sig)
                is_up = any(x in status for x in ['UP', 'ON', 'PICKUP', 'CLOSED', 'OCCURRED'])
                rtype = relay_type(sig_full)
                if not rtype: continue

                if rtype == 'Red' and is_up:
                    ev_time = row['dt']
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
                            'RTIS': pt, 'RTIS_Stn': pt['BASE_STN']
                        })
                    latch_aspect[key] = "Red"
                elif rtype in ['Green', 'Double Yellow', 'Yellow']:
                    if is_up: latch_aspect[key] = rtype
                    else: last_down_event[key] = (rtype, row['dt'])

            final_events = []
            if raw_events:
                df = pd.DataFrame(raw_events).sort_values(['Stn', 'Sig', 'Time'])
                for _, grp in df.groupby(['Stn', 'Sig']):
                    dt_check = grp['Time'].diff().shift(-1).dt.total_seconds()
                    final_events.extend(grp[dt_check.isna() | (dt_check > 15)].to_dict('records'))

            self.events = sorted(final_events, key=lambda x: x['Time'])
            self.filtered_events = self.events.copy()
            self.root.after(0, self.update_tree_display)

        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Error", str(e)))
        finally:
            self.root.after(0, lambda: self.load_btn.config(state=tk.NORMAL))

    def update_tree_display(self):
        self.tree.delete(*self.tree.get_children())
        for i, ev in enumerate(self.filtered_events, 1):
            t_ms = ev['Time'].strftime('%H:%M:%S.%f')[:-3]
            self.tree.insert('', tk.END, values=(i, ev['Stn'], ev['Sig'], t_ms, ev['Aspect'], f"{ev['Speed']} km/h", ev['RTIS_Stn']))

    def on_event_select(self, event):
        sel = self.tree.selection()
        if not sel: return
        ev = self.filtered_events[self.tree.index(sel[0])]
        rt_row = ev['RTIS']
        self.ax.clear()
        
        sub = self.rtis[(self.rtis['CumDist'] >= rt_row['CumDist'] - 1000) & (self.rtis['CumDist'] <= rt_row['CumDist'] + 1000)]
        self.ax.set_facecolor(self.bg_map.get(ev['Aspect'], "#FFFFFF"))
        self.ax.plot(sub['Logging Time'], sub['Speed'], color='#1A237E', lw=2.5, label='Speed')
        self.ax.axvline(x=ev['Time'], color='red', linestyle='--', linewidth=2)
        
        # PRECISE ANNOTATION ON GRAPH
        time_ms = ev['Time'].strftime('%H:%M:%S.%f')[:-3]
        info_box = f"STN: {ev['Stn']}\nSIG: {ev['Sig']}\nTIME: {time_ms}\nSPEED: {ev['Speed']} km/h"
        
        self.ax.annotate(info_box, xy=(ev['Time'], ev['Speed']), xytext=(20, 20), textcoords='offset points',
                         bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="red", lw=2, alpha=0.9),
                         arrowprops=dict(arrowstyle="-|>", connectionstyle="arc3,rad=0.3", color="red"),
                         fontweight='bold', fontsize=10)

        self.ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        self.ax.set_title(f"DETAILED ANALYSIS: {ev['Stn']} | {ev['Sig']} | {ev['Aspect']} -> RED", fontweight='bold')
        self.ax.set_ylabel("Speed (km/h)", fontweight='bold')
        self.ax.grid(True, alpha=0.3)
        self.canvas.draw()

if __name__ == "__main__":
    root = tk.Tk()
    app = RailwayDashboardV43(root)
    root.mainloop()