import streamlit as st
import pandas as pd
import json
import requests
import schedule
import threading
import os
import pytz
import time as _time
import signal
import subprocess
from datetime import datetime
from pathlib import Path
from config import (
    tickers_path,
    refresh_token_link_path,
    refresh_token_path,
    access_token_path,
    authtoken_link,
    time_zone,
)
from utils import *
from schwab import create_header, get_refresh_token
from tastytrade import get_instruments, generate_access_token_for_tastytrade
import sys

# ----------------------------------------
# Streamlit Page Configuration
# ----------------------------------------
st.set_page_config(
    page_title="Trading Bot Control Panel",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ----------------------------------------
# Session State Initialization
# ----------------------------------------
if "tickers_data" not in st.session_state:
    try:
        with open(tickers_path, "r") as file:
            st.session_state.tickers_data = json.load(file)
    except FileNotFoundError:
        st.session_state.tickers_data = {}

# We'll keep track of the file's last modification time, so we can reload only when it changes.
if "tickers_file_mtime" not in st.session_state:
    try:
        st.session_state.tickers_file_mtime = os.path.getmtime(tickers_path)
    except Exception:
        st.session_state.tickers_file_mtime = None

if "refresh_token_link" not in st.session_state:
    try:
        with open(refresh_token_link_path, "r") as file:
            st.session_state.refresh_token_link = json.load(file)
    except FileNotFoundError:
        st.session_state.refresh_token_link = {"refresh_link": ""}

if "bot_status" not in st.session_state:
    st.session_state.bot_status = "Stopped"

if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = None

if "strategy_status" not in st.session_state:
    st.session_state.strategy_status = "Stopped"

# ----------------------------------------
# Helper Functions
# ----------------------------------------

def refresh_access_token():
    """
    Refresh the Schwab access token using the stored refresh token.
    Updates session_state.last_refresh on success.
    Returns (bool, str) for success status and message.
    """
    try:
        with open(refresh_token_path, "r") as file:
            refresh_token = file.read().strip()
        headers = create_header("Basic")
        data = {"grant_type": "refresh_token", "refresh_token": refresh_token}

        response = requests.post(authtoken_link, headers=headers, data=data)
        response.raise_for_status()
        resp_json = response.json()
        with open(access_token_path, "w") as file:
            file.write(resp_json["access_token"])

        tz = pytz.timezone(time_zone)
        st.session_state.last_refresh = datetime.now(tz)
        return True, "Access token refreshed successfully"
    except Exception as e:
        return False, f"Error refreshing access token: {str(e)}"


def scheduled_refresh_access_token():
    """
    Periodically called to keep the Schwab access token fresh.
    """
    success, msg = refresh_access_token()
    now = datetime.now(pytz.timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] (Scheduled) {msg}")


def scheduled_generate_tastytrade_token():
    """
    Generate Tastytrade token at scheduled time.
    """
    try:
        generate_access_token_for_tastytrade()
        now = datetime.now(pytz.timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] (Scheduled) Tastytrade token generated successfully.")
    except Exception as e:
        now = datetime.now(pytz.timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] (Scheduled) Error generating Tastytrade token: {str(e)}")


def scheduled_get_instruments():
    """
    Fetch instruments at scheduled time.
    """
    try:
        get_instruments()
        now = datetime.now(pytz.timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] (Scheduled) Instruments fetched successfully.")
    except Exception as e:
        now = datetime.now(pytz.timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] (Scheduled) Error fetching instruments: {str(e)}")


def save_tickers_data():
    """
    Save st.session_state.tickers_data to tickers_path as JSON.
    Returns True on success, False otherwise.
    """
    try:
        with open(tickers_path, "w") as file:
            json.dump(st.session_state.tickers_data, file, indent=2)
        # Update our stored mtime so the next reload does not re‐read unnecessarily
        st.session_state.tickers_file_mtime = os.path.getmtime(tickers_path)
        return True
    except Exception as e:
        st.error(f"Error saving tickers data: {str(e)}")
        return False


def validate_refresh_link(link: str):
    """
    Validate the provided Schwab refresh‐token link.
    If valid, save it to session_state and file.
    Returns (bool, str) for success status and message.
    """
    try:
        is_valid = get_refresh_token(link)
        if is_valid:
            st.session_state.refresh_token_link["refresh_link"] = link
            with open(refresh_token_link_path, "w") as file:
                json.dump(st.session_state.refresh_token_link, file, indent=2)
            return True, "Token refreshed successfully"
        else:
            return False, "Link is expired or invalid"
    except Exception as e:
        return False, f"Error validating link: {str(e)}"


def get_current_trades():
    """
    Read JSON files from the 'trades' directory.
    Each filename (without .json) is treated as a ticker.
    Returns a dict: { ticker: trade_data_dict, ... }.
    """
    trades = {}
    trades_dir = "trades"
    if os.path.exists(trades_dir):
        for filename in os.listdir(trades_dir):
            if filename.endswith(".json"):
                ticker = filename.replace(".json", "")
                try:
                    with open(os.path.join(trades_dir, filename), "r") as f:
                        trade_data = json.load(f)
                        if trade_data:
                            trades[ticker] = trade_data
                except Exception as e:
                    st.error(f"Error reading trades for {ticker}: {str(e)}")
    return trades


def stop_strategy_processes():
    """
    Stops all strategy-related processes (tick_producer.py and strategy_consumer.py).
    Returns (bool, str) for success status and message.
    """
    try:
        def stop_process(process_name):
            """Stops all processes with the given script name."""
            try:
                result = subprocess.run(
                    ["pgrep", "-f", process_name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                pids = result.stdout.strip().split("\n")

                if not pids or pids == ['']:
                    print(f"No running processes found for {process_name}.")
                    return 0

                terminated_count = 0
                for pid in pids:
                    try:
                        print(f"Terminating {process_name} (PID: {pid})")
                        os.kill(int(pid), signal.SIGTERM)
                        terminated_count += 1
                    except Exception as e:
                        print(f"Failed to terminate PID {pid}: {e}")
                
                return terminated_count
            except Exception as e:
                print(f"Error while searching for {process_name}: {e}")
                return 0

        # Stop both processes
        tick_producer_count = stop_process("tick_producer.py")
        strategy_consumer_count = stop_process("strategy_consumer.py")
        
        total_terminated = tick_producer_count + strategy_consumer_count
        
        if total_terminated > 0:
            st.session_state.strategy_status = "Stopped"
            return True, ""
        else:
            return True, ""
            
    except Exception as e:
        return False, f"Error stopping strategy processes: {str(e)}"


def start_strategy_processes():
    """
    Starts the strategy processes by running process_launcher.py.
    Returns (bool, str) for success status and message.
    """
    try:
        base_dir = Path(__file__).parent.resolve()
        process_launcher_path = base_dir / "process_launcher.py"
        
        if not process_launcher_path.exists():
            return False, f"process_launcher.py not found at {process_launcher_path}"
        
        # Start process_launcher.py as a background process
        process = subprocess.Popen(
            [sys.executable, str(process_launcher_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True  # This detaches the process from the current session
        )
        
        st.session_state.strategy_status = "Running"
        return True, f"Strategy processes started successfully (PID: {process.pid})"
        
    except Exception as e:
        return False, f"Error starting strategy processes: {str(e)}"


def restart_strategy():
    """
    Restarts the strategy by stopping current processes and starting new ones.
    Returns (bool, str) for success status and message.
    """
    # First stop existing processes
    stop_success, stop_msg = stop_strategy_processes()
    if not stop_success:
        return False, f"Failed to stop processes: {stop_msg}"
    
    # Wait a moment for processes to fully terminate
    _time.sleep(2)
    
    # Start new processes
    start_success, start_msg = start_strategy_processes()
    if not start_success:
        return False, f"Failed to start processes: {start_msg}"
    
    return True, f""


def check_strategy_status():
    """
    Check if strategy processes are currently running.
    Updates st.session_state.strategy_status.
    """
    try:
        # Check for tick_producer.py
        result_producer = subprocess.run(
            ["pgrep", "-f", "tick_producer.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Check for strategy_consumer.py
        result_consumer = subprocess.run(
            ["pgrep", "-f", "strategy_consumer.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        producer_running = bool(result_producer.stdout.strip())
        consumer_running = bool(result_consumer.stdout.strip())
        
        if producer_running and consumer_running:
            st.session_state.strategy_status = "Running"
        elif producer_running or consumer_running:
            st.session_state.strategy_status = "Partial"
        else:
            st.session_state.strategy_status = "Stopped"
            
    except Exception as e:
        print(f"Error checking strategy status: {e}")
        st.session_state.strategy_status = "Unknown"


# ----------------------------------------
# New Scheduled Jobs
# ----------------------------------------

def scheduled_reload_tickers():
    try:
        if os.path.exists(tickers_path):
            current_mtime = os.path.getmtime(tickers_path)
            # store last_mtime in global or module-level var instead of st.session_state
            if not hasattr(scheduled_reload_tickers, "last_mtime"):
                scheduled_reload_tickers.last_mtime = current_mtime
            if current_mtime != scheduled_reload_tickers.last_mtime:
                with open(tickers_path, "r") as f:
                    loaded = json.load(f)
                # Instead of trying to update st.session_state here, just print/log
                print("[Scheduled] Reloaded tickers.json")
                scheduled_reload_tickers.last_mtime = current_mtime
    except Exception as e:
        print(f"[Scheduled] Error reloading tickers.json: {str(e)}")


def scheduled_validate_refresh_link():
    try:
        # Use a safe variable, not session_state
        if not hasattr(scheduled_validate_refresh_link, "last_link"):
            with open(refresh_token_link_path, "r") as f:
                link_data = json.load(f)
                scheduled_validate_refresh_link.last_link = link_data.get("refresh_link", "")

        current_link = scheduled_validate_refresh_link.last_link
        if current_link:
            is_valid = get_refresh_token(current_link)
            now = datetime.now(pytz.timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{now}] Refresh link valid? {is_valid}")
    except Exception as e:
        print(f"[Scheduled] Error validating refresh link: {str(e)}")


# ----------------------------------------
# Background Scheduler (with `schedule`)
# ----------------------------------------
def start_background_scheduler():
    """
    Sets up all scheduled jobs and runs them in a separate daemon thread.
    """
    schedule.clear()

    # Every 25 minutes: refresh the Schwab access token
    schedule.every(25).minutes.do(scheduled_refresh_access_token)

    # Every day at 09:00 IST: generate Tastytrade token
    schedule.every().day.at("09:00").do(scheduled_generate_tastytrade_token)

    # Every day at 09:05 IST: fetch instruments
    schedule.every().day.at("09:05").do(scheduled_get_instruments)

    # Every 10 seconds: reload tickers.json if it changed on disk
    schedule.every(10).seconds.do(scheduled_reload_tickers)

    # Every 5 seconds: validate the stored refresh‐token link
    schedule.every(5).seconds.do(scheduled_validate_refresh_link)

    # Every 30 seconds: check strategy status
    schedule.every(30).seconds.do(check_strategy_status)

    def run_scheduler_loop():
        while True:
            schedule.run_pending()
            _time.sleep(1)

    thread = threading.Thread(target=run_scheduler_loop, daemon=True)
    thread.start()

BOT_STATE_FILE = "bot_state.json"

def set_bot_status(status: str):
    with open(BOT_STATE_FILE, "w") as f:
        json.dump({"status": status}, f)


# Ensure scheduler starts only once
if "scheduler_started" not in st.session_state:
    start_background_scheduler()
    st.session_state.scheduler_started = True


# ----------------------------------------
# Sidebar: Bot Control & Token Management
# ----------------------------------------
with st.sidebar:
    st.header("Strategy Control")
    
    # Check current strategy status
    check_strategy_status()
    
    # # Display strategy status
    # if st.session_state.strategy_status == "Running":
    #     st.success("🟢 Strategy is Running")
    # elif st.session_state.strategy_status == "Partial":
    #     st.warning("🟡 Strategy Partially Running")
    # elif st.session_state.strategy_status == "Stopped":
    #     st.error("🔴 Strategy is Stopped")
    # else:
    #     st.info("❓ Strategy Status Unknown")
    
    # Strategy control buttons
    col_stop, col_restart,col_start = st.columns(3)
    
    # with col_start:
    #     if st.button("▶️ Start", type="primary", help="Start strategy processes"):
    #         success, message = start_strategy_processes()
    #         if success:
    #             st.success(message)
    #         else:
    #             st.error(message)
    
    with col_stop:
        if st.button("⏹️ Stop", type="secondary", help="Stop strategy processes"):
            stop_strategy_processes()

    with col_restart:
        if st.button(" Restart", type="primary", help="Restart strategy processes"):
            restart_strategy()
    
    st.divider()

    st.header("Token Management")

    # Last refresh time (manually triggered or scheduled)
    if st.session_state.last_refresh:
        st.info(f"Last refresh: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")

    # Manual refresh button
    if st.button("Refresh Access Token"):
        success, message = refresh_access_token()
        if success:
            st.success(message)
        else:
            st.error(message)

    st.divider()

    # Quick Actions
    st.header("Quick Actions")

    if st.button("Generate Tastytrade Token"):
        try:
            generate_access_token_for_tastytrade()
            st.success("Tastytrade token generated!")
        except Exception as e:
            st.error(f"Error generating token: {str(e)}")

    if st.button("Update Instruments"):
        try:
            get_instruments()
            st.success("Instruments updated!")
        except Exception as e:
            st.error(f"Error updating instruments: {str(e)}")

# ----------------------------------------
# Main Content: Tabs
# ----------------------------------------
st.title("🚀 Trading Bot Control Panel")

tab1, tab2 = st.tabs(
    ["📊 Trading Parameters", "🔗 Token Links"]
)

# ---------------------------
# Tab 1: Trading Parameters
# ---------------------------
with tab1:
    st.header("Trading Parameters Configuration")

    # Expandable section to add a new ticker
    with st.expander("➕ Add New Ticker", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            new_ticker = st.text_input("Ticker Symbol", placeholder="e.g., AAPL or /ES")
            
            time_frame_input = st.text_input(
                "Time Frame (e.g. 1Min, 5Min, 1Hour, 1Day, 516t, 1600t)",
                placeholder="Examples: 1Min, 5Min, 15Min, 30Min, 1Hour, 4Hour, 1Day, 516t, 1160t, 1600t"
            )

            # Validate and simplify timeframe
            valid_suffixes = ["Min", "Hour", "Day", "t"]
            simplified_time_frame = None
            invalid_timeframe = False

            if time_frame_input:
                if any(time_frame_input.endswith(suffix) for suffix in valid_suffixes):
                    if time_frame_input.endswith("Min"):
                        simplified_time_frame = time_frame_input.replace("Min", "")
                    elif time_frame_input.endswith("Hour"):
                        simplified_time_frame = time_frame_input.replace("Hour", "h")
                    elif time_frame_input.endswith("Day"):
                        simplified_time_frame = time_frame_input.replace("Day", "d")
                    elif time_frame_input.endswith("t"):
                        simplified_time_frame = time_frame_input
                    else:
                        invalid_timeframe = True
                else:
                    invalid_timeframe = True

            if invalid_timeframe:
                st.error("❌ Invalid timeframe format. Use formats like 1Min, 5Min, 1Hour, 1Day, or 516t.")

            schwab_qty = st.number_input("Schwab Quantity", min_value=0, value=0)

        with col2:
            trade_enabled = st.selectbox("Trade Enabled", ["TRUE", "FALSE"], index=1)
            period1 = st.number_input("Period 1", min_value=1, value=10)
            trend_line1 = st.selectbox(
                "Trend Line 1", ["EMA", "SMA", "WilderSmoother"], index=0
            )

        with col3:
            period2 = st.number_input("Period 2", min_value=1, value=20)
            trend_line2 = st.selectbox(
                "Trend Line 2", ["EMA", "SMA", "WilderSmoother"], index=0
            )
            tastytrade_qty = st.number_input(
                "Tastytrade Quantity", min_value=0, value=0
            )

        if st.button("Add Ticker", type="primary"):
            if not new_ticker:
                st.error("Please enter a ticker symbol")
            elif not simplified_time_frame:
                st.error("Please enter a valid timeframe")
            else:
                st.session_state.tickers_data[new_ticker] = [
                    simplified_time_frame,
                    str(schwab_qty),
                    trade_enabled,
                    str(period1),
                    trend_line1,
                    str(period2),
                    trend_line2,
                    str(tastytrade_qty),
                ]
                if save_tickers_data():
                    st.success(f"✅ Added {new_ticker} successfully!")
                    st.rerun()

    # Display & edit existing tickers
    if st.session_state.tickers_data:
        st.subheader("Current Trading Parameters")

        # Convert session_state dict to DataFrame
        df_data = []
        for ticker, params in st.session_state.tickers_data.items():
            # Expand simplified time frame to display format
            time_frame = params[0]
            if time_frame.endswith('t'):
                display_time_frame = time_frame
            elif time_frame.endswith('h'):
                display_time_frame = time_frame.replace('h', 'Hour')
            elif time_frame.endswith('d'):
                display_time_frame = time_frame.replace('d', 'Day')
            else:
                display_time_frame = time_frame + 'Min'

            df_data.append([ticker, display_time_frame] + params[1:])

        df = pd.DataFrame(
            df_data,
            columns=[
                "Ticker",
                "Time Frame",
                "Schwab Qty",
                "Trade",
                "Period1",
                "Trend Line1",
                "Period2",
                "Trend Line2",
                "Tastytrade Qty",
            ],
        )

        # Editable data editor
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                "Time Frame": st.column_config.TextColumn(
                    "Time Frame",
                    help="Examples: 1Min, 5Min, 15Min, 1Hour, 1Day, 516t",
                ),
                "Trade": st.column_config.SelectboxColumn(
                    "Trade",
                    options=["TRUE", "FALSE"],
                ),
                "Trend Line1": st.column_config.SelectboxColumn(
                    "Trend Line 1", options=["EMA", "SMA", "WilderSmoother"]
                ),
                "Trend Line2": st.column_config.SelectboxColumn(
                    "Trend Line 2", options=["EMA", "SMA", "WilderSmoother"]
                ),
            },
        )

        col_save, col_clear = st.columns([1, 4])
        with col_save:
            if st.button("Save Changes", type="primary"):
                new_tickers_data = {}
                errors = []
                for idx, row in edited_df.iterrows():
                    ticker = row["Ticker"]
                    time_frame = row["Time Frame"]
                    simplified_time_frame = None

                    if time_frame.endswith("Min"):
                        simplified_time_frame = time_frame.replace("Min", "")
                    elif time_frame.endswith("Hour"):
                        simplified_time_frame = time_frame.replace("Hour", "h")
                    elif time_frame.endswith("Day"):
                        simplified_time_frame = time_frame.replace("Day", "d")
                    elif time_frame.endswith("t"):
                        simplified_time_frame = time_frame
                    else:
                        errors.append(f"Row {idx+1} ({ticker}): Invalid Time Frame '{time_frame}'")

                    if simplified_time_frame:
                        params = [
                            simplified_time_frame,
                            str(row["Schwab Qty"]),
                            row["Trade"],
                            str(row["Period1"]),
                            row["Trend Line1"],
                            str(row["Period2"]),
                            row["Trend Line2"],
                            str(row["Tastytrade Qty"]),
                        ]
                        new_tickers_data[ticker] = params

                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    st.session_state.tickers_data = new_tickers_data
                    if save_tickers_data():
                        st.success("Changes saved successfully!")
                        st.rerun()

        with col_clear:
            if st.button("Clear All Tickers", type="secondary"):
                st.session_state.tickers_data = {}
                if save_tickers_data():
                    st.success("All tickers cleared!")
                    st.rerun()
    else:
        st.info("No tickers configured. Add some tickers to get started!")

# ---------------------------
# Tab 2: Token Links
# ---------------------------
with tab2:
    st.header("Token Links Management")

    # Current stored link
    current_link = st.session_state.refresh_token_link.get("refresh_link", "")

    st.subheader("Schwab Refresh Token Link")

    new_link = st.text_input(
        "Enter new refresh token link:",
        value=current_link,
        placeholder="Paste your Schwab refresh token link here",
    )

    col_validate, col_test = st.columns(2)
    with col_validate:
        if st.button("Validate & Save Link", type="primary"):
            if new_link:
                success, message = validate_refresh_link(new_link)
                if success:
                    st.success(message)
                else:
                    st.error(message)
            else:
                st.error("Please enter a refresh token link")

    with col_test:
        if st.button("Test Current Link"):
            if current_link:
                success, message = validate_refresh_link(current_link)
                if success:
                    st.success("Current link is valid!")
                else:
                    st.error("Current link is invalid or expired")
            else:
                st.warning("No refresh link configured")

    # Display current link status
    if current_link:
        display_link = current_link if len(current_link) <= 60 else f"{current_link[:60]}..."
        st.info(f"Current link: {display_link}")
    else:
        st.warning("No refresh token link configured")

# ----------------------------------------
# Footer & Real-time Status Placeholder
# ----------------------------------------
st.divider()
st.caption("Trading Bot Control Panel - Built with Streamlit")

# Real-time status display
status_placeholder = st.empty()
with status_placeholder.container():
    col1, col2 = st.columns(2)
    with col1:
        if st.session_state.strategy_status == "Running":
            st.info(f"🔄 Strategy running... Last update: {datetime.now().strftime('%H:%M:%S')}")
        elif st.session_state.strategy_status == "Partial":
            st.warning(f"⚠️ Strategy partially running... Last update: {datetime.now().strftime('%H:%M:%S')}")
        else:
            st.error(f"⏹️ Strategy stopped... Last update: {datetime.now().strftime('%H:%M:%S')}")
    
    with col2:
        st.info(f"📊 {len(st.session_state.tickers_data)} tickers configured")