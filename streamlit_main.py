#!/usr/bin/env python3
"""
Trading Bot Streamlit Application Runner
Combines the UI and scheduler into a single application
"""

import streamlit as st
import subprocess
import sys
import os
import threading
import time
from pathlib import Path
from scheduler import bot_scheduler
import update_equities

# Add the current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

try:
    from config import *
except ImportError as e:
    st.error(f"Failed to import required modules: {e}")
    st.stop()

def run_trading_bot():
    """Run the main trading bot"""
    try:
        # Import and run the main trading bot
        update_equities()  # This is a placeholder for the actual trading bot logic
    except ImportError:
        st.error("Could not import main trading bot. Please check your trading bot file.")
    except Exception as e:
        st.error(f"Error running trading bot: {e}")

def main():
    """Main Streamlit application"""
    
    # Set up the page
    st.set_page_config(
        page_title="🚀 Trading Bot Control Center",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize session state for scheduler
    if 'scheduler_running' not in st.session_state:
        st.session_state.scheduler_running = False
    
    if 'trading_bot_running' not in st.session_state:
        st.session_state.trading_bot_running = False
    
    # Main title
    st.title("🚀 Trading Bot Control Center")
    st.markdown("---")
    
    # Status section
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("📊 System Status")
        if st.session_state.scheduler_running:
            st.success("🟢 Scheduler Running")
        else:
            st.error("🔴 Scheduler Stopped")
    
    with col2:
        st.subheader("🤖 Trading Bot")
        if st.session_state.trading_bot_running:
            st.success("🟢 Bot Active")
        else:
            st.error("🔴 Bot Inactive")
    
    with col3:
        st.subheader("⚡ Quick Actions")
        if st.button("🔄 Restart All Systems"):
            # Restart logic here
            st.info("Restarting systems...")
    
    # Control Panel
    st.markdown("---")
    st.subheader("🎛️ Control Panel")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("▶️ Start Scheduler", type="primary"):
            if not st.session_state.scheduler_running:
                success = bot_scheduler.start()
                if success:
                    st.session_state.scheduler_running = True
                    st.success("Scheduler started!")
                    st.rerun()
                else:
                    st.error("Failed to start scheduler")
            else:
                st.warning("Scheduler already running")
    
    with col2:
        if st.button("⏹️ Stop Scheduler"):
            if st.session_state.scheduler_running:
                bot_scheduler.stop()
                st.session_state.scheduler_running = False
                st.success("Scheduler stopped!")
                st.rerun()
            else:
                st.warning("Scheduler not running")
    
    with col3:
        if st.button("🚀 Start Trading Bot", type="primary"):
            if not st.session_state.trading_bot_running:
                # Start trading bot in background thread
                threading.Thread(target=run_trading_bot, daemon=True).start()
                st.session_state.trading_bot_running = True
                st.success("Trading bot started!")
                st.rerun()
            else:
                st.warning("Trading bot already running")
    
    with col4:
        if st.button("🛑 Stop Trading Bot"):
            if st.session_state.trading_bot_running:
                st.session_state.trading_bot_running = False
                st.success("Trading bot stopped!")
                st.rerun()
            else:
                st.warning("Trading bot not running")
    
    # System Information
    st.markdown("---")
    st.subheader("📋 System Information")
    
    # Get scheduler status
    if st.session_state.scheduler_running:
        status = bot_scheduler.get_bot_status()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Configured Tickers", status.get("configured_tickers", 0))
            if status.get("last_token_refresh"):
                st.text(f"Last Token Refresh: {status['last_token_refresh'].strftime('%H:%M:%S')}")
            if status.get("last_param_update"):
                st.text(f"Last Param Update: {status['last_param_update'].strftime('%H:%M:%S')}")
        
        with col2:
            st.metric("Scheduler Running", "🟢" if status.get("running") else "🔴")
            if "error" in status:
                st.error(f"Scheduler Error: {status['error']}")
    
    else:
        st.info("Scheduler is not running. Start the scheduler to see system information.")

    # Footer
    st.markdown("---")
    st.caption("Trading Bot Control Center - Powered by Streamlit")

if __name__ == "__main__":
    main()