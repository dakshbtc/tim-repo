#!/bin/bash

echo "🔁 Running stop_process.py to terminate any managed processes..."
python3 stop_process.py


sleep 2
echo "✅ Old processes terminated."

# Create logs directory if not exists
mkdir -p logs

# Initialize bot state to Stopped
echo '{"status": "Stopped"}' > bot_state.json
echo "🚦 Initialized bot_state.json to 'Stopped'"

# Start process_launcher.py (process_launcher)
echo "🚀 Starting process_launcher.py (process_launcher.py)..."
nohup python3 -u process_launcher.py > logs/process_launcher.out 2>&1 &

# Start Streamlit UI
echo "📺 Starting Streamlit UI (update_equities_prod.py)..."
nohup streamlit run update_equities_prod.py > logs/update_equities.out 2>&1 &

echo "✅ All services started. Check logs for details."