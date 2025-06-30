#!/bin/bash

echo "🔁 Stopping old processes..."

# Kill any existing main_equities.py or update_equities.py processes
pkill -f main_equities.py
pkill -f update_equities.py

sleep 2

echo "✅ Old processes terminated."

# Create logs directory if not exists
mkdir -p logs

# Initialize bot state to Stopped
echo '{"status": "Stopped"}' > bot_state.json
echo "🚦 Initialized bot_state.json to 'Stopped'"

# Start main_equities.py
echo "🚀 Starting main_equities.py..."
nohup python3 -u process_launcher.py > logs/process_launcher.out 2>&1 &

# Start Streamlit UI
echo "📺 Starting Streamlit UI (update_equities.py)..."
nohup streamlit run update_equities_prod.py > logs/update_equities.out 2>&1 &

echo "✅ All services started. Check logs for details."
