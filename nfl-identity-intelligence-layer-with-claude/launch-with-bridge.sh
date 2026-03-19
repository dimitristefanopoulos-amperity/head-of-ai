#!/bin/bash

# Launch script for Identity Engine + MCP Bridge
# Starts both services in the background

cd "$(dirname "$0")"

echo "Starting MCP Bridge on port 5081..."
python mcp_bridge.py &
BRIDGE_PID=$!

echo "Waiting for bridge to start..."
sleep 2

echo "Starting Identity Engine on port 5080..."
python app.py &
APP_PID=$!

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Both services started"
echo "═══════════════════════════════════════════════════════════"
echo "  MCP Bridge PID:  $BRIDGE_PID"
echo "  Flask App PID:   $APP_PID"
echo ""
echo "  To stop:"
echo "    kill $BRIDGE_PID $APP_PID"
echo "═══════════════════════════════════════════════════════════"
echo ""

# Wait for both processes
wait
