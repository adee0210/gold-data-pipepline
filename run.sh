#!/bin/bash

# Script to manage Candlestick ETF Extractor
# Usage: ./run.sh start|stop|restart|status

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/candlestick_etf.pid"
LOG_FILE="$SCRIPT_DIR/main.log"

PYTHON_CMD="$SCRIPT_DIR/.venv/bin/python $SCRIPT_DIR/src/main.py"

start() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Candlestick ETF Extractor is already running (PID: $PID)"
            return 1
        else
            echo "Removing stale PID file"
            rm "$PID_FILE"
        fi
    fi

    echo "Starting Candlestick ETF Extractor..."
    nohup $PYTHON_CMD > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Candlestick ETF Extractor started (PID: $(cat "$PID_FILE"))"
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "PID file not found. Is the process running?"
        return 1
    fi

    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Stopping Candlestick ETF Extractor (PID: $PID)..."
        kill "$PID"
        sleep 2
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Process still running, force killing..."
            kill -9 "$PID"
        fi
        rm "$PID_FILE"
        echo "Candlestick ETF Extractor stopped"
    else
        echo "Process not running, removing stale PID file"
        rm "$PID_FILE"
    fi
}

restart() {
    stop
    sleep 2
    start
}

status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Candlestick ETF Extractor is running (PID: $PID)"
        else
            echo "PID file exists but process is not running"
        fi
    else
        echo "Candlestick ETF Extractor is not running"
    fi
}

case "$1" in
    start)
        start
        ;; 
    stop)
        stop
        ;; 
    restart)
        restart
        ;; 
    status)
        status
        ;; 
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;; 
esac
