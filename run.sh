#!/bin/bash

# Script to manage gold_data_project service
# Usage: ./run.sh start|stop|restart|status

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/gold_data_project.pid"
NOHUP_LOG="$SCRIPT_DIR/nohup.out"

PYTHON_CMD="$SCRIPT_DIR/.venv/bin/python $SCRIPT_DIR/src/main.py"

start() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "gold_data_project is already running (PID: $PID)"
            return 1
        else
            echo "Removing stale PID file"
            rm "$PID_FILE"
        fi
    fi

    echo "Starting gold_data_project..."
    # Không redirect vào main.log - để Python logger tự quản lý
    # Output/error của process sẽ vào nohup.out (nếu cần debug nohup)
    nohup $PYTHON_CMD > /dev/null 2>&1 &
    echo $! > "$PID_FILE"
    echo "gold_data_project started (PID: $(cat "$PID_FILE"))"
    echo "Logs are managed by Python logger in main.log with rotation"
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "PID file not found. Is the process running?"
        return 1
    fi

    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Stopping gold_data_project (PID: $PID)..."
        kill "$PID"
        sleep 2
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Process still running, force killing..."
            kill -9 "$PID"
        fi
        rm "$PID_FILE"
        echo "gold_data_project stopped"
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

monitor() {
    echo "Starting monitor mode - will auto-restart on crashes..."
    while true; do
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if ! ps -p "$PID" > /dev/null 2>&1; then
                echo "$(date): Process crashed or stopped, restarting..."
                rm -f "$PID_FILE"
                start
            fi
        else
            echo "$(date): No PID file found, starting process..."
            start
        fi
        sleep 10  # Check every 10 seconds
    done
}

status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "gold_data_project is running (PID: $PID)"
        else
            echo "PID file exists but process is not running"
        fi
    else
        echo "gold_data_project is not running"
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
    monitor)
        monitor
        ;; 
    status)
        status
        ;; 
    *)
        echo "Usage: $0 {start|stop|restart|monitor|status}"
        exit 1
        ;; 
esac
