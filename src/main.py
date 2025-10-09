import sys
import os
import signal

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from src.pipepline.historical_metatrader_pipepline import HistoricalMetatraderPipepline
from src.pipepline.realtime_metatrader_pipepline import RealtimeMetatraderPipepline


def main():
    # Run Metatrader historical data once
    hist = HistoricalMetatraderPipepline()
    hist.run()

    # Start realtime pipeline (blocking loop)
    realtime = RealtimeMetatraderPipepline()

    def handle_sigterm(signum, frame):
        print("Received stop signal, exiting...")
        raise SystemExit()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    realtime.run_realtime()


if __name__ == "__main__":
    main()
