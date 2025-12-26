import sys
import os
import signal

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from src.pipepline.historical_metatrader_pipepline import HistoricalMetatraderPipepline
from src.pipepline.realtime_metatrader_pipepline import RealtimeMetatraderPipepline


def main():
    try:
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
    except SystemExit:
        print("System exit requested")
        sys.exit(0)
    except Exception as e:
        print(f"Critical error in main pipeline: {e}")
        import traceback

        traceback.print_exc()
        print("Pipeline crashed, but this should not happen. Check logs for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
