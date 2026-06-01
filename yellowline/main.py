"""
main.py — Pipeline orchestrator for YellowLine Analytics.

Starts the stream simulator in a background thread, then launches
each streaming query. Adjust which milestones run by commenting them in/out.

Run with: python main.py
"""

import threading
from utils.simulator import simulate_stream


def main():
    # ── Start the stream simulator ────────────────────────────────────────────
    print("[main] starting stream simulator...")
    simulator_thread = threading.Thread(
        target=simulate_stream,
        daemon=True,  # dies automatically when main process exits
    )
    simulator_thread.start()

    # ── Launch streaming queries ──────────────────────────────────────────────
    # Uncomment milestones as you complete them.
    # Run one at a time while learning; combine once each layer is stable.

    # Milestone 1
    # from src.bronze.ingest import run as bronze_run

    # bronze_run()

    # Milestone 2 — uncomment when Bronze is working
    from src.silver.clean import run as silver_run

    silver_run()

    # Milestone 3 — uncomment when Silver is working
    # from src.gold.revenue import run as revenue_run
    # from src.gold.volume import run as volume_run
    # revenue_run()
    # volume_run()

    # Milestone 4 — uncomment when Silver is working
    # from src.anomaly.detect import run as anomaly_run
    # anomaly_run()

    # Milestone 5 — uncomment when Bronze is working
    # from src.stream_join.trips import run as join_run
    # join_run()


if __name__ == "__main__":
    main()
