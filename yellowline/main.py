"""
main.py — Pipeline orchestrator for YellowLine Analytics.

Starts the stream simulator in a background thread, then launches
each streaming query. Adjust which milestones run by commenting them in/out.

Run with: python main.py
"""

import argparse
import threading
from config import SIMULATOR_BATCH_SIZE, SIMULATOR_LIMIT_ROWS
from utils.simulator import simulate_stream


def main(mode: str, limit_rows: int, batch_size: int):
    # ── Start the stream simulator ────────────────────────────────────────────
    if mode == "raw" or mode == "all":
        print("[main] starting stream simulator...")
        simulator_thread = threading.Thread(
            kwargs={"batch_size": batch_size, "limit_rows": limit_rows},
            target=simulate_stream,
            daemon=mode != "raw",  # if we specify raw, the thread takes over the daemon
        )
        simulator_thread.start()

    # ── Launch streaming queries ──────────────────────────────────────────────
    # Uncomment milestones as you complete them.
    # Run one at a time while learning; combine once each layer is stable.

    # Milestone 1
    if mode == "bronze" or mode == "all":
        from src.bronze.ingest import run as bronze_run

        bronze_run()

    # Milestone 2 — uncomment when Bronze is working
    if mode == "silver" or mode == "all":
        from src.silver.clean import run as silver_run

        silver_run()

    # Milestone 3 — uncomment when Silver is working
    if "gold-vol" in mode or mode == "all":
        from src.gold.volume import run as volume_run

        volume_run()

    if "gold-rev" in mode or mode == "all":
        from src.gold.revenue import run as revenue_run

        revenue_run()
    #
    # Milestone 4 — uncomment when Silver is working
    if mode == "anomaly_detection" or mode == "all":
        from src.anomaly.detect import run as anomaly_run

        anomaly_run()

    # Milestone 5 — uncomment when Bronze is working
    # from src.stream_join.trips import run as join_run
    # join_run()

    # TODO: read Silver as a stream


if __name__ == "__main__":
    parser = argparse.ArgumentParser("choose to prompt a rerun of raw")

    parser.add_argument(
        "--batch-size",
        type=int,
        help="limit the size of each batch",
        default=SIMULATOR_BATCH_SIZE,
    )

    parser.add_argument(
        "--limit-rows",
        type=int,
        help="limit rows read in from the dataframe",
        default=SIMULATOR_LIMIT_ROWS,
    )

    parser.add_argument(
        "--mode",
        choices=[
            "raw",
            "bronze",
            "silver",
            "gold-rev",
            "gold-vol",
            "all",
            "anomaly_detection",
        ],
        help="choose which stream to start running, all to trigger all of them",
    )

    args = parser.parse_args()
    main(args.mode, args.limit_rows, args.batch_size)
