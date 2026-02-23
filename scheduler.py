"""
scheduler.py — Daily Pipeline Scheduler.

Wraps the full pipeline run in APScheduler for production cron-style
scheduling. Designed to run as a persistent daemon process on a server,
triggering the pipeline at a configured time every day (default 07:00 London).

Features:
    - Timezone-aware scheduling (Europe/London — handles BST/GMT automatically)
    - Graceful shutdown on SIGINT / SIGTERM
    - Retry on failure with configurable delay
    - Rotating file logging independent of main.py log

Usage:
    python scheduler.py                  # Run daemon (blocks)
    python scheduler.py --run-now        # Trigger one immediate run then exit
    python scheduler.py --config custom.yaml

Cron equivalent (if you prefer crontab over this scheduler):
    0 7 * * 1-5 cd /path/to/project && python main.py --full-run
"""

import argparse
import logging
import logging.handlers
import os
import signal
import sys
from datetime import datetime
from pathlib import Path

import yaml

# APScheduler v3.x
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger


logger = logging.getLogger(__name__)


def _configure_scheduler_logging(log_dir: str) -> None:
    """Set up dedicated rotating log for the scheduler process.

    Args:
        log_dir: Directory for log files.
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / "scheduler.log"

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=14, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(stream_handler)

    # Suppress APScheduler internals below WARNING
    logging.getLogger("apscheduler").setLevel(logging.WARNING)


def _run_full_pipeline(config_path: str, max_retries: int, retry_delay: int) -> None:
    """Execute the full detection pipeline with retry logic.

    This function is called by APScheduler on each scheduled trigger.
    It imports and runs main.run_pipeline() so the scheduler and CLI
    share identical pipeline logic.

    Args:
        config_path: Path to configuration YAML.
        max_retries: Maximum retry attempts on failure.
        retry_delay: Seconds to wait between retries.
    """
    import time

    # Import here to ensure fresh module state on each run
    import argparse as _ap
    from main import run_pipeline, _configure_logging

    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 70)
    logger.info("SCHEDULED PIPELINE RUN — %s", run_time)
    logger.info("=" * 70)

    # Build a namespace that mirrors --full-run
    args = _ap.Namespace(
        config=config_path,
        log_level="INFO",
        full_run=True,
        generate_data=False,
        detect=False,
        report=False,
        dashboard=False,
        alert=False,
    )

    for attempt in range(1, max_retries + 1):
        try:
            exit_code = run_pipeline(args, logger)
            if exit_code == 0:
                logger.info(
                    "Scheduled run completed successfully (attempt %d)", attempt
                )
                return
            else:
                logger.error(
                    "Pipeline returned non-zero exit code %d (attempt %d)",
                    exit_code,
                    attempt,
                )
        except Exception as exc:
            logger.error(
                "Pipeline raised exception (attempt %d): %s",
                attempt,
                exc,
                exc_info=True,
            )

        if attempt < max_retries:
            logger.info("Retrying in %d seconds...", retry_delay)
            time.sleep(retry_delay)

    logger.error(
        "Pipeline failed after %d attempt(s) — will retry at next scheduled time",
        max_retries,
    )


def _parse_args() -> argparse.Namespace:
    """Parse scheduler-specific CLI arguments.

    Returns:
        Parsed Namespace.
    """
    parser = argparse.ArgumentParser(
        prog="scheduler",
        description="Daily APScheduler daemon for the Cost Leakage Detector pipeline.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration YAML (default: config.yaml)",
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Execute one pipeline run immediately then exit (useful for testing)",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point: configure scheduler and start the blocking daemon."""
    args = _parse_args()

    # Load config
    config_path = args.config
    try:
        with open(config_path, "r") as fh:
            cfg = yaml.safe_load(fh)
    except FileNotFoundError:
        print(f"ERROR: Configuration file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    log_dir = cfg.get("paths", {}).get("log_dir", "logs")
    _configure_scheduler_logging(log_dir)

    sched_cfg = cfg.get("scheduler", {})
    run_time = sched_cfg.get("run_time", "07:00")
    timezone = sched_cfg.get("timezone", "Europe/London")
    max_retries = sched_cfg.get("max_retries", 3)
    retry_delay = sched_cfg.get("retry_delay_seconds", 300)

    run_hour, run_minute = map(int, run_time.split(":"))

    if args.run_now:
        logger.info("--run-now flag set — executing pipeline immediately")
        _run_full_pipeline(config_path, max_retries, retry_delay)
        logger.info("Immediate run complete — exiting")
        return

    scheduler = BlockingScheduler(timezone=timezone)
    scheduler.add_job(
        func=_run_full_pipeline,
        trigger=CronTrigger(
            hour=run_hour,
            minute=run_minute,
            timezone=timezone,
        ),
        kwargs={
            "config_path": config_path,
            "max_retries": max_retries,
            "retry_delay": retry_delay,
        },
        id="daily_leakage_detection",
        name="Daily Cost Leakage Detection Pipeline",
        replace_existing=True,
        misfire_grace_time=600,  # 10 min grace if server was down
    )

    def _handle_shutdown(signum, frame):
        logger.info("Shutdown signal received — stopping scheduler gracefully")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    logger.info(
        "Scheduler started — daily run at %s %s | timezone: %s",
        run_time,
        timezone,
        timezone,
    )
    logger.info("Press Ctrl+C or send SIGTERM to stop.")

    scheduler.start()


if __name__ == "__main__":
    main()
