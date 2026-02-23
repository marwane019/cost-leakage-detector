"""
main.py — Operations Cost Leakage Detector — CLI Entry Point.

Provides a command-line interface to run any combination of pipeline stages:
  1. generate-data   — Create synthetic transaction dataset
  2. detect          — Run anomaly detection engine
  3. score           — Apply severity scoring
  4. report          — Generate Excel workbook
  5. dashboard       — Build interactive HTML dashboard
  6. alert           — Send Slack notification for Critical findings
  7. full-run        — Execute all stages in sequence (default for scheduler)

Usage examples:
    python main.py --full-run
    python main.py --generate-data --detect --report
    python main.py --detect --alert
    python main.py --full-run --config custom_config.yaml

Environment:
    SLACK_WEBHOOK_URL   Slack incoming webhook (optional; enables live alerts)
    LOG_LEVEL           Override log verbosity (default: INFO)
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import yaml


def _configure_logging(log_dir: str = "logs", level: str = "INFO") -> None:
    """Set up rotating file handler and stream handler for the pipeline.

    Creates a timestamped log file in `log_dir` and mirrors output to stdout.
    Log level is read from the LOG_LEVEL environment variable or the `level`
    parameter.

    Args:
        log_dir: Directory to write log files into.
        level: Default log level string (DEBUG, INFO, WARNING, ERROR).
    """
    import logging.handlers

    effective_level = os.environ.get("LOG_LEVEL", level).upper()
    numeric_level = getattr(logging, effective_level, logging.INFO)

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_filename = (
        Path(log_dir) / f"pipeline_{datetime.today().strftime('%Y%m%d')}.log"
    )

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Rotating file handler: 10 MB max, keep 7 days
    file_handler = logging.handlers.RotatingFileHandler(
        log_filename, maxBytes=10 * 1024 * 1024, backupCount=7, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)

    logging.getLogger("urllib3").setLevel(logging.WARNING)


def _parse_args() -> argparse.Namespace:
    """Define and parse command-line arguments.

    Returns:
        Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(
        prog="cost-leakage-detector",
        description=(
            "Operations Cost Leakage Detector — "
            "Automated anomaly detection and reporting pipeline.\n\n"
            "Run --full-run to execute all pipeline stages in sequence."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --full-run
  python main.py --generate-data
  python main.py --detect --report --dashboard
  python main.py --detect --alert
  python main.py --full-run --config custom_config.yaml --log-level DEBUG
        """,
    )

    parser.add_argument(
        "--config",
        default="config.yaml",
        metavar="PATH",
        help="Path to configuration YAML file (default: config.yaml)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level (default: INFO)",
    )

    stages = parser.add_argument_group("Pipeline Stages")
    stages.add_argument(
        "--generate-data",
        action="store_true",
        help="Generate synthetic procurement transaction dataset",
    )
    stages.add_argument(
        "--detect",
        action="store_true",
        help="Run multi-rule anomaly detection engine",
    )
    stages.add_argument(
        "--report",
        action="store_true",
        help="Generate Excel workbook with flagged findings",
    )
    stages.add_argument(
        "--dashboard",
        action="store_true",
        help="Build interactive Plotly HTML dashboard",
    )
    stages.add_argument(
        "--alert",
        action="store_true",
        help="Send Slack alert for Critical-severity findings",
    )
    stages.add_argument(
        "--full-run",
        action="store_true",
        help="Execute all pipeline stages: generate → detect → report → dashboard → alert",
    )

    return parser.parse_args()


def run_pipeline(
    args: argparse.Namespace,
    logger: logging.Logger,
) -> int:
    """Execute the requested pipeline stages and return an exit code.

    Detection and scoring results are shared in memory between stages
    so the pipeline avoids redundant disk reads.

    Args:
        args: Parsed CLI arguments.
        logger: Configured root logger.

    Returns:
        0 on success, 1 on any unhandled error.
    """
    # Lazy imports — keep module load time fast and avoid import errors
    # when a user only wants --generate-data (which needs fewer deps)
    from src.data_generator import generate_dataset
    from src.detector import run_detection
    from src.scorer import score_flagged_transactions, build_executive_summary
    from src.reporter import generate_report
    from src.dashboard import generate_dashboard
    from src.alerter import send_alert

    config_path = args.config
    do_all = args.full_run

    flagged = None
    raw_summary = None
    scored = None
    exec_summary = None

    # -------------------------------------------------------------------------
    # Stage 1: Generate data
    # -------------------------------------------------------------------------
    if do_all or args.generate_data:
        logger.info("=" * 60)
        logger.info("STAGE 1: Data Generation")
        logger.info("=" * 60)
        try:
            df = generate_dataset(config_path)
            logger.info(
                "Data generation complete — %d transactions written", len(df)
            )
        except Exception as exc:
            logger.error("Data generation failed: %s", exc, exc_info=True)
            return 1

    # -------------------------------------------------------------------------
    # Stage 2: Detection (required for report / dashboard / alert)
    # -------------------------------------------------------------------------
    if do_all or args.detect or args.report or args.dashboard or args.alert:
        logger.info("=" * 60)
        logger.info("STAGE 2: Anomaly Detection")
        logger.info("=" * 60)
        try:
            flagged, raw_summary = run_detection(config_path)
            logger.info(
                "Detection complete — %d flags | est. leakage £%.2f",
                len(flagged),
                raw_summary["total_leakage_gbp"],
            )
        except FileNotFoundError as exc:
            logger.error(
                "Transaction data not found. Run --generate-data first.\n%s", exc
            )
            return 1
        except Exception as exc:
            logger.error("Detection failed: %s", exc, exc_info=True)
            return 1

    # -------------------------------------------------------------------------
    # Stage 3: Scoring (required for report / dashboard / alert)
    # -------------------------------------------------------------------------
    if flagged is not None and len(flagged) > 0:
        logger.info("=" * 60)
        logger.info("STAGE 3: Severity Scoring")
        logger.info("=" * 60)
        try:
            scored = score_flagged_transactions(flagged, config_path)
            exec_summary = build_executive_summary(scored, raw_summary, config_path)
            sev = exec_summary["severity_breakdown"]
            logger.info(
                "Scoring complete — Critical: %d | High: %d | Medium: %d | Low: %d",
                sev.get("Critical", 0),
                sev.get("High", 0),
                sev.get("Medium", 0),
                sev.get("Low", 0),
            )
        except Exception as exc:
            logger.error("Scoring failed: %s", exc, exc_info=True)
            return 1
    elif flagged is not None and len(flagged) == 0:
        logger.info("No anomalies detected — all transactions are within thresholds.")

    # -------------------------------------------------------------------------
    # Stage 4: Excel Report
    # -------------------------------------------------------------------------
    if do_all or args.report:
        if scored is None or exec_summary is None:
            logger.warning("No scored data available — skipping report generation.")
        else:
            logger.info("=" * 60)
            logger.info("STAGE 4: Excel Report Generation")
            logger.info("=" * 60)
            try:
                report_path = generate_report(scored, exec_summary, config_path)
                logger.info("Report generated: %s", report_path)
            except Exception as exc:
                logger.error("Report generation failed: %s", exc, exc_info=True)
                return 1

    # -------------------------------------------------------------------------
    # Stage 5: Interactive Dashboard
    # -------------------------------------------------------------------------
    if do_all or args.dashboard:
        if scored is None or exec_summary is None:
            logger.warning("No scored data available — skipping dashboard generation.")
        else:
            logger.info("=" * 60)
            logger.info("STAGE 5: Interactive Dashboard")
            logger.info("=" * 60)
            try:
                dash_path = generate_dashboard(scored, exec_summary, config_path)
                logger.info("Dashboard generated: %s", dash_path)
            except Exception as exc:
                logger.error("Dashboard generation failed: %s", exc, exc_info=True)
                return 1

    # -------------------------------------------------------------------------
    # Stage 6: Slack Alert
    # -------------------------------------------------------------------------
    if do_all or args.alert:
        if scored is None or exec_summary is None:
            logger.warning("No scored data available — skipping Slack alert.")
        else:
            logger.info("=" * 60)
            logger.info("STAGE 6: Slack Alerting")
            logger.info("=" * 60)
            try:
                delivered = send_alert(scored, exec_summary, config_path)
                if delivered:
                    logger.info("Alert stage complete")
                else:
                    logger.warning("Alert delivery failed — check webhook configuration")
            except Exception as exc:
                logger.error("Alert stage failed: %s", exc, exc_info=True)
                # Non-fatal — pipeline continues

    # -------------------------------------------------------------------------
    # Final summary
    # -------------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    if exec_summary:
        logger.info(
            "  %-35s £%.2f",
            "Total estimated leakage:",
            exec_summary["headline_gbp"],
        )
        logger.info(
            "  %-35s %d",
            "Transactions analysed:",
            exec_summary["total_transactions_analysed"],
        )
        logger.info(
            "  %-35s %d",
            "Total flags raised:",
            exec_summary["total_flags"],
        )
        sev = exec_summary["severity_breakdown"]
        logger.info(
            "  Severity breakdown: Critical=%d | High=%d | Medium=%d | Low=%d",
            sev.get("Critical", 0),
            sev.get("High", 0),
            sev.get("Medium", 0),
            sev.get("Low", 0),
        )
    logger.info("=" * 60)
    return 0


def main() -> None:
    """Parse arguments, configure logging, and run the pipeline."""
    args = _parse_args()

    # Load config to get log directory
    try:
        with open(args.config, "r") as fh:
            cfg = yaml.safe_load(fh)
        log_dir = cfg.get("paths", {}).get("log_dir", "logs")
    except Exception:
        log_dir = "logs"

    _configure_logging(log_dir=log_dir, level=args.log_level)
    logger = logging.getLogger(__name__)

    # Default: if no stage flags provided, print help
    no_stage_selected = not any([
        args.full_run, args.generate_data, args.detect,
        args.report, args.dashboard, args.alert,
    ])
    if no_stage_selected:
        import subprocess
        subprocess.run([sys.executable, __file__, "--help"])
        sys.exit(0)

    logger.info(
        "Operations Cost Leakage Detector v1.0 | %s",
        datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
    )
    logger.info("Config: %s | Log level: %s", args.config, args.log_level)

    exit_code = run_pipeline(args, logger)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
