"""
Logging configuration for delta-chronicle.
Call setup_logging() at the start of any script or notebook.
"""
import logging
import sys


def setup_logging(level: str = "INFO"):
    """
    Configure delta-chronicle logging.

    Args:
        level: "DEBUG" | "INFO" | "WARNING" | "ERROR"
               Use DEBUG to see every upstream walk step.
               Use WARNING for quiet production use.

    Usage:
        from delta_chronicle.logging_config import setup_logging
        setup_logging("DEBUG")   # verbose
        setup_logging("WARNING") # quiet
    """
    logging.basicConfig(
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S"
    )
    # Silence noisy third-party loggers
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)