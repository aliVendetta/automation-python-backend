import logging
import sys

def setup_logging(level=logging.INFO):
    """
    Configure root logging to output to stdout with a standardized format.
    This ensures that logs are visible in the terminal/console, which is 
    required for Azure App Service log streaming.
    """
    # Create a formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove any existing handlers
    while root_logger.handlers:
        root_logger.removeHandler(root_logger.handlers[0])

    # Add stdout handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    # Prevent duplicate logs from azure or libraries that might have set up handlers
    root_logger.propagate = False
    
    logging.info("Logging initialized to stdout.")
