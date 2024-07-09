import logging
import os

update_id = '2024_07_01_run1'

def setup_logger(update_id):
    logger = logging.getLogger('workflow_logger')

    # Ensure the logs directory exists
    if not os.path.exists('logs'):
        os.makedirs('logs')
        
    # Generate a log file name based on the update_id and current time
    log_filename = f'logs/workflow_{update_id}.log'

    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(log_filename)
    
    # Create formatters and add them to handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(formatter)
    f_handler.setFormatter(formatter)
    
    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    
    # Set the log level
    logger.setLevel(logging.INFO)
    
    return logger

logger = setup_logger(update_id)
