
'''
def setup_logger():
    """
    Setup LOGGER object to handle logging
    """

    logger = logging.getLogger('distribute_files')
    stream_handler = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger
'''
