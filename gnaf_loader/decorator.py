"""
Decorator etl
"""

import sys
import datetime

from functools import wraps


# add profiler to function
def profiler(logger, switch='on'):
    """
    Decorator to profile a function
    """
    def decorator(func):
        """
        Wrapper for arguments
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            """
            Function to profile a function
            """
            if switch == 'on':
                logger.info('Function \'%s\' is being called from \'%s\'' \
                            % (func.__name__, sys._getframe(1).f_code.co_name))
                start_time = datetime.datetime.now()
                logger.info('Start running \'%s\' function', func.__name__)
                result = func(*args, **kwargs)
                logger.info('Result: {0}'.format(result))
                logger.info('Finish running \'%s\' function', func.__name__)
                duration = datetime.datetime.now() - start_time
                logger.info('Duration running \'%s\' function is %ss' \
                            % (func.__name__, duration))
            else:
                result = func(*args, **kwargs)
            return result
        return wrapper
    return decorator


# handle exception
def exception(logger):
    """
    Decorator to handle generic exception
    """
    def decorator(func):
        """
        Wrapper for arguments
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            """
            Function to handle exception
            """
            try:
                return func(*args, **kwargs)
            except Exception as errors:
                logger.exception('Error when running %s function' \
                                 % (func.__name__))
                logger.exception(errors)
        return wrapper
    return decorator
