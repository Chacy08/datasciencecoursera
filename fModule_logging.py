# Logging config and set up
import logging
# ========================================================================================================================================
def create_logger(logger_name,log_path,is_error):

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages

    fh = logging.FileHandler(log_path+logger_name+'.log')
    fh.setLevel(logging.DEBUG)
    
     # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)   

    # add the handlers to the logger
    logger.addHandler(fh)
    
    if is_error == True:
        ch = logging.FileHandler(log_path+logger_name+'.log')
        ch.setLevel(logging.ERROR)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger
# ========================================================================================================================================
