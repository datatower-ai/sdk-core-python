import logging

from datatower_ai.__version__ import __version__


class Logger:
    logger = logging.getLogger("DT-Logger")
    is_print = False
    logger.setLevel(logging.INFO)

    __prefix = '[DataTower.ai-Python SDK v%s]' % __version__
    __formatter = logging.Formatter("%(levelname)s:%(asctime)s - " + __prefix + " - %(message)s")
    __handler = logging.StreamHandler()
    __handler.setFormatter(__formatter)
    logger.propagate = False
    logger.addHandler(__handler)

    __allowed_level = logging.INFO

    @staticmethod
    def set(enable: bool, log_level=logging.INFO):
        Logger.is_print = enable
        Logger.logger.setLevel(log_level)
        Logger.__allowed_level = log_level

    @staticmethod
    def log(msg=None, level=logging.INFO):
        if msg is not None and Logger.is_print and level >= Logger.__allowed_level:
            if level <= logging.DEBUG:
                #Logger.logger.debug(msg)
                print("DEBUG: {} - {}".format(Logger.__prefix, msg))
            elif level <= logging.INFO:
                #Logger.logger.info(msg)
                print("INFO: {} - {}".format(Logger.__prefix, msg))
            elif level <= logging.WARNING:
                #Logger.logger.warning(msg)
                print("WARNING: {} - {}".format(Logger.__prefix, msg))
            else:
                #Logger.logger.error(msg)
                print("ERROR: {} - {}".format(Logger.__prefix, msg))

    @staticmethod
    def debug(msg):
        Logger.log(msg, logging.DEBUG)

    @staticmethod
    def info(msg):
        Logger.log(msg, logging.INFO)

    @staticmethod
    def warning(msg):
        Logger.log(msg, logging.WARNING)

    @staticmethod
    def error(msg):
        Logger.log(msg, logging.ERROR)

    @staticmethod
    def exception(msg=""):
        Logger.logger.exception(msg)
