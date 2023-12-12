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

    @staticmethod
    def set(enable: bool, log_level=logging.INFO):
        Logger.is_print = enable
        Logger.logger.setLevel(log_level)

    @staticmethod
    def log(msg=None, level=logging.INFO):
        if msg is not None and Logger.is_print:
            if level <= logging.DEBUG:
                Logger.logger.debug(msg)
            elif level <= logging.INFO:
                Logger.logger.info(msg)
            elif level <= logging.WARNING:
                Logger.logger.warning(msg)
            else:
                Logger.logger.error(msg)

