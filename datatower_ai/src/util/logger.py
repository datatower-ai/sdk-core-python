import logging

from datatower_ai.__init__ import __version__


class Logger:
    logger = logging.getLogger("DT-Logger")
    is_print = False
    logger.setLevel(logging.INFO)

    @staticmethod
    def set(enable: bool, log_level=logging.INFO):
        Logger.is_print = enable
        Logger.logger.setLevel(log_level)

    @staticmethod
    def log(msg=None, level=logging.INFO):
        if msg is not None and Logger.is_print:
            prefix = '[DataTower.ai-Python SDK v%s]' % __version__
            if level <= logging.DEBUG:
                Logger.logger.debug("{}-{}".format(prefix, msg))
            elif level <= logging.INFO:
                Logger.logger.info("{}-{}".format(prefix, msg))
            elif level <= logging.WARNING:
                Logger.logger.warning("{}-{}".format(prefix, msg))
            else:
                Logger.logger.error("{}-{}".format(prefix, msg))

