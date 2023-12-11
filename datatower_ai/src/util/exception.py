
class DTException(Exception):
    pass


class DTIllegalDataException(DTException):
    """
    数据格式异常
    在发送的数据格式有误时，SDK 会抛出此异常，用户应当捕获并处理.
    """
    pass


class DTMetaDataException(DTException):
    """
    dt_id, acid, event_name, event_time 等元数据出错异常
    """


class DTNetworkException(DTException):
    """
    网络异常
    在因为网络或者不可预知的问题导致数据无法发送时，SDK会抛出此异常，用户应当捕获并处理.
    """
    pass