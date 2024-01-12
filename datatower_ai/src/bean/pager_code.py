# Common
# 4(Error)00(Common)00(Category)000(Code)
PAGER_CODE_COMMON = 40000000

# Common - Network
# 4(Error)00(Common)01(Network)000(Code)
PAGER_CODE_COMMON_NETWORK_ERROR = 40001000
# Retains 1xx ~ 5xx for standard http status codes
PAGER_CODE_SUB_NETWORK_MAX_RETRIES = 901
PAGER_CODE_SUB_NETWORK_CONNECTION = 902
PAGER_CODE_SUB_NETWORK_OVERSIZE = 903           # The body of a single event is oversize thus cannot be sent.
PAGER_CODE_SUB_NETWORK_OTHER = 999

# Common - Data
# 4(Error)00(Common)02(Data)000(Code)
PAGER_CODE_COMMON_DATA_ERROR = 40002000

# Consumer - AsyncBatch
# 4(Error)01(Consumer)01(AsyncBatchConsumer)000(Code)
PAGER_CODE_CONSUMER_AB_QUEUE_REACH_THRESHOLD = 40101001
PAGER_CODE_CONSUMER_AB_QUEUE_FULL = 40101002
