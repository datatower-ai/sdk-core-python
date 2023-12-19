# encoding:utf-8
import os
import sys
import logging

from datatower_ai import *
from datatower_ai.src.service.http_service import _HttpService
from datatower_ai.src.util._holder import _Holder

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(BASE_DIR)

    dt = DTAnalytics(
        DatabaseCacheConsumer(app_id="app_id_xxxx",
                              token="xxxxxxxxxxxxxxxxxxxxxxx",
                              server_url="https://test.roiqueryxxx.com/sync",
                              thread_keep_alive_ms=1000,
                              exceed_insertion_strategy=ExceedInsertionStrategy.IGNORE),
        debug=True, log_level=logging.DEBUG
    )

    # dt = DTAnalytics(
    #     AsyncBatchConsumer(app_id="app_id_xxxx",
    #                        token="xxxxxxxxxxxxxxxxxxxxxxx",
    #                        server_url="https://test.roiqueryxxx.com/sync",
    #                        close_retry=0),
    #     debug=True, log_level=logging.DEBUG
    # )

    # 查看日志
    # 需要初始化logging
    logging.basicConfig(level=logging.INFO)
    # 且打开日志开关
    dt.enable_log(True)
    _Holder().show_statistics = True

    meta = {
        "#bundle_id": "com.example.example",
    }

    dt.utils.track_timer_start("track_timer_event", dt_id="aaaa")

    properties = {
        "#event_time": 1669022011679,
        # 选填，设置这条event发生的时间戳，13位，精确到毫秒，如果不设置的话，则默认是当前时间，建议加上
        "$ip": "192.168.123.123",
        # 选填，设置用户的IP（非服务器的IP），DT会自动根据该IP解析所属国家，如果不设置的话，则默认无，建议加上
        "#event_syn": "xxx",
        # 选填，事件序列ID，字符串类型，重传的时候可以用于去重，窗口期1小时，建议加上
        "sku": "xxx",  # 自定义内容
        "price": 15,  # 自定义内容
        "order": "订单号xxx"  # 自定义内容
    }
    _HttpService._simulate = 10
    n = 1000
    # 设置事件数据
    for _ in range(n):
        dt.track(dt_id="aaaa", acid='ddd$fff', event_name="purchase", properties=properties, meta=meta)

    batch_size = 100
    for _ in range(max(1, n//batch_size)):
        dt.track_batch(
            *[Event(dt_id="bbbb", event_name="purchase_2", properties=properties, meta=meta) for _ in range(batch_size)]
        )
    # 立即发送数据
    dt.flush()


    user_properties = {
        "name": "datatower",
        "age": 3
    }
    # 设置用户属性
    dt.user_set(dt_id="aaaa", acid='ddd$fff', properties=user_properties, meta=meta)
    # 立即发送数据
    dt.flush()


    dt.utils.track_timer_pause("track_timer_event", dt_id="aaaa")

    dt.ad.report_show(ad_id="adadid", ad_type=0, platform=AdPlatform.LOVINJOYADS, location="x",
                      seq="voapnv2va", entrance="x", mediation=AdMediation.MAX, mediation_id="id_xxx", dt_id="aaaa",
                      meta=meta)

    dt.utils.track_timer_resume("track_timer_event", dt_id="aaaa")
    dt.utils.track_timer_stop("track_timer_event", dt_id="aaaa", meta=meta)
    dt.flush()


    # 关闭并退出dt，程序退出前需要调用此接口，避免缓存内的数据丢失
    dt.close()
