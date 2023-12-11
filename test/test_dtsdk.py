"""
@file   : __init__.py.py
@Date   : 2023/3/15
@author : vivi
"""
import logging
import pytest
import unittest
from datatower_ai import DTAnalytics, DebugConsumer, DTNetworkException, DTIllegalDataException, DTMetaDataException

APP_ID = "app_id_xxxx"
TOKEN = "xxxxxxxxxxxxxxxxxxxxxxx"
SERVER_URL = "https://test.roiquery.com/sync"

logging.basicConfig(level=logging.DEBUG)


class Fixtures(object):
    def __init__(self):
        self.dt = DTAnalytics(
            DebugConsumer(
                app_id=APP_ID,
                token=TOKEN,
                server_url=SERVER_URL
            )
        )


class DataTest(unittest.TestCase):
    dt = Fixtures().dt
    dt_id = "aaa"
    acid = "bbbb"
    event_name = "unittest"

    def test_meta_data(self):
        data = {"name": "test"}
        self.dt.track(dt_id=self.dt_id, event_name=self.event_name, properties=data)
        self.dt.track(acid=self.acid, event_name=self.event_name, properties=data)

        with pytest.raises(DTMetaDataException):
            self.dt.track(dt_id=self.dt_id, properties=data)
        with pytest.raises(DTMetaDataException):
            self.dt.track(acid=self.dt_id, properties=data)
        with pytest.raises(DTMetaDataException):
            self.dt.track(event_name=self.event_name, properties=data)

    def test_not_support_data_type(self):
        def func_type():
            pass

        data = {
            "key4": func_type,
            "key5": "helloworld",
            "key6": 123456,
            "key7": 0.123456,
            "key8": 9e-15,
            "key9": [1, 0.1, "helloworld", func_type],
        }

        with pytest.raises(DTIllegalDataException):
            self.dt.track(dt_id=self.dt_id, event_name=self.event_name, properties=data)
            self.dt.flush()
            self.dt.close()

    def test_nan_data(self):
        import numpy
        nan = numpy.nan
        data = {
            "key1": nan,
            "key5": "helloworld",
            "key6": 123456,
            "key7": 0.123456,
            "key8": 9e-15,
            "key9": [1, 0.1, "helloworld", nan],
        }
        with pytest.raises(DTIllegalDataException):
            self.dt.track(dt_id=self.dt_id, event_name=self.event_name, properties=data)
            self.dt.flush()
            self.dt.close()

    def test_inf_data(self):
        import numpy
        inf = numpy.inf
        data = {
            "key2": inf,
            "key3": -inf,
            "key4": [inf],
            "key5": "helloworld",
            "key6": 123456,
            "key7": 0.123456,
            "key8": 9e-15,
            "key9": [1, 0.1, "helloworld"],

        }
        with pytest.raises(DTIllegalDataException):
            self.dt.track(dt_id=self.dt_id, event_name=self.event_name, properties=data)
            self.dt.flush()
            self.dt.close()

    def test_datetime_data(self):
        import datetime
        data = {
            "key5": "helloworld",
            "key6": 123456,
            "key7": 0.123456,
            "key8": 9e-15,
            "key9": [1, 0.1, "helloworld", datetime.datetime.utcnow()],
            "key10": datetime.datetime.utcnow()
        }
        self.dt.track(dt_id=self.dt_id, event_name=self.event_name, properties=data)
        self.dt.flush()
        self.dt.close()
