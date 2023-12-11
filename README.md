# DataTower.ai

This is the official Python SDK for DataTower.ai.

## Easy Installation

You can get DataTower.ai SDK using pip.

```python
pip install datatower-ai
```

Once the SDK is successfully installed, use the SDK likes:

```python

from datatower_ai.sdk import DTAnalytics, DebugConsumer

dt = DTAnalytics(DebugConsumer(app_id="app_id_xxxx", token="xxxxxxxxxxxxxxxxxxxxxxx", server_url="https://xxxx"))

properties = {"abc": 123, "bcd": "xxx"}

dt.track(dt_id="aaaa", acid='bbbb', event_name="ad_click", properties=properties)

dt.flush()

dt.close()
```

## More details
See [here](https://docs.datatower.ai/python-sdk-integration)
