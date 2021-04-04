"""
Docstring goes here
"""
import json
import uuid

from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.redis.hooks.redis import RedisHook


class RedisXComBackend(BaseXCom):
    """
    Docstring goes here
    """

    @staticmethod
    def serialize_value(value: Any):
        """
        Docstring goes here
        """
        hook = RedisHook()
        hook.get_conn()
        redis = hook.redis
        key = f"data_{uuid.uuid4()}"
        xcom = {key: json.dumps(value)}
        redis.mset(xcom)
        return BaseXCom.serialize_value(key)

    @staticmethod
    def deserialize_value(result) -> Any:
        """
        Docstring goes here
        """
        result = BaseXCom.deserialize_value(result)
        if isinstance(result, str):
            hook = RedisHook()
            hook.get_conn()
            redis = hook.redis
            xcom = redis.mget(result)
            result = eval(xcom[0])
        return result
