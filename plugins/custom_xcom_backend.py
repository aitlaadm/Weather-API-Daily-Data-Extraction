from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import json
import uuid

class GCSXComBackend(BaseXCom):
    PREFIX = 'xcom_gcs://'
    BUCKET_NAME = 'your-gcs-bucket-name'  # Replace with your bucket name
    
    @staticmethod
    def serialize_value(value: Any) -> str:
        """Serializes value to a JSON string and uploads to GCS"""
        
        if value is None:
            return None
            
        key = f'xcom/{ uuid.uuid4() }.json'
        json_value = json.dumps(value).encode('utf-8')
        
        gcs_hook = GCSHook()
        gcs_hook.upload(
            bucket_name=GCSXComBackend.BUCKET_NAME,
            object_name=key,
            data=json_value,
            mime_type='application/json'
        )
        
        return f"{GCSXComBackend.PREFIX}{key}"
    
    @staticmethod
    def deserialize_value(value: str) -> Any:
        """Downloads and deserializes value from GCS"""
        
        if value is None:
            return None
            
        if not value.startswith(GCSXComBackend.PREFIX):
            return value
            
        key = value[len(GCSXComBackend.PREFIX):]
        gcs_hook = GCSHook()
        
        data = gcs_hook.download(
            bucket_name=GCSXComBackend.BUCKET_NAME,
            object_name=key
        ).decode('utf-8')
        
        return json.loads(data)