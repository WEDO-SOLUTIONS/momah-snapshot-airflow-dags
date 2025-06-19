import logging
import json
import requests
import concurrent.futures
from dateutil.parser import parse as date_parse
from typing import Dict, Any, List, Optional, Tuple
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from .common import config as Cfg

logger = logging.getLogger(__name__)

class SyncManager:
    """Manages the data validation, transformation, and API push logic."""

    def __init__(self, schema_name: str, api_base_url: str, access_token: str, x_brand_header: str, dag_run_id: str):
        self.schema_name = schema_name
        self.api_base_url = api_base_url
        self.access_token = access_token
        self.x_brand_header = x_brand_header
        self.dag_run_id = dag_run_id
        self.stats = self._reset_stats()
        self.skipped_records = []
        self.failed_chunks = []

    def _reset_stats(self):
        """Returns a clean dictionary for tracking detailed statistics."""
        return {"validated": 0, "skipped": 0, "added": 0, "updated": 0, "pushed": 0, "push_failed": 0, "deleted": 0, "delete_failed": 0}

    def _send_api_request_with_retry(self, method: str, url: str, headers: Dict, payload: Optional[Dict | List] = None, retries=3, timeout=60) -> bool:
        """Sends an API request with exponential backoff retry logic."""
        for attempt in range(retries):
            try:
                response = requests.request(method, url, headers=headers, json=payload, timeout=timeout)
                response.raise_for_status()
                return True
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{retries} failed for {method} {url}. Error: {e}")
                if e.response is not None and 400 <= e.response.status_code < 500:
                    logger.error(f"Client error {e.response.status_code}, no retries. Response: {e.response.text}")
                    return False
        return False

    def process_upsert_chunk(self, records_chunk: List[Dict], last_known_ids: set, attribute_mapper: dict, asset_id: str):
        """Processes a single chunk of records: validate, transform, and push."""
        api_config = Cfg.get_config(self.schema_name)
        valid_features = []

        for row in records_chunk:
            feature, reason = self._validate_and_convert_row(row, api_config, attribute_mapper)
            if feature:
                self.stats['validated'] += 1
                valid_features.append(feature)
                # Classify as Added or Updated for detailed statistics
                if str(row['id']) in last_known_ids:
                    self.stats['updated'] += 1
                else:
                    self.stats['added'] += 1
            else:
                self.stats['skipped'] += 1
                self.skipped_records.append({"reason": reason, "data": row})

        if valid_features:
            self.push_upserts_to_api(asset_id, valid_features)

    def _validate_and_convert_row(self, row: Dict[str, Any], api_config: Dict, attribute_mapper: dict) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Validates a single database row and converts it to a GeoJSON Feature."""
        if not row.get('id') or not row.get('last_modified_date'):
            return None, "Missing core 'id' or 'last_modified_date'"

        properties = {}
        for db_col, map_info in attribute_mapper.items():
            value = row.get(db_col)
            if map_info.get("mandatory") and value is None:
                return None, f"Missing mandatory attribute: '{db_col}'"
            properties[db_col] = value

        if not properties.get('latitude') or not properties.get('longitude'):
            return None, "Missing mandatory 'latitude' or 'longitude'"

        properties[f"{api_config['primary_name_column']}_ns"] = str(row.get(api_config['primary_name_column']))

        return {
            "type": "Feature", "id": str(row['id']),
            "geometry": {"type": "Point", "coordinates": [float(properties['longitude']), float(properties['latitude'])]},
            "properties": properties
        }, None

    def push_upserts_to_api(self, asset_id: str, features: List[Dict], max_workers=10, chunk_size=100):
        """Pushes GeoJSON features to the Urbi Pro PUT /data endpoint in parallel."""
        api_url = f"{self.api_base_url}/dynamic_asset/{asset_id}/data"
        headers = {'Authorization': f'Bearer {self.access_token}', 'X-Brand': self.x_brand_header, 'Content-Type': 'application/json'}
        chunks = [features[i:i + chunk_size] for i in range(0, len(features), chunk_size)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_chunk = {
                executor.submit(self._send_api_request_with_retry, "PUT", api_url, headers, {"type": "FeatureCollection", "features": chunk}): chunk
                for chunk in chunks
            }
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                if future.result():
                    self.stats['pushed'] += len(chunk)
                else:
                    self.stats['push_failed'] += len(chunk)
                    self.failed_chunks.append(chunk)

    def push_deletes_to_api(self, asset_id: str, ids_to_delete: List[str], max_workers=10, chunk_size=100):
        """Pushes deletions to the Urbi Pro DELETE /data endpoint in parallel."""
        if not ids_to_delete:
            return
        self.stats['deleted'] = len(ids_to_delete)
        api_url = f"{self.api_base_url}/dynamic_asset/{asset_id}/data"
        headers = {'Authorization': f'Bearer {self.access_token}', 'X-Brand': self.x_brand_header}
        chunks = [ids_to_delete[i:i + chunk_size] for i in range(0, len(ids_to_delete), chunk_size)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_chunk = {executor.submit(self._send_api_request_with_retry, "DELETE", api_url, headers, chunk): chunk for chunk in chunks}
            for future in concurrent.futures.as_completed(future_to_chunk):
                if not future.result():
                    self.stats['delete_failed'] += len(future_to_chunk[future])

    def upload_reports_to_s3(self):
        """Writes statistics, skipped records, and failed chunks directly to S3."""
        api_config = Cfg.get_config(self.schema_name)
        s3_hook = S3Hook(aws_conn_id='oci_s3_conn')
        bucket = api_config['s3_reports_bucket']
        base_key = f"{api_config['s3_reports_prefix']}/{self.dag_run_id}"
        logger.info(f"Uploading reports to S3 at s3://{bucket}/{base_key}/")
        stats_json = json.dumps(self.stats, indent=2)
        s3_hook.load_string(stats_json, key=f"{base_key}/stats_summary.json", bucket_name=bucket, replace=True)
        if self.skipped_records:
            skipped_json = json.dumps(self.skipped_records, indent=2, ensure_ascii=False, default=str)
            s3_hook.load_string(skipped_json, key=f"{base_key}/skipped_records.json", bucket_name=bucket, replace=True)
        if self.failed_chunks:
            failed_json = json.dumps(self.failed_chunks, indent=2, ensure_ascii=False, default=str)
            s3_hook.load_string(failed_json, key=f"{base_key}/failed_api_chunks.json", bucket_name=bucket, replace=True)
        logger.info("All reports successfully uploaded to S3.")