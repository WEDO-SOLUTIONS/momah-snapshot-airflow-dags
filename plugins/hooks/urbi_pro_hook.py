import logging
from typing import Any, Dict, List, Optional

from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

class UrbiProHook(HttpHook):
    """
    A custom Airflow Hook to interact with the Urbi Pro Dynamic Asset API.
    """
    def __init__(self, http_conn_id: str, **kwargs):
        super().__init__(method='POST', http_conn_id=http_conn_id, **kwargs)

    def _get_auth_and_brand_headers(self) -> Dict[str, str]:
        """Retrieves token and brand from the Airflow connection."""
        token = self.get_connection(self.http_conn_id).password
        brand = self.get_connection(self.http_conn_id).extra_dejson.get('x_brand_header', '2gis')
        if not token:
            raise AirflowException("API token not found in the password field of the connection.")
        return {'Authorization': f'Bearer {token}', 'X-Brand': brand}

    def create_or_update_asset(self, payload: Dict[str, Any], asset_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Creates a new dynamic asset or updates an existing one.

        :param payload: The full JSON payload for the asset schema.
        :param asset_id: The ID of the asset to update. If None, a new asset is created.
        """
        headers = self._get_auth_and_brand_headers()
        endpoint = "/dynamic_asset"
        method = "POST"

        if asset_id:
            payload["id"] = asset_id
            method = "PUT"
            log.info(f"Updating asset with ID: {asset_id}")
        else:
            log.info("Creating new dynamic asset.")
        
        self.method = method
        response = self.run(endpoint=endpoint, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()

    def clear_all_asset_data(self, asset_id: str, access_token: str) -> None:
        """
        Deletes all data objects from a dynamic asset.

        :param asset_id: The ID of the asset to clear.
        :param access_token: The specific push data access token for the asset.
        """
        headers = self._get_auth_and_brand_headers()
        headers['Authorization'] = f'Bearer {access_token}'  # Use the push token for data operations
        endpoint = f"/dynamic_asset/{asset_id}/data/all"
        
        log.warning(f"Sending request to delete ALL DATA from asset: {asset_id}")
        self.method = 'DELETE'
        response = self.run(endpoint=endpoint, headers=headers)
        response.raise_for_status()
        log.info(f"Successfully cleared all data from asset '{asset_id}'.")

    def push_data(self, asset_id: str, access_token: str, features: List[Dict], is_delete: bool = False) -> None:
        """
        Pushes a chunk of data (upserts or deletes) to the dynamic asset.

        :param asset_id: The ID of the asset.
        :param access_token: The push data access token.
        :param features: A list of GeoJSON features for upserts, or a list of IDs for deletes.
        :param is_delete: Set to True if this is a delete operation.
        """
        headers = self._get_auth_and_brand_headers()
        headers['Authorization'] = f'Bearer {access_token}'
        endpoint = f"/dynamic_asset/{asset_id}/data"

        payload = features
        if not is_delete:
            self.method = 'PUT'
            payload = {"type": "FeatureCollection", "features": features}
        else:
            self.method = 'DELETE'

        response = self.run(endpoint=endpoint, json=payload, headers=headers)
        response.raise_for_status()