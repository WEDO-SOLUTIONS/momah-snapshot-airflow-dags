import logging
from typing import Any, Dict, List, Optional

from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

class ProHook(HttpHook):
    """
    A custom Airflow Hook to interact with the Urbi Pro Dynamic Asset API.
    """
    def __init__(self, http_conn_id: str, **kwargs):
        # default method is POST; individual methods will override as needed
        super().__init__(method='POST', http_conn_id=http_conn_id, **kwargs)

    def _get_auth_and_brand_headers(self) -> Dict[str, str]:
        """Retrieves token and brand from the Airflow connection and sets JSON content type."""
        conn = self.get_connection(self.http_conn_id)
        token = conn.password
        brand = conn.extra_dejson.get('x_brand_header', '2gis')
        if not token:
            raise AirflowException("API token not found in the password field of the connection.")

        return {
            'Authorization': f'Bearer {token}',
            'X-Brand': brand,
            'Content-Type': 'application/json'
        }

    def create_or_update_asset(
        self,
        payload: Dict[str, Any],
        asset_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Creates a new dynamic asset or updates an existing one.
        """
        headers = self._get_auth_and_brand_headers()
        endpoint = "/dynamic_asset"
        method = 'PUT' if asset_id else 'POST'

        if asset_id:
            payload['id'] = asset_id
            log.info(f"Updating asset with ID: {asset_id}")
        else:
            log.info("Creating new dynamic asset.")

        orig_method = self.method
        self.method = method
        try:
            response = super().run(
                endpoint=endpoint,
                json=payload,
                headers=headers,
                extra_options={'timeout': (3.05, 30)}  # connect, read timeouts
            )
            response.raise_for_status()
            log.info(f"{method} {endpoint} → {response.status_code}")
            log.debug(f"Response body: {response.text}")
            return response.json()
        except Exception as e:
            raise AirflowException(f"{method} {endpoint} failed: {e}")
        finally:
            self.method = orig_method

    def clear_all_asset_data(self, asset_id: str) -> None:
        """
        Deletes all data objects from a dynamic asset using the master token.
        """
        headers = self._get_auth_and_brand_headers()
        endpoint = f"/dynamic_asset/{asset_id}/data/all"
        log.warning(f"Deleting ALL DATA from asset: {asset_id}")

        orig_method = self.method
        self.method = 'DELETE'
        try:
            response = super().run(
                endpoint=endpoint,
                headers=headers,
                extra_options={'timeout': (3.05, 30)}
            )
            response.raise_for_status()
            log.info(f"DELETE {endpoint} → {response.status_code}")
            log.debug(f"Response body: {response.text}")
        except Exception as e:
            raise AirflowException(f"DELETE {endpoint} failed: {e}")
        finally:
            self.method = orig_method

    def push_data(
        self,
        asset_id: str,
        access_token: str,
        features: List[Dict],
        is_delete: bool = False
    ) -> None:
        """
        Pushes a chunk of data (upserts or deletes) to the dynamic asset.
        """
        headers = self._get_auth_and_brand_headers()
        # Override Authorization for push-token
        headers['Authorization'] = f'Bearer {access_token}'
        endpoint = f"/dynamic_asset/{asset_id}/data"

        orig_method = self.method
        if is_delete:
            self.method = 'DELETE'
            payload = None
        else:
            self.method = 'PUT'
            payload = {'type': 'FeatureCollection', 'features': features}

        try:
            response = super().run(
                endpoint=endpoint,
                json=payload,
                headers=headers,
                extra_options={'timeout': (3.05, 30)}
            )
            response.raise_for_status()
            log.info(f"{self.method} {endpoint} → {response.status_code}")
            log.debug(f"Response body: {response.text}")
        except Exception as e:
            raise AirflowException(f"{self.method} {endpoint} failed: {e}")
        finally:
            self.method = orig_method
            if is_delete:
                log.info(f"Successfully deleted features from asset '{asset_id}'.")
            else:
                log.info(f"Successfully pushed {len(features)} features to asset '{asset_id}'.")