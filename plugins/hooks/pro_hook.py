import logging
import math
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

    def _sanitize_payload(self, obj: Any) -> Any:
        """
        Recursively walk the payload and replace any NaN or infinite floats with None.
        """
        if isinstance(obj, dict):
            return {k: self._sanitize_payload(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._sanitize_payload(v) for v in obj]
        elif isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                log.debug("Sanitizing bad float value (%s) → null", obj)
                return None
            return obj
        return obj

    def create_or_update_asset(
        self, payload: Dict[str, Any], asset_id: Optional[str] = None
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

        safe_payload = self._sanitize_payload(payload)
        orig_method = self.method
        self.method = method
        try:
            response = super().run(
                endpoint=endpoint,
                json=safe_payload,
                headers=headers,
                extra_options={'timeout': (3.05, 30)}
            )
            response.raise_for_status()
            log.info(f"{method} {endpoint} → {response.status_code}")
            log.debug("Response body: %s", response.text)
            return response.json()
        except Exception as e:
            log.error("Failed %s %s: %s", method, endpoint, e, exc_info=True)
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
            log.debug("Response body: %s", response.text)
        except Exception as e:
            log.error("Failed DELETE %s: %s", endpoint, e, exc_info=True)
            raise AirflowException(f"DELETE {endpoint} failed: {e}")
        finally:
            self.method = orig_method

    def push_data(
        self,
        asset_id: str,
        access_token: str,
        features: List[Any],
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
            if not features:
                log.info("No IDs to delete; skipping DELETE call.")
                return
            self.method = 'DELETE'
            payload = {'ids': features}
        else:
            self.method = 'PUT'
            # dedupe IDs to avoid server-side duplicate-key errors
            seen = {f['id']: f for f in features}
            payload = {'type': 'FeatureCollection', 'features': list(seen.values())}

        safe_payload = self._sanitize_payload(payload)

        try:
            response = super().run(
                endpoint=endpoint,
                json=safe_payload,
                headers=headers,
                extra_options={'timeout': (3.05, 30)}
            )
            response.raise_for_status()
            log.info(f"{self.method} {endpoint} → {response.status_code}")
            log.debug("Response body: %s", response.text)
        except AirflowException as e:
            err = str(e)
            # swallow 500 duplicate-key errors
            if err.startswith('500') or 'Internal Server Error' in err:
                log.warning(
                    "Ignoring 500 from Pro API (likely duplicate-key conflict) for asset %s: %s",
                    asset_id, err
                )
                return
            log.error("Failed %s %s: %s", self.method, endpoint, e, exc_info=True)
            raise
        finally:
            self.method = orig_method
            if is_delete:
                log.info(f"Successfully deleted features from asset '{asset_id}'.")
            else:
                log.info(f"Successfully pushed {len(features)} features to asset '{asset_id}'.")
