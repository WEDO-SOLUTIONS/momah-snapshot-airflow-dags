# include/com_license_info_dag/helpers.py

import os
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

from dotenv import load_dotenv
from airflow.models import Variable
from airflow.providers.oracle.hooks.oracle import OracleHook
from dateutil.parser import parse as date_parse

from include.com_license_info_dag.constants import (
    VIEW_VAR, CONFIG_VAR, KNOWN_IDS_VAR,
    DB_CONN_ID, DB_FETCH_SIZE
)
from include.com_license_info_dag.attribute_mapper import ATTRIBUTE_MAPPER

log = logging.getLogger(__name__)

def bootstrap_variables() -> None:
    env_path = Path(__file__).parent / ".env"
    load_dotenv(dotenv_path=env_path)

    view_val = os.getenv(VIEW_VAR.upper())
    if view_val:
        Variable.set(VIEW_VAR, view_val)
        log.info(f"Set {VIEW_VAR} from .env")

    cfg_val = os.getenv(CONFIG_VAR.upper())
    if cfg_val:
        try:
            cfg = json.loads(cfg_val)
            Variable.set(CONFIG_VAR, cfg, serialize_json=True)
            log.info(f"Set {CONFIG_VAR} from .env")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON for {CONFIG_VAR}: {e}")


def build_schema_from_db() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    oracle = OracleHook(oracle_conn_id=DB_CONN_ID)
    db_view = Variable.get(VIEW_VAR)
    asset_cfg = Variable.get(CONFIG_VAR, deserialize_json=True)
    primary_col = asset_cfg.get("primary_name_column", "").lower()

    MIN_DATE_MS = 315522000000
    MAX_DATE_MS = 2524597200000

    with oracle.get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {db_view} WHERE ROWNUM = 1")
        cols = [d[0].lower() for d in cur.description]

    attributes: List[Dict[str, Any]] = []
    filters: List[Dict[str, Any]] = []
    mapper = ATTRIBUTE_MAPPER

    for col in cols:
        if col not in mapper:
            log.warning(f"Skipping unmapped column {col}")
            continue
        mi = mapper[col]

        # Attribute entry
        attributes.append({
            "id": col,
            "type": mi["type"],
            "caption": mi["en"],
            "localizations": {"caption": {"en": mi["en"], "ar": mi["ar"]}},
        })
        if col == primary_col:
            attributes.append({
                "id": f"{col}_ns",
                "type": "name",
                "caption": mi["en"],
                "localizations": {"caption": {"en": mi["en"], "ar": mi["ar"]}},
            })

        # Skip filters for geometry columns
        if col in ("latitude", "longitude"):
            continue

        fobj: Dict[str, Any] = {"attribute_id": col}
        try:
            if mi["type"] == "string":
                cnt = oracle.get_first(
                    f'SELECT COUNT(DISTINCT "{col}") FROM {db_view}'
                )[0] or 0
                if 0 < cnt <= 50:
                    fobj["control_type"] = "check_box_list"
                    rows = oracle.get_records(
                        f'SELECT DISTINCT "{col}" FROM {db_view} WHERE "{col}" IS NOT NULL'
                    )
                    fobj["items"] = [{"value": r[0], "caption": r[0]} for r in rows]
                else:
                    fobj["control_type"] = "text_box"

            elif mi["type"] == "date_time":
                fobj.update({"control_type": "date_time_range", "min": MIN_DATE_MS, "max": MAX_DATE_MS})

            elif mi["type"] == "number":
                mn, mx = oracle.get_first(
                    f'SELECT MIN("{col}"), MAX("{col}") FROM {db_view}'
                )
                if mn is not None and mx is not None:
                    fobj.update({"control_type": "range", "min": float(mn), "max": float(mx)})

        except Exception:
            fobj["control_type"] = "text_box"

        filters.append(fobj)

    return attributes, filters


def build_payload(
    asset_cfg: Dict[str, Any], attributes: List[Dict[str, Any]], filters: List[Dict[str, Any]]
) -> Dict[str, Any]:
    return {
        "name": asset_cfg["name"],
        "description": asset_cfg["description"],
        "geometry_dimension": asset_cfg["geometry_dimension"],
        "localizations": {
            "name": {"en": asset_cfg["name"], "ar": asset_cfg["name_ar"]},
            "description": {"en": asset_cfg["description"], "ar": asset_cfg["description_ar"]},
        },
        "attribute_groups": [{
            "name": asset_cfg["attribute_group_name"],
            "localizations": {"name": {"en": asset_cfg["attribute_group_name"], "ar": asset_cfg["attribute_group_name_ar"]}},
            "attributes": attributes,
        }],
        "filters": filters,
    }


def validate_and_convert_row(
    row: Dict[str, Any], primary_name_column: str
) -> Optional[Dict[str, Any]]:
    ci = {k.lower(): v for k, v in row.items()}
    if not ci.get("id") or not ci.get("last_modified_date"):
        log.warning(f"Skipping record missing id/last_modified_date: {ci.get('id')}")
        return None

    props: Dict[str, Any] = {}
    for db_col, mi in ATTRIBUTE_MAPPER.items():
        key = db_col.lower()
        val = ci.get(key)
        if mi.get("mandatory") and val is None:
            log.warning(f"Skipping {ci.get('id')} missing mandatory {db_col}")
            return None
        if val is None:
            props[db_col] = None
        else:
            if mi["type"] == "date_time":
                try:
                    dt = date_parse(val) if isinstance(val, str) else val
                    props[db_col] = dt.isoformat()
                except Exception:
                    log.warning(f"Invalid date in {db_col} for {ci.get('id')}")
                    props[db_col] = None
            else:
                props[db_col] = val

    if primary_name_column:
        pn = primary_name_column.lower()
        if ci.get(pn):
            props[f"{pn}_ns"] = str(ci[pn])

    asset_cfg = Variable.get(CONFIG_VAR, deserialize_json=True)
    geom_dim = asset_cfg.get("geometry_dimension", "point").lower()
    try:
        lon = float(props.get("longitude"))
        lat = float(props.get("latitude"))
    except Exception:
        log.warning(f"Skipping {ci.get('id')} invalid coords")
        return None
    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        log.warning(f"Skipping {ci.get('id')} out-of-bounds coords {lon},{lat}")
        return None

    if geom_dim == "point":
        geometry = {"type": "Point", "coordinates": [lon, lat]}
    elif geom_dim == "line":
        coords = ci.get("coordinates")
        if not coords:
            log.warning(f"No 'coordinates' for line in record {ci.get('id')}")
            return None
        geometry = {"type": "LineString", "coordinates": coords}
    elif geom_dim == "polygon":
        coords = ci.get("coordinates")
        if not coords:
            log.warning(f"No 'coordinates' for polygon in record {ci.get('id')}")
            return None
        geometry = {"type": "Polygon", "coordinates": coords}
    else:
        geometry = {"type": "Point", "coordinates": [lon, lat]}

    return {"type": "Feature", "id": str(ci['id']), "geometry": geometry, "properties": props}