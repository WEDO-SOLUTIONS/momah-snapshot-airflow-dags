from typing import Dict, Any

# This dictionary maps DB column names to their display properties.

ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = {

    # Core location fields (fixed position - now present in CSV)
    "latitude": {"en": "Latitude", "ar": "خط العرض", "type": "number", "mandatory": False},  # Present but empty in data
    "longitude": {"en": "Longitude", "ar": "خط الطول", "type": "number", "mandatory": False},  # Present but empty in data
    "date": {"en": "Date", "ar": "التاريخ", "type": "date_time", "mandatory": True},

    # Followed by exact CSV column order (excluding the 3 fields above)
    "company_name_ar": {"en": "Company Name (AR)", "ar": "اسم الشركة", "type": "string", "mandatory": False},
    "company_name_en": {"en": "Company Name (EN)", "ar": "اسم الشركة (إنجليزي)", "type": "string", "mandatory": False},
    "amana_id": {"en": "Amana ID", "ar": "معرف الأمانة", "type": "string", "mandatory": False},
    "amana_name_ar": {"en": "Amana Name (AR)", "ar": "اسم الأمانة", "type": "string", "mandatory": False},
    "municipality_id": {"en": "Municipality ID", "ar": "معرف البلدية", "type": "string", "mandatory": False},
    "municipality_name_ar": {"en": "Municipality Name (AR)", "ar": "اسم البلدية", "type": "string", "mandatory": False},
    "street_area_km2": {"en": "Street Area (km²)", "ar": "مساحة الشوارع (كم²)", "type": "number", "mandatory": False},
    "street_inspected_area_km2": {"en": "Inspected Area (km²)", "ar": "المساحة المفتشة (كم²)", "type": "number", "mandatory": False},
    "coverage_percentage": {"en": "Coverage Percentage", "ar": "نسبة التغطية", "type": "number", "mandatory": False},

}