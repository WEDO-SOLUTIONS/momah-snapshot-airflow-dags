# /snapshot_pro_etl/mappers/vpc_amn_mun.py
from typing import Dict, Any

# Maps database columns to their display properties for the vpc_amn_mun schema.
ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = {
    "latitude": {"en": "Latitude", "ar": "خط العرض", "type": "number", "mandatory": True},
    "longitude": {"en": "Longitude", "ar": "خط الطول", "type": "number", "mandatory": True},
    "date": {"en": "Record Date", "ar": "تاريخ التسجيل", "type": "date_time", "mandatory": True},
    "amana_id": {"en": "Amana ID", "ar": "معرف الأمانة", "type": "string", "mandatory": False},
    "amana_name_ar": {"en": "Amana Name (AR)", "ar": "اسم الأمانة", "type": "string", "mandatory": False},
    "amana_name_en": {"en": "Amana Name (EN)", "ar": "اسم الأمانة (انجليزي)", "type": "string", "mandatory": False},
    "municipality_id": {"en": "Municipality ID", "ar": "معرف البلدية", "type": "string", "mandatory": False},
    "municipality_name_ar": {"en": "Municipality Name (AR)", "ar": "اسم البلدية", "type": "string", "mandatory": False},
    "municipality_name_en": {"en": "Municipality Name (EN)", "ar": "اسم البلدية (انجليزي)", "type": "string", "mandatory": False},
    "priority_level": {"en": "Priority Level", "ar": "مستوى الأولوية", "type": "string", "mandatory": False},
    "source_table": {"en": "Source Table", "ar": "الجدول المصدر", "type": "string", "mandatory": False},
    "vp_index": {"en": "VPI", "ar": "مؤشر التشوه البصري", "type": "number", "mandatory": False},
    "coverage": {"en": "Coverage", "ar": "التغطية", "type": "number", "mandatory": False},
    "ttr": {"en": "TTR", "ar": "متوسط وقت المعالجة", "type": "number", "mandatory": False},
    "repeat": {"en": "Repeat Count", "ar": "عدد التكرار", "type": "number", "mandatory": False},
    "coverage_amana": {"en": "Coverage Amana", "ar": "تغطية الأمانة", "type": "number", "mandatory": False},
    "vpi_amana": {"en": "VPI Amana", "ar": "مؤشر التشوه البصري للأمانة", "type": "number", "mandatory": False},
    "coverage_target_amana": {"en": "Coverage Target Amana", "ar": "المستهدف لتغطية الأمانة", "type": "number", "mandatory": False},
    "vpi_target_amana": {"en": "VPI Target Amana", "ar": "المستهدف لمؤشر التشوه البصري للأمانة", "type": "number", "mandatory": False},
    "street_inspected_area": {"en": "Street Inspected Area", "ar": "مساحة الشارع المفحوصة", "type": "string", "mandatory": False},
    "street_area": {"en": "Street Area", "ar": "مساحة الشارع", "type": "string", "mandatory": False},
    "units_calculated": {"en": "Units Calculated", "ar": "الوحدات المحسوبة", "type": "number", "mandatory": False},
}