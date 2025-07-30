from typing import Dict, Any

# This dictionary maps DB column names to their display properties.
ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = {

    # Core location fields
    "latitude": {"en": "Latitude", "ar": "خط العرض", "type": "number", "mandatory": True},
    "longitude": {"en": "Longitude", "ar": "خط الطول", "type": "number", "mandatory": True},

    # License information
    "license_number": {"en": "License Number", "ar": "رقم الرخصة", "type": "string", "mandatory": False},
    "license_issue_date": {"en": "Issue Date", "ar": "تاريخ الإصدار", "type": "date_time", "mandatory": True},
    "license_sadad_end_date": {"en": "Sadad End Date", "ar": "تاريخ انتهاء السداد", "type": "string", "mandatory": False},

    # Administrative divisions
    "amana_id": {"en": "Amana ID", "ar": "معرف الأمانة", "type": "string", "mandatory": False},
    "amana_name": {"en": "Amana Name", "ar": "اسم الأمانة", "type": "string", "mandatory": False},
    "municipality_id": {"en": "Municipality ID", "ar": "معرف البلدية", "type": "string", "mandatory": False},
    "municipality_name": {"en": "Municipality Name", "ar": "اسم البلدية", "type": "string", "mandatory": False},

    # License status
    "license_status_id": {"en": "License Status ID", "ar": "معرف حالة الرخصة", "type": "string", "mandatory": False},
    "license_status_name": {"en": "License Status", "ar": "حالة الرخصة", "type": "string", "mandatory": False},

    # Business details
    "shop_name": {"en": "Shop Name", "ar": "اسم المحل", "type": "string", "mandatory": False},
    "shop_area": {"en": "Shop Area", "ar": "مساحة المحل", "type": "string", "mandatory": False},

    # Economic activity classification
    "isic_number": {"en": "ISIC Number", "ar": "رقم النشاط", "type": "string", "mandatory": False},
    "isic_desc": {"en": "ISIC Description", "ar": "وصف النشاط", "type": "string", "mandatory": False},

    # Activity details
    "d_activities_id": {"en": "Detailed Activity ID", "ar": "معرف النشاط التفصيلي", "type": "string", "mandatory": False},
    "d_activities_name": {"en": "Detailed Activity", "ar": "النشاط التفصيلي", "type": "string", "mandatory": False},
    "m_activities_id": {"en": "Main Activity ID", "ar": "معرف النشاط الرئيسي", "type": "string", "mandatory": False},
    "m_activities_name": {"en": "Main Activity", "ar": "النشاط الرئيسي", "type": "string", "mandatory": False},

}