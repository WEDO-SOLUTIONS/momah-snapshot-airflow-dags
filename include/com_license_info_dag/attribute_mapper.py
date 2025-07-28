from typing import Dict, Any

# This dictionary maps DB column names to their display properties.

ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = {

    # Core location fields (fixed position)
    "LATITUDE": {"en": "Latitude", "ar": "خط العرض", "type": "number", "mandatory": True},
    "LONGITUDE": {"en": "Longitude", "ar": "خط الطول", "type": "number", "mandatory": True},
    "LICENSE_ISSUE_DATE": {"en": "Issue Date", "ar": "تاريخ الإصدار", "type": "string", "mandatory": True},

    # Followed by exact CSV column order (excluding latitude/longitude)
    "LICENSE_NUMBER": {"en": "License Number", "ar": "رقم الرخصة", "type": "string", "mandatory": False},
    "AMANA_ID": {"en": "Amana ID", "ar": "معرف الأمانة", "type": "string", "mandatory": False},
    "AMANA_NAME": {"en": "Amana Name", "ar": "اسم الأمانة", "type": "string", "mandatory": False},
    "MUNICIPALITY_ID": {"en": "Municipality ID", "ar": "معرف البلدية", "type": "string", "mandatory": False},
    "MUNICIPALITY_NAME": {"en": "Municipality Name", "ar": "اسم البلدية", "type": "string", "mandatory": False},
    "LICENSE_STATUS_ID": {"en": "License Status ID", "ar": "معرف حالة الرخصة", "type": "number", "mandatory": False},
    "LICENSE_STATUS_NAME": {"en": "License Status", "ar": "حالة الرخصة", "type": "string", "mandatory": False},
    "SHOP_NAME": {"en": "Shop Name", "ar": "اسم المحل", "type": "string", "mandatory": False},
    "SHOP_AREA": {"en": "Shop Area", "ar": "مساحة المحل", "type": "string", "mandatory": False},
    "ISIC_NUMBER": {"en": "ISIC Number", "ar": "رقم النشاط", "type": "string", "mandatory": False},
    "ISIC_DESC": {"en": "ISIC Description", "ar": "وصف النشاط", "type": "string", "mandatory": False},
    "D_ACTIVITIES_ID": {"en": "Detailed Activity ID", "ar": "معرف النشاط التفصيلي", "type": "number", "mandatory": False},
    "D_ACTIVITIES_NAME": {"en": "Detailed Activity", "ar": "النشاط التفصيلي", "type": "string", "mandatory": False},
    "LICENSE_SADAD_END_DATE": {"en": "Sadad End Date", "ar": "تاريخ انتهاء السداد", "type": "string", "mandatory": False},
    "M_ACTIVITIES_ID": {"en": "Main Activity ID", "ar": "معرف النشاط الرئيسي", "type": "number", "mandatory": False},
    "M_ACTIVITIES_NAME": {"en": "Main Activity", "ar": "النشاط الرئيسي", "type": "string", "mandatory": False},

}