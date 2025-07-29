from typing import Dict, Any

# This dictionary maps DB column names to their display properties.
ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = {

    # Core identification fields
    "REQUEST_ID": {"en": "Request ID", "ar": "معرف الطلب", "type": "string", "mandatory": True},
    "LICENSE_ID": {"en": "License ID", "ar": "معرف الرخصة", "type": "string", "mandatory": True},
    
    # License dates and status
    "ISSUE_DATE": {"en": "Issue Date", "ar": "تاريخ الإصدار", "type": "date_time", "mandatory": True},
    "EXPIRATION_DATE": {"en": "Expiration Date", "ar": "تاريخ الانتهاء", "type": "date_time", "mandatory": True},
    "LICENSE_STATUS_CODE": {"en": "License Status Code", "ar": "كود حالة الرخصة", "type": "string", "mandatory": False},
    "LICENSE_STATUS": {"en": "License Status", "ar": "حالة الرخصة", "type": "string", "mandatory": False},
    
    # Digging information
    "DIGGING_STATUS_ID": {"en": "Digging Status ID", "ar": "معرف حالة الحفر", "type": "string", "mandatory": False},
    "DIGGING_STATUS": {"en": "Digging Status", "ar": "حالة الحفر", "type": "string", "mandatory": False},
    "DIGGING_METHOD": {"en": "Digging Method", "ar": "طريقة الحفر", "type": "string", "mandatory": False},
    "DIGGING_DURATION": {"en": "Digging Duration", "ar": "مدة الحفر", "type": "string", "mandatory": False},
    "DIGGING_LATE_STATUS": {"en": "Digging Late Status", "ar": "حالة تأخر الحفر", "type": "string", "mandatory": False},
    
    # Project details
    "SITE_NAME": {"en": "Site Name", "ar": "اسم الموقع", "type": "string", "mandatory": False},
    "PROJECT_NAME": {"en": "Project Name", "ar": "اسم المشروع", "type": "string", "mandatory": False},
    "PROJECT_DESCRIPTION": {"en": "Project Description", "ar": "وصف المشروع", "type": "string", "mandatory": False},
    "WORK_NATURE": {"en": "Work Nature", "ar": "طبيعة العمل", "type": "string", "mandatory": False},
    "PATH_LENGTH_SUM": {"en": "Path Length Sum", "ar": "إجمالي طول المسار", "type": "string", "mandatory": False},
    
    # Project dates
    "PROJECT_START_DATE": {"en": "Project Start Date", "ar": "تاريخ بدء المشروع", "type": "string", "mandatory": False},
    "WORK_START_DATE": {"en": "Work Start Date", "ar": "تاريخ بدء العمل", "type": "string", "mandatory": False},
    "PROJECT_END_DATE": {"en": "Project End Date", "ar": "تاريخ انتهاء المشروع", "type": "string", "mandatory": False},

    # Location information
    "AMANA_NAME": {"en": "Amana Name", "ar": "اسم الأمانة", "type": "string", "mandatory": False},
    "CITY_NAME": {"en": "City Name", "ar": "اسم المدينة", "type": "string", "mandatory": False},
    "MUNICIPALITY_NAME": {"en": "Municipality Name", "ar": "اسم البلدية", "type": "string", "mandatory": False},
    "AMANA_CODE": {"en": "Amana Code", "ar": "كود الأمانة", "type": "string", "mandatory": False},
    "MUNICIPALITY_CODE": {"en": "Municipality Code", "ar": "كود البلدية", "type": "string", "mandatory": False},
    "WORKSITE_CODE": {"en": "Worksite Code", "ar": "كود موقع العمل", "type": "string", "mandatory": False},
    "PATH_CODE": {"en": "Path Code", "ar": "كود المسار", "type": "string", "mandatory": False},
    
    # Coordinates
    "LICENSE_COORDINATES": {"en": "License Coordinates", "ar": "إحداثيات الرخصة", "type": "string", "mandatory": False},
    "OFFICE_COORDINATES": {"en": "Office Coordinates", "ar": "إحداثيات المكتب", "type": "string", "mandatory": False},
    "START_POINT_LONGITUDE": {"en": "Start Point Longitude", "ar": "خط طول نقطة البداية", "type": "number", "mandatory": True},
    "START_POINT_LATITUDE": {"en": "Start Point Latitude", "ar": "خط عرض نقطة البداية", "type": "number", "mandatory": True},
    "END_POINT_LONGITUDE": {"en": "End Point Longitude", "ar": "خط طول نقطة النهاية", "type": "number", "mandatory": True},
    "END_POINT_LATITUDE": {"en": "End Point Latitude", "ar": "خط عرض نقطة النهاية", "type": "number", "mandatory": True},
    
    # Additional details
    "NAME_ARABIC": {"en": "Arabic Name", "ar": "الاسم العربي", "type": "string", "mandatory": False},
    "MAP_NUMBER": {"en": "Map Number", "ar": "رقم الخريطة", "type": "string", "mandatory": False},
    "HEAVY_EQUIPMENT_PERMISSION": {"en": "Heavy Equipment Permission", "ar": "تصريح المعدات الثقيلة", "type": "string", "mandatory": False},
    "CAMPING_ROOM_COUNT": {"en": "Camping Room Count", "ar": "عدد غرف المعسكر", "type": "string", "mandatory": False},
    
    # Contractor/Consultant information
    "CONSULTANT_NAME": {"en": "Consultant Name", "ar": "اسم الاستشاري", "type": "string", "mandatory": False},
    "CONSULTANT_CR_NUMBER": {"en": "Consultant CR Number", "ar": "رقم السجل التجاري للاستشاري", "type": "string", "mandatory": False},
    "CONTRACTOR_NAME": {"en": "Contractor Name", "ar": "اسم المقاول", "type": "string", "mandatory": False},
    "CONTRACTOR_CR_NUMBER": {"en": "Contractor CR Number", "ar": "رقم السجل التجاري للمقاول", "type": "string", "mandatory": False}

}