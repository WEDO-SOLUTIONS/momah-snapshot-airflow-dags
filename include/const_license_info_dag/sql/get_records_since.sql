SELECT 
    "LICENSE_NUMBER" AS "license_number",
    "LAST_MODIFIED_DATE" AS "date",
    "AMANA_ID" AS "amana_id",
    "AMANA_NAME" AS "amana_name",
    "MUNICIPALITY_ID" AS "municipality_id",
    "MUNICIPALITY_NAME" AS "municipality_name",
    "LONGITUDE" AS "longitude",
    "LATITUDE" AS "latitude",
    "LICENSE_STATUS_ID" AS "license_status_id",
    "LICENSE_STATUS_NAME" AS "license_status_name",
    "SHOP_NAME" AS "shop_name",
    "SHOP_AREA" AS "shop_area",
    "ISIC_NUMBER" AS "isic_number",
    "ISIC_DESC" AS "isic_desc",
    "D_ACTIVITIES_ID" AS "d_activities_id",
    "D_ACTIVITIES_NAME" AS "d_activities_name",
    "LICENSE_ISSUE_DATE" AS "license_issue_date",
    "LICENSE_SADAD_END_DATE" AS "license_sadad_end_date",
    "M_ACTIVITIES_ID" AS "m_activities_id",
    "M_ACTIVITIES_NAME" AS "m_activities_name",
    "ID" AS "id",
    "LAST_MODIFIED_DATE" AS "last_modified_date"
FROM "AMANAT_INTGR".CONST_LICENSE_INFO_SNAPSHOT clis
WHERE "LICENSE_STATUS_NAME" = 'سارية'
AND "LAST_MODIFIED_DATE" > TO_TIMESTAMP_TZ(:ts, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZH:TZM')