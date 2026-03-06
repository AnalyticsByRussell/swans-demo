-- models/marts/int_ca_crashes.sql
with stg as (
    select *
    from {{ ref('stg_ca_crashes') }}
)

select
    stg.raw_row_sk as raw_row_sk,
    stg.collision_id as collision_id,
    stg.report_number as report_number,
    stg.report_version as report_version,

    -- booleans
    case when lower(stg.is_preliminary)='true' then true
         when lower(stg.is_preliminary)='false' then false
         else null end as is_preliminary,
    case when lower(stg.hasphotographs)='true' then true
         when lower(stg.hasphotographs)='false' then false
         else null end as has_photographs,
    case when lower(stg.hitrun)='true' then true
         when lower(stg.hitrun)='false' then false
         else null end as hitrun,
    case when lower(stg.isdeleted)='true' then true
         when lower(stg.isdeleted)='false' then false
         else null end as is_deleted,
    case when lower(stg.ishighwayrelated)='true' then true
         when lower(stg.ishighwayrelated)='false' then false
         else null end as is_highway_related,
    case when lower(stg.istowaway)='true' then true
         when lower(stg.istowaway)='false' then false
         else null end as is_tow_away,

    -- integers
    case when stg.numberinjured ~ '^\d+$' then stg.numberinjured::int else null end as number_injured,
    case when stg.numberkilled ~ '^\d+$' then stg.numberkilled::int else null end as number_killed,

    -- timestamps
    case when stg.crash_date_time ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}' then stg.crash_date_time::timestamp else null end as crash_date_time,
    case when stg.createddate ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}' then stg.createddate::timestamp else null end as created_date,
    case when stg.modifieddate ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}' then stg.modifieddate::timestamp else null end as modified_date,
    case when stg.notificationdate ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}' then stg.notificationdate::timestamp else null end as notification_date,

    -- numerics
    case when stg.latitude ~ '^[\d.-]+$' then stg.latitude::numeric else null end as latitude,
    case when stg.longitude ~ '^[\d.-]+$' then stg.longitude::numeric else null end as longitude,
    case when stg.milepostdistance ~ '^[\d.-]+$' then stg.milepostdistance::numeric else null end as milepost_distance,
    case when stg.secondarydistance ~ '^[\d.-]+$' then stg.secondarydistance::numeric else null end as secondary_distance,

    -- text fields (as-is)
    stg.beat as beat,
    stg.city_id as city_id,
    stg.city_code as city_code,
    stg.city_name as city_name,
    stg.county_code as county_code,
    stg.city_is_active as city_is_active,
    stg.city_is_incorporated as city_is_incorporated,
    stg.collision_type_code as collision_type_code,
    stg.collision_type_description as collision_type_description,
    stg.day_of_week as day_of_week,
    stg.dispatchnotified as dispatch_notified,
    stg.judicialdistrict as judicial_district,
    stg.motorvehicleinvolvedwithcode as motorvehicle_involved_with_code,
    stg.motorvehicleinvolvedwithdesc as motorvehicle_involved_with_desc,
    stg.motorvehicleinvolvedwithotherdesc as motorvehicle_involved_with_other_desc,
    stg.weather_1 as weather_1,
    stg.weather_2 as weather_2,
    stg.road_condition_1 as road_condition_1,
    stg.road_condition_2 as road_condition_2,
    stg.special_condition as special_condition,
    stg.lightingcode as lighting_code,
    stg.lightingdescription as lighting_description,
    stg.milepostdirection as milepost_direction,
    stg.milepostmarker as milepost_marker,
    stg.milepostunitofmeasure as milepost_unit_of_measure,
    stg.pedestrianactioncode as pedestrian_action_code,
    stg.pedestrianactiondesc as pedestrian_action_desc,
    stg.primarycollisionfactorcode as primary_collision_factor_code,
    stg.primarycollisionfactordescription as primary_collision_factor_description,
    stg.primarycollisionfactoriscited as primary_collision_factor_is_cited,
    stg.primarycollisionpartynumber as primary_collision_party_number,
    stg.primary_rd as primary_rd,
    stg.secondary_rd as secondary_rd,
    stg.secondarydirection as secondary_direction,
    stg.secondaryunitofmeasure as secondary_unit_of_measure,
    stg.secondaryroad as secondary_road,
    stg.trafficcontroldevicecode as traffic_control_device_code,
    stg.chp555version as chp555_version,
    stg.isadditonalobjectstruck as is_additional_object_struck,
    stg.hasdigitalmediafiles as has_digital_media_files,
    stg.evidencenumber as evidence_number,
    stg.islocationrefertonarrative as is_location_ref_to_narrative,
    stg.isaoionesameaslocation as is_aoi_one_same_as_location,
    case when lower(stg.iscountyroad)='true' then true
         when lower(stg.iscountyroad)='false' then false
         else null end as is_county_road,
    case when lower(stg.isfreeway)='true' then true
         when lower(stg.isfreeway)='false' then false
         else null end as is_freeway,

    -- **new county enrichment fields**
    stg.county_name,
    stg.county_region,
    stg.county_latitude,
    stg.county_longitude,
    stg.unmatched_county_flag

from stg