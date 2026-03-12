

with raw_data as (
    select *
    from "swans_demo"."raw"."ca_crashes_json_only"
),

parsed as (
    select
        r.raw_row_id as raw_row_sk,
    r.raw_json ->> 'Collision Id' as collision_id,
    r.raw_json ->> 'Report Number' as report_number,
    r.raw_json ->> 'Report Version' as report_version,
    r.raw_json ->> 'Is Preliminary' as is_preliminary,
    r.raw_json ->> 'NCIC Code' as ncic_code,
    r.raw_json ->> 'Crash Date Time' as crash_date_time,
    r.raw_json ->> 'Collision Time' as collision_time,
    r.raw_json ->> 'Beat' as beat,
    r.raw_json ->> 'City Id' as city_id,
    r.raw_json ->> 'City Code' as city_code,
    r.raw_json ->> 'City Name' as city_name,
    r.raw_json ->> 'County Code' as county_code,
    r.raw_json ->> 'City Is Active' as city_is_active,
    r.raw_json ->> 'City Is Incorporated' as city_is_incorporated,
    r.raw_json ->> 'Collision Type Code' as collision_type_code,
    r.raw_json ->> 'Collision Type Description' as collision_type_description,
    r.raw_json ->> 'Day of Week' as day_of_week,
    r.raw_json ->> 'DispatchNotified' as dispatchnotified,
    r.raw_json ->> 'HasPhotographs' as hasphotographs,
    r.raw_json ->> 'HitRun' as hitrun,
    r.raw_json ->> 'IsDeleted' as isdeleted,
    r.raw_json ->> 'IsHighwayRelated' as ishighwayrelated,
    r.raw_json ->> 'IsTowAway' as istowaway,
    r.raw_json ->> 'JudicialDistrict' as judicialdistrict,
    r.raw_json ->> 'MotorVehicleInvolvedWithCode' as motorvehicleinvolvedwithcode,
    r.raw_json ->> 'MotorVehicleInvolvedWithDesc' as motorvehicleinvolvedwithdesc,
    r.raw_json ->> 'MotorVehicleInvolvedWithOtherDesc' as motorvehicleinvolvedwithotherdesc,
    r.raw_json ->> 'NumberInjured' as numberinjured,
    r.raw_json ->> 'NumberKilled' as numberkilled,
    r.raw_json ->> 'Weather 1' as weather_1,
    r.raw_json ->> 'Weather 2' as weather_2,
    r.raw_json ->> 'Road Condition 1' as road_condition_1,
    r.raw_json ->> 'Road Condition 2' as road_condition_2,
    r.raw_json ->> 'Special Condition' as special_condition,
    r.raw_json ->> 'LightingCode' as lightingcode,
    r.raw_json ->> 'LightingDescription' as lightingdescription,
    r.raw_json ->> 'Latitude' as latitude,
    r.raw_json ->> 'Longitude' as longitude,
    r.raw_json ->> 'MilePostDirection' as milepostdirection,
    r.raw_json ->> 'MilePostDistance' as milepostdistance,
    r.raw_json ->> 'MilePostMarker' as milepostmarker,
    r.raw_json ->> 'MilePostUnitOfMeasure' as milepostunitofmeasure,
    r.raw_json ->> 'PedestrianActionCode' as pedestrianactioncode,
    r.raw_json ->> 'PedestrianActionDesc' as pedestrianactiondesc,
    r.raw_json ->> 'PrimaryCollisionFactorCode' as primarycollisionfactorcode,
    r.raw_json ->> 'PrimaryCollisionFactorDescription' as primarycollisionfactordescription,
    r.raw_json ->> 'PrimaryCollisionFactorIsCited' as primarycollisionfactoriscited,
    r.raw_json ->> 'PrimaryCollisionPartyNumber' as primarycollisionpartynumber,
    r.raw_json ->> 'Primary Rd' as primary_rd,
    r.raw_json ->> 'Secondary Rd' as secondary_rd,
    r.raw_json ->> 'SecondaryDirection' as secondarydirection,
    r.raw_json ->> 'SecondaryDistance' as secondarydistance,
    r.raw_json ->> 'SecondaryUnitOfMeasure' as secondaryunitofmeasure,
    r.raw_json ->> 'SecondaryRoad' as secondaryroad,
    r.raw_json ->> 'TrafficControlDeviceCode' as trafficcontroldevicecode,
    r.raw_json ->> 'CreatedDate' as createddate,
    r.raw_json ->> 'ModifiedDate' as modifieddate,
    r.raw_json ->> 'IsCountyRoad' as iscountyroad,
    r.raw_json ->> 'IsFreeWay' as isfreeway,
    r.raw_json ->> 'CHP555Version' as chp555version,
    r.raw_json ->> 'IsAdditonalObjectStruck' as isadditonalobjectstruck,
    r.raw_json ->> 'NotificationDate' as notificationdate,
    r.raw_json ->> 'NotificationTimeDescription' as notificationtimedescription,
    r.raw_json ->> 'HasDigitalMediaFiles' as hasdigitalmediafiles,
    r.raw_json ->> 'EvidenceNumber' as evidencenumber,
    r.raw_json ->> 'IsLocationReferToNarrative' as islocationrefertonarrative,
    r.raw_json ->> 'IsAOIOneSameAsLocation' as isaoionesameaslocation

    from raw_data r
),

enhanced as (
    select
        p.*,
        f."Dataset County Name"     as county_name,
        f."Local County Name"       as county_region,
        f."Latitude"                as county_latitude,
        f."Longitude"               as county_longitude,
        case 
            when f."Dataset County Code" is null and p.county_code is not null then 1
            else 0
        end as unmatched_county_flag
    from parsed p
    left join "swans_demo"."fct"."ca_counties_lookup" f
        on p.county_code = f."Dataset County Code"::text
)

select *
from enhanced