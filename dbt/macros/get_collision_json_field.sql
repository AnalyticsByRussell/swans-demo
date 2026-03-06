{% macro get_collision_json_fields(json_column='raw_json', table_alias='r') %}
{%- set fields = [
    'Collision Id',
    'Report Number',
    'Report Version',
    'Is Preliminary',
    'NCIC Code',
    'Crash Date Time',
    'Collision Time',
    'Beat',
    'City Id',
    'City Code',
    'City Name',
    'County Code',
    'City Is Active',
    'City Is Incorporated',
    'Collision Type Code',
    'Collision Type Description',
    'Day of Week',
    'DispatchNotified',
    'HasPhotographs',
    'HitRun',
    'IsDeleted',
    'IsHighwayRelated',
    'IsTowAway',
    'JudicialDistrict',
    'MotorVehicleInvolvedWithCode',
    'MotorVehicleInvolvedWithDesc',
    'MotorVehicleInvolvedWithOtherDesc',
    'NumberInjured',
    'NumberKilled',
    'Weather 1',
    'Weather 2',
    'Road Condition 1',
    'Road Condition 2',
    'Special Condition',
    'LightingCode',
    'LightingDescription',
    'Latitude',
    'Longitude',
    'MilePostDirection',
    'MilePostDistance',
    'MilePostMarker',
    'MilePostUnitOfMeasure',
    'PedestrianActionCode',
    'PedestrianActionDesc',
    'PrimaryCollisionFactorCode',
    'PrimaryCollisionFactorDescription',
    'PrimaryCollisionFactorIsCited',
    'PrimaryCollisionPartyNumber',
    'Primary Rd',
    'Secondary Rd',
    'SecondaryDirection',
    'SecondaryDistance',
    'SecondaryUnitOfMeasure',
    'SecondaryRoad',
    'TrafficControlDeviceCode',
    'CreatedDate',
    'ModifiedDate',
    'IsCountyRoad',
    'IsFreeWay',
    'CHP555Version',
    'IsAdditonalObjectStruck',
    'NotificationDate',
    'NotificationTimeDescription',
    'HasDigitalMediaFiles',
    'EvidenceNumber',
    'IsLocationReferToNarrative',
    'IsAOIOneSameAsLocation'
] -%}

{# start the select list with raw_row_sk #}
{%- set select_list = [ table_alias ~ '.raw_row_id as raw_row_sk' ] -%}

{# iterate over fields and convert to proper snake_case #}
{%- for f in fields -%}
    {%- if f != 'Collision Id' -%}
        {%- set f_snake = f
            | replace(' ', '_')
            | replace('-', '_')
            | replace('/', '_')
            | lower
        -%}
        {%- do select_list.append(table_alias ~ '.' ~ json_column ~ " ->> '" ~ f ~ "' as " ~ f_snake) -%}
    {%- else -%}
        {# Collision Id gets its own alias #}
        {%- do select_list.append(table_alias ~ '.' ~ json_column ~ " ->> '" ~ f ~ "' as collision_id") -%}
    {%- endif -%}
{%- endfor -%}

{{ select_list | join(',\n    ') }}
{% endmacro %}