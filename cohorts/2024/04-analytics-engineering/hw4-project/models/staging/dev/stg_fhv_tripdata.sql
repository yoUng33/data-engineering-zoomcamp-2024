{{ config(materialized="view") }}

with
    tripdata as (
        select
            *,
            row_number() over (partition by dispatching_base_num, pickup_datetime) as rn
        from {{ source("dev", "external_fhv_2019_tripdata") }}
        where dispatching_base_num is not null
    )
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num',
    'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }}
    as dispatching_base_num,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }}
    as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }}
    as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- payment info
    cast(sr_flag as string) as sr_flag,
    cast(affiliated_base_number as string) as affiliated_base_number
from tripdata
-- where
--     rn = 1

    -- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
    -- {% if var('is_test_run', default=false) %}
    -- limit 100
    -- {% endif %}
    
