{{ config(
    materialized='table'
) }}

with fhv_tripdata as (
    select *,
        to_hex(md5(cast(coalesce(cast(dispatching_base_num as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_datetime as string), '_dbt_utils_surrogate_key_null_') as string))) as trip_id,
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number,
        'FHV' as service_type
    from `praxis-wall-411617`.`dbt_jmaund`.`stg_fhv_2019`
), 

dim_zones as (
    select * from `praxis-wall-411617`.`week4_dataset`.`taxi_zone_lookup`
    where borough != 'Unknown'
)

select
    t.dispatching_base_num as dispatching_base_num,
    t.affiliated_base_num  as affiliated_base_num,
    t.pickup_location_id   as pickup_location_id,
    pickup.borough         as pickup_borough,
    pickup.zone            as pickup_zone,
    pickup.service_zone    as pickup_service_zone,
    t.dropoff_location_id  as dropoff_location_id,
    dropoff.borough        as dropoff_borough,
    dropoff.zone           as dropoff_zone,
    dropoff.service_zone   as dropoff_service_zone,
    t.shared_ride_flag     as shared_ride_flag,
    t.pickup_datetime      as pickup_datetime,
    t.dropoff_datetime     as dropoff_datetime
from  
    fhv_tripdata t
inner join 
    taxi_zone_lookup on t.pickup_location_id  = pickup.location_id
inner join 
    taxi_zone_lookup on t.dropoff_location_id = dropoff.location_id
where 
    t.row_num = 1