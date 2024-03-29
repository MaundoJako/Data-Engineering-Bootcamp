{{ config(materialized='table') }}

select
    LocationID   as location_id,
    Borough      as borough,
    Zone         as zone,
    service_zone as service_zone
from 
    {{ ref('taxi_zone_lookup') }}