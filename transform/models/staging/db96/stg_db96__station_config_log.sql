select
    station_id,
    time_id,
    status,
    name,
    physical_lanes,
    use_speed,
    dt_set_id,
    state_postmile,
    abs_postmile as absolute_postmile,
    latitude,
    longitude,
    angle,
    seg_start as segment_start,
    seg_end as segment_end,
    segment_end - segment_start as length,
    controller_id
from {{ source('db96', 'station_config_log') }}
