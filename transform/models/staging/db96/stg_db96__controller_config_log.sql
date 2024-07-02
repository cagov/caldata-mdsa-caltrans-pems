select
    controller_id,
    time_id,
    status,
    name,
    state_postmile,
    abs_postmile as absolute_postmile,
    latitude,
    longitude,
    angle,
    line_num,
    stn_address
from {{ source('db96', 'controller_config_log') }}
