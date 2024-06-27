select
    detector_id,
    detector_type
from {{ source('db96', 'detector_config') }}
