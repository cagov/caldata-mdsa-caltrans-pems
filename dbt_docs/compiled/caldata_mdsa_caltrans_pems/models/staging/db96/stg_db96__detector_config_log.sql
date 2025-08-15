select
    detector_id,
    time_id,
    station_id,
    status,
    lane::int as lane,
    slot,
    volume_flag,
    logical_position
from RAW_PRD.db96.detector_config_log