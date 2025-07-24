

with
five_minute_agg as (
    select
        station_id,
        lane,
        detector_id,
        sample_date,
        sample_timestamp,
        district,
        county,
        city,
        freeway,
        direction,
        length,
        sample_ct,
        volume_sum,
        occupancy_avg,
        speed_five_mins,
        station_type,
        absolute_postmile,
        volume_imputation_method,
        speed_imputation_method,
        occupancy_imputation_method,
        station_valid_from,
        station_valid_to
    from ANALYTICS_PRD.imputation.int_imputation__detector_imputed_agg_five_minutes
    where 
    1=1
    
),

vmt_vht_metrics as (
    select
        *,
        --vehicle-miles/5-min
        volume_sum * length as vmt,
        --vehicle-hours/5-min
        volume_sum * length / nullifzero(speed_five_mins) as vht,
        --q is in miles per hour for single station
        vmt / nullifzero(vht) as q_value,
        -- travel time
        60 / nullifzero(q_value) as tti
    from five_minute_agg
),

delay_metrics as (
    select
        vvm.*,
        /*  The formula for delay is: F * (L/V - L/V_t). F = flow (volume),
        L = length of the segment, V = current speed, and V_t = threshold speed. */
        
            greatest(vvm.volume_sum * ((vvm.length / nullifzero(vvm.speed_five_mins)) - (vvm.length / 35)), 0)
                as delay_35_mph
            
                ,
            

        
            greatest(vvm.volume_sum * ((vvm.length / nullifzero(vvm.speed_five_mins)) - (vvm.length / 40)), 0)
                as delay_40_mph
            
                ,
            

        
            greatest(vvm.volume_sum * ((vvm.length / nullifzero(vvm.speed_five_mins)) - (vvm.length / 45)), 0)
                as delay_45_mph
            
                ,
            

        
            greatest(vvm.volume_sum * ((vvm.length / nullifzero(vvm.speed_five_mins)) - (vvm.length / 50)), 0)
                as delay_50_mph
            
                ,
            

        
            greatest(vvm.volume_sum * ((vvm.length / nullifzero(vvm.speed_five_mins)) - (vvm.length / 55)), 0)
                as delay_55_mph
            
                ,
            

        
            greatest(vvm.volume_sum * ((vvm.length / nullifzero(vvm.speed_five_mins)) - (vvm.length / 60)), 0)
                as delay_60_mph
            

        

    from vmt_vht_metrics as vvm
),

productivity_metrics as (
    select
        dm.*,
        /*
        The formula for Productivity is: Length * (1 - (actual flow / flow capacity))
        */
        
            case
                when dm.speed_five_mins >= 35
                    then 0
                else dm.length * (1 - (dm.volume_sum / mc.max_capacity_5min))
            end
                as lost_productivity_35_mph
            
                ,
            

        
            case
                when dm.speed_five_mins >= 40
                    then 0
                else dm.length * (1 - (dm.volume_sum / mc.max_capacity_5min))
            end
                as lost_productivity_40_mph
            
                ,
            

        
            case
                when dm.speed_five_mins >= 45
                    then 0
                else dm.length * (1 - (dm.volume_sum / mc.max_capacity_5min))
            end
                as lost_productivity_45_mph
            
                ,
            

        
            case
                when dm.speed_five_mins >= 50
                    then 0
                else dm.length * (1 - (dm.volume_sum / mc.max_capacity_5min))
            end
                as lost_productivity_50_mph
            
                ,
            

        
            case
                when dm.speed_five_mins >= 55
                    then 0
                else dm.length * (1 - (dm.volume_sum / mc.max_capacity_5min))
            end
                as lost_productivity_55_mph
            
                ,
            

        
            case
                when dm.speed_five_mins >= 60
                    then 0
                else dm.length * (1 - (dm.volume_sum / mc.max_capacity_5min))
            end
                as lost_productivity_60_mph
            

        

    from delay_metrics as dm
    inner join ANALYTICS_PRD.performance.int_performance__max_capacity as mc
        on dm.detector_id = mc.detector_id
)

select * from productivity_metrics