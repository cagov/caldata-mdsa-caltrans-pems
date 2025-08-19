


with raw as (
    select
        *,
        /* Create a timestamp truncated down to the nearest five
         minute bucket. This will be the the timestamp on which
         we aggregate. If a 30-second interval straddles two different
         buckets, it will be assigned to the one latter one due to
         the floor() call.
        */
        dateadd(
            'minute',
            floor(minute(sample_timestamp) / 5) * 5,
            trunc(sample_timestamp, 'hour')
        ) as sample_timestamp_trunc
    from ANALYTICS_PRD.clearinghouse.stg_clearinghouse__station_raw

    where 
    1=1
    
),

dmeta as (
    select * from ANALYTICS_PRD.vds.int_vds__detector_config
),

agg as (
    select
        id as station_id,
        sample_date,
        sample_timestamp_trunc as sample_timestamp,
        district,
        
            sum(volume_1) as volume_1,
        
            sum(volume_2) as volume_2,
        
            sum(volume_3) as volume_3,
        
            sum(volume_4) as volume_4,
        
            sum(volume_5) as volume_5,
        
            sum(volume_6) as volume_6,
        
            sum(volume_7) as volume_7,
        
            sum(volume_8) as volume_8,
        
        
            count_if(volume_1 = 0) as zero_vol_ct_1,
        
            count_if(volume_2 = 0) as zero_vol_ct_2,
        
            count_if(volume_3 = 0) as zero_vol_ct_3,
        
            count_if(volume_4 = 0) as zero_vol_ct_4,
        
            count_if(volume_5 = 0) as zero_vol_ct_5,
        
            count_if(volume_6 = 0) as zero_vol_ct_6,
        
            count_if(volume_7 = 0) as zero_vol_ct_7,
        
            count_if(volume_8 = 0) as zero_vol_ct_8,
        
        
            avg(occupancy_1) as occupancy_1,
        
            avg(occupancy_2) as occupancy_2,
        
            avg(occupancy_3) as occupancy_3,
        
            avg(occupancy_4) as occupancy_4,
        
            avg(occupancy_5) as occupancy_5,
        
            avg(occupancy_6) as occupancy_6,
        
            avg(occupancy_7) as occupancy_7,
        
            avg(occupancy_8) as occupancy_8,
        
        
            count_if(occupancy_1 = 0) as zero_occ_ct_1,
        
            count_if(occupancy_2 = 0) as zero_occ_ct_2,
        
            count_if(occupancy_3 = 0) as zero_occ_ct_3,
        
            count_if(occupancy_4 = 0) as zero_occ_ct_4,
        
            count_if(occupancy_5 = 0) as zero_occ_ct_5,
        
            count_if(occupancy_6 = 0) as zero_occ_ct_6,
        
            count_if(occupancy_7 = 0) as zero_occ_ct_7,
        
            count_if(occupancy_8 = 0) as zero_occ_ct_8,
        
        
            count_if(volume_1 = 0 and occupancy_1 > 0) as zero_vol_pos_occ_ct_1,
        
            count_if(volume_2 = 0 and occupancy_2 > 0) as zero_vol_pos_occ_ct_2,
        
            count_if(volume_3 = 0 and occupancy_3 > 0) as zero_vol_pos_occ_ct_3,
        
            count_if(volume_4 = 0 and occupancy_4 > 0) as zero_vol_pos_occ_ct_4,
        
            count_if(volume_5 = 0 and occupancy_5 > 0) as zero_vol_pos_occ_ct_5,
        
            count_if(volume_6 = 0 and occupancy_6 > 0) as zero_vol_pos_occ_ct_6,
        
            count_if(volume_7 = 0 and occupancy_7 > 0) as zero_vol_pos_occ_ct_7,
        
            count_if(volume_8 = 0 and occupancy_8 > 0) as zero_vol_pos_occ_ct_8,
        
        
            count_if(volume_1 > 0 and occupancy_1 = 0) as zero_occ_pos_vol_ct_1,
        
            count_if(volume_2 > 0 and occupancy_2 = 0) as zero_occ_pos_vol_ct_2,
        
            count_if(volume_3 > 0 and occupancy_3 = 0) as zero_occ_pos_vol_ct_3,
        
            count_if(volume_4 > 0 and occupancy_4 = 0) as zero_occ_pos_vol_ct_4,
        
            count_if(volume_5 > 0 and occupancy_5 = 0) as zero_occ_pos_vol_ct_5,
        
            count_if(volume_6 > 0 and occupancy_6 = 0) as zero_occ_pos_vol_ct_6,
        
            count_if(volume_7 > 0 and occupancy_7 = 0) as zero_occ_pos_vol_ct_7,
        
            count_if(volume_8 > 0 and occupancy_8 = 0) as zero_occ_pos_vol_ct_8,
        
        
            count_if(volume_1 > 20) as high_volume_ct_1,
        
            count_if(volume_2 > 20) as high_volume_ct_2,
        
            count_if(volume_3 > 20) as high_volume_ct_3,
        
            count_if(volume_4 > 20) as high_volume_ct_4,
        
            count_if(volume_5 > 20) as high_volume_ct_5,
        
            count_if(volume_6 > 20) as high_volume_ct_6,
        
            count_if(volume_7 > 20) as high_volume_ct_7,
        
            count_if(volume_8 > 20) as high_volume_ct_8,
        
        
            count_if(occupancy_1 > 0.7) as high_occupancy_ct_1,
        
            count_if(occupancy_2 > 0.7) as high_occupancy_ct_2,
        
            count_if(occupancy_3 > 0.7) as high_occupancy_ct_3,
        
            count_if(occupancy_4 > 0.7) as high_occupancy_ct_4,
        
            count_if(occupancy_5 > 0.7) as high_occupancy_ct_5,
        
            count_if(occupancy_6 > 0.7) as high_occupancy_ct_6,
        
            count_if(occupancy_7 > 0.7) as high_occupancy_ct_7,
        
            count_if(occupancy_8 > 0.7) as high_occupancy_ct_8,
        
        
            count_if(volume_1 is not null and occupancy_1 is not null) as sample_ct_1,
        
            count_if(volume_2 is not null and occupancy_2 is not null) as sample_ct_2,
        
            count_if(volume_3 is not null and occupancy_3 is not null) as sample_ct_3,
        
            count_if(volume_4 is not null and occupancy_4 is not null) as sample_ct_4,
        
            count_if(volume_5 is not null and occupancy_5 is not null) as sample_ct_5,
        
            count_if(volume_6 is not null and occupancy_6 is not null) as sample_ct_6,
        
            count_if(volume_7 is not null and occupancy_7 is not null) as sample_ct_7,
        
            count_if(volume_8 is not null and occupancy_8 is not null) as sample_ct_8,
        
        
            sum(volume_1 * speed_1)
            / nullifzero(sum(volume_1)) as speed_weighted_1        
                ,
            
        
            sum(volume_2 * speed_2)
            / nullifzero(sum(volume_2)) as speed_weighted_2        
                ,
            
        
            sum(volume_3 * speed_3)
            / nullifzero(sum(volume_3)) as speed_weighted_3        
                ,
            
        
            sum(volume_4 * speed_4)
            / nullifzero(sum(volume_4)) as speed_weighted_4        
                ,
            
        
            sum(volume_5 * speed_5)
            / nullifzero(sum(volume_5)) as speed_weighted_5        
                ,
            
        
            sum(volume_6 * speed_6)
            / nullifzero(sum(volume_6)) as speed_weighted_6        
                ,
            
        
            sum(volume_7 * speed_7)
            / nullifzero(sum(volume_7)) as speed_weighted_7        
                ,
            
        
            sum(volume_8 * speed_8)
            / nullifzero(sum(volume_8)) as speed_weighted_8        
        
    from raw
    group by station_id, sample_date, sample_timestamp_trunc, district
),


    agg_1 as (
        select
            station_id,
            sample_date,
            sample_timestamp,
            district,
            sample_ct_1 as sample_ct,
            1 as lane,
            volume_1 as volume_observed,
            round(iff(
                sample_ct_1 >= 10, volume_1,
                10 / nullifzero(sample_ct_1) * volume_1
            ))
                as volume_sum,
                --Represents the observed or normalized flow value based on the
                --number of samples recieved by the device
            zero_vol_ct_1 as zero_vol_ct,
            occupancy_1 as occupancy_avg,
            zero_occ_ct_1 as zero_occ_ct,
            zero_vol_pos_occ_ct_1 as zero_vol_pos_occ_ct,
            zero_occ_pos_vol_ct_1 as zero_occ_pos_vol_ct,
            high_volume_ct_1 as high_volume_ct,
            high_occupancy_ct_1 as high_occupancy_ct,
            speed_weighted_1 as speed_weighted
        from agg
    ),

    agg_2 as (
        select
            station_id,
            sample_date,
            sample_timestamp,
            district,
            sample_ct_2 as sample_ct,
            2 as lane,
            volume_2 as volume_observed,
            round(iff(
                sample_ct_2 >= 10, volume_2,
                10 / nullifzero(sample_ct_2) * volume_2
            ))
                as volume_sum,
                --Represents the observed or normalized flow value based on the
                --number of samples recieved by the device
            zero_vol_ct_2 as zero_vol_ct,
            occupancy_2 as occupancy_avg,
            zero_occ_ct_2 as zero_occ_ct,
            zero_vol_pos_occ_ct_2 as zero_vol_pos_occ_ct,
            zero_occ_pos_vol_ct_2 as zero_occ_pos_vol_ct,
            high_volume_ct_2 as high_volume_ct,
            high_occupancy_ct_2 as high_occupancy_ct,
            speed_weighted_2 as speed_weighted
        from agg
    ),

    agg_3 as (
        select
            station_id,
            sample_date,
            sample_timestamp,
            district,
            sample_ct_3 as sample_ct,
            3 as lane,
            volume_3 as volume_observed,
            round(iff(
                sample_ct_3 >= 10, volume_3,
                10 / nullifzero(sample_ct_3) * volume_3
            ))
                as volume_sum,
                --Represents the observed or normalized flow value based on the
                --number of samples recieved by the device
            zero_vol_ct_3 as zero_vol_ct,
            occupancy_3 as occupancy_avg,
            zero_occ_ct_3 as zero_occ_ct,
            zero_vol_pos_occ_ct_3 as zero_vol_pos_occ_ct,
            zero_occ_pos_vol_ct_3 as zero_occ_pos_vol_ct,
            high_volume_ct_3 as high_volume_ct,
            high_occupancy_ct_3 as high_occupancy_ct,
            speed_weighted_3 as speed_weighted
        from agg
    ),

    agg_4 as (
        select
            station_id,
            sample_date,
            sample_timestamp,
            district,
            sample_ct_4 as sample_ct,
            4 as lane,
            volume_4 as volume_observed,
            round(iff(
                sample_ct_4 >= 10, volume_4,
                10 / nullifzero(sample_ct_4) * volume_4
            ))
                as volume_sum,
                --Represents the observed or normalized flow value based on the
                --number of samples recieved by the device
            zero_vol_ct_4 as zero_vol_ct,
            occupancy_4 as occupancy_avg,
            zero_occ_ct_4 as zero_occ_ct,
            zero_vol_pos_occ_ct_4 as zero_vol_pos_occ_ct,
            zero_occ_pos_vol_ct_4 as zero_occ_pos_vol_ct,
            high_volume_ct_4 as high_volume_ct,
            high_occupancy_ct_4 as high_occupancy_ct,
            speed_weighted_4 as speed_weighted
        from agg
    ),

    agg_5 as (
        select
            station_id,
            sample_date,
            sample_timestamp,
            district,
            sample_ct_5 as sample_ct,
            5 as lane,
            volume_5 as volume_observed,
            round(iff(
                sample_ct_5 >= 10, volume_5,
                10 / nullifzero(sample_ct_5) * volume_5
            ))
                as volume_sum,
                --Represents the observed or normalized flow value based on the
                --number of samples recieved by the device
            zero_vol_ct_5 as zero_vol_ct,
            occupancy_5 as occupancy_avg,
            zero_occ_ct_5 as zero_occ_ct,
            zero_vol_pos_occ_ct_5 as zero_vol_pos_occ_ct,
            zero_occ_pos_vol_ct_5 as zero_occ_pos_vol_ct,
            high_volume_ct_5 as high_volume_ct,
            high_occupancy_ct_5 as high_occupancy_ct,
            speed_weighted_5 as speed_weighted
        from agg
    ),

    agg_6 as (
        select
            station_id,
            sample_date,
            sample_timestamp,
            district,
            sample_ct_6 as sample_ct,
            6 as lane,
            volume_6 as volume_observed,
            round(iff(
                sample_ct_6 >= 10, volume_6,
                10 / nullifzero(sample_ct_6) * volume_6
            ))
                as volume_sum,
                --Represents the observed or normalized flow value based on the
                --number of samples recieved by the device
            zero_vol_ct_6 as zero_vol_ct,
            occupancy_6 as occupancy_avg,
            zero_occ_ct_6 as zero_occ_ct,
            zero_vol_pos_occ_ct_6 as zero_vol_pos_occ_ct,
            zero_occ_pos_vol_ct_6 as zero_occ_pos_vol_ct,
            high_volume_ct_6 as high_volume_ct,
            high_occupancy_ct_6 as high_occupancy_ct,
            speed_weighted_6 as speed_weighted
        from agg
    ),

    agg_7 as (
        select
            station_id,
            sample_date,
            sample_timestamp,
            district,
            sample_ct_7 as sample_ct,
            7 as lane,
            volume_7 as volume_observed,
            round(iff(
                sample_ct_7 >= 10, volume_7,
                10 / nullifzero(sample_ct_7) * volume_7
            ))
                as volume_sum,
                --Represents the observed or normalized flow value based on the
                --number of samples recieved by the device
            zero_vol_ct_7 as zero_vol_ct,
            occupancy_7 as occupancy_avg,
            zero_occ_ct_7 as zero_occ_ct,
            zero_vol_pos_occ_ct_7 as zero_vol_pos_occ_ct,
            zero_occ_pos_vol_ct_7 as zero_occ_pos_vol_ct,
            high_volume_ct_7 as high_volume_ct,
            high_occupancy_ct_7 as high_occupancy_ct,
            speed_weighted_7 as speed_weighted
        from agg
    ),

    agg_8 as (
        select
            station_id,
            sample_date,
            sample_timestamp,
            district,
            sample_ct_8 as sample_ct,
            8 as lane,
            volume_8 as volume_observed,
            round(iff(
                sample_ct_8 >= 10, volume_8,
                10 / nullifzero(sample_ct_8) * volume_8
            ))
                as volume_sum,
                --Represents the observed or normalized flow value based on the
                --number of samples recieved by the device
            zero_vol_ct_8 as zero_vol_ct,
            occupancy_8 as occupancy_avg,
            zero_occ_ct_8 as zero_occ_ct,
            zero_vol_pos_occ_ct_8 as zero_vol_pos_occ_ct,
            zero_occ_pos_vol_ct_8 as zero_occ_pos_vol_ct,
            high_volume_ct_8 as high_volume_ct,
            high_occupancy_ct_8 as high_occupancy_ct,
            speed_weighted_8 as speed_weighted
        from agg
    ),


agg_unioned as (
    
        select * from agg_1
        union all
    
        select * from agg_2
        union all
    
        select * from agg_3
        union all
    
        select * from agg_4
        union all
    
        select * from agg_5
        union all
    
        select * from agg_6
        union all
    
        select * from agg_7
        union all
    
        select * from agg_8
        
    
),

agg_with_metadata as (
    select
        agg.*,
        dmeta.detector_id,
        dmeta.state_postmile,
        dmeta.absolute_postmile,
        dmeta.latitude,
        dmeta.longitude,
        dmeta.physical_lanes,
        dmeta.station_type,
        dmeta.county,
        dmeta.city,
        dmeta.freeway,
        dmeta.direction,
        dmeta.length,
        dmeta._valid_from as station_valid_from,
        dmeta._valid_to as station_valid_to
    from agg_unioned as agg inner join dmeta
        on
            agg.station_id = dmeta.station_id
            and agg.lane = dmeta.lane
            and 

    agg.sample_date >= dmeta._valid_from
    and ( agg.sample_date < dmeta._valid_to or dmeta._valid_to is null)



)

select * from agg_with_metadata