


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
    from ANALYTICS_PRD.db96.stg_db96__vds30sec
    where 
    1=1
    
),

agg as (
    select
        id,
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
        
            sum(volume_9) as volume_9,
        
            sum(volume_10) as volume_10,
        
            sum(volume_11) as volume_11,
        
            sum(volume_12) as volume_12,
        
            sum(volume_13) as volume_13,
        
            sum(volume_14) as volume_14,
        
        
            avg(occupancy_1) as occupancy_1,
        
            avg(occupancy_2) as occupancy_2,
        
            avg(occupancy_3) as occupancy_3,
        
            avg(occupancy_4) as occupancy_4,
        
            avg(occupancy_5) as occupancy_5,
        
            avg(occupancy_6) as occupancy_6,
        
            avg(occupancy_7) as occupancy_7,
        
            avg(occupancy_8) as occupancy_8,
        
            avg(occupancy_9) as occupancy_9,
        
            avg(occupancy_10) as occupancy_10,
        
            avg(occupancy_11) as occupancy_11,
        
            avg(occupancy_12) as occupancy_12,
        
            avg(occupancy_13) as occupancy_13,
        
            avg(occupancy_14) as occupancy_14,
        
        
            avg(speed_1) as speed_1
            
                ,
            
        
            avg(speed_2) as speed_2
            
                ,
            
        
            avg(speed_3) as speed_3
            
                ,
            
        
            avg(speed_4) as speed_4
            
                ,
            
        
            avg(speed_5) as speed_5
            
                ,
            
        
            avg(speed_6) as speed_6
            
                ,
            
        
            avg(speed_7) as speed_7
            
                ,
            
        
            avg(speed_8) as speed_8
            
                ,
            
        
            avg(speed_9) as speed_9
            
                ,
            
        
            avg(speed_10) as speed_10
            
                ,
            
        
            avg(speed_11) as speed_11
            
                ,
            
        
            avg(speed_12) as speed_12
            
                ,
            
        
            avg(speed_13) as speed_13
            
                ,
            
        
            avg(speed_14) as speed_14
            
        
    from raw
    group by id, sample_date, sample_timestamp_trunc, district
),


    agg_1 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            1 as lane,
            volume_1 as flow,
            occupancy_1 as occupancy,
            speed_1 as speed
        from agg
    ),

    agg_2 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            2 as lane,
            volume_2 as flow,
            occupancy_2 as occupancy,
            speed_2 as speed
        from agg
    ),

    agg_3 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            3 as lane,
            volume_3 as flow,
            occupancy_3 as occupancy,
            speed_3 as speed
        from agg
    ),

    agg_4 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            4 as lane,
            volume_4 as flow,
            occupancy_4 as occupancy,
            speed_4 as speed
        from agg
    ),

    agg_5 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            5 as lane,
            volume_5 as flow,
            occupancy_5 as occupancy,
            speed_5 as speed
        from agg
    ),

    agg_6 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            6 as lane,
            volume_6 as flow,
            occupancy_6 as occupancy,
            speed_6 as speed
        from agg
    ),

    agg_7 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            7 as lane,
            volume_7 as flow,
            occupancy_7 as occupancy,
            speed_7 as speed
        from agg
    ),

    agg_8 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            8 as lane,
            volume_8 as flow,
            occupancy_8 as occupancy,
            speed_8 as speed
        from agg
    ),

    agg_9 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            9 as lane,
            volume_9 as flow,
            occupancy_9 as occupancy,
            speed_9 as speed
        from agg
    ),

    agg_10 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            10 as lane,
            volume_10 as flow,
            occupancy_10 as occupancy,
            speed_10 as speed
        from agg
    ),

    agg_11 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            11 as lane,
            volume_11 as flow,
            occupancy_11 as occupancy,
            speed_11 as speed
        from agg
    ),

    agg_12 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            12 as lane,
            volume_12 as flow,
            occupancy_12 as occupancy,
            speed_12 as speed
        from agg
    ),

    agg_13 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            13 as lane,
            volume_13 as flow,
            occupancy_13 as occupancy,
            speed_13 as speed
        from agg
    ),

    agg_14 as (
        select
            id,
            sample_date,
            sample_timestamp,
            district,
            14 as lane,
            volume_14 as flow,
            occupancy_14 as occupancy,
            speed_14 as speed
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
        union all
    
        select * from agg_9
        union all
    
        select * from agg_10
        union all
    
        select * from agg_11
        union all
    
        select * from agg_12
        union all
    
        select * from agg_13
        union all
    
        select * from agg_14
        
    
)

select * from agg_unioned