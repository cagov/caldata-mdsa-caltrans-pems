with imputation_five_mins as (
    select *
    from {{ ref('int_imputation__detector_imputed_agg_five_minutes') }}
)

select * from imputation_five_mins
