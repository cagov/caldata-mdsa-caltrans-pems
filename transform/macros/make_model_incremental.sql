
{% macro make_model_incremental(date_col) -%}

{% if is_incremental() %}
        -- Look back to account for any late-arriving data
            {{ date_col }} > (
                select
                    dateadd(
                        day,
                        {{ var("incremental_model_look_back") }},
                        max({{ date_col }})
                    )
                from {{ this }}
            )
    {% else %}
    1=1
    {% endif %}

    {%- endmacro %}
