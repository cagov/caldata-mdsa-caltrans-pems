
{% macro make_model_incremental(date_col, multiplier=1) -%}

{% if is_incremental() %}
        -- Look back to account for any late-arriving data
        where
            {{ date_col }} > (
                select
                    dateadd(
                        day,
                        {{ var("incremental_model_look_back") }},
                        max({{ date_col }})
                    )
                from {{ this }}
            )
            {% if target.name != 'prd' %}
                and {{ date_col }} >= (
                    dateadd(
                        day,
                        {{ var("dev_model_look_back") }},
                        current_date()
                    )
                )
            {% endif %}
    {% elif target.name != 'prd' %}
        where {{ date_col }} >= dateadd(day, {{ multiplier }} * {{ var("dev_model_look_back") }}, current_date())
    {% endif %}

    {%- endmacro %}