
{% macro make_model_duplicate_proof(primary_id_0, primary_id_1, primary_id_2, primary_id_3) -%}

qualify row_number() over (partition by {{ primary_id_0 }}, {{ primary_id_1 }}, {{ primary_id_2 }}, {{ primary_id_3 }}) = 1 

    {%- endmacro %}