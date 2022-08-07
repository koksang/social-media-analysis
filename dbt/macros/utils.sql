{% macro filter_tweets(size) %}

    {#-- Extra indentation so it appears inline when script is compiled. -#}

    {% set size = size|upper %}

    {% if target.name == "prod" %}
        {# -- Checks so ensure allowed warehouse size -#}
        {{ check__warehouse_size(size) }}
        {{ log("Warehouse resize - Target: " ~ target.name + ", size: " ~ size) }}
        ALTER WAREHOUSE {{ target.warehouse }} SET WAREHOUSE_SIZE={{ size }}

    {% else %}
        {{ log("Not resizing warehouse as only supports target `prod`. Got Target: " ~ target.name) }}
    
    {% endif %}

{% endmacro %}