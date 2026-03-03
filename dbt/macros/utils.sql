-- Utility macros
{% macro count_distinct(column) %}
    count(distinct {{ column }})
{% endmacro %}