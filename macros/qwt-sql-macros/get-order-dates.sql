{% macro get_min_orderdate() %}

{% set get_minorderdate_query %} 
select
min(orderdate)
from {{ ref('fct_orders') }} 
{% endset %}
 
{% set results = run_query(get_minorderdate_query) %}
 
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0][0] %}
{% else %}
{% set results_list = [] %}
{% endif %}
 
{{ return(results_list) }}
 
{% endmacro %}



{% macro get_max_orderdate() %}
 
{% set get_maxorderdate_query %}
 
select
max(orderdate)
from {{ ref('fct_orders') }}
 
{% endset %}
 
{% set results = run_query(get_maxorderdate_query) %}
 
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0][0] %}
{% else %}
{% set results_list = [] %}
{% endif %}
 
{{ return(results_list) }}
 
{% endmacro %}