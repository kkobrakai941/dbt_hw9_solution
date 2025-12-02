{% macro amount_bucket(amount) %}
  case
    when {{ amount }} < var('low_bound') then 'low'
    when {{ amount }} < var('medium_bound') then 'medium'
    else 'high'
  end
{% endmacro %}
