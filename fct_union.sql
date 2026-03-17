{% macro fct_union(columns, tables, where_clauses=None) %}

{% for relation in tables %}

    {% set existing_columns = adapter.get_columns_in_relation(relation) %}
    {% set existing_column_names = existing_columns
        | map(attribute='name')
        | map('upper')
        | list %}

    select
    {% for col in columns -%}
        {% if col | upper in existing_column_names -%}
            {{ col }}
        {% else -%}
            null as {{ col }}
        {% endif -%}
        {% if not loop.last -%},{% endif -%}
    {% endfor %}

    from {{ relation }}

    {%- if where_clauses and loop.index0 < where_clauses|length -%}
        {% set table_where = where_clauses[loop.index0] -%}
        {% if table_where -%}
            {% if table_where is string -%}
                where {{ table_where }}
            {% elif table_where is iterable and table_where is not string %}
                where {{ table_where | join(' and ') }}
            {%- endif %}
        {%- endif %}
    {%- endif %}

    {% if not loop.last -%}
    union all
    {%- endif -%}

{% endfor %}

{% endmacro %}
