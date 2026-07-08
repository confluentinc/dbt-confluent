{# Unit test materialization for Confluent.
   Instead of using CTEs for input fixtures (which don't support watermarks),
   we create real tables using CREATE TABLE ... LIKE and insert fixture data.
   CTE parsing is handled in Python by adapter.parse_unit_test_ctes(). #}
{%- materialization unit, adapter='confluent' -%}
  {%- set relations = [] -%}
  {%- set expected_rows = config.get('expected_rows') -%}
  {%- set expected_sql = config.get('expected_sql') -%}
  {%- if (expected_rows | length) > 0 -%}
    {%- set tested_expected_column_names = expected_rows[0].keys() -%}
  {%- else -%}
    {%- set tested_expected_column_names = get_columns_in_query(sql) -%}
  {%- endif -%}

  {# Parse CTEs and extract main query in Python #}
  {%- set parsed = adapter.parse_unit_test_ctes(model['extra_ctes'], sql) -%}
  {%- set main_sql = parsed['main_sql'] -%}

  {# For each CTE, create a real table with fixture data #}
  {%- for cte in parsed['ctes'] -%}
    {%- set original_relation = adapter.get_relation(this.database, this.schema, cte['original_identifier']) -%}
    {%- if not original_relation -%}
      {%- do exceptions.raise_compiler_error(
            "The original relation referenced in tests does not exist: "
            ~ this.database ~ '.' ~ this.schema ~ '.' ~ cte['original_identifier']
        ) -%}
    {%- endif -%}
    {%- set temp_relation = api.Relation.create(
        database=this.database,
        schema=this.schema,
        identifier=cte['cte_name'],
        type='table'
    ) -%}

    {{ drop_relation_if_exists(temp_relation) }}

    {# Fixture cleanup runs in the adapter's post_model_hook, which fires even
       when a later fixture or the main query fails — inline cleanup at the end
       of this materialization would leak every fixture created so far.
       Registered before the CREATE (the deferred drop is IF EXISTS). #}
    {%- do adapter.defer_drop(temp_relation) -%}

    {% call statement('create_' ~ loop.index, execution_mode="streaming_ddl") -%}
      {# Exclude options to avoid copying connector settings (e.g. 'faker')
         that would prevent the table from being used as a sink for INSERT. #}
      CREATE TABLE {{ temp_relation }} LIKE {{ original_relation }} ( EXCLUDING OPTIONS )
    {%- endcall %}
    {%- do adapter.defer_statement_delete(load_result('create_' ~ loop.index).response.statement_name) -%}

    {% call statement('insert_' ~ loop.index) -%}
      INSERT INTO {{ temp_relation }} {{ cte['body'] }}
    {%- endcall %}
    {# Deleting the fixture statements (before the deferred drops run) avoids
       stranding a still-RUNNING INSERT in DEGRADED when its table is dropped,
       and keeps per-test-run statements from lingering 30 days in the
       statement list. #}
    {%- do adapter.defer_statement_delete(load_result('insert_' ~ loop.index).response.statement_name) -%}
  {%- endfor -%}

  {# Get column metadata from the TESTED MODEL (not 'this', which is the unit test node) #}
  {%- set tested_relation = adapter.get_tested_model_relation(
      model['tested_node_unique_id'], this.database, this.schema
  ) -%}
  {%- set columns_in_relation = adapter.get_columns_in_relation(tested_relation) -%}
  {%- set column_name_to_data_types = {} -%}
  {%- set column_name_to_quoted = {} -%}
  {%- for column in columns_in_relation -%}
    {%- do column_name_to_data_types.update({column.name|lower: column.data_type}) -%}
    {%- do column_name_to_quoted.update({column.name|lower: column.quoted}) -%}
  {%- endfor -%}

  {%- set expected_column_names_quoted = [] -%}
  {%- for column_name in tested_expected_column_names -%}
    {%- do expected_column_names_quoted.append(column_name_to_quoted[column_name|lower]) -%}
  {%- endfor -%}

  {%- if not expected_sql -%}
    {%- set expected_sql = get_expected_sql(expected_rows, column_name_to_data_types, column_name_to_quoted) -%}
  {%- endif -%}
  {%- set unit_test_sql = get_unit_test_sql(main_sql, expected_sql, expected_column_names_quoted) -%}

  {# The query returns both actual and expected rows via UNION ALL, so double the limit #}
  {%- call statement('main', fetch_result=True) -%}
{{ unit_test_sql }}
  {%- endcall -%}

  {{ return({'relations': relations}) }}
{%- endmaterialization -%}
