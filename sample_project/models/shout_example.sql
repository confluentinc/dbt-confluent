{{ config(materialized='streaming_table') }}

select
    {{ function('shout') }}('hello') as shouted,       -- HELLO!
    {{ function('shout') }}(cast(null as string)) as shouted_null  -- null
