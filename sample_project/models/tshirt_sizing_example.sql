{{ config(materialized='streaming_table') }}

select
    {{ function('is_smaller') }}('S', 'M')  as s_smaller_than_m,   -- true
    {{ function('is_smaller') }}('XL', 'M') as xl_smaller_than_m,  -- false
    {{ function('is_smaller') }}('foo', 'M') as unknown_size       -- false (unknown size)
