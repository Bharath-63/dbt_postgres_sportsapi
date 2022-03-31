{{ config (
    materialized="table"
)}}

select * from (

with __dbt__cte__lists_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "aumdwmdk".public._airbyte_raw_lists
select
    jsonb_extract_path(_airbyte_data, 'golf') as golf,
    jsonb_extract_path(_airbyte_data, 'cricket') as cricket,
    jsonb_extract_path(_airbyte_data, 'football') as football,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "aumdwmdk".public._airbyte_raw_lists as table_alias
-- lists
where 1 = 1

),  __dbt__cte__lists_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__lists_ab1
select
    golf,
    cricket,
    football,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from __dbt__cte__lists_ab1
-- lists
where 1 = 1

),  __dbt__cte__lists_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__lists_ab2
select
    md5(cast(coalesce(cast(golf as
    varchar
), '') || '-' || coalesce(cast(cricket as
    varchar
), '') || '-' || coalesce(cast(football as
    varchar
), '') as
    varchar
)) as _airbyte_lists_hashid,
    tmp.*
from __dbt__cte__lists_ab2 tmp
-- lists
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__lists_ab3
select
    golf,
    cricket,
    football,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_lists_hashid
from __dbt__cte__lists_ab3
-- lists from "aumdwmdk".public._airbyte_raw_lists
where 1 = 1

  );