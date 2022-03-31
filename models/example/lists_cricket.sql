{{ config (
    materialized="table"
)}}


with __dbt__cte__lists_cricket_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "aumdwmdk".public."lists"

select
    _airbyte_lists_hashid,
    jsonb_extract_path_text(_airbyte_nested_data, 'match') as "match",
    jsonb_extract_path_text(_airbyte_nested_data, 'start') as "start",
    jsonb_extract_path_text(_airbyte_nested_data, 'region') as region,
    jsonb_extract_path_text(_airbyte_nested_data, 'country') as country,
    jsonb_extract_path_text(_airbyte_nested_data, 'stadium') as stadium,
    jsonb_extract_path_text(_airbyte_nested_data, 'tournament') as tournament,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "aumdwmdk".public."lists" as table_alias
-- cricket at lists/cricket
cross join jsonb_array_elements(
        case jsonb_typeof(cricket)
        when 'array' then cricket
        else '[]' end
    ) as _airbyte_nested_data
where 1 = 1
and cricket is not null

),  __dbt__cte__lists_cricket_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__lists_cricket_ab1
select
    _airbyte_lists_hashid,
    cast("match" as
    varchar
) as "match",
    cast("start" as
    varchar
) as "start",
    cast(region as
    varchar
) as region,
    cast(country as
    varchar
) as country,
    cast(stadium as
    varchar
) as stadium,
    cast(tournament as
    varchar
) as tournament,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from __dbt__cte__lists_cricket_ab1
-- cricket at lists/cricket
where 1 = 1

),  __dbt__cte__lists_cricket_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__lists_cricket_ab2
select
    md5(cast(coalesce(cast(_airbyte_lists_hashid as
    varchar
), '') || '-' || coalesce(cast("match" as
    varchar
), '') || '-' || coalesce(cast("start" as
    varchar
), '') || '-' || coalesce(cast(region as
    varchar
), '') || '-' || coalesce(cast(country as
    varchar
), '') || '-' || coalesce(cast(stadium as
    varchar
), '') || '-' || coalesce(cast(tournament as
    varchar
), '') as
    varchar
)) as _airbyte_cricket_hashid,
    tmp.*
from __dbt__cte__lists_cricket_ab2 tmp
-- cricket at lists/cricket
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__lists_cricket_ab3
select
    _airbyte_lists_hashid,
    "match",
    "start",
    region,
    country,
    stadium,
    tournament,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_cricket_hashid
from __dbt__cte__lists_cricket_ab3
-- cricket at lists/cricket from "aumdwmdk".public."lists"
where 1 = 1

  )
  select * from aumdwmdk.public

