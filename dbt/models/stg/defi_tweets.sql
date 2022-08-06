with defi_tweets as (

    select 
        * except(entity)
    from 
        {{ ref("tweets") }}
    where 
        ( contains_substr(content, "defi") )
        and entity not in ("defi")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where entity in ("defi")

)

select * from defi_tweets
union all
select * from base