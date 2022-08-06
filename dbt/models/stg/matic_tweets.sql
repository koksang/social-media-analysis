with matic_tweets as (

    select 
        * except(entity)
    from 
        {{ source("fct", "tweets") }}
    where
        contains_substr(content, "matic")
        and entity not in ("matic")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where entity in ("matic")

)

select * from matic_tweets
union all
select * from base