with charles_hoskinson_tweets as (

    select 
        * except(entity)
    from 
        {{ ref("tweets") }}
    where 
        ( contains_substr(content, "charles hoskinson") or contains_substr(content, "charles") or contains_substr(content, "hoskinson") )
        and entity not in ('"charles hoskinson"')

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where entity in ('"charles hoskinson"')

)

select * from charles_hoskinson_tweets
union all
select * from base