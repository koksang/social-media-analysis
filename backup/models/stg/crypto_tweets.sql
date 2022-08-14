with crypto_tweets as (

    select 
        * except(entity)
    from 
        {{ source("marts", "fct_tweets") }}
    where 
        ( contains_substr(content, "crypto") or contains_substr(content, "cryptocurrency") )
        and not contains_substr(entity, "crypto")

)

, base as (

    select * except(entity) from {{ source("marts", "fct_tweets") }}
    where contains_substr(entity, "crypto")

)

select * from crypto_tweets
union all
select * from base