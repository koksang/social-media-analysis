with web3_tweets as (

    select 
        * except(entity)
    from 
        {{ source("fct", "tweets") }}
    where 
        contains_substr(content, "web3")
        and not contains_substr(entity, "web3")

)

, base as (

    select * except(entity) from {{ source("fct", "tweets") }}
    where contains_substr(entity, "web3")

)

select * from web3_tweets
union all
select * from base