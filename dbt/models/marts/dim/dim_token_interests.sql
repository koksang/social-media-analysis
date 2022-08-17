{{
    config(
        unique_key = ["id", "entity"]
    )
}}

with tweet_tokens as (

    select

        id
        , url
        , token

    from 
        {{ ref("tweet_tokens") }}
        , unnest(tokens) token
    where 
        tokens is not null

)

, tweets as (

    select 

        id
        , url
        , created_timestamp
    
    from {{ source("marts", "fct_tweets") }}

)

, final as (

    select

        a.id
        , a.url
        , a.created_timestamp
        , b.token
        
    from
        tweets a
    right join
        tweet_tokens b
    on
        a.id = b.id
        and a.url = b.url
)

select * from final
