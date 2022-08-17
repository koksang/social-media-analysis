{{
    config(
        unique_key = ["id", "token", "created_timestamp"]
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

        b.id
        , b.url
        , b.created_timestamp
        , a.token
        
    from
        tweet_tokens a
    left join
        tweets b
    on
        a.id = b.id
        and a.url = b.url
)

select * from final
