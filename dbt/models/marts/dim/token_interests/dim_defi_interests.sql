with tweets as (

    select 
    
        id
        , url
        , created_timestamp

    from {{ ref("defi_tweets") }}

)

, mentioned_tokens as (

    select 
    
        id
        , url
        , token

    from 
        {{ ref("tweet_mentioned_tokens") }}
        , unnest(tokens) token

)

, joined as (
    select

        a.id
        , a.url
        , a.created_timestamp
        , b.token
        
    from
        tweets a
    left join
        mentioned_tokens b
    on
        a.id = b.id
        and a.url = b.url
)

select * from joined 