with tweets as (

    select 
    
        id
        , url
        , created_timestamp

    from {{ ref("vitalik_buterin_tweets") }}

)

, tweet_token_interests as (

    select 
    
        id
        , url
        , token

    from 
        {{ ref("tweet_token_interests") }}

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
        tweet_token_interests b
    on
        a.id = b.id
        and a.url = b.url
)

select * from joined