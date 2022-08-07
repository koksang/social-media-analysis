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
        , count(token = "ada") ada_count
        , count(token = "bnb") bnb_count
        , count(token = "btc") btc_count
        , count(token = "erg") erg_count
        , count(token = "matic") matic_count
        , count(token = "eth") eth_count
        , count(token = "sol") sol_count
        , count(token = "avax") avax_count
        , count(token = "doge") doge_count
        , count(token = "shib") shib_count
        , count(token = "link") link_count
        , count(token = "xrp") xrp_count

    from 
        {{ ref("tweet_mentioned_tokens") }}
        , unnest(tokens) token
    group by
        1, 2

)

select

    b.*
    , extract(week from a.created_timestamp) week 

from
    tweets a
left join
    mentioned_tokens b
on
    a.id = b.id
    and a.url = b.url