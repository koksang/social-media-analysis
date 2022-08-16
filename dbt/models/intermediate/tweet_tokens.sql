with tweets as (

    select

        id
        , url
        , content
        , entity
        , created_timestamp

    from
        {{ source("marts", "fct_tweets") }}

)

, final as (

    select
    
        id
        , url
        , created_timestamp
        , array_agg(mentioned_tokens ignore nulls) tokens
        
    from
        tweets
        , unnest([
            if( contains_substr(content, "ada") or contains_substr(content, "cardano") or contains_substr(entity, "ada"), "ada", null)
            , if ( regexp_contains(lower(content), r"(bnb|binance coin|binance smart chain|bsc)") or contains_substr(entity, "bnb"), "bnb", null)
            , if ( contains_substr(content, "btc") or contains_substr(content, "bitcoin") or contains_substr(entity, "btc"), "btc", null)
            , if ( contains_substr(content, "erg") or contains_substr(content, "ergo") or contains_substr(entity, "erg"), "erg", null)
            , if ( contains_substr(content, "eth") or contains_substr(content, "ethereum") or contains_substr(entity, "eth"), "eth", null)
            , if ( contains_substr(content, "matic") or contains_substr(content, "polygon") or contains_substr(entity, "matic"), "matic", null)
            , if ( contains_substr(content, "sol") or contains_substr(content, "solana") or contains_substr(entity, "sol"), "sol", null)
            , if ( contains_substr(content, "avax") or contains_substr(content, "avalanche") or contains_substr(entity, "avax"), "avax", null)
            , if ( contains_substr(content, "doge") or contains_substr(content, "dogecoin") or contains_substr(entity, "doge"), "doge", null)
            , if ( contains_substr(content, "shib") or contains_substr(content, "shibatoken") or contains_substr(entity, "shib"), "shib", null)
            , if ( contains_substr(content, "link") or contains_substr(content, "chainlink") or contains_substr(entity, "link"), "link", null)
            , if ( contains_substr(content, "xrp") or contains_substr(content, "ripple") or contains_substr(entity, "xrp"), "xrp", null)
        ]) mentioned_tokens
    group by
        1, 2, 3

)

select * from final