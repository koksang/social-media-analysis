with tweets as (

    select

        id
        , url
        , content
        , created_timestamp

    from
        {{ source("fct", "tweets") }}

)

select
    
    id
    , url
    , created_timestamp
    , array_agg(mentioned_tokens ignore nulls) tokens
    
from
    tweets
    , unnest([
        if( contains_substr(content, "ada") or contains_substr(content, "cardano"), "ada", null)
        , if ( regexp_contains(lower(content), r"(bnb|binance coin|binance smart chain|bsc)"), "bnb", null)
        , if ( contains_substr(content, "btc") or contains_substr(content, "bitcoin"), "btc", null)
        , if ( contains_substr(content, "erg") or contains_substr(content, "ergo"), "erg", null)
        , if ( contains_substr(content, "eth") or contains_substr(content, "ethereum"), "eth", null)
        , if ( contains_substr(content, "matic") or contains_substr(content, "polygon"), "matic", null)
        , if ( contains_substr(content, "sol") or contains_substr(content, "solana"), "sol", null)
        , if ( contains_substr(content, "avax") or contains_substr(content, "avalanche"), "avax", null)
        , if ( contains_substr(content, "doge") or contains_substr(content, "dogecoin"), "doge", null)
        , if ( contains_substr(content, "shib") or contains_substr(content, "shibatoken"), "shib", null)
        , if ( contains_substr(content, "link") or contains_substr(content, "chainlink"), "link", null)
        , if ( contains_substr(content, "xrp") or contains_substr(content, "ripple"), "xrp", null)
    ]) mentioned_tokens
group by
    1, 2, 3