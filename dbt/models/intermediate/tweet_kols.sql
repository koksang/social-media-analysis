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
        , array_agg(mentioned_kols ignore nulls) kols
        
    from
        tweets
        , unnest([
            if( contains_substr(content, "elon") or contains_substr(content, "elon musk") or contains_substr(entity, "elon"), "elonmusk", null)
            , if ( contains_substr(content, "vitalik") or contains_substr(content, "vitalik buterin") or contains_substr(entity, "vitalik"), "vitalikbuterin", null)
            , if ( contains_substr(content, "cz peng") or contains_substr(content, "cz") or contains_substr(content, "czbinance") or contains_substr(content, "cz_binance"), "czpeng", null)
            , if ( contains_substr(content, "michael saylor") or contains_substr(entity, "michaelsaylor"), "michaelsaylor", null)
        ]) mentioned_kols
    group by
        1, 2, 3

)

select * from final