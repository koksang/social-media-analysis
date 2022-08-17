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
            , if ( contains_substr(content, "charles hoskinson") or contains_substr(entity, "charleshoskinson"), "charleshoskinson", null)
            , if ( contains_substr(content, "michael saylor") or contains_substr(entity, "michaelsaylor"), "michaelsaylor", null)
            , if ( contains_substr(content, "jack dorsey") or contains_substr(entity, "jackdorsey"), "jackdorsey", null)
            , if ( contains_substr(content, "brian amstrong") or contains_substr(entity, "brianamstrong"), "brianamstrong", null)
            , if ( regexp_contains(lower(content), r"(sam bankman|sbf|sambankman)") or contains_substr(entity, "sambankman"), "sambankman", null)
            , if ( contains_substr(content, "cathy wood") or contains_substr(entity, "cathywood"), "cathywood", null)
            , if ( contains_substr(content, "justin sun") or contains_substr(entity, "justinsun"), "justinsun", null)
        ]) mentioned_kols
    group by
        1, 2, 3

)

select * from final