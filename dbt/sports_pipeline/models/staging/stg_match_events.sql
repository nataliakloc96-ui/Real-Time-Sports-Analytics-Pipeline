with source as (
    select * from {{ source('raw', 'match_events') }}
),

renamed as (
    select
        event_id,
        match_id,
        competition_code,
        matchday,

        home_team_id,
        home_team_name,
        away_team_id,
        away_team_name,

        score_home,
        score_away,

        status,
        match_timestamp,
        match_date,
        ingested_at,
        kafka_partition,
        kafka_offset
    from source
)

select * from renamed