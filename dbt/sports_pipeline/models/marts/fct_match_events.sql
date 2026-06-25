with matches as (
    select * from {{ ref('stg_match_events') }}
),

home_teams as (
    select team_id, team_name from {{ ref('dim_teams') }}
),

away_teams as (
    select team_id, team_name from  {{ ref('dim_teams') }} 
)

select
    matches.match_id,
    matches.event_id,
    matches.competition_code,
    matches.matchday,
    matches.home_team_id,
    home_teams.team_name as home_team_name,
    matches.away_team_id,
    away_teams.team_name as away_team_name,
    matches.score_home,
    matches.score_away,

    case
        when matches.score_home is null then null
        when matches.score_home > matches.score_away then 'HOME_WIN'
        when matches.score_home < matches.score_away then 'AWAY_WIN'
        else 'DRAW'
    end as result,

    matches.status,
    matches.match_timestamp,
    matches.match_date,
    matches.ingested_at
from matches
left join home_teams on matches.home_team_id = home_teams.team_id
left join away_teams on matches.away_team_id = away_teams.team_id
