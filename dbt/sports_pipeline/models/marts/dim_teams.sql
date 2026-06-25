with home_teams as (
    select distinct
        home_team_id as team_id,
        home_team_name as team_name
    from {{ ref('stg_match_events') }}
    where home_team_id is not null
),

away_teams as (
    select distinct
        away_team_id as team_id,
        away_team_name as team_name
    from {{ ref('stg_match_events') }}
    where away_team_id is not null
),

all_teams as (
    select * from home_teams
    union
    select * from away_teams
)

select
    team_id,
    team_name
from all_teams
order by team_name