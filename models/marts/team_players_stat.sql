MODEL(
    name qatar_fifa_world_cup_sqlmesh.team_players_stat_mart,
    kind FULL,
    partitioned_by DATE(ingestionDate),
    clustered_by teamName,
    dialect bigquery,
    gateway bigquery,
    tags ["mart"],
    column_descriptions (
        teamName = 'Team name',
        teamTotalGoals = 'Team total goals',
        goalKeeper = 'Goal keeper stats struct',
        topScorers = 'Top scorers struct',
        bestPassers = 'Best passers struct'
    )
);

WITH
    team_players_stat_raw AS (
        SELECT *
        FROM qatar_fifa_world_cup_sqlmesh.team_players_stat_raw_cleaned
    ),

    goalKeepersStats AS (
        SELECT
            nationality,
            STRUCT(
                playerName,
                appearances,
                savePercentage,
                cleanSheets
                ) AS goalKeeperStatsStruct
        FROM team_players_stat_raw
        WHERE isGoalKeeperStatsExist IS TRUE
    ),

    goalKeeperStatsPerTeam AS (
        SELECT
            nationality,
            ARRAY_AGG(goalKeeperStatsStruct ORDER BY goalKeeperStatsStruct.savePercentage DESC LIMIT 1)[OFFSET(0)] AS stats
        FROM goalKeepersStats
        GROUP BY nationality
    )

SELECT
    statRaw.nationality AS teamName,
    nationalTeamKitSponsor,
    fifaRanking,
    SUM(goalsScored) AS teamTotalGoals,
    CURRENT_TIMESTAMP() AS ingestionDate,
    goalKeeperStatsPerTeam.stats AS goalKeeper,

    @build_player_stats(goalsScored, appearances, brandSponsorAndUsed, club, position, playerDob, playerName) AS topScorers,
    @build_player_stats(assistsProvided, appearances, brandSponsorAndUsed, club, position, playerDob, playerName) AS bestPassers,
    @build_player_stats(dribblesPerNinety, appearances, brandSponsorAndUsed, club, position, playerDob, playerName) AS bestDribblers,
    @build_player_stats(appearances, appearances, brandSponsorAndUsed, club, position, playerDob, playerName) AS playersMostAppearances,
    @build_player_stats(totalDuelsWonPerNinety, appearances, brandSponsorAndUsed, club, position, playerDob, playerName) AS playersMostDuelsWon,
    @build_player_stats(interceptionsPerNinety, appearances, brandSponsorAndUsed, club, position, playerDob, playerName) AS playersMostInterception,
    @build_player_stats(tacklesPerNinety, appearances, brandSponsorAndUsed, club, position, playerDob, playerName) AS playersMostSuccessfulTackles

FROM team_players_stat_raw AS statRaw
JOIN goalKeeperStatsPerTeam ON statRaw.nationality = goalKeeperStatsPerTeam.nationality
GROUP BY
    statRaw.nationality,
    nationalTeamKitSponsor,
    fifaRanking,
    goalKeeperStatsPerTeam.stats;
