MODEL(
    name qatar_fifa_world_cup.team_players_stat_raw_cleaned,
    kind VIEW,
    dialect duckdb
);

SELECT
    nationality,
    TRY_CAST(goalsScored AS INTEGER) AS goalsScored,
    TRY_CAST(assistsProvided AS INTEGER) AS assistsProvided,
    TRY_CAST(dribblesPerNinety AS DOUBLE) AS dribblesPerNinety,
    TRY_CAST(appearances AS INTEGER) AS appearances,
    TRY_CAST(totalDuelsWonPerNinety AS DOUBLE) AS totalDuelsWonPerNinety,
    TRY_CAST(interceptionsPerNinety AS DOUBLE) AS interceptionsPerNinety,
    TRY_CAST(tacklesPerNinety AS DOUBLE) AS tacklesPerNinety,
    brandSponsorAndUsed,
    club,
    savePercentage,
    CASE
        WHEN savePercentage <> '-' THEN TRUE
        ELSE FALSE
        END AS isGoalKeeperStatsExist,
    fifaRanking,
    position,
    playerName,
    cleanSheets,
    nationalTeamKitSponsor,
    nationalTeamJerseyNumber,
    playerDob
FROM qatar_fifa_world_cup.team_players_stat_raw
