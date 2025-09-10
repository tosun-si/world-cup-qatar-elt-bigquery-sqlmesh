MODEL (
  name qatar_fifa_world_cup.team_players_stat_raw,
  kind SEED (
    path '../../seeds/team_players_stat_raw.csv'
  ),
  columns (
    nationality TEXT,
    fifaRanking TEXT,
    nationalTeamKitSponsor TEXT,
    position TEXT,
    nationalTeamJerseyNumber TEXT,
    playerDob TEXT,
    club TEXT,
    playerName TEXT,
    appearances TEXT,
    goalsScored TEXT,
    assistsProvided TEXT,
    dribblesPerNinety TEXT,
    interceptionsPerNinety TEXT,
    tacklesPerNinety TEXT,
    totalDuelsWonPerNinety TEXT,
    savePercentage TEXT,
    cleanSheets TEXT,
    brandSponsorAndUsed TEXT
  )
);
