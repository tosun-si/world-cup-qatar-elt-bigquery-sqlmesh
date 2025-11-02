MODEL (
  name qatar_fifa_world_cup_sqlmesh.team_fifa_ranking,
  kind SEED (
    path '../../seeds/team_fifa_ranking.csv'
  ),
  dialect bigquery,
  gateway bigquery,
  columns (
    teamName TEXT,
    fifaRanking INTEGER
  )
);
