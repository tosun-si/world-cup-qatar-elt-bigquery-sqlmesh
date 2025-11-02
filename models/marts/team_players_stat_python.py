import typing as t
from datetime import datetime

from bigframes.pandas import DataFrame
from sqlmesh import ExecutionContext, model


@model(
    "qatar_fifa_world_cup_sqlmesh.team_players_stat_mart_python",
    kind="FULL",
    partitioned_by="DATE(ingestionDate)",
    clustered_by="teamName",
    columns={
        "teamName": "STRING",
        "nationalTeamKitSponsor": "STRING",
        "fifaRanking": "INT64",
        "teamTotalGoals": "INT64",
        "ingestionDate": "TIMESTAMP",
        "topScorerName": "STRING",
        "topScorerGoals": "INT64",
        "topScorerClub": "STRING",
        "topScorerPosition": "STRING",
        "bestPasserName": "STRING",
        "bestPasserAssists": "INT64",
        "bestPasserClub": "STRING",
        "bestDribblerName": "STRING",
        "bestDribblerDribbles": "FLOAT64",
        "bestDribblerClub": "STRING",
        "goalkeeperName": "STRING",
        "goalkeeperSavePercentage": "FLOAT64",
        "goalkeeperCleanSheets": "INT64",
    }
)
def execute(
        context: ExecutionContext,
        start: datetime,
        end: datetime,
        execution_time: datetime,
        **kwargs: t.Any,
) -> DataFrame:
    """
    Pure Python Model for FIFA World Cup Team Statistics using BigFrames

    - Boolean filtering and per-team grouping
    - Grouping and aggregations
    - Merging datasets
    - Creating derived columns with CamelCase naming
    """

    print("📊 Loading team players statistics...")
    source_table = context.table("qatar_fifa_world_cup_sqlmesh.team_players_stat_raw_cleaned")
    clean_table = source_table.replace("`", "")

    df = context.bigframe.read_gbq(clean_table)
    print(f"✅ Loaded {len(df)} records")

    # 🧤 Goalkeeper stats
    print("🧤 Processing goalkeeper statistics...")
    goalkeepers = df[df["isgoalkeeperstatsexist"] == True][
        ["nationality", "playername", "savepercentage", "cleansheets"]
    ]
    best_goalkeepers = (
        goalkeepers.groupby("nationality", as_index=False)
        .first()
        .rename(
            columns={
                "nationality": "teamName",
                "playername": "goalkeeperName",
                "savepercentage": "goalkeeperSavePercentage",
                "cleansheets": "goalkeeperCleanSheets",
            }
        )
    )

    # ⚽ Top scorers
    print("⚽ Identifying top scorers...")
    scorers = df[df["goalsscored"] > 0][["nationality", "playername", "goalsscored", "club", "position"]]
    top_scorers = (
        scorers.groupby("nationality", as_index=False)
        .first()
        .rename(
            columns={
                "nationality": "teamName",
                "playername": "topScorerName",
                "goalsscored": "topScorerGoals",
                "club": "topScorerClub",
                "position": "topScorerPosition",
            }
        )
    )

    # 🎯 Best passers
    print("🎯 Identifying best passers...")
    passers = df[df["assistsprovided"] > 0][["nationality", "playername", "assistsprovided", "club"]]
    best_passers = (
        passers.groupby("nationality", as_index=False)
        .first()
        .rename(
            columns={
                "nationality": "teamName",
                "playername": "bestPasserName",
                "assistsprovided": "bestPasserAssists",
                "club": "bestPasserClub",
            }
        )
    )

    # 🏃 Best dribblers
    print("🏃 Identifying best dribblers...")
    dribblers = df[df["dribblesperninety"] > 0][["nationality", "playername", "dribblesperninety", "club"]]
    best_dribblers = (
        dribblers.groupby("nationality", as_index=False)
        .first()
        .rename(
            columns={
                "nationality": "teamName",
                "playername": "bestDribblerName",
                "dribblesperninety": "bestDribblerDribbles",
                "club": "bestDribblerClub",
            }
        )
    )

    # 📈 Team-level aggregation
    print("📈 Aggregating team statistics...")
    team_stats = (
        df.groupby("nationality", as_index=False)
        .agg({
            "nationalteamkitsponsor": "max",
            "fifaranking": "max",
            "goalsscored": "sum",
        })
        .rename(
            columns={
                "nationality": "teamName",
                "goalsscored": "teamTotalGoals",
                "nationalteamkitsponsor": "nationalTeamKitSponsor",
                "fifaranking": "fifaRanking",
            }
        )
    )

    print("🔗 Merging all statistics...")
    result = team_stats

    # Ensure all derived DataFrames have teamName column (string type)
    for name, df_tmp in {
        "best_goalkeepers": best_goalkeepers,
        "top_scorers": top_scorers,
        "best_passers": best_passers,
        "best_dribblers": best_dribblers,
    }.items():
        if "teamName" not in df_tmp.columns:
            print(f"⚠️  {name} missing 'teamName' column — creating empty one.")
            df_tmp["teamName"] = ""
        df_tmp["teamName"] = df_tmp["teamName"].astype(str)

    # Normalize types before merging
    result["teamName"] = result["teamName"].astype(str)

    # Perform merges
    result = result.merge(best_goalkeepers, on="teamName", how="left")
    result = result.merge(top_scorers, on="teamName", how="left")
    result = result.merge(best_passers, on="teamName", how="left")
    result = result.merge(best_dribblers, on="teamName", how="left")

    # ✅ Add metadata
    result = result.assign(ingestionDate=execution_time)

    # ✅ Final columns
    final_columns = [
        "teamName",
        "nationalTeamKitSponsor",
        "fifaRanking",
        "teamTotalGoals",
        "ingestionDate",
        "topScorerName",
        "topScorerGoals",
        "topScorerClub",
        "topScorerPosition",
        "bestPasserName",
        "bestPasserAssists",
        "bestPasserClub",
        "bestDribblerName",
        "bestDribblerDribbles",
        "bestDribblerClub",
        "goalkeeperName",
        "goalkeeperSavePercentage",
        "goalkeeperCleanSheets",
    ]
    result = result[final_columns]

    print(f"🎉 Processing complete! Generated stats for {len(result)} teams")
    return result
