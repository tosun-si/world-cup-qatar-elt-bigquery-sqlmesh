import io
import typing as t
from datetime import datetime

import pandas as pd
from bigframes.pandas import DataFrame
from google.cloud import storage
from sqlmesh import ExecutionContext, model


@model(
    "qatar_fifa_world_cup_sqlmesh.team_players_stat_raw",
    kind="FULL",
    columns={
        "nationality": "STRING",
        "fifaRanking": "INT64",
        "nationalTeamKitSponsor": "STRING",
        "position": "STRING",
        "nationalTeamJerseyNumber": "INT64",
        "playerDob": "STRING",
        "club": "STRING",
        "playerName": "STRING",
        "appearances": "STRING",
        "goalsScored": "STRING",
        "assistsProvided": "STRING",
        "dribblesPerNinety": "STRING",
        "interceptionsPerNinety": "STRING",
        "tacklesPerNinety": "STRING",
        "totalDuelsWonPerNinety": "STRING",
        "savePercentage": "STRING",
        "cleanSheets": "STRING",
        "brandSponsorAndUsed": "STRING",
    }
)
def execute(
        context: ExecutionContext,
        start: datetime,
        end: datetime,
        execution_time: datetime,
        **kwargs: t.Any,
) -> DataFrame:
    print("📦 Loading data from GCS...")

    client = storage.Client(project="gb-poc-373711")
    blobs = client.list_blobs(
        "mazlum_dev",
        prefix="world_cup_team_stats/input/world_cup_team_players_stats_raw_",
    )

    dfs = []
    for blob in blobs:
        if blob.name.endswith(".json"):
            print(f"⬇️ Downloading {blob.name}")
            content = blob.download_as_text(encoding="utf-8")

            df_part = pd.read_json(io.StringIO(content), lines=True)
            dfs.append(df_part)

    if not dfs:
        raise RuntimeError("❌ No JSON files found in the GCS prefix!")

    df = pd.concat(dfs, ignore_index=True)
    print(f"✅ Loaded {len(df)} rows from GCS.")

    return context.bigframe.read_pandas(df)
