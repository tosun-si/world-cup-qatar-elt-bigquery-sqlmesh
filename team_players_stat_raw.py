# models/external/team_players_stat_raw.py
import typing as t
from datetime import datetime

import gcsfs
import pandas as pd
from bigframes.pandas import DataFrame
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

    fs = gcsfs.GCSFileSystem(project="gb-poc-373711")
    paths = fs.glob("gs://mazlum_dev/world_cup_team_stats/input/world_cup_team_players_stats_raw_*.json")

    return pd.concat([pd.read_json(fs.open(p), lines=True) for p in paths])
