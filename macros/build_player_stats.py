from sqlmesh import macro

@macro()
def build_player_stats(evaluator, stat_column, appearances, sponsor, club, position, dob, player_name):
    return f"""
    (
        SELECT STRUCT_PACK(
            'stat_value', MAX("{stat_column}"),
            'top_player', (
                SELECT STRUCT_PACK(
                    'player_name', "{player_name}",
                    'appearances', "{appearances}",
                    'sponsor', "{sponsor}",
                    'club', "{club}",
                    'position', "{position}",
                    'dob', "{dob}"
                )
                FROM (
                    SELECT *
                    FROM team_players_stat_raw
                    WHERE TRY_CAST("{stat_column}" AS DOUBLE) > 0
                    ORDER BY TRY_CAST("{stat_column}" AS DOUBLE) DESC
                    LIMIT 1
                )
            )
        )
    )
    """
