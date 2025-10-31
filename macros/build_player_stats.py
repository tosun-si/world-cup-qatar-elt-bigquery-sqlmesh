from sqlmesh import macro


@macro()
def build_player_stats(
        evaluator,
        stat_indicator,
        appearances,
        brand_sponsor_and_used,
        club,
        position,
        player_dob,
        player_name,
):
    return f"""
    STRUCT(
        MAX({stat_indicator}) AS {stat_indicator},
        ARRAY_AGG(
            IF(
                {stat_indicator} = 0 OR {stat_indicator} = 0.00,
                NULL,
                STRUCT(
                    {appearances},
                    {brand_sponsor_and_used},
                    {club},
                    {position},
                    {player_dob},
                    {player_name}
                )
            )
            ORDER BY {stat_indicator} DESC LIMIT 1
        )[OFFSET(0)] AS players
    )
    """
