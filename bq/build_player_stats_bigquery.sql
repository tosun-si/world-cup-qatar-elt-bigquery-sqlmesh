%macro build_player_stats(stat_column, appearances, sponsor, club, position, dob, player)

STRUCT(
    MAX({{ stat_column }}) AS {{ stat_column }},
    (
        SELECT ARRAY_AGG(
            IF(
                {{ stat_column }} = 0 OR {{ stat_column }} = 0.00,
                NULL,
                STRUCT(
                    {{ appearances }},
                    {{ sponsor }},
                    {{ club }},
                    {{ position }},
                    {{ dob }},
                    {{ player }}
                )
            )
            ORDER BY {{ stat_column }} DESC
            LIMIT 1
        )[OFFSET(0)]
    ) AS players
)

%endmacro
