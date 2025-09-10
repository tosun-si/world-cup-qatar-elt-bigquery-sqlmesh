{% macro build_player_stats(
    stat_indicator,
    appearances,
    brandSponsorAndUsed,
    club,
    position,
    playerDob,
    playerName
) %}
STRUCT(
    MAX({{ stat_indicator }}) AS {{ stat_indicator }},
    (
        SELECT
            ARRAY_AGG(
                CASE
                    WHEN {{ stat_indicator }} = 0 OR {{ stat_indicator }} = 0.00 THEN NULL
                    ELSE ROW(
                        {{ appearances }},
                        {{ brandSponsorAndUsed }},
                        {{ club }},
                        {{ position }},
                        {{ playerDob }},
                        {{ playerName }}
                    )
                END
                ORDER BY {{ stat_indicator }} DESC
                LIMIT 1
            )[1]
    ) AS players
)
{% endmacro %}
