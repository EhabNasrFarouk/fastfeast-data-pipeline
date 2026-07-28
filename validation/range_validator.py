import polars as pl

def parse_bound(value) -> float:
    if isinstance(value, str) and value.strip().upper() in ("INF", "-INF"):
        return float(value.strip().upper().replace("INF", "inf"))
    return float(value)


def range_validator(lf: pl.LazyFrame, range_rules: dict[str, list]) -> pl.LazyFrame:
    error_frames = []
    for col_nm, (low, high) in range_rules.items():
        low_bound = parse_bound(low)
        high_bound = parse_bound(high)

        errors = (
            lf.filter(
                ((pl.col(col_nm) < low_bound) | (pl.col(col_nm) > high_bound))
            )
            .select(
                "row_number",
                pl.lit(col_nm).alias("column_name"),
                pl.lit("OUT_OF_RANGE").alias("error_type"),
                pl.col(col_nm).cast(pl.String).alias("invalid_value"),
            )
        )
        error_frames.append(errors)

    if not error_frames:
        return lf.clear().select(
            "row_number",
            pl.lit(None, dtype=pl.String).alias("column_name"),
            pl.lit(None, dtype=pl.String).alias("error_type"),
            pl.lit(None, dtype=pl.String).alias("invalid_value"),
        )
    return pl.concat(error_frames)


test_df = pl.DataFrame({
    "row_number": [1, 2, 3, 4, 5, 6, 7, 8],
    "rating_avg": [4.5, 1.0, 5.0, 5.5, 0.9, 3.2, None, 2.8],       # range [1.0, 5.0]
    "on_time_rate": [0.95, 0.0, 1.0, 1.2, -0.1, 0.5, 0.5, None],   # range [0.0, 1.0]
    "completed_deliveries": [120, 0, 5000, -5, 300, 0, 1, 250],    # range [0, INF]
})
test_lf = test_df.lazy()

range_rules = {
    "rating_avg": [1.0, 5.0],
    "on_time_rate": [0.0, 1.0],
    "completed_deliveries": [0, "INF"],
}

result = range_validator(test_lf, range_rules).collect()
print(result)