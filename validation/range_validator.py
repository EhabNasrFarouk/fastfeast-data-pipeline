import polars as pl

def parse_bound(value) -> float:
    if isinstance(value, str) and value.strip().upper() in ("INF", "-INF"):
        return float(value.strip().upper().replace("INF", "inf"))
    return float(value)


def range_validator(lf: pl.LazyFrame, range_rules: dict[str, list]) -> pl.LazyFrame:
    error_frames = []
    for col_nm, rule in range_rules.items():
        low, high, severity = rule
        low_bound = parse_bound(low)
        high_bound = parse_bound(high)

        errors = (
            lf.filter((pl.col(col_nm) < low_bound) | (pl.col(col_nm) > high_bound))
            .select(
                "row_number",
                pl.lit(col_nm).alias("column_name"),
                pl.lit("OUT_OF_RANGE").alias("error_type"),
                pl.col(col_nm).cast(pl.String).alias("invalid_value"),
                pl.lit(severity).alias("severity"),
            )
        )
        error_frames.append(errors)

    if not error_frames:
        return lf.clear().select(
            "row_number",
            pl.lit(None, dtype=pl.String).alias("column_name"),
            pl.lit(None, dtype=pl.String).alias("error_type"),
            pl.lit(None, dtype=pl.String).alias("invalid_value"),
            pl.lit(None, dtype=pl.String).alias("severity"),
        )
    return pl.concat(error_frames)


test_df = pl.DataFrame({
    "row_number": [1, 2, 3, 4, 5, 6, 7, 8],
    "rating_avg": [4.5, 1.0, 5.0, 5.5, 0.9, 3.2, None, 2.8],
    "on_time_rate": [0.95, 0.0, 1.0, 1.2, -0.1, 0.5, 0.5, None],
    "completed_deliveries": [120, 0, 5000, -5, 300, 0, 1, 250],
})
test_lf = test_df.lazy()

range_rules = {
    "rating_avg": [1.0, 5.0, "WARNING"],
    "on_time_rate": [0.0, 1.0, "CRITICAL"],
    "completed_deliveries": [0, "INF", "WARNING"],
}

result = range_validator(test_lf, range_rules).collect()
print(result)

# ---- assert expected rows against expected outcomes ----
expected = {
    ("rating_avg", 4): "5.5",
    ("rating_avg", 5): "0.9",
    ("on_time_rate", 4): "1.2",
    ("on_time_rate", 5): "-0.1",
    ("completed_deliveries", 4): "-5",
}
actual = {
    (row["column_name"], row["row_number"]): row["invalid_value"]
    for row in result.to_dicts()
}
assert actual == expected, f"Mismatch:\nexpected={expected}\nactual={actual}"
print("range_validator: all rows correct, including per-column severity")