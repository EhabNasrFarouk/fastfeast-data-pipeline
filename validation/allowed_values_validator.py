import polars as pl

def allowed_values_validator(lf: pl.LazyFrame, allowed_values_rules: dict[str, list]) -> pl.LazyFrame:
    error_frames = []
    for col_nm, allowed in allowed_values_rules.items():
        errors = (
            lf.filter(
             ~pl.col(col_nm).cast(pl.String).is_in(allowed)
            )
            .select(
                "row_number",
                pl.lit(col_nm).alias("column_name"),
                pl.lit("INVALID_VALUE").alias("error_type"),
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
    "row_number": [1, 2, 3, 4, 5, 6, 7],
    "shift": ["morning", "evening", "night", "afternoon", "MORNING", None, "night"],
    "vehicle_type": ["bike", "car", "motorbike", "truck", "car", "bike", None],
})
test_lf = test_df.lazy()

allowed_values_rules = {
    "shift": ["morning", "evening", "night"],
    "vehicle_type": ["bike", "motorbike", "car"],
}

result = allowed_values_validator(test_lf, allowed_values_rules).collect()
print(result)