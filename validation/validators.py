import polars as pl

# ------------------------------ Null Validator ------------------------------
def null_validator(lf: pl.LazyFrame, columns: list[str]) -> pl.LazyFrame | None:
    error_frames = []
      
    for col_nm in columns:
        errors = (
            lf.filter(pl.col(col_nm).is_null())
                .select(
                    "row_number",
                    pl.lit(col_nm).alias("column_name"),
                    pl.lit("NULL_VALUE").alias("error_type"),
                    pl.col(col_nm).cast(pl.String).alias("invalid_value")
                )
        )
        error_frames.append(errors)

    return None if not error_frames else pl.concat(error_frames)


# ------------------------------ Duplicate Validator ------------------------------
def duplicate_validator(lf: pl.LazyFrame, columns: list[str]) -> pl.LazyFrame | None:
    error_frames = []
           
    for col_nm in columns:
        errors = (
            lf.filter(
                    pl.col(col_nm).is_duplicated() &
                    ~pl.col(col_nm).is_first_distinct()
                )
                .select(
                    "row_number",
                    pl.lit(col_nm).alias("column_name"),
                    pl.lit("DUPLICATE_KEY").alias("error_type"),
                    pl.col(col_nm).cast(pl.String).alias("invalid_value")
                )
        )
        error_frames.append(errors)

    return None if not error_frames else pl.concat(error_frames)


# ------------------------------ Data Type Validator ------------------------------
def data_type_validator(lf: pl.LazyFrame, columns: dict[str, list]) -> pl.LazyFrame | None:
    error_frames = []
    STR_TO_DTYPE = {
        "pl.Int64": pl.Int64,
        "pl.Utf8": pl.String,
        "pl.Boolean": pl.Boolean,
        "pl.Date": pl.Date,
        "pl.Timestamp": pl.Datetime,
        "pl.Decimal": pl.Decimal
    }
                
    for col_nm, data_type in columns.items():
        #  print(col_nm, data_type)
        errors = (
            lf.filter(
                    pl.col(col_nm).is_not_null() &
                    pl.col(col_nm).cast(STR_TO_DTYPE[data_type], strict=False).is_null()
                )
                .select(
                    "row_number",
                    pl.lit(col_nm).alias("column_name"),
                    pl.lit("INVALID_DTYPE").alias("error_type"),
                    pl.col(col_nm).cast(pl.String).alias("invalid_value")
                )
        )
        error_frames.append(errors)

    return None if not error_frames else pl.concat(error_frames)


# ------------------------------ Range Validator ------------------------------
def parse_bound(value) -> float:
    if isinstance(value, str) and value.strip().upper() in ("INF", "-INF"):
        return float(value.strip().upper().replace("INF", "inf"))
    return float(value)


def range_validator(lf: pl.LazyFrame, range_rules: dict[str, list]) -> pl.LazyFrame | None:
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

    return None if not error_frames else pl.concat(error_frames)


# ------------------------------ Regex Validator ------------------------------
def regex_validator(lf: pl.LazyFrame, regex_rules: dict[str, str]) -> pl.LazyFrame | None:
    error_frames = []
    for col_nm, pattern in regex_rules.items():
        errors = (
            lf.filter(
                ~pl.col(col_nm).cast(pl.String).str.contains(pattern)
            )
            .select(
                "row_number",
                pl.lit(col_nm).alias("column_name"),
                pl.lit("REGEX_MISMATCH").alias("error_type"),
                pl.col(col_nm).cast(pl.String).alias("invalid_value"),
            )
        )
        error_frames.append(errors)

    return None if not error_frames else pl.concat(error_frames)


# ------------------------------ Allowed Values Validator ------------------------------
def allowed_values_validator(lf: pl.LazyFrame, allowed_values_rules: dict[str, list]) -> pl.LazyFrame | None:
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

    return None if not error_frames else pl.concat(error_frames)