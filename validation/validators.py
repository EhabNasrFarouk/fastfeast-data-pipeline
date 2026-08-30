import polars as pl
from streamlit import columns


ERROR_SCHEMA = {
    "row_number": pl.UInt32,
    "column_name": pl.String,
    "error_type": pl.String,
    "invalid_value": pl.String,
    "severity": pl.String,
}


def empty_errors() -> pl.LazyFrame:
    return pl.DataFrame(schema=ERROR_SCHEMA).lazy()


# ------------------------------ Null Validator ------------------------------
def null_validator(lf: pl.LazyFrame, not_null_rules: dict[str, str]) -> pl.LazyFrame | None:
    error_frames = []
      
    for col_nm , severity in not_null_rules.items():
        errors = (
            lf.filter(pl.col(col_nm).is_null())
                .select(
                    "row_number",
                    pl.lit(col_nm).alias("column_name"),
                    pl.lit("NULL_VALUE").alias("error_type"),
                    pl.col(col_nm).cast(pl.String).alias("invalid_value"),
                    pl.lit(severity).alias("severity"),
                )
        )
        error_frames.append(errors)

    # pl.concat(error_frames).collect()
    # print("NULL Validation", "\n------------------------------------\n")
    return empty_errors() if not error_frames else pl.concat(error_frames)


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
                    pl.col(col_nm).cast(pl.String).alias("invalid_value"),
                    pl.lit("CRITICAL").alias("severity"),
                )
        )
        error_frames.append(errors)

    # pl.concat(error_frames).collect()
    # print("Duplicate Validation", "\n------------------------------------\n")
    return empty_errors() if not error_frames else pl.concat(error_frames)


# ------------------------------ Data Type Validator ------------------------------
def data_type_validator(lf: pl.LazyFrame, columns: dict[str, list], date_formats=None) -> pl.LazyFrame | None:
    error_frames = []
    STR_TO_DTYPE = {
        "pl.Int64": pl.Int64,
        "pl.Utf8": pl.String,
        "pl.Boolean": pl.Boolean,
        "pl.Date": pl.Date,
        "pl.Timestamp": pl.Datetime,
        "pl.Decimal": pl.Float64,
        "pl.Time": pl.Time
    }

    # date_format = "%m/%d/%Y %H:%M"
    float_to_int = False
    for col_nm, data_type in columns.items():
        # Handling date formats & converting from float to int
        # if col_nm == "date_format":
        #     date_format = data_type
        #     continue
        if col_nm == "float_to_int":
            float_to_int = True
            continue


        # ----------------------------------------------------------------------------
        target_type = STR_TO_DTYPE[data_type]
        parsed_expr = pl.col(col_nm).cast(STR_TO_DTYPE[data_type], strict=False)

        if target_type == pl.Date:
            parsed_expr = pl.col(col_nm).str.to_date(date_formats[col_nm], strict=False)
        
        elif target_type == pl.Datetime:
            parsed_expr = pl.col(col_nm).str.to_datetime(date_formats[col_nm], strict=False)

        elif target_type == pl.Time:
            parsed_expr = pl.col(col_nm).str.replace(r"\.[0-9]$", "").str.to_time("%H:%M", strict=False)
        
        elif target_type == pl.Int64 and float_to_int:
            parsed_expr = pl.col(col_nm).cast(pl.Float64).cast(pl.Int64, strict=False)

        elif target_type == pl.Boolean:
            lc = pl.col(col_nm).str.to_lowercase().str.strip_chars()
            parsed_expr = (
                pl.when(lc.is_in(["true", "1", "yes", "t", "y"])).then(pl.lit(True))
                .when(lc.is_in(["false", "0", "no", "f", "n"])).then(pl.lit(False))
                .otherwise(None)
            )
        
        errors = (
            lf.filter(
                    pl.col(col_nm).is_not_null() &
                    parsed_expr.is_null()
                )
                .select(
                    "row_number",
                    pl.lit(col_nm).alias("column_name"),
                    pl.lit("INVALID_DTYPE").alias("error_type"),
                    pl.col(col_nm).cast(pl.String).alias("invalid_value"),
                    pl.lit("CRITICAL").alias("severity"),
                )
        )
        error_frames.append(errors)

    # pl.concat(error_frames).collect()
    # print("Date Type Validation", "\n------------------------------------\n")
    return empty_errors() if not error_frames else pl.concat(error_frames)

# ------------------------------ Range Validator ------------------------------
def parse_bound(value) -> float:
    if isinstance(value, str) and value.strip().upper() in ("INF", "-INF"):
        return float(value.strip().upper().replace("INF", "inf"))
    return float(value)


def range_validator(lf: pl.LazyFrame, range_rules: dict[str, list]) -> pl.LazyFrame | None:
    error_frames = []
    for col_nm, rules in range_rules.items():
        low_bound = parse_bound(rules[0])
        high_bound = parse_bound(rules[1])
        severity = rules[2]

        errors = (
            lf.filter(
                ((pl.col(col_nm).cast(pl.Float64) < low_bound) | (pl.col(col_nm).cast(pl.Float64) > high_bound))
            )
            .select(
                "row_number",
                pl.lit(col_nm).alias("column_name"),
                pl.lit("OUT_OF_RANGE").alias("error_type"),
                pl.col(col_nm).cast(pl.String).alias("invalid_value"),
                pl.lit(severity).alias("severity"),
            )
        )
        error_frames.append(errors)

    # pl.concat(error_frames).collect()
    # print("Range Validation", "\n------------------------------------\n")
    return empty_errors() if not error_frames else pl.concat(error_frames)


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
                pl.lit("WARNING").alias("severity"),
            )
        )
        error_frames.append(errors)

    # pl.concat(error_frames).collect()
    # print("Regex Validation", "\n------------------------------------\n")
    return empty_errors() if not error_frames else pl.concat(error_frames)


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
                pl.lit("WARNING").alias("severity"),
            )
        )
        error_frames.append(errors)

    # pl.concat(error_frames).collect()
    # print("Allowed Value Validation", "\n------------------------------------\n")
    return empty_errors() if not error_frames else pl.concat(error_frames)
