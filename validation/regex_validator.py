import sys
import polars as pl
from pathlib import Path

def regex_validator(lf: pl.LazyFrame, regex_rules: dict[str, str]) -> pl.LazyFrame:
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

    if not error_frames:
        return lf.clear().select(
            "row_number",
            pl.lit(None, dtype=pl.String).alias("column_name"),
            pl.lit(None, dtype=pl.String).alias("error_type"),
            pl.lit(None, dtype=pl.String).alias("invalid_value"),
            pl.lit("WARNING").alias("severity"),
        )
    return pl.concat(error_frames)


test_df = pl.DataFrame({
    "row_number": [1, 2, 3, 4, 5, 6, 7, 8],
    "phone": [
        "01012345678",   # valid (010)
        "01198765432",   # valid (011)
        "01234567890",   # valid (012)
        "01512345678",   # INVALID - starts 015, not in [0-25]
        "0101234567",    # INVALID - only 10 digits
        "010123456789",  # INVALID - 12 digits
        "01A12345678",   # INVALID - contains a letter
        None,            # null - should be skipped by regex_validator
    ],
    "email": [
        "ahmed@example.com",     # valid
        "sara.k@company.co",     # valid
        "not-an-email",          # INVALID - no @
        "double@@example.com",   # INVALID - technically matches your regex actually, see note below
        "spaced user@example.com",  # INVALID - contains a space before @
        "user@nodot",            # INVALID - no . after @
        "user@example.com",      # valid
        None,                    # null - should be skipped
    ],
})

test_lf = test_df.lazy()

regex_rules = {
    "phone": r"^01[0-25][0-9]{8}$",
    "email": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
}

result = regex_validator(test_lf, regex_rules).collect()
print(result)