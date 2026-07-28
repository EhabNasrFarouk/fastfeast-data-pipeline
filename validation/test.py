import polars as pl

metadata = {
    "not_null": ["customer_id", "segment_id", "phone", "gender", "region_id", "signup_date"]
}

validation_exprs = []
for column in metadata["not_null"]:
    validation_exprs.append(
        pl.col(column).is_null().alias(f"{column}__not_null")
    )

print(validation_exprs)