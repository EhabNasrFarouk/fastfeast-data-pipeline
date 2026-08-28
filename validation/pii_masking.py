import polars as pl
from config.config_loader import load_metadata
def hash_expr(col_name: str) -> pl.Expr:

    return (
        pl.col(col_name)
        .hash()
        .cast(pl.String)
        .alias(col_name)
    )

def mask_expr(col_name:str) -> pl.Expr:
    col = pl.col(col_name)
    length = col.str.len_chars()
    return (
        pl.when(col.is_null())
        .then(pl.lit(None,dtype=pl.String))
        .when(length <= 4)
        .then(col.str.replace_all(r".","*"))
        .otherwise(
            pl.concat_str(
                [
                    col.str.slice(0,2),
                    pl.lit("*").repeat_by(length - 4).list.join(""),
                    col.str.slice(-2,2)
                ]
            )
        )
        .alias(col_name)
    )

STRATEGIES = {
    "mask":mask_expr,
    "hash": hash_expr
}


def pii_mask(lf : pl.LazyFrame,layer_key:str, source_table: str) -> pl.LazyFrame:

  config  = load_metadata()[layer_key][source_table]
  pii_rules = config.get("pii", {})
  expressions  = [
     STRATEGIES[strategy](col_name)
     for col_name , strategy in pii_rules.items()
  ]    

  return  lf.with_columns(expressions) if expressions else lf