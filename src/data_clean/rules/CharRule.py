from data_clean.rules import Rule
from pyspark.sql import DataFrame, functions as F


class Char_Rule(Rule):
    @staticmethod
    def non_exec(df: DataFrame, col: str, *args, **kwargs) -> DataFrame:
        return df
