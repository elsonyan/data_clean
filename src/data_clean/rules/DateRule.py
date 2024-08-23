from data_clean.rules import Rule
from pyspark.sql import DataFrame


class Date_Rule(Rule):
    @staticmethod
    def non_exec(df: DataFrame, col: str, *args, **kwargs) -> DataFrame:
        return df
