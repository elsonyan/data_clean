from data_clean.rules import Rule
from pyspark.sql import DataFrame, functions as F


class String_Rule(Rule):

    @staticmethod
    def null_2_string(df: DataFrame, col: str, *args, **kwargs) -> DataFrame:
        return df.withColumn(col, F.when(F.col(col).isNull(), "").otherwise(F.col(col)))

    @staticmethod
    def string_2_null(df: DataFrame, col: str, *args, **kwargs) -> DataFrame:
        return df.withColumn(col, F.when("" != F.col(col), F.col(col)).otherwise(None))

    @staticmethod
    def upper_string(df: DataFrame, col: str, *args, **kwargs) -> DataFrame:
        return df.withColumn(col, F.upper(col))


rule_detail = """
String:
    null_2_str: null -> ''
    str_2_null: '' -> null
    upper_str:  'a' -> 'A'
"""
