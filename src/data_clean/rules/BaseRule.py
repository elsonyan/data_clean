from abc import ABC
from enum import Enum
from pyspark.pandas import DataFrame
from data_clean.cleansing_utils import Origin_Rule

# this module include all rules .
# Define multiple rules. Each field type has its own operation logic, specific functions or properties.

class Plan_type(Enum):
    STRING = "string"
    BIGINT = "bigint"
    INT = "int"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    CHAR = "char"
    DOUBLE = "double"

    def __str__(self) -> str:
        return str(self.value)


class Rule(ABC):
    def __init__(self):
        self._name: str = self.__class__.__name__

    # def __getattribute__(self, name):
    #     # use object.__getattribute__ to get the private attr
    #     try:
    #         return object.__getattribute__(self, name.lower())
    #     except Exception as e:
    #         print(e)
    #         return None

    def exec(self, df: DataFrame, origin_rule: Origin_Rule, col: str):
        """
        this function is the entry ,
        :param df: source dataframe
        :param origin_rule: the rules in yaml file . parse as a object
        :param col: Single column needs cleaning
        :return: dataframe : like df.withColumn(col, F.concat(F.col(col).cast("string"), F.lit("hello")))
        """
        operation = self._mapping_rule(getattr(origin_rule, "_rule_name"))
        return operation(df=df, origin_rule=origin_rule, col=col)

    def _mapping_rule(self, rule: str):
        attr = getattr(self, rule)
        return attr
