from pyspark.sql import DataFrame
from data_clean.rules import Rule, match_rule
from data_clean.cleansing_utils import Origin_Rule, load_yaml_rules, Queue, entire_exist, Node, intersection
from dataclasses import dataclass


# load yaml obj as a Rule obj
def load_rule(rule_detail: Origin_Rule) -> Rule:
    """

    :param rule_detail: base rule in yaml (not nested)
    :return: Rule class which extended from 'Rule'
    """
    _rule = match_rule(getattr(rule_detail, "data_type"))
    for prop in dir(rule_detail):
        if not prop.startswith('__'):  # except __ func
            attr = getattr(rule_detail, prop)
            setattr(_rule, prop, attr)
    return _rule()


def parse_rules(origin_rules: Origin_Rule, *match_rules: str) -> Queue:
    """
    This method is used to resolve nested methods and implement method priorities
    :param origin_rules: The base rule in yaml file ,convert as a origin_rules object
    :param match_rules: The rules added
    :return: Queue include base rule in yaml (not nested)
    """
    rule_queue = Queue()

    # Prioritization
    def extract_rule(_rule):
        # get rules by
        attr = getattr(origin_rules, _rule)
        if isinstance(attr, list):
            for r in attr:
                extract_rule(r)
        else:
            # if get a class , must be clean operation
            if attr.__class__.__name__ == Origin_Rule.__name__:
                setattr(attr, "_rule_name", _rule)
                rule_queue.append(attr)
            else:
                extract_rule(attr)

    for rule in match_rules:
        extract_rule(rule)
    return rule_queue


@dataclass
class Execution:
    """
    execution: include Rule class which extended from 'Rule' and columns provide
    func exec: main process logic
    """
    rule: Rule = None
    columns: tuple = None
    origin_rule: Origin_Rule = None

    def exec(self, df: DataFrame) -> DataFrame:
        def transform(_df: DataFrame, _rule: Origin_Rule, _col: str) -> DataFrame:
            return self.rule.exec(_df, _rule, _col)

        # make sure all columns exists in Dataframe
        col_list = [c[0] for c in df.dtypes if c[1].lower() == getattr(self.rule, "data_type").lower()]
        if self.columns[0] == "*":
            expect_columns = col_list
        else:
            expect_columns = intersection(col_list, list(self.columns))
        if entire_exist(col_list, expect_columns):
            for _col in expect_columns:
                df = transform(df, self.origin_rule, _col)
            return df
        else:
            raise Exception("col not matched in DataFrame")


class Cleansing:
    def __init__(self,
                 df: DataFrame = None,
                 yaml_path: str = None):
        self.df: DataFrame = df
        self.origin_rules: Origin_Rule = load_yaml_rules(yaml_path)
        self.execution_plan: Queue = Queue()
        self.rule_step: Queue = Queue()
        self.column_step: Queue = Queue()

    def add_rule(self, *rules: str):
        """
        :param rules: load rules
        :return: self (instance)
        """
        self.rule_step.append(rules)
        return self

    def add_column(self, *columns: str):
        """
        :param columns:
        :return: self (instance)
        """
        self.column_step.append(columns)
        return self

    def _arrange_execution_plan(self):
        """
        arrange the all plans in self.rule_step and self.column_step to self.execution_plan
        """
        # Rules and fields to be applied should match. Make sure each batch of fields has corresponding rules.
        if self.rule_step.size != self.column_step.size:
            raise Exception("Rule and Column plans are not matching")
        size = self.column_step.size
        for _ in range(size):
            rule = self.rule_step.shift
            column = self.column_step.shift
            # plan queue , for each 'add_column' step
            rule_sorted = parse_rules(self.origin_rules, *rule)
            while True:
                tmp_plan: Origin_Rule = rule_sorted.shift
                if not tmp_plan:
                    break
                # make sure rules has data_type attribute
                if not hasattr(tmp_plan, 'data_type'):
                    raise Exception(f"Missed 'data_type' from {tmp_plan}")
                rule: Rule = load_rule(tmp_plan)
                self.execution_plan.append(Execution(rule, column, tmp_plan))
        print("total executions:", self.execution_plan.size)

    def show_execution_plan(self):
        self._arrange_execution_plan()

        # self.execution_plan.list()

        def showNode(node: Node):
            data = node.data
            print("Rule_Class:", data.rule.__class__.__name__, "Columns:", data.columns, "Origin_Rule:", data.origin_rule._rule_name)

        # 'rule': <elson.data_clean.rules.StringRule.String_Rule object at 0x7f41a077a6b0>, 'columns': ('id', 'name', 'country'), 'origin_rule': <elson.utils.cleansing_utils.Origin_Rule object at 0x7f41b004e0b0>
        current = self.execution_plan.head
        while True:
            if not current:
                break
            showNode(current)
            current = current.next

    def exec(self):
        self.show_execution_plan()
        print("===================>")
        while True:
            execution: Execution = self.execution_plan.shift
            if not execution:
                break
            print("executing:", execution)
            self.df = execution.exec(self.df)
        return self.df

# if __name__ == '__main__':
#     # rate_df = spark.createDataFrame(data=[{'name': 'Alice', 'age': 20}])
#     @dataclass
#     class DataFrame:
#         data: int

#     rate_df: DataFrame = DataFrame(0)
#     cleansing = Cleansing(rate_df, r"C:\Users\elson.sc.yan\Desktop\stretch_goal\src\elson\data_clean\test_rules.yaml")
#     cleansing.add_rule("Rule3").add_rule("BaseRule2") \
#         .add_column("col1").add_column("col2") \
#         .exec()
