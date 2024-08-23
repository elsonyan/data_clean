from data_clean.rules.BaseRule import Plan_type, Rule
from data_clean.rules.BigIntRule import BigInt_Rule
from data_clean.rules.BoolRule import Bool_Rule
from data_clean.rules.CharRule import Char_Rule
from data_clean.rules.DateRule import Date_Rule
from data_clean.rules.DoubleRule import Double_Rule
from data_clean.rules.FloatRule import Float_Rule
from data_clean.rules.IntRule import Int_Rule
from data_clean.rules.StringRule import String_Rule
from data_clean.rules.TimestampRule import Timestamp_Rule


def match_rule(plan_type: Plan_type) -> Rule.__class__:
    # base_module = "elson.data_clean.rules"
    # module = import_module(base_module)
    global result
    s = str(plan_type)
    if s == "string":
        result = String_Rule
    elif s == "bigint":
        result = BigInt_Rule
    elif s == "int":
        result = Int_Rule
    elif s == "boolean":
        result = Bool_Rule
    elif s == "date":
        result = Date_Rule
    elif s == "timestamp":
        result = Timestamp_Rule
    elif s == "char":
        result = Char_Rule
    elif s == "double":
        result = Double_Rule
    elif s == "float":
        result = Float_Rule

    return result


def external_rule(date_type):
    def decorator_wrapper(func):
        global result
        s = date_type.lower()
        if s == "string":
            result = String_Rule
        elif s == "bigint":
            result = BigInt_Rule
        elif s == "int":
            result = Int_Rule
        elif s == "boolean":
            result = Bool_Rule
        elif s == "date":
            result = Date_Rule
        elif s == "timestamp":
            result = Timestamp_Rule
        elif s == "char":
            result = Char_Rule
        elif s == "double":
            result = Double_Rule
        elif s == "float":
            result = Float_Rule
        func = staticmethod(func)
        setattr(result, func.__name__, func)

        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return decorator_wrapper


def show_rules():
    module_name = 'elson.data_clean.rules'
    from importlib import import_module
    # detail_ = [m.rule_detail for m in dir(modules) if dir(m).__contains__("rule_detail")]
    # print("\n".join(detail_))
    pass
