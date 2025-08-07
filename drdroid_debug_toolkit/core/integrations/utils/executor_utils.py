from typing import Dict

from google.protobuf.struct_pb2 import Struct

from core.integrations.source_api_processors.lambda_function_processor import LambdaFunctionProcessor
from core.protos.literal_pb2 import LiteralType
from core.protos.playbooks.source_task_definitions.lambda_function_task_pb2 import Lambda
from core.protos.ui_definition_pb2 import FormField
from core.protos.playbooks.playbook_commons_pb2 import PlaybookExecutionStatusType


def apply_result_transformer(result_dict, lambda_function: Lambda.Function) -> Dict:
    lambda_function_processor = LambdaFunctionProcessor(lambda_function.definition.value,
                                                        lambda_function.requirements)
    transformer_result = lambda_function_processor.execute(result_dict)
    if not isinstance(transformer_result, Dict):
        raise ValueError("Result transformer should return a dictionary")
    transformer_result = {f"${k}" if not k.startswith("$") else k: v for k, v in transformer_result.items()}
    return transformer_result


def resolve_value(value, gk, gv):
    """Replace occurrences of gk with gv in a value. If the value is a dict,
    replace in each string value in the dict."""
    if isinstance(value, str):
        return value.replace(gk, gv)
    elif isinstance(value, dict):
        return {k: (v.replace(gk, gv) if isinstance(v, str) else v) for k, v in value.items()}
    else:
        return value

def resolve_global_variables(form_fields: [FormField], global_variable_set: Struct,
                             source_task_type_def: Dict) -> (Dict, Dict):
    all_string_fields = [ff.key_name.value for ff in form_fields if ff.data_type == LiteralType.STRING]
    all_string_array_fields = [ff.key_name.value for ff in form_fields if ff.data_type == LiteralType.STRING_ARRAY]
    all_composite_fields = {}
    for ff in form_fields:
        if ff.is_composite:
            all_composite_fields[ff.key_name.value] = ff.composite_fields

    task_local_variable_map = {}
    for gk, gv in global_variable_set.items():
        if gv is None:
            raise Exception(f"Global variable {gk} is None")
        for tk, tv in source_task_type_def.items():
            if gk in tv:
                task_local_variable_map[gk] = gv
            if tk in all_string_fields:
                source_task_type_def[tk] = tv.replace(gk, gv)
            elif tk in all_string_array_fields:
                resolved_items = []
                for item in source_task_type_def[tk]:
                    resolved_items.append(resolve_value(item, gk, gv))
                source_task_type_def[tk] = resolved_items
            elif tk in all_composite_fields:
                composite_fields = all_composite_fields[tk]
                for item in source_task_type_def[tk]:
                    for cf in composite_fields:
                        if cf.data_type == LiteralType.STRING:
                            if gv is None:
                                raise Exception(f"Global variable {gk} is None")
                            item[cf.key_name.value] = item[cf.key_name.value].replace(gk, gv)
    return source_task_type_def, task_local_variable_map

def check_multiple_task_results(task_result):
    if isinstance(task_result, list):
        return True
    else:
        return False
