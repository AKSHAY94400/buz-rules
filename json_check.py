import json

CompareKeyValueyes_rule = {
                        'rule_type': 'static',
                        'function': 'CompareKeyValue',
                        'parameters': {'left_param':{'source':'input_config', 'table':'ocr','column':'bank_name'},
                       'operator':'==',
                       'right_param':{'source':'input', 'value':''}
                      }    
}

simple_select2_rule = {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'vendor_master',
            'select_column': 'bank_name',
            'lookup_filters':[
                {
                    'column_name': 'vendor_name',
                    'compare_with':  {'source':'input_config', 'table':'ocr','column':'vendor_name'}
                }
            ]
        }
}

imple_assignyes_rule= {'rule_type': 'static',
                'function': 'Assign',
                'parameters': {'assign_table':{'table':'ocr', 'column':'bank_name'}, 
                'assign_value':{'source':'rule', 'value':simple_select2_rule}
                      }
}

condition_rule = {'rule_type' : 'condition',
            'evaluations' :[ {'conditions': [CompareKeyValueyes_rule], 'executions':[imple_assignyes_rule]}
                                       
                ]
}


x = json.dumps(condition_rule)
print(x)