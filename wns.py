import json
simple1_select_rule = {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'wns_po_accrual_receipt_report',
            'select_column': 'Expense/capex Account Description',
            'lookup_filters':[
                {
                    'column_name': 'SRN GRN No',
                    'compare_with':  {'source':'input_config','table':'ocr','column':'srn_number'}
                }
            ]
        }
}
simple1_assignyesg_rule= {'rule_type': 'static',
                'function': 'Assign',
                'parameters': {'assign_table':{'table':'ocr', 'column':'gl_description'}, 
                       'assign_value':{'source':'rule', 'value':simple1_select_rule}
                      }
}
CompareKeyValuese_rule = {'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'input_config','table':'ocr','column':'po_flag'},
                       'operator':'==',
                       'right_param':{'source':'input', 'value':'PO'}
                      }
       }
conditionbranch = { 'rule_type' : 'condition',
                'evaluations': [{'conditions':[CompareKeyValuese_rule], 'executions':[simple1_assignyesg_rule]}
                               ]
                }
print(json.dumps(conditionbranch))                