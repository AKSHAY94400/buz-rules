import Lib
import _StaticFunctions
import _BooleanReturnFunctions
import _AssignFunction
import pandas as pd
import json


# comment below two for local testing
#from ace_logger import Logging
#logging = Logging()

# uncomment these below lines for local testing
import logging 
logger=logging.getLogger() 
logger.setLevel(logging.DEBUG) 



@Lib.add_methods_from(_StaticFunctions, _BooleanReturnFunctions, _AssignFunction) 
class BusinessRules():
    
    def __init__(self, case_id, rules, table_data, decision = False):
        self.case_id = case_id
        self.rules = rules
        self.data_source = table_data
        self.is_decision = decision

        # fields which we are maintaining
        self.changed_fields = {}
        self.params_data = {}
        self.params_data['input'] = []

    def evaluate_business_rules(self):
        """Evaluate all the rules"""
        for rule in self.rules:
            logging.info("\n Evaluating the rule: " +f"{rule} \n")
            output = self.evaluate_rule(rule)
            logging.info("\n Output: " +f"{output} \n")
        # update the changes fields in database
        logging.info(f"\nchanged fields are \n{self.changed_fields}\n")
        return self.changed_fields
    
    def evaluate_rule(self,rule):
        """Evaluate the rule"""
        logging.info(f"\nEvaluating the rule \n{rule}\n")

        rule_type = rule['rule_type']
        
        if  rule_type == 'static':
            function_name = rule['function']
            parameters = rule['parameters']
            return  self.evaluate_static(function_name, parameters)
    
        if rule_type == 'condition':
            evaluations = rule['evaluations']
            return self.evaluate_condition(evaluations)
    
    def conditions_met(self, conditions):
        """Evaluate the conditions and give out the final decisoin
        
        """
        eval_string = ''
        # return True if there are no conditions...that means we are doing else..
        if not conditions:
            return True
        # evaluate the conditions
        for condition in conditions:
            logging.info(f"Evaluting the condition {condition}")
            if condition == 'AND' or condition == 'OR':
                eval_string += ' '+condition.lower()+' '
            else:
                eval_string += ' '+str(self.evaluate_rule(condition))+' '
        logging.info(f"\n eval string is {eval_string} \n output is {eval(eval_string)}")
        return eval(eval_string)

    def evaluate_condition(self, evaluations):
        """Execute the conditional statements.

        Args:
            evaluations(dict) 
        Returns:
            decision(boolean) If its is_decision.
            True If conditions met and it is done with executions.
            False For any other case (scenario).
        """
        for each_if_conditions in evaluations:
            conditions = each_if_conditions['conditions']
            executions = each_if_conditions['executions']
            logging.info(f'\nconditions got are \n{conditions}\n')
            logging.info(f'\nexecutions got are \n{executions}\n')
            decision = self.conditions_met(conditions)
            
            """
            Why this self.is_decision and decision ?
                In decison tree there are only one set of conditions to check
                But other condition rules might have (elif conditions which needs to be evaluate) 
            """
            if self.is_decision:
                if decision:
                    for rule in executions:
                        self.evaluate_rule(rule)
                logging.info(f"\n Decision got for the (for decision tree) condition\n {decision}")    
                return decision
            if decision:
                for rule in executions:
                    self.evaluate_rule(rule)
                return True
        return False

    def get_param_value(self, param_object):
        """Returns the parameter value.

        Args:
            param_object(dict) The param dict from which we will parse and get the value.
        Returns:
            The value of the parameter
        Note:
            It returns a value of type we have defined. 
            If the parameter is itself a rule then it evaluates the rule and returns the value.
        """
        logging.info(f"\nPARAM OBJECT IS {param_object}\n")
        param_source = param_object['source']
        if param_source == 'input_config':
            table_key = param_object['table']
            column_key = param_object['column']
            table_key = table_key.strip() # strip for extra spaces
            column_key = column_key.strip() # strip for extra spaces
            logging.debug(f"\ntable is {table_key} and column key is {column_key}\n")
            try:
                data = {}
                # update params data
                data['type'] = 'from_table'
                data['table'] = table_key
                data['column'] = column_key
                data['value'] = self.data_source[table_key][column_key]
                self.params_data['input'].append(data)
                return data['value']
            except Exception as e:
                logging.error(f"\ntable or column key not found\n")
                logging.error(str(e))
                logging.info(f"\ntable data is {self.data_source}\n")
        if param_source == 'rule':
            param_value = param_object['value']
            return self.evaluate_rule(param_value)
        if param_source == 'input':
            param_value = param_object['value']
            # param_value = str(param_value).strip() # converting into strings..need to check
            return  param_value

#####################################  DELOITTE ##################################################

########################################## Invoice Header_rule ######################################################
Taxinvoice = {'rule_type':'static',
        'function': 'Contains_string',
        'parameters' :{'word':{'source':'input_config','table':'ocr','column':'Invoice Header'},
                       'strings_list':['proforma','proforma invoice'],      
        }
}

move_queue = {'rule_type':'static',
        'function': 'AssignQ',
        'parameters' :{'assign_table':{'source':'input_config','table':'process_queue','column':'queue'},
                       'assign_value':{'source':'input','value':'checker'},      
        }
}
######################################### isResident rule #######################################################

simple_select_rule = {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'vendor_master',
            'select_column': 'country',
            'lookup_filters':[
                {
                    'column_name': 'Vendor_Name',
                    'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Vendor_Name'}
                }
            ]
        }
}

compare_resident_rule = { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':simple_select_rule},
                       'operator':'==',
                       'right_param':{'source':'input', 'value': 'India'}
        }
}


move_queue = {'rule_type':'static',
        'function': 'AssignQ',
        'parameters' :{'assign_table':{'source':'input_config','table':'process_queue','column':'queue'},
                       'assign_value':{'source':'input','value':'checker'},      
        }
}

########################################## Client_Name check ######################################

Client_Name_rule = {'rule_type':'static',
                    'function':'Contains',
                    'parameters': { 'table_name': 'client_master','column_name': 'Client_Name',
                                'value':{'source':'input_config', 'table':'ocr', 'column':'Client_Name'}
                        }
            }

######################################### PAN rules #############################################
compare_PAN_empty_rule = { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'input_config', 'table':'ocr', 'column':'PAN'},
                       'operator':'==',
                       'right_param':{'source':'input', 'value': ''}
        }
}

pan_contains_rule = {'rule_type':'static',
                    'function':'Contains',
                    'parameters': { 'table_name': 'vendor_master','column_name': 'Vendor_Name',
                                'value':{'source':'input_config', 'table':'ocr', 'column':'PAN'}
                        }
            }

pan_select_rule = {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'Vendor Master',
            'select_column': 'PAN',
            'lookup_filters':[
                {
                    'column_name': 'Vendor Name',
                    'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Vendor Name'}
                }
            ]
        }
}

assign_from_rule = { 'rule_type': 'static',
        'function': 'Assign',
        'parameters': { 'assign_table':{'source':'input_config','table':'ocr', 'column':'Vendor PAN'},
                        'assign_value':{'source':'rule', 'value':pan_select_rule}
        }
}


pan_compare= { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'input_config', 'table':'ocr', 'column':'Vendor PAN'},
                       'operator':'==',
                       'right_param':{'source':'rule', 'value': pan_select_rule}
        }
}

######################################## invoice_total_rule ###################################################
invoice_total_rule = {
    'rule_type':'static',
    'function': 'Transform',
    'parameters':[
            {'param':{'source':'input_config', 'table':'ocr', 'column':'line_amount'}},
            {'operator':'+'},
            {'param':{'source':'input_config', 'table':'ocr', 'column':'gst_amount'}},
            {'operator':'+'},
            {'param':{'source':'input_config', 'table':'ocr', 'column':'tds_amount'}}
            
    ]
}

############################## compare taxable_amount #######################################

compare_taxable_amount_rule = { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'input_config', 'table':'ocr', 'column':'taxable_amount'},
                       'operator':'<=',
                       'right_param':{'source':'input', 'value': 10000}
        }
}

#################################### pincode empty rule ########################################

compare = {'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'input_config', 'table':'ocr', 'column':'address'},
                       'operator':'==',
                       'right_param':{'source':'input', 'value':''}
                      }
       }

select_GSTIN = {
                    'rule_type':'static',
                    'function': 'Select',
                    'parameters': {
                            'from_table': 'vendor_master',
                            'select_column': 'GSTIN',
                            'lookup_filters':[
                                {
                                    'column_name': 'Vendor_Name',
                                    'compare_with':  {'source':'input_config', 'table':'ocr','column':'Vendor_Name'}
                                }
                            ]
                        }
                }


Assign = {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'ocr', 'column':'GSTIN'},
                            'assign_value':{'source':'rule','value': select_GSTIN }
                            }
        }


condition1 = { 'rule_type' : 'condition',
                'evaluations': [{ 'conditions':[compare], 'executions':[Assign ]}]
                }

######################################### PAN validation ##################################
get_PAN_length_rule = {
    'rule_type':'static',
    'function': 'GetLength',
    'parameters': {'param':{'source':'input_config', 'table':'ocr', 'column': 'PAN'},
                      }
}

Compare_PAN_length_rule = {'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':get_PAN_length_rule},
                       'operator':'==',
                       'right_param':{'source':'input', 'value':10}
                      }
       }

get_range_rule1 = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'input_config', 'table':'ocr', 'column': 'PAN'},
                    'range':{'start_index': 0, 'end_index': 5}
                }
}

get_range_rule2 = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'input_config', 'table':'ocr', 'column': 'PAN'},
                    'range':{'start_index': 5, 'end_index': 9}
                }
}

get_range_rule3 = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'input_config', 'table':'ocr', 'column': 'PAN'},
                    'range':{'start_index': 9, 'end_index': 10}
                }
}

alpha_rule= {'rule_type':'static',
        'function': 'Alnum_num_alpha',
        'parameters' :{'word':{'source':'rule','value':get_range_rule1},
                       'option':'alpha',      
        }
}

alpha_rule2= {'rule_type':'static',
        'function': 'Alnum_num_alpha',
        'parameters' :{'word':{'source':'rule','value':get_range_rule3},
                       'option':'alpha',      
        }
}

num_rule= {'rule_type':'static',
        'function': 'Alnum_num_alpha',
        'parameters' :{'word':{'source':'rule','value':get_range_rule2},
                       'option':'numeric',      
        }
}

alnum_rule= {'rule_type':'static',
        'function': 'Alnum_num_alpha',
        'parameters' :{'word':{'source':'input_config','table':'ocr','column':'PAN'},
                       'option':'alnum',      
        }
}

pan_check= { 'rule_type' : 'condition',
                'evaluations': [{ 'conditions':[Compare_PAN_length_rule,'AND', alpha_rule,'AND',alpha_rule2,'AND',num_rule], 'executions':[]}]
                }

###################################### Pincode regex ############################################
pincode_regex = {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'phrase':{'source':'input_config','table':'ocr','column':'Address_of_Vendor'},
                       'regex_str':"[^0-9][0-9]{6,6}[^0-9]",
                       'reg_model':'search'      
        }
        }

pincode_range  = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'rule','value':pincode_regex},
                    'range':{'start_index': 1, 'end_index': 7}
                }
}

pincode_empty_check= {'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':pincode_range},
                       'operator':'==',
                       'right_param':{'source':'input', 'value':''}
                      }
       }

Get_GST_Validation_from_pincode = {
                        'rule_type':'static',
                        'function':'Select',
                        'parameters': {
                                'from_table': 'Pincode_GST_Validation',
                                'select_column': 'GST_Code',
                                'lookup_filters':[
                                    {
                                        'column_name': 'pincode',
                                        'compare_with':  {'source':'rule', 'value':pincode_range}
                                    }
                                ]
                            }
                    }


pincode_zfill = {'rule_type':'static',
        'function': 'Zfill',
        'parameters' :{'value':{'source':'rule','value':Get_GST_Validation_from_pincode},
                       'zfill_no':2,      
        }
        }

vendor_GSTIN_zfill = {'rule_type':'static',
        'function': 'Zfill',
        'parameters' :{'value':{'source':'input_config','table':'ocr','column':'Vendor_GSTIN'},
                       'zfill_no':2,      
        }
        }

Compare_GSTIN_rule = {'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':pincode_zfill},
                       'operator':'==',
                       'right_param':{'source':'rule', 'value':vendor_GSTIN_zfill}
                      }
       }


select_GSTIN_from_vendor_master = {
                        'rule_type':'static',
                        'function':'Select',
                        'parameters': {
                                'from_table': 'vendor_master',
                                'select_column': 'GSTIN',
                                'lookup_filters':[
                                    {
                                        'column_name': 'Vendor_Name',
                                        'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Vendor_Name'}
                                    }
                                ]
                            }
                    }

Assign_VendorGSTIN= {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'ocr', 'column':'Vendor_GSTIN'},
                            'assign_value':{'source':'rule','value': select_GSTIN_from_vendor_master }
                            }
        }
############################################# NON_GST_Validation rule ######################################################
Compare_ocr_GSTIN= { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'input_config', 'table':'ocr', 'column':'Vendor_GSTIN'},
                       'operator':'==',
                       'right_param':{'source':'input', 'value': ""}
        }
}

Compare_vendor_master_GSTIN= { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':select_GSTIN_from_vendor_master},
                       'operator':'==',
                       'right_param':{'source':'input', 'value': ""}
        }
}
 
tolerence_calculate = {'rule_type':'static',
    'function': 'Transform',
    'parameters':[
            {'param':{'source':'input_config', 'table':'ocr', 'column':'Total_Value'}},
            {'operator':'-'},
            {'param':{'source':'input_config', 'table':'ocr', 'column':'Net_Payable_Amount'}},    
    ]
}
tolerence_check = { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':tolerence_calculate},
                       'operator':'>',
                       'right_param':{'source':'input', 'value': 1}
        }
}

gstin_emptyy= { 'rule_type' : 'condition',
                'evaluations': [{ 'conditions':[Compare_ocr_GSTIN,'AND', Compare_vendor_master_GSTIN], 'executions':[]}]
                }
######################################### GSTIN validation ##################################
get_GSTIN_length_rule = {
    'rule_type':'static',
    'function': 'GetLength',
    'parameters': {'param':{'source':'input_config', 'table':'ocr', 'column': 'Vendor_GSTIN'},
                      }
}

Compare_GSTIN_length_rule = {'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':get_GSTIN_length_rule},
                       'operator':'==',
                       'right_param':{'source':'input', 'value':15}
                      }
       }

get_range_rule1 = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'input_config', 'table':'ocr', 'column': 'Vendor_GSTIN'},
                    'range':{'start_index': 0, 'end_index': 2}
                }
}

get_range_rule2 = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'input_config', 'table':'ocr', 'column': 'Vendor_GSTIN'},
                    'range':{'start_index': 2, 'end_index': 12}
                }
}

get_range_rule3 = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'input_config', 'table':'ocr', 'column': 'Vendor_GSTIN'},
                    'range':{'start_index': 12, 'end_index': 13}
                }
}



get_range_rule4 = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'input_config', 'table':'ocr', 'column': 'Vendor_GSTIN'},
                    'range':{'start_index': 13, 'end_index': 14}
                }
}



get_range_rule5 = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'input_config', 'table':'ocr', 'column': 'Vendor_GSTIN'},
                    'range':{'start_index': 14, 'end_index': 15}
                }
}

select_satecode_from_Pincode_GST_Validation = {
                        'rule_type':'static',
                        'function':'Select',
                        'parameters': {
                                'from_table': 'Pincode_GST_Validation',
                                'select_column': 'GST_Code',
                                'lookup_filters':[
                                    {
                                        'column_name': 'pincode',
                                        'compare_with':  {'source':'rule', 'value':pincode_range}
                                    }
                                ]
                            }
                    }

GST_state_code_zfill = {'rule_type':'static',
        'function': 'Zfill',
        'parameters' :{'value':{'source':'rule','value':select_satecode_from_Pincode_GST_Validation},
                       'zfill_no':2,      
        }
        }

check_rule1= {'rule_type':'static',
              'function': 'CompareKeyValue',
              'parameters': {'left_param':{'source':'rule', 'value':get_range_rule1},
                             'operator':'==',
                             'right_param':{'source':'rule', 'value':GST_state_code_zfill}
                      }
       }


check_rule2= {'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':get_range_rule2},
                       'operator':'==',
                       'right_param':{'source':'input_config', 'table':'ocr', 'column':'Vendor_PAN'}
                      }
       }


check_rule3= {'rule_type':'static',
        'function': 'Alnum_num_alpha',
        'parameters' :{'word':{'source':'rule','value':get_range_rule3},
                       'option':'numeric',      
        }
}

check_rule4 = {'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':get_range_rule4},
                       'operator':'==',
                       'right_param':{'source':'input', 'value':'Z'}
                      }
       }


check_rule5 = {'rule_type':'static',
        'function': 'Alnum_num_alpha',
        'parameters' :{'word':{'source':'rule','value':get_range_rule5},
                       'option':'alnum',      
        }
}

GSTIN_check= { 'rule_type' : 'condition',
                'evaluations': [{ 'conditions':[Compare_GSTIN_length_rule,'AND', check_rule1,'AND',check_rule2,'AND', check_rule3,'AND',check_rule4, 'AND' ,check_rule5], 'executions':[]}]
                }

#################################  CGST or SGST/UTGST Calculation #################################################

Vendor_pincode_regex = {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'phrase':{'source':'input_config','table':'ocr','column':'Address_of_Vendor'},
                       'regex_str':"[^0-9][0-9]{6,6}[^0-9]",
                       'reg_model':'search'      
        }
        }

Vendor_pincode_range  = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'rule','value':Vendor_pincode_regex},
                    'range':{'start_index': 1, 'end_index': 7}
                }
}

Client_pincode_regex = {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'phrase':{'source':'input_config','table':'ocr','column':'Address_of_Client'},
                       'regex_str':"[^0-9][0-9]{6,6}[^0-9]",
                       'reg_model':'search'      
        }
        }

Client_pincode_range  = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'rule','value':Client_pincode_regex},
                    'range':{'start_index': 1, 'end_index': 7}
                }
}

comparing_vendor_client_pincode = {'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':Vendor_pincode_range},
                       'operator':'==',
                       'right_param':{'source':'rule', 'value':Client_pincode_range}
                      }
       }



select_GSTgroupcode_vendor_master = {
                        'rule_type':'static',
                        'function':'Select',
                        'parameters': {
                                'from_table': 'vendor_master',
                                'select_column': 'GST_groupcode',
                                'lookup_filters':[
                                    {
                                        'column_name': 'Vendor_Name',
                                        'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Vendor_Name'}
                                    }
                                ]
                            }
                    }

GST_groupcode = {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'phrase':{'source':'rule', 'value':select_GSTgroupcode_vendor_master},
                       'regex_str':"[0-9]*$",
                       'reg_model':'search'      
        }
        }

half_percent_Lineamount = {
    'rule_type':'static',
    'function': 'Transform',
    'parameters':[
            {'param':{'source':'input_config', 'table':'ocr','column':'line_amount'}},
            {'operator':'*'},
            {'param':{'source':'rule', 'value':GST_groupcode}},
            {'operator':'/'},
            {'param':{'source':'input', 'value':'2'}},
            {'operator':'/'},
            {'param':{'source':'input', 'value':'100'}},
    ]
}

Assign_CGST= {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'ocr', 'column':'GST/CGST Amount'},
                            'assign_value':{'source':'rule','value': half_percent_Lineamount }
                            }
        }

Assign_SGST_UGST= {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'ocr', 'column':'GST/SGST Amount'},
                            'assign_value':{'source':'rule','value': half_percent_Lineamount }
                            }
        }


tolerence_netpayableamount_and_gsts_total = {
                                            'rule_type':'static',
                                            'function': 'Transform',
                                            'parameters':[
                                                    {'param':{'source':'input_config', 'table':'ocr','column':'Net_Payable_Amount'}},
                                                    {'operator':'-'},
                                                    {'param':{'source':'input_config', 'table':'ocr','column':'line_amount'}},
                                                    {'operator':'+'},
                                                    {'param':{'source':'input_config', 'table':'ocr','column':'GST/CGST Amount'}},
                                                    {'operator':'+'},
                                                    {'param':{'source':'input_config', 'table':'ocr','column':'GST/SGST Amount'}},
                                            ]
}

Compare_tolerence1= { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':tolerence_netpayableamount_and_gsts_total},
                       'operator':'>',
                       'right_param':{'source':'input', 'value': 1}
        }
}
Compare_tolerence2= { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':tolerence_netpayableamount_and_gsts_total},
                       'operator':'<',
                       'right_param':{'source':'input', 'value': -1}
        }
}

tolerence_condition = { 'rule_type' : 'condition',
                'evaluations': [{ 'conditions':[Compare_tolerence1,'OR', Compare_tolerence2], 'executions':[]}]
                }

######################################################### IGST ##############################
select_GSTgroupcode_vendor_master = {
                        'rule_type':'static',
                        'function':'Select',
                        'parameters': {
                                'from_table': 'vendor_master',
                                'select_column': 'GST_groupcode',
                                'lookup_filters':[
                                    {
                                        'column_name': 'Vendor_Name',
                                        'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Vendor_Name'}
                                    }
                                ]
                            }
                    }


GST_groupcode = {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'phrase':{'source':'rule', 'value':select_GSTgroupcode_vendor_master},
                       'regex_str':"[0-9]*$",
                       'reg_model':'search'      
        }
        }


percent_Lineamount = {
    'rule_type':'static',
    'function': 'Transform',
    'parameters':[
            {'param':{'source':'input_config', 'table':'ocr','column':'line_amount'}},
            {'operator':'*'},
            {'param':{'source':'rule', 'value':GST_groupcode}},
            {'operator':'/'},
            {'param':{'source':'input', 'value':'100'}},
    ]
}


Assign_IGST= {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'ocr', 'column':'GST/IGST Amount' },
                            'assign_value':{'source':'rule','value': percent_Lineamount }
                            }
        }


tolerence_netpayableamount_and_gsts_total = {
                                            'rule_type':'static',
                                            'function': 'Transform',
                                            'parameters':[
                                                    {'param':{'source':'input_config', 'table':'ocr','column':'Net_Payable_Amount'}},
                                                    {'operator':'-'},
                                                    {'param':{'source':'input_config', 'table':'ocr','column':'line_amount'}},
                                                    {'operator':'+'},
                                                    {'param':{'source':'input_config', 'table':'ocr','column':'GST/IGST Amount' }}
                                
                                            ]
}

Compare_tolerence1= { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':tolerence_netpayableamount_and_gsts_total},
                       'operator':'>',
                       'right_param':{'source':'input', 'value': 1}
        }
}
Compare_tolerence2= { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':tolerence_netpayableamount_and_gsts_total},
                       'operator':'<',
                       'right_param':{'source':'input', 'value': -1}
        }
}

tolerence_condition = { 'rule_type' : 'condition',
                'evaluations': [{ 'conditions':[Compare_tolerence1,'OR', Compare_tolerence2], 'executions':[]}]
                }
########################################### RCM #################################################

RCM_select_rule = {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'vendor_master',
            'select_column': 'RCM_applicability',
            'lookup_filters':[
                {
                    'column_name': 'Vendor_Name',
                    'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Vendor_Name'}
                }
            ]
        }
}

compare_RCM = { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':RCM_select_rule},
                       'operator':'==',
                       'right_param':{'source':'input', 'value': 'YES'}
        }
}

select_GSTgroupcode_vendor_master = {
                        'rule_type':'static',
                        'function':'Select',
                        'parameters': {
                                'from_table': 'vendor_master',
                                'select_column': 'GST_groupcode',
                                'lookup_filters':[
                                    {
                                        'column_name': 'Vendor_Name',
                                        'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Vendor_Name'}
                                    }
                                ]
                            }
                    }

GST_groupcode = {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'phrase':{'source':'rule', 'value':select_GSTgroupcode_vendor_master},
                       'regex_str':"[0-9]*$",
                       'reg_model':'search'      
        }
        }

half_percent_Lineamount = {
    'rule_type':'static',
    'function': 'Transform',
    'parameters':[
            {'param':{'source':'input_config', 'table':'ocr','column':'line_amount'}},
            {'operator':'*'},
            {'param':{'source':'rule', 'value':GST_groupcode}},
            {'operator':'/'},
            {'param':{'source':'input', 'value':'2'}},
            {'operator':'/'},
            {'param':{'source':'input', 'value':'100'}},
    ]
}

Assign_CGST_RCM= {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'ocr', 'column':'GST/CGST RCM Amount'},
                            'assign_value':{'source':'rule','value': half_percent_Lineamount }
                            }
        }
######################################## ITC Rule ################################################

get_itc_value = {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'vendor_master',
            'select_column': 'ITC account (Debit)',
            'lookup_filters':[
                {
                    'column_name': 'Vendor_Name',
                    'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Vendor_Name'}
                }
            ]
        }
}

check_itc_isnum =  {'rule_type':'static',
                    'function': 'Alnum_num_alpha',
                    'parameters' :{'word':{'source':'rule','value':get_itc_value},
                    'option':'is_numeric',      
        }
}



Vendor_pincode_regex = {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'phrase':{'source':'input_config','table':'ocr','column':'Address_of_Vendor'},
                       'regex_str':"[^0-9][0-9]{6,6}[^0-9]",
                       'reg_model':'search'      
        }
        }

Vendor_pincode_range  = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'rule','value':Vendor_pincode_regex},
                    'range':{'start_index': 1, 'end_index': 7}
                }
}

Client_pincode_regex = {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'phrase':{'source':'input_config','table':'ocr','column':'Address_of_Client'},
                       'regex_str':"[^0-9][0-9]{6,6}[^0-9]",
                       'reg_model':'search'      
        }
        }

Client_pincode_range  = {
    'rule_type':'static',
    'function': 'GetRange',
    'parameters': { 'value':{'source':'rule','value':Client_pincode_regex},
                    'range':{'start_index': 1, 'end_index': 7}
                }
}

comparing_vendor_client_pincode = {'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':Vendor_pincode_range},
                       'operator':'==',
                       'right_param':{'source':'rule', 'value':Client_pincode_range}
                      }
       }

Assign_itc_IGST= {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'ocr', 'column':'GST/IGST ITC Amount' },
                            'assign_value':{'source':'input_config', 'table':'ocr', 'column':'GST/IGST Amount'}
                            }
        }




Assign_itc_CGST= {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'ocr', 'column':'GST/CGST ITC Amount' },
                            'assign_value':{'source':'input_config', 'table':'ocr', 'column':'GST/CGST Amount'}
                            }
        }



Assign_itc_SGST= {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'ocr', 'column':'GST/SGST ITC Amount' },
                            'assign_value':{'source':'input_config', 'table':'ocr', 'column':'GST/SGST Amount'}
                            }
        }


ITC_condition = { 'rule_type' : 'condition',
                'evaluations': [{ 'conditions':[check_itc_isnum,'OR', comparing_vendor_client_pincode], 'executions':[Assign_itc_IGST,Assign_itc_CGST,Assign_itc_SGST]}]
                }

################################################## BANK_ACC AND IFSC ################################################
Bank_acc_select = {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'vendor_master',
            'select_column': 'Bank Account Number',
            'lookup_filters':[
                {
                    'column_name': 'Vendor_Name',
                    'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Vendor_Name'}
                }
            ]
        }
}

compare_bank_acc = { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':Bank_acc_select},
                       'operator':'==',
                       'right_param':{'source':'input_config', 'table': 'ocr', 'column':'Bank account number'}
        }
}

Bank_ifsc_select = {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'vendor_master',
            'select_column': 'IFSC Code',
            'lookup_filters':[
                {
                    'column_name': 'Vendor_Name',
                    'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Vendor_Name'}
                }
            ]
        }
}

compare_bank_ifsc = { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':Bank_ifsc_select},
                       'operator':'==',
                       'right_param':{'source':'input_config', 'table': 'ocr', 'column':'IFSC Code'}
        }
}

acc_and_ifsc_condition= { 'rule_type' : 'condition',
                'evaluations': [{ 'conditions':[compare_bank_acc,'AND', compare_bank_ifsc], 'executions':[]}]
                }

######################################## Credit_Note_Amount #######################################

Taxinvoice_credit = {'rule_type':'static',
        'function': 'Contains_string',
        'parameters' :{'word':{'source':'input_config','table':'ocr','column':'Invoice Header'},
                       'strings_list':['credit note']
        }
}

amount_trans_negative = {
    'rule_type':'static',
    'function': 'Transform',
    'parameters':[
            {'param':{'source':'input_config', 'table':'ocr', 'column':'Total value'}},
            {'operator':'*'},
            {'param':{'source':'input', 'value':-1}},       
    ]
}


Assign_neg_amount= {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'ocr', 'column':'Total value' },
                            'assign_value':{'source':'rule', 'value':amount_trans_negative}
                            }
        }

amount_trans_negative_condition = { 'rule_type' : 'condition',
                'evaluations': [{ 'conditions':[Taxinvoice_credit], 'executions':[Assign_neg_amount , Assign_neg_amount, Assign_neg_amount , Assign_neg_amount , Assign_neg_amount , Assign_neg_amount , Assign_neg_amount , Assign_neg_amount , Assign_neg_amount]}]
                }


###################################### LTD AND TDS #########################################

client_id_rule =  {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'client_master',
            'select_column': 'Client ID',
            'lookup_filters':[
                {
                    'column_name': 'Client_Name',
                    'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Client_Name'}
                }
            ]
        }
}


tds_code_rule =  {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'vendor_master',
            'select_column': 'TDS Code',
            'lookup_filters':[
                {
                    'column_name': 'Vendor_Name',
                    'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Vendor_Name'}
                }
            ]
        }
}

tds_percent =  {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'TDS Master',
            'select_column': 'TDS Rates(in%)',
            'lookup_filters':[
                {
                    'column_name': 'Code for mapping',
                    'compare_with':  {'source':'input', 'value':tds_code_rule}
                }
            ]
        }
}

ltd_tds= {'rule_type': 'static',
                        'function': 'LTDandTDScalc',
                        'parameters': {'Assessable_Value':{'source':'input_config','table':'ocr','column':'Assessable Value'},
                                        'client_name':{'source':'input_config','table':'ocr','column':'Client_Name'},
                                        'client_id':{'source':'rule','value':client_id_rule},
                                        'ven_name':{'source':'input_config','table':'ocr','column':'Vendor_Name'},
                                        'in_date':{'source':'input_config','table':'ocr','column':'created_date'},
                                        'tds_rate':{'source':'rule','value':tds_percent}

                        }
        }

################################################# HSN / SAC ##############################################

################################################# Client_name ############################################


partial_match_rule = {  'rule_type': 'static',
                        'function': 'PartialMatch',
                        'parameters':{
                            'words_table' : 'client_master',
                            'words_column':'client_name',
                            'match_word' : {'source':'input_config','table':'ocr','column':'client_name'}
                        }
}

Assign_client_name= { 'rule_type': 'static',
        'function': 'Assign',
        'parameters': { 'assign_table':{'source':'input_config','table':'ocr', 'column':'client_name'},
                        'assign_value':{'source':'rule', 'value':partial_match_rule}
        }
}

##########################################################################################################

partial_match_rule = {  'rule_type': 'static',
                        'function': 'PartialMatch',
                        'parameters':{
                            'words_table' : 'vendor_master',
                            'words_column':'vendor_name',
                            'match_word' : {'source':'input_config','table':'ocr','column':'Vendor Name'}
                        }
}

amount_replace = {  'rule_type': 'static',
                        'function': 'PartialMatch',
                        'parameters':{
                        'data':{'source':'input','value':''},
                        'to_replace': {'source':'input','value':''},
                        'replace_with': {'source':'input','value':''}

    }
}

##################################################################################################
ocr = {'id':'1',
    'Invoice Header':'credit note1234567',
    'Vendor Name':'"NIELSEN (india) PRIVATE LIMITED"',
    'Client Name':'Geller',
    'PAN':'ABNDF2345Q',
    'gst_amount':'20',
    'tds_amount':'10',
    'taxable_amount': 10,
    'Vendor_GSTIN':'01ABNDF2345Q9ZX',
    'Address_of_Vendor':'52-223, 535463 Main Road,hansenberg,PIN,12345678',
    'Address_of_Client':'52-223, 535463 Main Road,hansenberg,PIN,12345678',
    'Total_Value':'4501.25',
    'Net_Payable_Amount':'1319.50',
    'line_amount':'1450',
    'GST/CGST Amount':'7',
    'GST/SGST Amount':'8',
    'Vendor_PAN':'ABNDF2345Q',
    'GST/IGST Amount' :'9',
    'GST/IGST RCM Amount':'1',
    'GST/CGST RCM Amount':'2',
    'GST/SGST RCM Amount':'3',
    'GST/IGST ITC Amount':'',
    'GST/CGST ITC Amount':'',
    'GST/SGST ITC Amount':'',
    'IFSC Code':'0423',
    'Bank account number':'042315403',
    'Total value':'497',
    'Assessable Value':'5000',
    'created_date':'2020-01-29'
}

vendor_master = {'id':['1','2','3'],
    'vendor_name':['NIELSEN INDIA PRIVATE LIMITED','Chandler','Ross'],
    'country':['India','America','india'],
    'PAN':['ACDFG1234Z','QWERT4569G','QWERT2234D'],
    'GSTIN':['123','12345','678'],
    'GST_groupcode':['RMS9','C12','56UH12'],
    'RCM_applicability':['YES','NO','YES'],
    'ITC account (Debit)':['54uy6','-',''],
    'Bank Account Number':['042315403','3456','123456'],
    'IFSC Code':['042','9870','6789'],
    'TDS Code':['"194C-2"','194C-3','194C-2']
}

client_master = {'id':['1','2','3'],
    'Client_Name':['Lucifer','Chandler','Geller'],
    'country':['India','America','india'],
    'PAN':['ACDFG1234Z','QWERT4569G','QWERT2234D'],
    'Client ID':['CID0001','CID0002','CID0003']
}


Ltd_table = {
    'Client ID':['CID0001','CID0001'],
    'Vendor Name':['BHARTI AIRTEL LIMITED','sat AIRTEL LIMITED'],
    'LTD Limit':['1,52,250','1,52,250'],
    'LTD rate':['10','0.2'],
    'start date':['2019-04-29','2019-04-24'],
    'end date':['2020-03-31','2019-04-29'],
    'stacked_amount':['5','1']
}

TDS_Master = {
            'Code_for_mapping':["194C-2",'194-C3'],
            'TDS_Rates(in%)':['10','20']
}

Pincode_GST_Validation = {'statename':['Delhi','Punjab','Goa'],'pincode':[535463,12345,1234567],'GST_Code':[1,2,3]}

####################################################### ICICI ##############################################

"""
ocr = {'case_id':'1',
    'Invoice Header':'Tax Invoice1234567',
'Vendor_Name':'"Lucifer"',  
    'Client_Name':'Geller',
    'PAN':'ABNDF234',
    'net_amount':123,
    'raw_amount':456,
    'tot_amount':112
    }

process_queue = {
                'case_id':'1',
                'template_prediction_record':'"bill"'
}

trained_info = {
                'template_name':['bill','bill2'],
                'unit_data':['{"group1":100, "group2":10 }','{ "group1":100000 }']
}


field_definition = {
    'unique_name':['net_amount','raw_amount','tot_amount'],
    'group_name':['group1','group1','group2']
}

############################################################################################

get_dict = {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'trained_info',
            'select_column': 'unit_data',
            'lookup_filters':[
                {
                    'column_name': 'template_name',
                    'compare_with':  {'source':'input_config', 'table':'process_queue', 'column':'template_prediction_record'}
                }
            ]
        }
}

amount_trans = {
    'rule_type':'static',
    'function': 'MultiplyAmountFields',
    'parameters': {
            'dict_data':{'source':'rule','value':get_dict},
            'group_name':{'source':'input','value':'group_name'},
            'field':{'source':'input','value':'unique_name'},
            'table':'field_definition'
        }
}

mail_check = {'rule_type':'static',
        'function': 'Contains_string',
        'parameters' :{'word':{'source':'input_config','table':'ocr','column':'Case Reference Number'},
                       'strings_list':['Email'] 
        }
}

"""

value_get = {
    'rule_type':'static',
    'function': 'Select',
    'parameters': {
            'from_table': 'vendor_master',
            'select_column': 'Type',
            'lookup_filters':[
                {
                    'column_name': 'Company ID',
                    'compare_with':  {'source':'input_config', 'table':'ocr', 'column':'Company ID'}
                }
            ]
        }
}


move_queue = {'rule_type':'static',
        'function': 'AssignQ',
        'parameters' :{'assign_table':{'source':'input_config','table':'process_queue','column':'queue'},
                       'assign_value':{'source':'input','value':'checker'},      
        }
}


compare_type_rule = { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'rule', 'value':value_get},
                       'operator':'==',
                       'right_param':{'source':'input', 'value': ''}
        }
}


compare_consumer_rule = { 'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source':'input_config', 'table':'process_queue','column':'queue'},
                       'operator':'==',
                       'right_param':{'source':'input', 'value': ''}
        }
}



queue_condition = { 'rule_type' : 'condition',
                'evaluations': [{ 'conditions':[compare_type_rule], 'executions':[move_queue]},
                                { 'conditions':[compare_type_rule], 'executions':[move_queue]},
                                { 'conditions':[compare_type_rule], 'executions':[move_queue]},
                                { 'conditions':[compare_type_rule], 'executions':[move_queue]},
                                { 'conditions':[compare_type_rule], 'executions':[move_queue]}]
                }


consumer_condition = { 'rule_type' : 'condition',
                'evaluations': [{ 'conditions':[compare_consumer_rule], 'executions':[queue_condition]}]
                }


#################################################################################################
a  = BusinessRules('1',[pincode_regex], {'ocr':ocr,'vendor_master':vendor_master,'client_master':client_master,'Pincode_GST_Validation':Pincode_GST_Validation, 'LTD':Ltd_table, 'TDS_Master':TDS_Master})
print (a.evaluate_business_rules())
print(ocr)

