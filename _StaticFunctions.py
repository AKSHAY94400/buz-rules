try:
    import Lib
except:
    from . import Lib
import pandas as pd
from datetime import date,timedelta,datetime
import logging
import json

import os
import calendar
from dateutil import parser
import re
from difflib import SequenceMatcher
#from db_utils import DB
try:
    from ace_logger import Logging
    
    logging = Logging()
except:
    import logging 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 
 
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
}

__methods__ = [] # self is a BusinessRules Object
register_method = Lib.register_method(__methods__)

@register_method
def evaluate_static(self, function, parameters):
    if function == 'Assign':
        return self.doAssign(parameters)
    if function == 'AssignQ':
        return self.doAssignQ(parameters)
    if function == 'CompareKeyValue':
        return self.doCompareKeyValue(parameters)
    if function == 'GetLength':
        return self.doGetLength(parameters)
    if function == 'GetRange':
        return self.doGetRange(parameters)
    if function == 'Select':
        return self.doSelect(parameters)
    if function == 'Transform':
        return self.doTransform(parameters)
    if function == 'Count':
        return self.doCount(parameters)
    if function == 'Contains':
        return self.doContains(parameters)
    if function == 'ContainsMaster':
        return self.doContainsMaster(parameters)
    if function == 'case_gen':
        return self.docase_gen(parameters)
    if function == 'authority':
        return self.doAuthority(parameters)
    if function == 'due_date_generate':
        return self.Dodue_date_generate(parameters)
    if function == 'Bank_due_date_generate':
        return self.BankDodue_date_generate(parameters)
    if function == 'Get_holidays_fromdatabase':
        return self.get_holidays_fromdatabase(parameters)
    if function == 'sat_and_sun_holidays':
        return self.doSat_and_sun_holidays(parameters)
    if function == 'ContainsAccount_GHO':
        return self.doContainsAccount_GHO(parameters)
    if function == 'Contains_UCIC':
        return self.doContains_UCIC(parameters)
    if function == 'FindDuplicates':
        return self.doFindDuplicates(parameters)
    if function == 'FindDuplicatesTables':
        return self.doFindDuplicatesTables(parameters)
    if function == 'FindTotal':
        return self.doFindTotal(parameters)
    if function == 'DateParsing':
        return self.doDateParsing(parameters)
    if function == 'DateParsingMarch':
        return self.doDateParsingMarch(parameters)
    if function == 'Replace':
        return self.doReplace(parameters)
    if function == 'Split':
        return self.doSplit(parameters)
    if function == 'Return':
        return self.doReturn(parameters)
    if function == 'RegexColumns':
        return self.doRegexColumns(parameters)
    if function == 'alnum_and_contains':
        return self.alnum_and_contains(parameters)
    if function == 'AlphaNumCheck':
        return self.doAlphaNumCheck(parameters)
    if function == 'FindDuplicateID':
        return self.doFindDuplicateID(parameters)
    if function == 'DateTransform':
        return self.doDateTransform(parameters)
    if function == 'PartialMatch':
        return self.doPartialMatch(parameters)
    if function == 'FileManagerUpdate':
        return self.doFileManagerUpdate(parameters)
    if function == 'MultiplyAmountFields':
        return self.doMultiplyAmountFields(parameters)
    if function == 'UnitsCalculation':
        return self.doUnitsCalculation(parameters)
    if function == 'UnitsCalculationColumn':
        return self.doUnitsCalculationColumn(parameters)
    if function == 'Round':
        return self.doRound(parameters)
    
    if function == 'Contains_string':
        return self.doContains_string(parameters)
    if function == 'Alnum_num_alpha':
        return self.doAlnum_num_alpha(parameters)
    if function == 'Regex':
        return self.doRegex(parameters)
    if function == 'Zfill':
        return self.doZfill(parameters)
    if function == 'MultiplyAmountFields':
        return self.doMultiplyAmountFields(parameters)
    if function == 'LTDandTDScalc':
        return self.doLTDandTDScalc(parameters)
    if function == 'AmountCompare':
        return self.AmountCompare(parameters)

@register_method
def doGetLength(self, parameters):
    """Returns the lenght of the parameter value.
    Args:
        parameters (dict): The parameter from which the needs to be taken. 
    eg:
       'parameters': {'param':{'source':'input', 'value':5},
                      }
    Note:
        1) Recursive evaluations of rules can be made.
    
    """
    try:
        value = len(self.get_param_value(parameters['param']))
    except Exception as e:
        logging.error(e)
        logging.error(f"giving the defalut lenght 0")
        value = 0
    return value

@register_method
def doGetRange(self, parameters):
    """Returns the parameter value within the specific range.
    Args:
        parameters (dict): The source parameter and the range we have to take into. 
    eg:
       'parameters': {'value':{'source':'input', 'value':5},
                        'range':{'start_index': 0, 'end_index': 4}
                      }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Range is the python range kind of (exclusive of the end_index)
    """
    logging.info(f"parameters got are {parameters}")
    value = self.get_param_value(parameters['value'])
    range_ = parameters['range']
    start_index = range_['start_index']
    end_index = range_['end_index']
    try:
        return (str(value)[start_index: end_index])
    except Exception as e:
        logging.error(f"some error in the range function")
        logging.error(e)
    return ""

@register_method
def doSelect(self, parameters):
    """Returns the vlookup value from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'ocr',
            'select_column': 'highlight',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    column_name_to_select = parameters['select_column']
    lookup_filters = parameters['lookup_filters']

    # convert the from_table dictionary into pandas dataframe
    try:
        master_data = self.data_source[from_table]
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}

    master_df = pd.DataFrame(master_data) 

    # build the query
    query = ""
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = self.get_param_value(lookup['compare_with'])
        query += f"`{lookup_column}` == '{compare_value}' & "
    query = query.strip(' & ') # the final strip for the extra &
    logging.info(f"query got is {query}")
    result_df = master_df.query(query)
    result_df = result_df.reset_index(drop=True)

    # get the wanted column from the dataframe
    if not result_df.empty:
        try:
            return result_df[column_name_to_select][0] # just return the first value of the matches
        except Exception as e:
            logging.error(f"error in selecting the required data from the result")
            logging.error(e)
            return ""

@register_method
def doTransform(self, parameters) :
    """Returns the evalated data of given equations
    Args:
        parameters (dict): The source parameter which includes values and operators.
    eg:
        'parameters':[
            {'param':{'source':'input', 'value':5}},
            {'operator':'+'},
            {'param':{'source':'input', 'value':7}},
            {'operator':'-'},
            {'param':{'source':'input', 'value':1}},
            {'operator':'*'},
            {'param':{'source':'input', 'value':3}}
        ]
    Note:
        1) Recursive evaluations of rules can be made.
    """
    equation = ''
    logging.info(f"parameters got are {parameters}")
    for dic in parameters :
        for element,number_operator in dic.items() :
            if element == 'param' :
                value = f'{self.get_param_value(number_operator)}'
                value = value.replace(',','')
            elif element == 'operator' :
                value = f' {number_operator} '
        equation = equation + value
    return(eval(equation))

@register_method
def doContains(self, parameters):
    """ Returns true value if the data is present in the data_source
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            cpt_check_rule = {'rule_type': 'static',
                'function': 'Contains',
                'parameters': { 'table_name': 'ocr','column_name': 'cpt_codes',
                                'value':{'source':'input', 'value':92610}
                        }
            }
    """
    logging.info(f"parameters got are {parameters}")
    table_name = parameters['table_name']
    column_name = parameters['column_name']
    value = self.get_param_value(parameters['value'])
    print(value)
    column_values = self.data_source[table_name]
    print(type(column_values),column_values)
    if value in self.data_source[table_name][column_name]:
        return True
    else :
        return False

@register_method
def doContainsMaster(self, parameters):
    """ Returns true value if the data is present in the data_source
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            cpt_check_rule = {'rule_type': 'static',
                'function': 'Contains',
                'parameters': { 'table_name': 'ocr','column_name': 'cpt_codes',
                                'value':{'source':'input', 'value':92610}
                        }
            }
    """
    logging.info(f"parameters got are {parameters}")
    table_name = parameters['table_name']
    column_name = parameters['column_name']
    value = self.get_param_value(parameters['value'])
    print(value)
    df = pd.DataFrame(self.data_source[table_name])
    if value in list(df[column_name]):
        return True
    else :
        return False

@register_method
def doCount(self, parameters):
    """Returns the count of records from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'ocr',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    lookup_filters = parameters['lookup_filters']

    # convert the from_table dictionary into pandas dataframe
    try:
        master_data = self.data_source[from_table]
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}

    master_df = pd.DataFrame(master_data) 

    # build the query
    query = ""
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = self.get_param_value(lookup['compare_with'])
        query += f"{lookup_column} == {compare_value} & "
    query = query.strip(' & ') # the final strip for the extra &
    result_df = master_df.query(query)

    # get the wanted column from the dataframe
    if not result_df.empty:
        try:
            return len(result_df) # just return the first value of the matches
        except Exception as e:
            logging.error(f"error in selecting the required data from the result")
            logging.error(e)
            return 0
    else:
        return 0


######################################### HSBC ##################################################
def file_manager_folder_id_updates(db, case_id, folder_id_mapping):
    query = f"select id, folder_id from file_manager where case_id = '{case_id}'"
    file_manager_db = db.execute(query)
    for i,j in file_manager_db.iterrows():
        print(int(file_manager_db['folder_id'][i]))
        old_value = int(file_manager_db['folder_id'][i])
        if old_value in folder_id_mapping:
            new_value = folder_id_mapping[old_value]
            update_query = f"update file_manager set folder_id = {new_value} where case_id ='{case_id}' and folder_id = {old_value}"
            db.execute(update_query)
    return True

def insert_update_records_(db, input_data, case_id):
    try:
        max_id_query = f"SELECT max(id) from folder_manager"
        parent_id = db.execute_default_index(max_id_query)
        start_id = list(parent_id['max(id)'])[0]+1
        folder_id_mapping = {}
        for button_id,unique_keys in input_data.items():
            get_parent_id_query = f"SELECT * from folder_manager WHERE unique_key=%s"
            params = [button_id]
            df = db.execute(get_parent_id_query, params=params)
            parent_id = df.index.values[0]
            logging.info(f"parent_id got is {parent_id}")  
            for unique_key in unique_keys:
                query = f"SELECT * from folder_manager WHERE unique_key=%s"
                params = [unique_key]
                try:
                    df = db.execute(query, params=params)
                    old_folder_id = df.index.values[0]
                    new_unique_key = str(unique_key)+'_'+str(button_id)
                    logging.info(f"new_unique_key got is {new_unique_key}")
                    folder_name = list(df['folder_name'])[0]
                    logging.info(f"folder got is {folder_name}")
                    insert_query = f"INSERT INTO `folder_manager` VALUES ('{start_id}', '{new_unique_key}', '{folder_name}', '{parent_id}')"
                    db.execute(insert_query)
                    folder_id_mapping[old_folder_id] = start_id
                    start_id += 1
                    
                except Exception as e:
                    logging.error("Unable into insert the new record")
                    logging.error(str(e))
                    
    except Exception as e:
        print (e)
        return False
    print (folder_id_mapping)
    
    try:
        # update the values in the file_manager
        file_manager_folder_id_updates(db, case_id, folder_id_mapping)
        
    except Exception as e:
        # ask mahendra
        logging.error("update is not happening")
        logging.error(e)
    
    return True

@register_method
def doFileManagerUpdate(self, parameters):
    """ 
        Generate Case Reference number.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        update_records =  {'rule_type': 'static',
                                 'function': 'FileManagerUpdate',
                                 'parameters': {'database':{'source':'input','value':"file_manager"}, 
                                        'input_data':{'source':'input', 'value':{8:["uploads", "email"], 9:["uncategorized"]}
                            },
                      }
    }
    """
    logging.info(f"parameters got are {parameters}")
    database = self.get_param_value(parameters['database'])
    input_data = self.get_param_value(parameters['input_data'])
    db = DB(database, tenant_id=self.tenant_id, **db_config)
    return insert_update_records_(db, input_data, self.case_id)

@register_method
def docase_gen(self, parameters):
    """ 
        Generate Case Reference number.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        case_ref_generation =  {'rule_type': 'static',
                                 'function': 'case_gen',
                                 'parameters': {'code':{'source':'input','value':"LEA"}, 
                                        'auth':{'source':'rule', 'value':authortiy_rule},
                                        'id':{'source':'input','value':1}
                      }
    }
    """
    logging.info(f"parameters got are {parameters}")
    code = self.get_param_value(parameters["code"])
    authority = self.get_param_value(parameters["auth"])
    #ID = self.get_param_value(parameters["id"])
    #ID.zfill(2)
    """ID = ID.split("-")
    ID = ID[1]"""
    ocr_db = DB('extraction', tenant_id=self.tenant_id, **db_config)
    query_ocr = "Select * from `ocr`"
    ocr_extracted = ocr_db.execute(query_ocr)
    logging.info("extraction of ocr is done")
    ocr_df = pd.DataFrame(ocr_extracted)
    ID = len(ocr_df)
    ID = str(ID).zfill(3)
    print("###### In case reference generation function ######")
    try:
        today = date.today()
        today = today.strftime(f"%d%m%y")
        case_ref = code+"-"+authority+"-"+today+"-"+ID
        print(f'generated Casereference _number is : ',case_ref)
        return case_ref
    except Exception as e:
        logging.error("Cannot Generate Case Reference number ")
        logging.error(e)
        return False

@register_method
def doAuthority(self, parameters):

    """ 
        Get Authority in Shortform.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        authortiy_rule = {'rule_type': 'static',
                        'function': 'authority',
                        'parameters': {'authoritie':{'source':'input','value':"Enforcement Directorate"}, 
                      }
    }   
    """
    logging.info(f"parameters got are {parameters}")
    auth = self.get_param_value(parameters["authoritie"])
    mail = self.get_param_value(parameters["mail"])
    try:
        if mail or mail == 'true':
                auth = "EML"
        else:
            if auth == "Enforcement Directorate":
                auth = "ED"
            elif auth == "Economic Offences Wing":
                auth = "EOW"
            elif auth == "National Investigation Agency":
                auth = "NIA"
            elif auth == "Central Bureau of Investigation":
                auth = "CBI"
            elif auth == "Income Tax Department":
                auth = "ITD"
        print(auth)
        return auth
    except Exception as e:
        logging.error(f"Cannot Generate Case Reference number ERROR : in doAuthority_function")
        logging.error(e)
        return False

@register_method
def Dodue_date_generate(self, parameters):
    """ 
        Due_Date_generation.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        duedate_rule = {'rule_type': 'static',
                        'function': 'due_date_generate',
                        'parameters': {'Extended_days':{'source':'input_config','table':"ocr",'column':'Default Extension'}, 
                      }
    }   
    """
    logging.info(f"parameters got are {parameters}")
    holidays = self.get_param_value(parameters["holidays"])
    Extended_days = self.get_param_value(parameters["Extended_days"])
    print(f'Extended_days',Extended_days)
    print("=====================>")
    print(holidays)
    Extended_days,_ = Extended_days.split(" ")
    Extended_days = int(Extended_days)
    try:
        today = date.today()
        due_date = today + timedelta(days = int(Extended_days))
        print(f'due_date is :',due_date)
        while True:
            if str(due_date) not in holidays :
                print(f'due_date2 is :',due_date)
                return due_date
            else:
                due_date = due_date + timedelta(days = 1)
                print(f'due_date3 is :',due_date)
    except Exception as e:
        logging.error(f"=======> Cannot Generate DueDate_generate ERROR : in Dodue_date_generate_function")
        logging.error(e)
        return False

@register_method
def BankDodue_date_generate(self, parameters):
    """ 
        Bank_Due_Date_generation.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        duedate_rule = { 'rule_type': 'static',
        'function': 'Bank_due_date_generate',
        'parameters': {'Due_date':{'source':'input_config','table':"ocr",'column':'Due Date(Notice)'}
        }
    }
    """
    holidays = self.get_param_value(parameters["holidays"])
    print(holidays)
    logging.info(f"parameters got are {parameters}")
    Due_date = self.get_param_value(parameters["Due_date"])
    Receipt_time = self.get_param_value(parameters["Receipt_time"])
    try:
        today = date.today()
        today_time = datetime.today()
        date_format = "%Y-%m-%d"
        a = datetime.strptime(str(today), date_format)
        b = datetime.strptime(str(Due_date), date_format)
        diff_days = (b - a).days
        print(f'diff_days are =====> ',diff_days)
        Due_date_date = datetime.strptime(str(Due_date),"%Y-%m-%d").date()
        if diff_days == 0 or diff_days == -1 or diff_days == 1 or diff_days == 2 or diff_days == 3 :
            Due_date1 = date.today()
            todaydate = datetime.now()
            Receipt_time = datetime.strptime(Receipt_time,"%Y-%m-%d %H:%M:%S")
            today12pm = todaydate.replace(hour=12, minute=0, second=0, microsecond=0)
            if Receipt_time > today12pm:
                Due_date1 = today + timedelta(days = 1)
            else:
                Due_date1 = date.today()
        elif diff_days == 7 :
            Due_date1 = Due_date_date - timedelta(days = 2)
        elif diff_days == 15 or diff_days == 16 :
            Due_date1 = Due_date_date - timedelta(days = 3)
        elif diff_days >= 17 :
            Due_date1 = today + timedelta(days = 15)
        else:
            Due_date1 = Due_date_date - timedelta(days = 2)
        print(f'Duedate in Bank_is ===> ',Due_date1 )

        while True:
            if str(Due_date1) not in holidays :
                print(f'Bank_Due_date is ======> ',Due_date1)
                return str(Due_date1)
            else:
                Due_date1 = Due_date1 - timedelta(days = 1)
        print(f'Bank_Due_date is ======> ',Due_date1)
    except Exception as e:
        logging.error(f"===========>Cannot Generate BankDodue_date_generate ERROR : in BankDodue_date_generate_function")
        logging.error(e)
        return False

@register_method
def doSat_and_sun_holidays(self, parameters):

    """ 
        holidays generation.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        dosat_sun_rule = {'rule_type': 'static',
                        'function': 'dosat_sun_generate',
                        'parameters': {'Extended_days':{'source':'input_config','table':"ocr",'column':'Default Extension'}, 
                      }
    }   
    """
    logging.info(f"parameters got are {parameters}")
    try:
        year = int(datetime.now().year)
        print(f"year is : ",year)
        def all_sundays(year):
        # January 1st of the given year
            dt1 = date(year, 1, 1)
        # First Sunday of the given year  
            print("########## in all_sundays function ")     
            dt1 += timedelta(days = 6 - dt1.weekday())
            while dt1.year == year:
                yield dt1
                dt1 += timedelta(days = 7)
        years1=[]
        for s in all_sundays(year):
            years1.append(str(s))
        print(f'all_sundays are',years1)

        def second_saturdays(year):
            dt2 = date(year, 1, 1) 
            dt2 += timedelta(days = 5 - dt2.weekday()) 
            while dt2.year == year:
                if 8 <= dt2.day <= 14 :
                    yield dt2
                dt2 += timedelta(days = 7)
        years2=[]
        for s2 in second_saturdays(year):
            years2.append(str(s2))
        print(f'all_second_saturdays are',years2)

        def fourth_saturdays(year):
            dt3 = date(year, 1, 1) 
            dt3 += timedelta(days = 5 - dt3.weekday()) 
            while dt3.year == year:
                if 22 <= dt3.day <= 28 :
                    yield dt3
                dt3 += timedelta(days = 7)
        years3=[]
        for s3 in fourth_saturdays(year):
            years3.append(str(s3))
        print(f'all_fourth_saturdays are',years3)
    except Exception as e:
        logging.error(f"Cannot Generate Holiday_list ERROR : in sat_sun_holiday_generate_function")
        logging.error(e)
        return False

    Holiday_list = years1+years2+years3
    print(Holiday_list)
    return Holiday_list

@register_method
def get_holidays_fromdatabase(self, parameters):
    logging.info(f"parameters got are {parameters}")
    from_table1 = self.get_param_value(parameters['from_table1'])
    from_column1 = self.get_param_value(parameters['from_column1'])
    sun_sat_holidays_list = self.get_param_value(parameters['sun_sat_holidays'])
    try:
        holidays_df = (self.data_source[from_table1])
        print(holidays_df)
        print("============= @ ========================>")
        holidays_df = pd.DataFrame(holidays_df)
        holidays_df[from_column1] = holidays_df[from_column1].astype(str)
        holidays_df[from_column1] =  pd.to_datetime(holidays_df[from_column1],dayfirst=True,errors='coerce').dt.strftime('%Y-%m-%d')
        holidays_list = holidays_df[from_column1].tolist()
        print(holidays_list)
        total_holidays = holidays_list + sun_sat_holidays_list
        print(f'total_holidays are:',total_holidays)
        return total_holidays
    except Exception as e:
        logging.error(f"=========> Error in Adding Holidays ")
        logging.error(e)        

@register_method
def doContainsAccount_GHO(self, parameters):
    """ Returns assigned value if the data is present in the data_source
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        { 'rule_type':'static',
        'function': 'ContainsAccount_GHO',
        'parameters' : {
                        'table_name_ocr': 'ocr',
                        'map_column':'Mapping Table',
                        'column1_map_id': 'Customer ID',
                        'column2_map_to': 'Account Number',
                        'table_name_acc': 'close_account_dump',
                        'column1_acc_id': 'CUSTOMER_ID',
                        'column2_acc_to': 'ACCOUNT_NO',
                        'value1':{'source':'input_config', 'table':'ocr', 'column': 'Mapping Table'},
                        }
        }
        """
    logging.info(f"parameters got are {parameters}")
    table_name_ocr = parameters['table_name_ocr']
    map_column = parameters['map_column']
    column1_map_id =  parameters['column1_map_id']
    column2_map_to =  parameters['column2_map_to']
    table_name_acc =  parameters['table_name_acc']
    column1_acc_id =  parameters['column1_acc_id']
    column2_acc_to =  parameters['column2_acc_to']
    value_map = self.get_param_value(parameters['value1'])
    print(table_name_ocr)
    print(table_name_acc)
    try:
        print(type(value_map))
        value_map = json.loads(value_map)
        dict_list = []
        map_dict = [dict_list.append(e) for e in value_map[0]["rowData"]]
        map_df = pd.DataFrame(dict_list)
        df_acc = self.data_source[table_name_acc]
        df_acc = pd.DataFrame(df_acc)
        if (column2_map_to == 'Account Number') or (column2_map_to == 'GHO Code'):
            for index, row in map_df.iterrows():
                for indexer, rows in df_acc.iterrows():
                    if row[column1_map_id] == rows[column1_acc_id]:
                        if row[column2_map_to] == '':
                            row[column2_map_to] = rows[column2_acc_to]
            print(map_df)
            map_dict = map_df.to_dict(orient = 'records')
            for i in range(map_df['Customer ID'].count()):
                value_map[0]["rowData"][i][column2_map_to] = map_dict[i][column2_map_to]
            value_map = json.dumps(value_map)
            #self.data_source[table_name_ocr][map_column] = value_map
            return value_map
        else:
            for index, row in map_df.iterrows():
                for indexer, rows in df_acc.iterrows():
                    if row[column1_map_id] == rows[column1_acc_id]:
                        row[column2_map_to] = rows[column2_acc_to]
            print(map_df)
            map_dict = map_df.to_dict(orient = 'records')
            for i in range(map_df['Customer ID'].count()):
                value_map[0]["rowData"][i][column2_map_to] = map_dict[i][column2_map_to]
            value_map = json.dumps(value_map)
            #self.data_source[table_name_ocr][map_column] = value_map
            return value_map
    except Exception as e:
        logging.error(f"Error in ===============> doContainsAccount_GHO Function")
        logging.error(e)
        return value_map

@register_method
def doContains_UCIC(self, parameters):
    """ Returns BOOLEAN value if the data is present in the data_source
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        { 'rule_type':'static',
        'function': 'Contains_UCIC',
        'parameters' : {
                        'column1_map_id': 'Customer ID',
                        'table_name_acc': 'close_account_dump',
                        'column1_acc_id': 'CUSTOMER_ID',
                        'value1':{'source':'input_config', 'table':'ocr', 'column': 'Mapping Table'},
                        }
        }
        """
    logging.info(f"parameters got are {parameters}")
    column1_map_id =  parameters['column1_map_id']
    table_name_acc =  parameters['table_name_acc']
    column1_acc_id =  parameters['column1_acc_id']
    value_map = self.get_param_value(parameters['value1'])
    print(value_map)
    print(table_name_acc)
    try:
        dict_list = []
        value_map = json.loads(value_map)
        map_dict = [dict_list.append(e) for e in value_map[0]["rowData"]]
        map_df = pd.DataFrame(dict_list)
        df_acc = self.data_source[table_name_acc]
        df_acc = pd.DataFrame(df_acc)
        print(df_acc)
        id_list = list(df_acc[column1_acc_id])
        print(id_list)
        for index, row in map_df.iterrows():
            if row[column1_map_id] in id_list :
                return 'Match Found'
        return 'Match Not Found'       
    except Exception as e:
        logging.error(f"Error in ===============> doContains_UCIC Function")
        logging.error(e)
        return False

@register_method
def alnum_and_contains(self, parameters):
    """    
        'paramaters':{
                    'from_table':'ocr',
                    'columns':''
        }

    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    column_name = parameters['column']
    value = self.get_param_value(parameters['value'])

    ocr_db = DB('extraction', tenant_id=self.tenant_id, **db_config)
    query_ocr = "Select * from `ocr`"
    ocr_extracted = ocr_db.execute(query_ocr)
    logging.info("extraction of ocr is done")
    ocr_df = pd.DataFrame(ocr_extracted)
    ocr_df[column_name] = ocr_df[column_name].str.strip()              
    ocr_df[column_name] = ocr_df[column_name].str.replace(r"[^a-zA-Z\d]+", "")
    value = re.sub(r"[^a-zA-Z\d]+", "",value)
    print(value)
    print(ocr_df[column_name])
    print(list(ocr_df[column_name]).count(str(value)))
    if list(ocr_df[column_name]).count(str(value)) > 1:
        print('True')
        return True
    else :
        print('False')
        return False

@register_method
def doFindDuplicateID(self, parameters):
    """ Returns Dupilcate id value if the ID is present in the ocr
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
       """
    logging.info(f"parameters got are {parameters}")
    Reference_number_in = self.get_param_value(parameters['Reference_number'])
    column_drop = parameters['column_drop']
    data = self.get_param_value(parameters['data'])
    #ocr_extracted = self.data_source['ocr_tot']
    try:
        try:
            ocr_db = DB('extraction', tenant_id=self.tenant_id, **db_config)
            query_ocr = "Select * from `ocr`"
            ocr_extracted = ocr_db.execute(query_ocr)
            logging.info("extraction of ocr is done")
            ocr_df = pd.DataFrame(ocr_extracted)
            ocr_df.drop( ocr_df[ ocr_df[column_drop] == Reference_number_in ].index , inplace=True)
            master_dict = {}
            for index ,rows in ocr_df.iterrows():
                if rows['Mapping Table']:
                    Customer_ids_list=[]
                    Customer_ids_map = json.loads(rows['Mapping Table'])
                    print(Customer_ids_map[0]["rowData"])
                    try:
                        map_dict = [Customer_ids_list.append(cid['Customer ID']) for cid in Customer_ids_map[0]["rowData"]]
                        ref_key = rows[column_drop]
                        print(Customer_ids_list,ref_key)
                        master_dict.update({ref_key :  Customer_ids_list})
                    except Exception as e:
                        print(e)
        except Exception as e:
            logging.info(f"Error in Generating all case_id's dict")
            logging.info(e)

        dict_list = []
        value_map = json.loads(data)
        map_dict = [dict_list.append(e['Customer ID']) for e in value_map[0]["rowData"]]
        print(dict_list)
        for c_id in dict_list:
            for key,value in master_dict.items():
                if c_id in value:
                    print(f'Reference number is =========>',key)
                    self.data_source['ocr']['Duplicate Case Reference Number'] = key
                    return True

    except Exception as e:
        logging.info('Error in DuplicateID function')
        logging.info(e)
        
########################################################## IndusIND #######################################################################

@register_method
def doFindDuplicates(self, parameters):
    """  Finds duplicate rows based on the columns mentioned, returns true if duplicates exists
        'paramaters':{
                    'from_table':'ocr',
                    'columns':['','']
        }

        NOTE : works only for columns in single datatable
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    filter_columns = parameters['columns']

    ocr_db = DB('extraction', tenant_id=self.tenant_id, **db_config)
    query_ocr = "Select * from `ocr`"
    ocr_extracted = ocr_db.execute(query_ocr)
    logging.info("extraction of ocr is done")
    for column in filter_columns:
        try:
            compare_value = self.data_source[from_table][column]
            ocr_extracted = ocr_extracted[ocr_extracted[column].astype(str) == str(compare_value)]
        except Exception as e:
            logging.error("dataframe filter failed")
            logging.error(e)
    
    logging.info(f"filtered df is {ocr_extracted}")
    if len(ocr_extracted) > 1:
        return True
    else:
        return False

@register_method
def doFindDuplicatesTables(self, parameters):
    """ Finds duplicate rows based on the columns mentioned, returns true if duplicates exists
        'paramaters':{
                    'duplicate_columns':['',''],
                    'merge_column':''
        }

       'NOTE': specifically for ocr and process queue 
    """
    logging.info(f"parameters got are {parameters}")
    duplicate_columns = parameters['duplicate_columns']
    merge_column = parameters['merge_column']

    ocr_db = DB('extraction', tenant_id=self.tenant_id, **db_config)
    query_ocr = "Select * from `ocr`"
    ocr_extracted = ocr_db.execute(query_ocr)
    logging.info("extraction of ocr is done")
    
    query_process = f"select * from `process_queue`"
    process_queue_db = DB('queues', tenant_id=self.tenant_id, **db_config)
    df_process = process_queue_db.execute(query_process)
    logging.info("extraction of process queue is done")

    merge_df = pd.merge(ocr_extracted,df_process,on=merge_column)

    for column in duplicate_columns:
        try:
            compare_value = merge_df[merge_df['case_id']==self.case_id][column]
            compare_value.reset_index(inplace=True, drop=True)
            merge_df = merge_df[merge_df[column].astype(str) == str(compare_value[0])]
        except Exception as e:
            logging.error("dataframe filter failed")
            logging.error(e)
    
    logging.info(f"filtered df is {ocr_extracted}")
    if len(merge_df) > 1:
        return True
    else:
        return False

@register_method
def doFindTotal(self, parameters):
    """ returns the sum of a column in the ui table
    "parameters":{
        'data':{'source':'input_config','table':'ocr','column':'table'},
        'key_name':'CPT Code'
    }

    """
    logging.info(f"parameters got are {parameters}")
    data = self.get_param_value(parameters['data'])
    key_name = parameters['key_name']
    try:
        
        data = json.loads(data)
        truth_values = ['Indusind' in e[key_name] for e in data[0]["rowData"]]
        amounts = []
        for (values, truth_value) in zip(data[0]["rowData"], truth_values):
            if truth_value:
                amounts.append(int(values['']))
        total = sum(amounts)
        return total
    except Exception as e:
        logging.error(f"connot calculate total amount")
        logging.error(e)
        return ""
        
@register_method
def doDateParsing(self, parameters):
    """Checks whether given date is last day of that month or not, returns true if yes
        parameters:{
            "input_date":{"source":"input_config","table":"ocr","column":"Stock Date"
        }
    """
    logging.info(f"parameters got are {parameters}")
    input_date = self.get_param_value(parameters['input_date'])
    try:
        input_date = input_date.replace('.','-')
        input_date = input_date.replace('/','-')
        input_date = input_date.replace(' ','-')
        input_date = input_date.replace('st','')
        input_date = input_date.replace('th','')
    except:
        input_date = input_date
    
    logging.info(f"date got is {input_date}")
    list_31 = ['jan','mar','may','jul','aug','oct','dec','01','03','05','07','08','10','12','january','	march','may','july','august','october','december','1','3','5','7','8']
    list_30 = ['apr','jun','sep','nov','04','06','09','11','april','june','september','november','4','6','9']
    try:
        input_list = input_date.split("-")
        if len(input_list) == 2:
            if input_list[0].lower() in list_31:
                input_date = "31-"+input_date
            elif input_list[0].lower() in list_30:
                input_date = "30-"+input_date
            else:
                feb_last = calendar.monthrange(int(input_list[1]),2)[1]
                input_date = str(feb_last)+"-"+input_date
        logging.info(f"Converted date is {input_date}")
    except Exception as e :
        logging.error("Cannot convert date")
        logging.error(e)

    try:
        input_date = parser.parse(input_date,default=datetime(2019, 10, 3))
        date_list = str(input_date).split("-")
        input_day = date_list[2][0:2]
        input_month = date_list[1]
        input_year = date_list[0]
        logging.info(f"input date is: {input_day}")
        month_last = calendar.monthrange(int(input_year),int(input_month))[1]
        logging.info(f"last day of given month is {month_last}")
        if str(input_day) == str(month_last):
            logging.info(f"given date and month's last date are same")
            return True
        else:
            logging.info(f"given date and month's last date are different")
            return False
    except Exception as e:
        logging.error("Cannot compare two dates")
        logging.error(e)
        return False

@register_method
def doDateParsingMarch(self, parameters):
    """Checks whether given date is last day of that month or not, returns true if yes
        'parameters':{
            "input_date":{"source":"input_config","table":"ocr","column":"Stock Date"
        }
        NOTE: works same for all months except march, if march returns true for any date
    """
    logging.info(f"parameters got are {parameters}")
    input_date = self.get_param_value(parameters['input_date'])
    try:
        input_date = input_date.replace('.','-')
        input_date = input_date.replace('suspicious','')
        input_date = input_date.replace('/','-')
        input_date = input_date.replace(' ','-')
        input_date = input_date.replace('st','')
        input_date = input_date.replace('th','')
    except:
        input_date = input_date
        
    logging.info(f"date got is {input_date}")
    list_31 = ['jan','may','jul','aug','oct','dec','01','05','07','08','10','12','january','may','july','august','october','december','1','3','5','7','8']
    list_30 = ['apr','jun','sep','nov','04','06','09','11','april','june','september','november','4','6','9']
    try:
        input_list = input_date.split("-")
        if len(input_list) == 2:
            if input_list[0].lower() in list_31:
                input_date = "31-"+input_date
            elif input_list[0].lower() in list_30:
                input_date = "30-"+input_date
            elif(input_list[0].lower()=='march') or (input_list[0].lower()=='mar') or (input_list[0]=='03'):
                print(input_date)
                return True
            else:
                feb_last = calendar.monthrange(int(input_list[1]),2)[1]
                input_date = str(feb_last)+"-"+input_date
        logging.info(f"Converted date is {input_date}")
    except Exception as e :
        logging.error("Cannot convert date")
        logging.error(e)

    try:
        input_date = parser.parse(input_date,default=datetime(2019, 10, 3))
        date_list = str(input_date).split("-")
        logging.info(f"Date list is : {date_list}")
        input_day = date_list[2][0:2]
        input_month = date_list[1]
        if (input_month.lower()=='march') or (input_month.lower()=='mar') or (input_month=='03'):
            return True
        input_year = date_list[0]
        logging.info(f"input date is: {input_day}")
        month_last = calendar.monthrange(int(input_year),int(input_month))[1]
        logging.info(f"last day of given month is {month_last}")
        if str(input_day) == str(month_last):
            logging.info(f"given date and month's last date are same")
            return True
        else:
            logging.info(f"given date and month's last date are different")
            return False
    except Exception as e:
        logging.error("Cannot compare two dates")
        logging.error(e)
        return False

@register_method
def doReplace(self, parameters):
    """ Replaces value in a string and returns the updated value
    'parameters':{
        'data':{'source':'input','value':''}
        'to_replace':{'source':'input','value':''}
        'replace_with':{'source':'input','value':''}
    }
    """
    logging.info(f"parameters got are {parameters}")
    to_replace = self.get_param_value(parameters['to_replace'])
    replace_with = self.get_param_value(parameters['replace_with'])
    data = self.get_param_value(parameters['data'])
    try:
        data = str(data).replace(to_replace,replace_with)
        return data
    except Exception as e:
        logging.error("Replacing value failed")
        logging.error(e)
        return data

@register_method
def doSplit(self, parameters):
    """ Replaces value in a string and returns the required index
    'parameters':{
        'data':{'source':'input','value':''},
        'symbol_to_split':{'source':'input','value':''},
        'required_index':{'source':'input','value':''}
    }
    """
    logging.info(f"parameters got are {parameters}")
    symbol_to_split = self.get_param_value(parameters['symbol_to_split'])
    required_index = self.get_param_value(parameters['required_index'])
    data = self.get_param_value(parameters['data'])
    try:
        data_split = str(data).split(symbol_to_split)
        logging.info(f"splited data is {data_split}")
        return data_split[required_index]
    except Exception as e:
        logging.error("Spliting value failed")
        logging.error(e)
        return data

@register_method
def doAlphaNumCheck(self, parameters):
    """ Returns true value if the string is Alpha or num or alnum
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
         {'rule_type':'static',
        'function': 'Alnum_num_alpha',
        'parameters' :{'word':{'source':'rule','value':get_range_rule1},
                       'option':'alpha',      
        }
        }
    """
    logging.info(f"parameters got are {parameters}")
    word = self.get_param_value(parameters['word'])
    option = parameters['option']
    try:
        if option == 'alpha':
            bool_value = word.isalpha()
            logging.info(f'{word} is alpha {bool_value}')
        if option == 'numeric':
            bool_value = word.isnumeric()
            logging.info(f'{word} is numeric {bool_value}')
        if option == 'alnum':
            bool_value = word.isalnum()
            logging.info(f'{word} is numeric {bool_value}')
        return bool_value
    except Exception as e:
        logging.error("Error In doAlphaNumCheck function")
        logging.error(e)
        return False

@register_method
def doRegexColumns(self, parameters):
    """ Returns a value by applying given Regex pattern on value
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'table_name':"",
                        'columns':[],
                       'regex_str':"\d{6}"
        }
        }
        NOTE: can apply for more than one column at a single time
    """
    logging.info(f"parameters got are {parameters}")
    
    table_name = parameters['table_name']
    columns = parameters['columns']
    regex_str = parameters['regex_str']
    try:
        regex = re.compile(f'{regex_str}')
    except Exception as e :
        logging.error("Error In regex pattern")
    for column in columns:
        phrase = self.data_source[table_name][column]
        logging.info(f"GOT THE VAlUE FOR COLUMN {column} IS {phrase}")
        if not phrase:
            phrase = '0'
        phrase = str(phrase).replace(",","")
        try:
            matches = re.findall(regex, str(phrase))
            if matches[0]:
                logging.info(f"LIST OF MATCHES GOT ARE : {matches}")
                matches = matches[0]
            else:
                matches = 0
        except Exception as e:
            logging.debug("REGEX MATCH GOT FAILED SO DEFAULT VALUE 0 GOT ASSIGNED")
            matches = 0
        logging.debug(f"MATCHES GOT FOR {column} COLUMN IS : {matches}")
        self.data_source[table_name][column] = str(matches)
        try:
            if table_name not in self.changed_fields:
                self.changed_fields[table_name] = {}
            self.changed_fields[table_name][column] = str(matches)
            logging.info(f"updated the changed fields\n changed_fields are {self.changed_fields}")
        except Exception as e:
            logging.error(f"error in assigning and updating the changed fields in regex function for : {column}")
            logging.error(e)
    return True

@register_method
def doReturn(self, parameters):
    """Returns the mentioned value
    'parameters':{
        'value_to_return':{"source":"input_config","table":"ocr","column":"Stock Date"}
    }
    """
    logging.info(f"parameters got are {parameters}")
    try:
        value_to_return = self.get_param_value(parameters['value_to_return'])
        return value_to_return
    except Exception as e:
        logging.error("cannot get value")
        logging.error(e)
        return ""

@register_method
def doRound(self, parameters):
    """Rounds a number to the required number of decimals
    'parameters' : {
        'value': {"source":"input_config","table":"ocr","column":""},
        'round_upto': {"source":"input_config","table":"ocr","column":""
    }
    """
    logging.info(f"parameters got are {parameters}")
    value = self.get_param_value(parameters['value'])
    round_upto = self.get_param_value(parameters['round_upto'])
    try:
        value = round(float(value), round_upto)
    except Exception as e :
        logging.error("ROUND FAILED SO RETURNING SAME VALUE")
        logging.error(e)
        value = value
    logging.info(f"Value after round is : {value}")
    return value

@register_method
def doMultiplyAmountFields(self, parameters):
    logging.info(f"parameters got are {parameters}")
    dict_data = self.get_param_value(parameters['dict_data'])
    group_name = self.get_param_value(parameters['group_name'])
    field_name = self.get_param_value(parameters['field'])
    print(dict_data)
    dict_data = json.loads(dict_data)
    print(type(dict_data))
    table  = parameters['table']
    try:
        logging.info(f'dict_data is : {dict_data}')
        stages_df = self.data_source[table]
        stages_df = pd.DataFrame(stages_df)
        for stage,value in dict_data.items():
            stages_list = list(stages_df.loc[stages_df[group_name] == stage][field_name])
            for field in stages_list:
                curr_value = self.data_source['ocr'][field]*value
                print(curr_value)
                ocr_db = DB('extraction', tenant_id=self.tenant_id, **db_config)
                query_ocr = f"UPDATE `ocr` SET `{field}` = '{curr_value}' WHERE `case_id` = '{self.case_id}'"
                ocr_extracted = ocr_db.execute(query_ocr)
                
    except Exception as e:
        logging.error("Error In doMultiplyAmountFields function ")
        logging.error(e)
    
@register_method
def doUnitsCalculation(self, parameters) :
    """
    'parameters' : {
        'addontable_data': {'source':'input','value':''},
        'unit_unique_value' : {'source':'input','value':''},
        'field_unique_name':{},
        'operator':'',
        'key_name':'',
        'table':'',
        'column':''
    }
    """
    logging.info(f"parameters got are : {parameters}")
    addontable_data = self.get_param_value(parameters['addontable_data'])
    unit_unique_value = self.get_param_value(parameters['unit_unique_value'])
    field_unique_name = self.get_param_value(parameters['field_unique_name'])
    operator = parameters['operator']
    key_name = parameters['key_name']
    table_name = parameters['table']
    column_name = parameters['column']

    queue_db = DB('queues', tenant_id=self.tenant_id, **db_config)
    unit_query = f"SELECT `value` from `dropdown_definition` WHERE `unique_name`=%s and `dropdown_option` = %s"
    params = [field_unique_name,unit_unique_value]
    units_df = queue_db.execute_default_index(unit_query,params = params)
    logging.info(f"Got the units value from database is {units_df}")
    if not units_df.empty:
        unit_value = list(units_df['value'])[0]
        unit_value = str(unit_value).replace(",","")
    else:
        unit_value = 1
    logging.info(f"Units value to multiply is : {unit_value}")
    if addontable_data:
        row_data = addontable_data[0]['row_data']
        for row in row_data:
            row[key_name] = row[key_name].replace(",","")
            row[key_name] = eval(f"{row[key_name]} {operator} {unit_value}")

        addontable_data[0]['row_data'] = row_data
        logging.info(f"data getting inserted is : {row_data}")
        try:
            if table_name not in self.changed_fields:
                self.changed_fields[table_name] = {}
            self.changed_fields[table_name][column_name] = addontable_data
            logging.info(f"updated the changed fields\n changed_fields are {self.changed_fields}")
        except Exception as e:
            logging.error(f"error in assigning and updating the changed fields in regex function for : {column_name}")
            logging.error(e)
    return True

@register_method
def doUnitsCalculationColumn(self, parameters):
    """
    'parameters':{
        'table_name' :[],
        'column_names' : {} 
    }
    note: column_names are key value pairs of value and unit columns
    """
    logging.info(f"parameters got are : {parameters}")
    table_name = parameters['table_name']
    column_names = parameters['column_names']
    

    queue_db = DB('queues', tenant_id=self.tenant_id, **db_config)

    for data_column, unit_column in column_names.items():
        default_params = {
            'data_column' : {"source":"input_config","table":"ocr","column":str(data_column)},
            'unit_column': {"source":"input_config","table":"ocr","column":str(unit_column)}
        }
        data = self.get_param_value(default_params['data_column'])
        data = str(data).replace(",","")
        try:
            data =float(data)
        except:
            data = 0
        logging.info(f"Got the data is : {data}")
        units_unique_value = self.get_param_value(default_params['unit_column'])
        unit_query = f"SELECT `value` from `dropdown_definition` WHERE `unique_name`=%s and `dropdown_option` = %s"
        params = [unit_column,units_unique_value]
        logging.info(f"query is : {unit_query} where values are {params}")
        units_df = queue_db.execute_default_index(unit_query,params = params)
        logging.info(f"Got the units value from database for column {data_column} is {units_df}")
        if not units_df.empty:
            unit_value = list(units_df['value'])[0]
            unit_value = str(unit_value).replace(",","")
        else:
            unit_value = 1
        logging.info(f"Units value to multiply for column {data_column} is : {unit_value}")

        try:
            data = data * units_value
        except:
            data = eval(f"{data}*{unit_value}")

        self.data_source[table_name][data_column] = str(data)
        try:
            if table_name not in self.changed_fields:
                self.changed_fields[table_name] = {}
            self.changed_fields[table_name][data_column] = str(data)
            logging.info(f"updated the changed fields\n changed_fields are {self.changed_fields}")
        except Exception as e:
            logging.error(f"error in assigning and updating the changed fields in regex function for : {data_column}")
            logging.error(e)

    return True
######################################### INVESCO ##################################################
@register_method
def doDateTransform(self, parameters):
    """ Takes date as input and converts it into required format
        'parameters':{
            'input_date' : {"source":"input_config","table":"ocr","column":"Stock Date"},
            'output_format' : 'dd-mm-yy'
        }
    """
    logging.info(f"parameters got are {parameters}")
    input_date = self.get_param_value(parameters['input_date'])
    output_format = parameters['output_format']
    input_date = pd.Series(input_date)
    try:
        if output_format == 'dd-mmm-yy':
            input_date = pd.to_datetime(input_date,dayfirst=True,errors='coerce').dt.strftime('%d-%b-%y')
            return input_date[0]
        elif output_format == 'dd-mm-yyyy':
            input_date = pd.to_datetime(input_date,dayfirst=True,errors='coerce').dt.strftime('%d-%m-%Y')
            return input_date[0]
        elif output_format == 'dd-mm-yy':
            input_date = pd.to_datetime(input_date,dayfirst=True,errors='coerce').dt.strftime('%d-%m-%y')
            return input_date[0]

    except Exception as e:
        logging.error("cannot convert date")
        logging.error(e)
        return input_date

@register_method
def doPartialMatch(self, parameters):
    """ Returns highest matched string
    'parameters':{
        'words_table' : '',
        'words_column':'',
        'match_word' : {"source":"input_config","table":"ocr","column":"Stock Date"}
    }

    """
    logging.info(f"parameters got are {parameters}")
    words_table = parameters['words_table']
    words_column = parameters['words_column']
    match_word = self.get_param_value(parameters['match_word'])
    data = self.data_source[words_table]
    data = pd.DataFrame(data)
    words = list(data[words_column])
    print(words_table,words_column,match_word)
    print(words)
    max_ratio = 0
    match_got = ""
    for word in words:
        try:
            ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
            if ratio > 75 and ratio > max_ratio:
                max_ratio = ratio
                match_got = word
                print(match_got)
        except Exception as e:
            logging.error("cannnot find match")
            logging.error(e)
 
    logging.info(f"match is {match_got} and ratio is {max_ratio}")
    return match_got
    

######################################### DELOITTE ##################################################
@register_method
def doContains_string(self,parameters):
    """ Returns true value if the string is present in the word
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            cpt_check_rule = {'rule_type': 'static',
                'function': 'Contains',
                'parameters': { 'table_name': 'ocr','column_name': 'cpt_codes',
                                'value':{'source':'input', 'value':92610}
                        }
            }
    """
    logging.info(f"parameters got are {parameters}")
    word = self.get_param_value(parameters['word'])
    strings_list = parameters['strings_list']
    try:
        if word != "":
            for string in strings_list:
                if string.lower() in word.lower():
                    return True
            return False
    except Exception as e:
        print('==========> Error in doContains_string')
        print(e)

@register_method
def doAlnum_num_alpha(self,parameters):
    """ Returns true value if the string is Alpha or num or alnum
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
         {'rule_type':'static',
        'function': 'Alnum_num_alpha',
        'parameters' :{'word':{'source':'rule','value':get_range_rule1},
                       'option':'alpha',      
        }
        }
    """
    logging.info(f"parameters got are {parameters}")
    word = self.get_param_value(parameters['word'])
    option = parameters['option']
    try:
        if option == 'alpha':
            bool_value = word.isalpha()
            logging.info(f'{word} is alpha {bool_value}')
        if option == 'numeric':
            bool_value = word.isnumeric()
            logging.info(f'{word} is numeric {bool_value}')
        if option == 'alnum':
            bool_value = word.isalnum()
            logging.info(f'{word} is numeric {bool_value}')
        if option == 'is_numeric':
            try:
                bool_value = float(word).is_integer()
                logging.info(f'{word} is numeric {bool_value}')
            except:
                return False
        return bool_value
    except Exception as e:
        logging.error("Error In doAlnum_num_alpha function")
        logging.error(e)
        return False

@register_method
def doRegex(self,parameters):
    """ Returns a value by doing Regex on it
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'phrase':{'source':'input_config','table':'ocr','column':'Address_of_Vendor'},
                       'regex_str':"\d{6}",      
        }
        }
    """
    logging.info(f"parameters got are {parameters}")
    phrase = self.get_param_value(parameters['phrase'])
    regex_str = parameters['regex_str']
    reg_model = parameters['reg_model']
    try:
        logging.info(f'regex is : {regex_str}')
        logging.info(f'phrase is : {phrase}')
        regex= re.compile(f'{regex_str}')
        if reg_model == 'search':
            matches= re.search(regex, phrase)
            if matches:
                matches = matches[0]
            else:
                matches = ''
        if reg_model == 'match':
            matches= re.match(regex, phrase)
        if reg_model == 'findall':
            matches= re.findall(regex, phrase)
            if matches:
                matches = matches[0]
            else:
                matches = ''
        print(matches)
        return matches
    except Exception as e:
        logging.error("Error In doRegex function")
        logging.error(e)
        return False

@register_method
def doZfill(self,parameters):
    """ Returns a value by doing Regex on it
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'value':{'source':'input_config','table':'ocr','column':'Address_of_Vendor'},
                       'zfill_no':2,      
        }
        }
    """
    logging.info(f"parameters got are {parameters}")
    value = self.get_param_value(parameters['value'])
    Zfill_no = parameters['zfill_no']
    try:
        logging.info(f'value is : {value}')
        logging.info(f'Zfill_no is : {Zfill_no}')
        value = str(value).zfill(Zfill_no)
        return value
    except Exception as e:
        logging.error("Error In doZfill function")
        logging.error(e)
        return value


@register_method
def doLTDandTDScalc(self,parameters):
    logging.info(f"parameters got are {parameters}")
    Assessable_Value = self.get_param_value(parameters['Assessable_Value'])
    client_name = self.get_param_value(parameters['client_name'])
    client_id = self.get_param_value(parameters['client_id'])
    ven_name = self.get_param_value(parameters['ven_name'])
    in_date = self.get_param_value(parameters['in_date'])
    tds_rate = self.get_param_value(parameters['tds_rate'])
    try:
        ocr_db = DB('extraction', tenant_id=self.tenant_id, **db_config)
        get_report = f"SELECT * FROM `LTD Master` WHERE `start date` <= '{in_date}' AND `end date` >= '{in_date}' AND `Vendor Name` = '{ven_name}' AND `Client_ID` = {client_id}"
        df = business_rules_db.execute_(get_report)
        if len(df) != 0:
            if float(str(df['stacked_amount'][0]).replace(',','')) <= float(str(df['LTD Limit'][0]).replace(',','')) :
                ltd_rate = (df['LTD rate'][0])
                ltd_amount = float(Assessable_Value)*float(ltd_rate)/100
                print(ltd_amount)
                ocr_db = DB('extraction', tenant_id=self.tenant_id, **db_config)
                get_report = f"UPDATE `ocr` SET `LTD Amount` = {ltd_amount}  WHERE `case_id` = {self.case_id}"
                df = business_rules_db.execute_(get_report)
            else:
                tds_amount = float(Assessable_Value)*float(tds_rate)/100
                print(tds_amount)
                ocr_db = DB('extraction', tenant_id=self.tenant_id, **db_config)
                get_report = f"UPDATE `ocr` SET `TDS Amount` = {tds_amount}  WHERE `case_id` = {self.case_id}"
                df = business_rules_db.execute_(get_report)
    except Exception as e:
        logging.error("Error In ltd_tds function")
        logging.error(e)

@register_method
def AmountCompare(self,parameters):
    left_param, operator, right_param = parameters['left_param'], parameters['operator'], parameters['right_param'] 
    left_param_value, right_param_value = self.get_param_value(left_param), self.get_param_value(right_param)
    logging.debug(f"left param value is {left_param_value} and type is {type(left_param_value)}")
    logging.debug(f"right param value is {right_param_value} and type is {type(right_param_value)}")
    logging.debug(f"operator is {operator}")
    try:
        left_param_value = left_param_value.replace(',','')
        right_param_value = right_param_value.replace(',','')
        if operator == ">=":
            print(float(left_param_value) >= float(right_param_value))
            return (float(left_param_value) >= float(right_param_value))
    except Exception as e:
        logging.debug(f"error in compare key value {left_param_value} {operator} {right_param_value}")
        logging.debug(str(e))
        return False
# sample test
#git