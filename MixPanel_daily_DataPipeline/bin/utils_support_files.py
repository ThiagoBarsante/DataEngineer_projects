"""
    This package aux provide support, better readability and maintanence to the program company_datapipeline_mixpanel.py
"""

import datetime
import pandas as pd
import subprocess as prc
#import math    ## keep original value NaN

def f_short_name(text):
    """function short event name - requested detailed in item 2."""

    # split the text
    words = text.lower()
    words = words.split()
    
    short_name = '_'

    # for each word in the line:
    for word in words:
        short_name = short_name + word[0:3]
    
    ## remove special characters
    str_replace=['_', '/', '-', '*']
    for v_replace in str_replace:
        short_name = short_name.replace(v_replace, '')
    
    
    return short_name


def f_properties_name(text):
    """function used to replace special characters. Used to get new column names for the dataframe """
    
    list_columns = []
    
    for properties_name in text:
        properties_name = properties_name.lower()
                
        ## remove special characters
        str_replace=['$', ' ', '-', '*', ':']
        for v_replace in str_replace:
            properties_name = properties_name.replace(v_replace, '')
        
        ## print(properties_name)
        list_columns.append(properties_name)
        
    return(list_columns)

def f_rename_properties(text, v_short_colname):
    """rename properties : this aux function will rename the properties name except 
            the distinct id and conversation id detailed"""
    
    new_column_name = []
    for property_name in f_properties_name(text):
        if property_name not in ['distinct_id', 'conversationid']:
            property_name = property_name + v_short_colname
            
        new_column_name.append(property_name)
        
    return(new_column_name)


    
def labelEncode_value_ab(x):
    """ Label Encode function"""
    ### -------------------------------------------------------------- 
    ## LABEL ENCODE : With Voice = 1 No Voice = 2 Else NaN = 0  - Others x

    if x == 'With Voice':
        return 1
    elif x == 'No Voice':
        return 2
#    elif math.isnan(x): ## if value Nan = 0 then 0 
#        return 0
    else:
        return x
        

def f_path_rename(text):
    """Function to rename path columns - aux function"""

    
    list_columns = []
    
    for properties_name in text:
        properties_name = properties_name.lower()
        
        properties_name = properties_name.replace(' ', '_')
        
        properties_name = 'path_' + properties_name
        
        ## print(properties_name)
        list_columns.append(properties_name)
 
    return(list_columns)    
    
def f_convert_str_to_datetime(text):
    """item 10. Number of hours between 2 dates. 
    function to convert string to date time """

    ## Missing data 01-11-2019
    try:
        txt_datetime = datetime.datetime.strptime(text, '%Y-%m-%dT%H:%M:%S')
    except:
        txt_datetime = ''

    return txt_datetime


def delta_days_hours(td):
    """function to calculate the number of hours between 2 dates"""    
    return (td.days * 24) + td.seconds//3600

    
def f_hours_since_start(begin_date, end_date):
    """ function to calculate the number of hours between 2 dates - exceptions will not return values"""
    try:
        hours_since_start = delta_days_hours (end_date - begin_date)
    except:
        hours_since_start = ''
    
    return hours_since_start

def f_move_log_files(msg_shell):
    """function to move file to /log_dir"""    
    print(msg_shell)
    shell_result = prc.check_output(msg_shell, shell=True)
    print(shell_result)


def f_workaround_local_json(dir_directory, execution_date, json_filename, str_log):
    ''' This funcition impement a local workaround insteand of using curl
    Files downloaded using curl are stored at ./data/BACKUP/LOCAL_WORK_AROUND_JSON/
    
    
        Workaround to run every day, even if no data to process
        Obs. If the execution day = yesterday copy day 04/11/2019 else copy an empty file
    
    '''
    
    v_yesterday = datetime.date.today() - datetime.timedelta(days=1) ## yesterday
    v_yesterday_str = v_yesterday.strftime("%Y-%m-%d")
    
    filename = '../dir_json_stg/' + str_log + json_filename
    v_aux_dir = './support_datafiles/'

    if execution_date == '2019-11-04':
        curl_1_aux = 'cp ' + v_aux_dir + '2019-11-04_mixpanel_ALL_EVENTS.json ' + filename
    elif execution_date == '2019-11-03':
        curl_1_aux = 'cp ' + v_aux_dir + '2019-11-03_mixpanel_ALL_EVENTS.json ' + filename
    elif execution_date == '2019-11-02':
        curl_1_aux = 'cp ' + v_aux_dir + '2019-11-02_mixpanel_ALL_EVENTS.json ' + filename
    elif execution_date == '2019-11-01':
        curl_1_aux = 'cp ' + v_aux_dir + '2019-11-01_mixpanel_ALL_EVENTS.json ' + filename
    elif execution_date == '2019-10-31':
        curl_1_aux = 'cp ' + v_aux_dir + '2019-10-31_mixpanel_ALL_EVENTS.json ' + filename
    elif execution_date == v_yesterday_str:
        curl_1_aux = 'cp ' + v_aux_dir + '2019-11-04_mixpanel_ALL_EVENTS.json ' + filename        
    else:
        curl_1_aux = 'cp ' + v_aux_dir + '2019-10-31_mixpanel_ALL_EVENTS.json ' + filename
    
    return curl_1_aux


def f_workaround_default_event_df_5():
    '''MERGE DATAFRAMES AND GET DEFAULT VALUES
            Default data datrafema for Event 5 - Missing - Subscription on day 01-11-2010
            This object is necessary because day 01-11 does not have event file in the database
    '''
    
    ## df_default_EVENT_5 = pd.read_csv('../bin/DF_DEFAUL_COLUMNS/EVENT_5__DEFAULT_DF_Subscription_Confirmed.CSV', 
    v_filename = './support_datafiles/EVENT_5__DEFAULT_DF_Subscription_Confirmed.CSV'
    df_default_EVENT_5 = pd.read_csv(v_filename, 
                                 sep=',', header=0,
                                dtype = {
'app_build_numbersubcon'              :object ,
'app_releasesubcon'                   :object,
'app_versionsubcon'                   :object,
'app_version_stringsubcon'            :object,
'carriersubcon'                       :object,
'citysubcon'                          :object,
'device_idsubcon'                     :object,
'had_persisted_distinct_idsubcon'     :float,
'insert_idsubcon'                     :object,
'ios_ifasubcon'                       :object,
'lib_versionsubcon'                   :object,
'manufacturersubcon'                  :object,
'modelsubcon'                         :object,
'ossubcon'                            :object,
'os_versionsubcon'                    :object,
'radiosubcon'                         :object,
'regionsubcon'                        :object,
'screen_heightsubcon'                 :float,
'screen_widthsubcon'                  :float,
'wifisubcon'                          :float,
'conversationid'                      :object,
'conversationindexsubcon'             :float,
'currencysubcon'                      :object,
'datedayofthemonthsubcon'             :float,
'datedayoftheweeksubcon'              :float,
'datehourofthedaysubcon'              :object,
'datelocaltimesubcon'                 :object,
'eventidsubcon'                       :object,
'frequencysubcon'                     :object,
'idsubcon'                            :object,
'platformsubcon'                      :object,
'pricesubcon'                         :object,
'subscriptionoriginsubcon'            :object,
'distinct_id'                         :object,
'mp_country_codesubcon'               :object,
'mp_device_modelsubcon'               :object,
'mp_libsubcon'                        :object,
'mp_processing_time_mssubcon'         :float,
'timesubcon'                          :float,
'eventsubcon'                         :object
    }
                                )
    return df_default_EVENT_5

def f_include_default_path_df(original_dataframe):
    """ This function will include all Path missing in a dataframe based on STARNDARD DEFAULT DF_EVENTS
            with all paths if it is missing in the original dataframe. 
        The first record of the STANDARD has all information required to be included """

    ## read first row only
    filename = './support_datafiles/DEFAULT_DATAFRAME__ALL_events_RECORD_1.csv'
    df_aux = pd.read_csv(filename, header=0, sep=';', nrows=1) 
    path_standard_colummns = df_aux.columns
    
    ## get all path columns from STANDARD DF
    path_aux = []
    for column in path_standard_colummns:
        if column.startswith('path_'):
            path_aux.append(column)
    df_aux = df_aux[path_aux]

    ## get all path columns from parameter dataframe
    path_df_colummns = original_dataframe.columns
    path_aux_df = []
    for column in path_df_colummns:
        if column.startswith('path_'):
            path_aux_df.append(column)
    
    ## missing path: Example 1 path_missing = list(set(path_aux) - set(path_aux_df)) or
    path_missing = [x for x in path_aux if x not in path_aux_df]
    df_aux = df_aux[path_missing] ## .astype('Int64')
    
    df_final_path = original_dataframe.join(df_aux)
    df_final_path = df_final_path.reindex(sorted(df_final_path.columns), axis=1)
    
#    print(df_final_path.dtypes) ## Obs. Float for missing values (convert to int later)
    
    return df_final_path

def f_update_date(old_date, add_days):
    """ this function return the number of days between 2 dates"""
    
    try:
        new_date = old_date + datetime.timedelta(days=add_days)
    except:
        new_date = old_date

    return new_date

