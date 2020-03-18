
""" This program run one complete datapipeline with raw data from mixpanel (json files API)
        and generate one structured file format to be used in a Data Science project

                Resume
                - validate startup process
                - check if the configuration and variables are setup
                - run mixpanel json api to download 5 events from specific day (daily execution)
                - merge all events and do feature engineering (Label Encode, One Hot Encode...)
                - export the results to .csv (local)
                - export the result to a cloud provider (GCP) and provide the logic to AWS
                - cleanup old processed files(.csv, .log, .json and .zip)
                - some exceptions are generated intentionaly to be catched  by scheduler
                tools/platforms when executed
                Basic execution info and setup
                - Directory structure requirements
                ./bin
                ./log
                ./data_dir   => configuration file
                ./json_dir   => temp directory to download the json files
                Configuration file wiht additional parameters
                - the configuration file must have the same name of .py file with extension .cfg

"""
import os
import sys
import subprocess as prc
import datetime
import pandas as pd
import boto3

## Auxiliary functions from python module utils...py
from utils_support_files import (f_short_name,
                                 f_rename_properties,
                                 f_path_rename,
                                 labelEncode_value_ab,
                                 delta_days_hours,
                                 f_hours_since_start,
                                 f_convert_str_to_datetime,
                                 f_workaround_local_json,
                                 f_workaround_default_event_df_5,
                                 f_include_default_path_df,
                                 f_update_date)


#### -------------------------------------------   DATA PIPELINE
print('print - Process start...')

# events to be downloaded
V_EVENTS = ['Company First Start', 'Conversation Started',
            'Conversation Completed', 'A/B - Onboard - Voice',
            'Subscription Confirmed']

## Initial setup (log files)
V_BOL_EXCEPTION = False
V_BOL_LOG_FILE = False

PYTHON_FILEN_NAME = os.path.basename(__file__)
V_LOG_FILENAME = PYTHON_FILEN_NAME[:-3] + '.log'
V_CONFIG_FILENAME = PYTHON_FILEN_NAME[:-3] + '.cfg'
V_STG_JSON_FILENAME = PYTHON_FILEN_NAME[:-3] + '_STG.json'

V_LOG_DIR = '../log/'
if not os.path.isdir(V_LOG_DIR):
    V_BOL_EXCEPTION = True
    print('EXCEPTION : Invalid log dir -  ', V_LOG_DIR)
    raise 'Invalid log directory. Create the log dir first '


### --------------------------------------------------------------
## FUNCTIONS

def f_log(msg='', time=0):
    """ Aux function to print msg and generate log to file"""
    if time == 1:
        now = datetime.datetime.now()
        v_time = now.strftime("%Y-%m-%d %H:%M:%S")
    else:
        v_time = ''
    v_log_msg = v_time + ' | ' + msg
    print(v_log_msg)
    if V_BOL_LOG_FILE:
        F.write('\n' + v_log_msg)


def f_setup_config(v_config_filename):
    """This function read the configuration file"""

    df_conf_file = pd.read_csv(v_config_filename, delimiter="|", header=0)
    api_key = df_conf_file[df_conf_file.CONFIG_VAR == 'API_KEY']['VALUE'].values[0]
    data_dir = df_conf_file[df_conf_file.CONFIG_VAR == 'DATA_DIR']['VALUE'].values[0]
    json_log_dir = df_conf_file[df_conf_file.CONFIG_VAR == 'JSON_DIR']['VALUE'].values[0]
    gcs_bucket = df_conf_file[df_conf_file.CONFIG_VAR == 'GCP_BUCKET']['VALUE'].values[0]
    # gcs_service_account_key = df_conf_file[df_conf_file.CONFIG_VAR == 'GCP_SERVICE_ACOUNT_KEY']['VALUE'].values[0]
    # aws_key = df_conf_file[df_conf_file.CONFIG_VAR == 'AWS_ACCESS_KEY']['VALUE'].values[0]
    # aws_secret_key = df_conf_file[df_conf_file.CONFIG_VAR == 'AWS_SECRET_ASSES_KEY']['VALUE'].values[0]
    aws_s3 = df_conf_file[df_conf_file.CONFIG_VAR == 'AWS_S3_BUCKET']['VALUE'].values[0]
    export_csv = df_conf_file[df_conf_file.CONFIG_VAR == 'EXPORT_CSV']['VALUE'].values[0]
    cleanup_days = df_conf_file[df_conf_file.CONFIG_VAR == 'CLEANUP_DAYS']['VALUE'].values[0]

    # return api_key, gcs_bucket, gcs_service_account_key, aws_key, aws_secret_key, \
    #     aws_s3, data_dir, json_log_dir, export_csv, cleanup_days
    return api_key, gcs_bucket, aws_s3, data_dir, json_log_dir, export_csv, cleanup_days

def f_cleanup_process(days, dir_cleanup):
    """This function will delete all files older than  x days (minmal is 5) in the directory
            Only .zip, .json and .csv and .log files will be deleted
    """
    try:
        cleanup_days_aux = int(days)
    except:
        cleanup_days_aux = 5

    if cleanup_days_aux < 5:
        cleanup_days_aux = 5

    file_type = ['*.csv', '*.zip', '*.json', '*.log']

    for file_remove in file_type:
        v_files_deleted = dir_cleanup + file_remove
        msg_shell = 'find ' + v_files_deleted + '  -mtime +' +str(cleanup_days_aux) + '  -exec rm {} \;'
#        print(msg_shell)
        try:
            f_log('Delete files older than ' + days + ' days. FILES: ' + v_files_deleted, 1)
            shell_result = prc.check_output(msg_shell, shell=True)
            print(shell_result)
        except:
            msg = 'Zip files: problem to delete'

    msg = 'OK'

    return msg


def f_message_start_error(msg=''):
    """ Function to raise a msg if do not start the data pipeline correctly """

    f_log('')
    f_log('---------------  SETUP ALL INFORMATION IN THE CONFIG FILE')
    f_log('')
    f_log('---- Exception..')
    f_log('')
    f_log('-----------  Run the program with parameter local, gcp or aws')
    f_log('--------------  local = local / on-premise execution')
    f_log('--------------  gcp - Google cloud shell')
    f_log('--------------  aws - Amazon Web Services')
    f_log('')
    f_log('-------------  EXAMPLE')
    f_log('')
    f_log('-- Local daily execution')
    f_log('python company_mixpanel_daily_datapilene.py local')
    f_log('')
    f_log('-- GCP daily execution')
    f_log('python company_mixpanel_daily_datapilene.py gcp')
    f_log('')
    f_log('-- AWS daily execution')
    f_log('python company_mixpanel_daily_datapilene.py aws')
    f_log('')
    f_log('')
    f_log('-- Run the program (local, gcp or aws) for especifict day. ' + \
          'Re-execution date: Format DD-MM-YYYY')
    f_log('python company_mixpanel_daily_datapilene.py local 31-10-2019')
    f_log('')
    f_log('EXCEPTION : problem running the program', 1)
    if V_BOL_LOG_FILE:
        F.close()
    raise ValueError(msg)

def def_aux_json_api_download(api_key_cfg, events, download_date, dir_json_data, json_filename):
    """ This function use mixpanel json API to download all 5 events from a specific day
            download json files
    """

    v_filename = dir_json_data + V_DT_LOG_STR + json_filename

    fullstr_events = '","'.join(events)

    curl_1_aux = "curl https://data.mixpanel.com/api/2.0/export/ \
    -u " + api_key_cfg + " \
    -d from_date=\"" + download_date + "\" \
    -d to_date=\"" + download_date + "\" \
    -d event='[\"" + fullstr_events + "\"]' >> " + v_filename

    f_log(curl_1_aux, 1)

    ######  - Simple comment about the code below
    ## INFO - all curl commands were downloaded from 31-10-2019 until 04-11-2019
    ## into ./bin/support_datafiles/ for local execution - Work around to run on
    ## Linux and Windows (curl execution)
    ## FOR PRODUCTION ENVIROMENT JUST COMMENT THE LINE  RELATED DO f_workaround_local_json
    curl_1_aux = f_workaround_local_json('../data/', V_DT_MIXPANEL_STR, json_filename, V_DT_LOG_STR)
    curl_shell = prc.check_output(curl_1_aux, shell=True)

    ## Keep this line to see the result of the subprocess
    print(curl_shell)

    ## check if the file had downloaded events
    if os.stat(v_filename).st_size == 0:
        f_log('EXCEPTION : JSON API DOWNLOAD - NO DATA to process - check internet  \
              connection or the execution date parameter', 1)
        raise 'No data to process - check internet connection or the execution date parameter'
    else:
        f_log('JSON API DOWNLOAD - OK', 1)
        df_aux_json = pd.read_json(v_filename, lines=True)

    return df_aux_json

def f_dataframe_jsonfile(event_desc, event_list, df_json):
    """ This function read on json file and convert it to a pandas dataframe format """

    v_aux_event_list = 'EVENT_XXX'

    ## comment: idx in python as in c language start with 0, so 1 is 0, 2 is 1..
    if event_desc == 'event_1_company_start':
        v_aux_event_list = event_list[0]
    elif event_desc == 'event_2_conversation_start':
        v_aux_event_list = event_list[1]
    elif event_desc == 'event_3_conversation_completed':
        v_aux_event_list = event_list[2]
    elif event_desc == 'event_4_AB_OnBoard_Voice':
        v_aux_event_list = event_list[3]
    elif event_desc == 'event_5_subscription_confirmed':
        v_aux_event_list = event_list[4]
        df_default_aux = f_workaround_default_event_df_5()

    else:
        f_log('EXCEPTION : Raise exception - Event not mapped', 1)

    ## filter data frame by event
    df_aux = df_json
    df_aux = df_aux.query("event == @v_aux_event_list")

    ## validate total records in the dataframe created
    total_records = df_aux.shape[0]

    if total_records == 0:
        df_result = df_default_aux
    else:
        df_result = pd.io.json.json_normalize(df_aux['properties'])
        df_result['event'] = v_aux_event_list
        ### SHORT NAME
        short_colname = f_short_name(df_result['event'][0])
        ### rename properties name
        df_result.columns = f_rename_properties(df_result.columns, short_colname)

    return df_result


## ----------------------------------------------------------------------------- PROGRAM START
## ------------------------------    Log file creation
try:
    f_log('Creation of log file ...')
    V_DT_LOG = datetime.datetime.now()
    V_DT_LOG_STR = V_DT_LOG.strftime("%Y%m%d_%H%M%S") ## 2020-02-16 16:18:25.280030
    V_LOG_FILENAME = V_LOG_DIR + V_DT_LOG_STR + '_' + V_LOG_FILENAME
    F = open(V_LOG_FILENAME, "w+")
    V_BOL_LOG_FILE = True
except:
    print('Exception running the program...')
    V_BOL_EXCEPTION = True
    f_log('EXCEPTION : Problem to create log file -  ' + V_LOG_FILENAME)

####  -----------------------------------------  START VALIDATION
    ## VALIDATION OF MANY OPTIONS AND GIVE JUST ONE MSG. IF ANY OF EXCEPTIONS OCCURS
if (len(sys.argv) not in [2, 3]):
    f_message_start_error(msg='Re-start the process with correct parameter. local or gcp or aws')

if len(sys.argv) == 2:
    if sys.argv[1] not in ['local', 'gcp', 'aws']:
        V_BOL_EXCEPTION = True
        f_log('EXCEPTION : Wrong parameter local/gcp/aws -' + sys.argv[1])
    V_DT_MIXPANEL = datetime.date.today() - datetime.timedelta(days=1) ## yesterday
    V_DT_MIXPANEL_STR = V_DT_MIXPANEL.strftime("%Y-%m-%d")
elif len(sys.argv) == 3:
    ## VALIDATE THE DATE AS PAREMETER
    try:
        V_DT_MIXPANEL = datetime.datetime.strptime(sys.argv[2], '%d-%m-%Y').date()
        V_DT_MIXPANEL_STR = V_DT_MIXPANEL.strftime("%Y-%m-%d")
    except:
        V_BOL_EXCEPTION = True
        f_log('EXCEPTION : Invalid date - ' + sys.argv[2])

####  -----------------------------------------  VALIDATION OF CONFIG FILES AND DIR
try:
    # V_API_KEY, V_GCS_BUCKET, V_GCS_SERVICE_ACCOUNT_KEY, V_AWS_KEY, \
    #     V_AWS_SECRET_KEY, V_AWS_S3, V_DATA_DIR, V_JSON_DIR, V_EXPORT_CSV, \
    #         V_CLEANUP_DAYS = f_setup_config(V_CONFIG_FILENAME)
    V_API_KEY, V_GCS_BUCKET, V_AWS_S3, V_DATA_DIR, V_JSON_DIR, V_EXPORT_CSV, \
            V_CLEANUP_DAYS = f_setup_config(V_CONFIG_FILENAME)
except:
    V_BOL_EXCEPTION = True
    print('EXCEPTION : Invalid configuration file - ', V_CONFIG_FILENAME)

# VALIDATE IF DATA DIR, BACKUP AND LOG DIR EXISTS
if not os.path.isdir(V_DATA_DIR):
    V_BOL_EXCEPTION = True
    f_log('EXCEPTION : Invalid data directory - ' + V_DATA_DIR)

if not os.path.isdir(V_JSON_DIR):
    V_BOL_EXCEPTION = True
    f_log('EXCEPTION : Invalid directory - ' + V_JSON_DIR)

####   ------------  Check if work around EVENT 5 missing for day 01-11-2019
V_FILE_EVENT_5 = './support_datafiles/EVENT_5__DEFAULT_DF_Subscription_Confirmed.CSV'
if not os.path.isfile(V_FILE_EVENT_5):
    V_BOL_EXCEPTION = True
    f_log('EXCEPTION : File not found. Pending file (setup it manually) - \
          workaround DEFAULT_DF_EVENT_5 01-11-2019: - ' + V_FILE_EVENT_5)

####  -----------------------  Raise a problem to re-start the process
if V_BOL_EXCEPTION:
    f_message_start_error('EXCEPTION : Re-start the program with correct parameter \
                          and config file. Read all messages above...')

######################### MAIN  FUNCTION
f_log('-------------------------------------- PROGRAM EXECUTION')
f_log('------------------------------------------------  Data pipeline start')
f_log('Python program: ' + sys.argv[0])
f_log('local/cloud parameter: ' + sys.argv[1])
f_log('Execution date: ' + V_DT_MIXPANEL_STR)
f_log('Data dir: '+ V_DATA_DIR)
f_log('Log dir: '+ V_LOG_DIR)
f_log('Json download file dir: '+ V_JSON_DIR)
f_log('Log file name: ' + V_LOG_FILENAME)

f_log(' ', 0)
f_log('--------------------  Starting execution -------------------------- ', 1)

### --------------------------------------------------------------
## Download JSON FILE
f_log('Download JSON file and create one dataframe for each EVENT', 1)
DF_ALL_EVENTS = def_aux_json_api_download(V_API_KEY, V_EVENTS,
                                          V_DT_MIXPANEL_STR,
                                          V_JSON_DIR,
                                          json_filename='_mixpanel_ALL_events_STG.json')

DF_COMPANY_START = f_dataframe_jsonfile('event_1_company_start',
                                        V_EVENTS, df_json=DF_ALL_EVENTS)
DF_CONVERSATION_START = f_dataframe_jsonfile('event_2_conversation_start',
                                             V_EVENTS, df_json=DF_ALL_EVENTS)
DF_CONVERSATION_COMPLETED = f_dataframe_jsonfile('event_3_conversation_completed',
                                                 V_EVENTS, df_json=DF_ALL_EVENTS)
DF_AB_ONBOARD_VOICE = f_dataframe_jsonfile('event_4_AB_OnBoard_Voice',
                                           V_EVENTS, df_json=DF_ALL_EVENTS)
DF_SUBSCRIPTION_CONFIRMED = f_dataframe_jsonfile('event_5_subscription_confirmed',
                                                 V_EVENTS, df_json=DF_ALL_EVENTS)

### --------------------------------------------------------------
f_log('Merge all Data frames and filter rows and columns ... ', 1)

## Merge ALL 5 Data Frames - item 5.
## Obs. Remove all duplicates if necessary in a production environment before apply merge
## sample : DF_COMPANY_START = DF_COMPANY_START.drop_duplicates() ...

DF_ALL_EVENTS = DF_COMPANY_START

## Merge Data frames
DF_ALL_EVENTS = DF_ALL_EVENTS.merge(DF_CONVERSATION_START, left_on='distinct_id',
                                    right_on='distinct_id', how='left')

DF_ALL_EVENTS = DF_ALL_EVENTS.merge(DF_CONVERSATION_COMPLETED,
                                    left_on=['distinct_id', 'conversationid'],
                                    right_on=['distinct_id', 'conversationid'], how='left')

DF_ALL_EVENTS = DF_ALL_EVENTS.merge(DF_AB_ONBOARD_VOICE,
                                    left_on=['distinct_id', 'conversationid'],
                                    right_on=['distinct_id', 'conversationid'], how='left')

DF_ALL_EVENTS = DF_ALL_EVENTS.merge(DF_SUBSCRIPTION_CONFIRMED,
                                    left_on=['distinct_id', 'conversationid'],
                                    right_on=['distinct_id', 'conversationid'], how='left')

# ''' validation export results -- full execution (flow 1 + flow 2) vs daily (just one flow)
# ## DF_ALL_EVENTS.to_csv('../data/DF_ALL_EVENTS__DAILY_FULL.CSV')
# VALIDATION OK!
# divergence...
# MISSING CONVERSATION vISvgqOIowcQn54xDyfIFrswYYr9Tonz - Event start on day 01-11
# On day 04-11 just events Conversation Started and A/B - Onboard - Voice and because
# this do not  have information on day 04-11
# '''

### --------------------------------------------------------------
## Filter columns - item 6.
FILTER_COLUMNS = ['distinct_id',
                  'conversationid',
                  'app_versionconsta',
                  'conversationindexconsta',
                  'conversationstartedatconsta',
                  'valueabonbvoi',
                  'pathscompletedconcom',
                  'datelocaltimecomfirsta',
                  'frequencysubcon']

for coluna in FILTER_COLUMNS:
    if coluna not in DF_ALL_EVENTS.columns:
        print(coluna)

DF_ALL_EVENTS = DF_ALL_EVENTS[FILTER_COLUMNS]

## Filter columns without conversation : calc hours_since_start (drop na)
DF_ALL_EVENTS = DF_ALL_EVENTS.dropna(subset=['conversationstartedatconsta',
                                             'datelocaltimecomfirsta'])

# """ Comments
# ## Filter records - item 7. however the only record with
# ##### frequencysubcon == 'monthly do not have distinct_id and conversationid
# ##### correlated. Code code just for the requirement without any change in the data
# """
DF_ALL_EVENTS = DF_ALL_EVENTS.query("frequencysubcon != 'monthly'", inplace=False)

f_log('Label Encode valueabonbvoi ...', 1)

DF_ALL_EVENTS['valueabonbvoi'] = DF_ALL_EVENTS['valueabonbvoi'].apply(labelEncode_value_ab)

f_log('One Hot Encode paths ... ', 1)

### --------------------------------------------------------------
## ONE HOT ENCODE pathscompletedconcom - fill NA = N for NoPath
DF_ALL_EVENTS['pathscompletedconcom'] = \
    DF_ALL_EVENTS['pathscompletedconcom'].fillna(value='N')
DF_OHE = pd.Series(DF_ALL_EVENTS['pathscompletedconcom']). \
    apply(frozenset).to_frame(name='pathscompletedconcom')

for pathscompletedconcom in frozenset.union(*DF_OHE.pathscompletedconcom):
    DF_OHE[pathscompletedconcom] = DF_OHE.apply(lambda _: int(pathscompletedconcom in _.pathscompletedconcom), axis=1)

## remove_columns pathscompletedconcom_V2, sort dataframe index and
####   rename Paths columns path1..., path2... path3... and merge all events
DF_OHE = DF_OHE.drop(columns=['pathscompletedconcom'])
DF_OHE.columns = f_path_rename(DF_OHE.columns)
DF_OHE = f_include_default_path_df(DF_OHE)
DF_OHE.fillna(0, inplace=True)
INT_COLUMNS = DF_OHE.columns
DF_OHE = DF_OHE[INT_COLUMNS].astype(int)

DF_ALL_EVENTS = DF_ALL_EVENTS.join(DF_OHE)
DF_ALL_EVENTS = DF_ALL_EVENTS.drop(columns=['pathscompletedconcom'])

f_log('Calculate number of hours...', 1)
DF_ALL_EVENTS['conversationstartedatconsta'] = \
    DF_ALL_EVENTS['conversationstartedatconsta'].apply(f_convert_str_to_datetime)
DF_ALL_EVENTS['datelocaltimecomfirsta'] = \
    DF_ALL_EVENTS['datelocaltimecomfirsta'].apply(f_convert_str_to_datetime)

## SIMULATE EXECUTION FOR YESTERDAY - UPDATE all_events to yesterday date INSTEAD OF 04-11-2019
EXECUTION_DATE = datetime.date.today() - datetime.timedelta(days=1) ## yesterday
EXECUTION_DATE_STR = EXECUTION_DATE.strftime("%Y-%m-%d")
if V_DT_MIXPANEL_STR == EXECUTION_DATE_STR:
    OLD_DATE = datetime.date(2019, 11, 4)## SAME DAY OF WORKAROUND TO COPY FILES
    ADD_DAYS = EXECUTION_DATE - OLD_DATE

    DF_ALL_EVENTS['conversationstartedatconsta'] = f_update_date(
        DF_ALL_EVENTS.conversationstartedatconsta, ADD_DAYS.days)

    DF_ALL_EVENTS['datelocaltimecomfirsta'] = f_update_date(
        DF_ALL_EVENTS.datelocaltimecomfirsta, ADD_DAYS.days)

## OLD CALC , REPLACE BY THE LAMBDA FUNCTION BELOW WITH TRY EXCEPTION
DF_ALL_EVENTS['hours_since_start'] = DF_ALL_EVENTS.apply(lambda x: f_hours_since_start(
    x['datelocaltimecomfirsta'], x['conversationstartedatconsta']), axis=1)

### --------------------------------------------------------------
## Label Target info : Number of conversations started
f_log('Processing target - number of conversations ...', 1)
DF_GROUP = DF_CONVERSATION_START.groupby('distinct_id').conversationid.nunique()
DF_GROUP = DF_GROUP.to_frame(name='num_convo').reset_index()

### --------------------------------------------------------------
## Data wrangling 2 - Amount of time in hours between Conversations
f_log('Processing additional feature - amount of yours...', 1)
COL_HOURS = ['distinct_id', 'conversationid', 'conversationstartedatconsta']
DF_AMOUNT_TIME = DF_CONVERSATION_START.copy()
DF_AMOUNT_TIME = DF_AMOUNT_TIME[COL_HOURS]
DF_AMOUNT_TIME['conversationstartedatconsta'] = DF_AMOUNT_TIME['conversationstartedatconsta'].apply(f_convert_str_to_datetime)
DF_AMOUNT_TIME = DF_AMOUNT_TIME.sort_values(by=['distinct_id', 'conversationstartedatconsta'], ascending=True)

TIME_LIST = []
DELTA_LIST = []
V_OLD_USER = ''
V_OLD_DATETIME = ''

for index, row in DF_AMOUNT_TIME.iterrows():
    v_current_user = row['distinct_id']
    v_current_datetime = row['conversationstartedatconsta']

    if index == 0:
        V_OLD_USER = row['distinct_id']
        V_OLD_DATETIME = row['conversationstartedatconsta']
        v_hour = 0
        v_delta = 0
    else:
        if V_OLD_USER != v_current_user:
            v_hour = 0
            v_delta = 0
            V_OLD_DATETIME = v_current_datetime
            V_OLD_USER = v_current_user
        else:
            v_hour = delta_days_hours(v_current_datetime - V_OLD_DATETIME)
            v_delta = v_current_datetime - V_OLD_DATETIME
            V_OLD_DATETIME = v_current_datetime
            V_OLD_USER = v_current_user

    TIME_LIST.append(v_hour)
    DELTA_LIST.append(v_delta)

DF_AMOUNT_TIME['hours_since_last'] = TIME_LIST
DF_AMOUNT_TIME['delta_hours_since_last'] = DELTA_LIST

DF_ALL_EVENTS = DF_ALL_EVENTS.merge(DF_GROUP, left_on=['distinct_id'],
                                    right_on=['distinct_id'], how='left')

COLUMNS_DELTA_HOURS = ['distinct_id', 'conversationid', 'hours_since_last']
DF_AMOUNT_TIME = DF_AMOUNT_TIME[COLUMNS_DELTA_HOURS]

DF_ALL_EVENTS = DF_ALL_EVENTS.merge(DF_AMOUNT_TIME, left_on=['distinct_id', 'conversationid'],
                                    right_on=['distinct_id', 'conversationid'], how='left')

### --------------------------------------------------------------
## Export the results to CSV - final output
V_EXPORT_FILENAME = V_DATA_DIR + V_DT_MIXPANEL_STR + '-' + V_EXPORT_CSV
f_log('Export results to csv (Linux  storage): ' + V_EXPORT_FILENAME, 1)
DF_ALL_EVENTS.to_csv(V_EXPORT_FILENAME, index=False)

### --------------------------------------------------------------
### GCP EXECUTION
if sys.argv[1] == 'gcp':
    try:
        f_log('GCP EXECUTION ... ',1)
        # TODO: Setup GCP service account and GS BUCKET to run the command gsutil cp ...
        ## AND UNCOMMENT GCP LINES BELOW
        f_log('GCP : exporting results (gs bucket):' + V_GCS_BUCKET + '/' + V_EXPORT_FILENAME, 1)
        # GSUTIL_MSG = 'gsutil cp ' + V_EXPORT_FILENAME + ' ' + V_GCS_BUCKET
        # f_log(GSUTIL_MSG, 1)
        # CP_GCS = prc.check_output(GSUTIL_MSG, shell=True)
        # print(CP_GCS)
    except:
        f_log('EXCEPTION : problem to save file to google cloud storage')

### --------------------------------------------------------------
### AWS EXECUTION
if sys.argv[1] == 'aws':
    f_log('AWS EXECUTION : access key (credentials local file) ', 1)
    SHORT_FILENAME = V_DT_MIXPANEL_STR + '-' + V_EXPORT_CSV
    ## TODO: setup aws credentials to run the upload file using boto3
    ### AND UNCOMMENT AWS LINES BELOW
    print(V_AWS_S3 + '/datapipeline_tmp/')
    f_log('Exporting results to AWS S3 : ' + V_AWS_S3 + '/datapipeline_tmp/'  + SHORT_FILENAME, 1)
    # S3_FILE_UPLOAD = boto3.client('s3', region_name='us-east-1')
    # ## dir_bucket = 'datapipeline_tmp'
    # S3_FILE_UPLOAD.upload_file(V_EXPORT_FILENAME,
    #                            AWS_BUCKET,
    #                            'datapipeline_tmp/{}'.format(SHORT_FILENAME))

### --------------------------------------------------------------
### Cleanup process
###
f_log('Cleanup process in  days...  ' + V_CLEANUP_DAYS, 1)
if not os.name == 'nt':
    f_cleanup_process(V_CLEANUP_DAYS, V_LOG_DIR)
    f_cleanup_process(V_CLEANUP_DAYS, V_DATA_DIR)
    f_cleanup_process(V_CLEANUP_DAYS, V_JSON_DIR)
    f_log('Cleanup done in Linux ', 1)


f_log('--------------------  Process end -------------------------- ', 1)

### -------------------------------------------------------------- Close file
if V_BOL_LOG_FILE:
    F.close()
    V_BOL_LOG_FILE = False

print('print - Process End')
