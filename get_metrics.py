from google.cloud import monitoring
import pprint as pp
import pandas as pd
import numpy as np
import json
from time import gmtime, strftime
from datetime import datetime

# config vars for bq
project_id = "my-project"
private_key = "/path/to/XXX.json"

# yyyymmdd suffix for target bq table
bq_suffix = strftime("%Y%m%d", gmtime())

# monitoring client
client = monitoring.Client()

# list of metrics to pull
metrics = ['compute.googleapis.com/instance/cpu/utilization', 
           'compute.googleapis.com/instance/cpu/usage_time',
           'compute.googleapis.com/instance/disk/write_ops_count',
           'compute.googleapis.com/instance/disk/write_bytes_count',
           'compute.googleapis.com/instance/disk/read_ops_count',
           'compute.googleapis.com/instance/disk/read_bytes_count',
           'compute.googleapis.com/instance/network/received_bytes_count',
           'compute.googleapis.com/instance/network/received_packets_count',
           'compute.googleapis.com/instance/network/sent_bytes_count',
           'compute.googleapis.com/instance/network/sent_packets_count',
           'storage.googleapis.com/network/received_bytes_count',
           'storage.googleapis.com/network/sent_bytes_count',
           'bigquery.googleapis.com/query/count',
           'bigquery.googleapis.com/query/scanned_bytes_billed',
           'bigquery.googleapis.com/storage/uploaded_bytes_billed',
           'bigquery.googleapis.com/storage/uploaded_row_count',
           'bigquery.googleapis.com/query/execution_times',
           'cloudsql.googleapis.com/database/cpu/utilization',
           'cloudsql.googleapis.com/database/cpu/usage_time',
           'cloudsql.googleapis.com/database/disk/bytes_used',
           'cloudsql.googleapis.com/database/disk/read_ops_count',
           'cloudsql.googleapis.com/database/disk/write_ops_count',
           'cloudsql.googleapis.com/database/disk/utilization',
           'cloudsql.googleapis.com/database/memory/usage',
           'cloudsql.googleapis.com/database/memory/utilization',
           'cloudsql.googleapis.com/database/mysql/queries',
           'cloudsql.googleapis.com/database/network/connections',
           'cloudsql.googleapis.com/database/network/sent_bytes_count',
          ]

# pull last N minutes from stackdriver
minutes_ago = 15

# loop over each metric
for metric in metrics:
    print("\nMetric: {0}\n".format(metric))
    
    # pull type of metric to control what table to write to (to handle different schemas)
    metric_base = metric.split("/")[0].replace('.googleapis.com', '')
    
    # target table name to stream results into
    bq_table_name = "stackdriver.metrics_" + metric_base + "_" + bq_suffix
    
    # get query results
    query_results = client.query(metric, minutes = minutes_ago)
    
    # process each result
    for result in query_results:
    
        print('\nRESULT:\n')
        pp.pprint(result)
    
        print("==========================================")
         
        # dict for results
        data = {}
        data['metric'] = json.dumps(result.metric)
        data['labels'] = json.dumps(result.labels)
        data['resource'] = json.dumps(result.resource)
        data['metric_kind'] = json.dumps(result.metric_kind)
        data['value_type'] = json.dumps(result.value_type)
        
        # create df for meta data
        df = pd.io.json.json_normalize(data)
        
        # points data
        st, et, v = zip(*result.points)
        pdata = {}
        pdata['start_time'] = st
        pdata['end_time'] = et
        pdata['value'] = v
        
        # create df for points data values
        df_st = pd.DataFrame(np.array(st), columns=['start_time'])
        df_et = pd.DataFrame(np.array(et), columns=['end_time'])
        df_v = pd.DataFrame(np.array(v), columns=['values'])
        df_p = pd.concat([df_st, df_et, df_v], axis=1)
        
        # add dummy keys to use for merging two df's together
        df['mykey'] = 1
        df_p['mykey'] = 1
        
        # merge into a single df
        df_all = df.merge(df_p)
        
        # add timestamp for table
        df_all['load_job_info'] = str(datetime.now())
        
        # clean col names
        df_all.columns = df_all.columns.str.replace('.', '_')
        
        # convert all cols to string to ensure stable types in bq
        for col in df_all:
            df_all[col] = df_all[col].apply(str)
        
        # look at data to be sent        
        print("-------------------------------")
        print(df_all.head(2))
        print("-------------------------------")
        
        print("\n...writing to...{0}\n".format(bq_table_name))
        
        # send to bq
        df_all.to_gbq(bq_table_name, project_id = project_id, private_key = private_key, if_exists='append')
        
        print("==========================================")
    