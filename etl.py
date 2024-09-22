from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import timedelta

from airflow.contrib.operators.ssh_operator import SSHOperator

from airflow.operators.dummy_operator import DummyOperator
from airflow import settings
from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook
from airfow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

dag = DAG(
    dag_id = "customer_360_pipeline1",
    start_date = days_ago(1)
)


# 1).Checking if the file is available and until then keep the dag on hold
sensor = HttpSensor(
    task_id = "watch_for_orders",
    http_conn_id = "order_s3",
    endpoint = "orders.csv",
    response_check = lambda response:response.status_code == 200,
    dag = dag,
    retry_delay = timedelta(minutes=5),
    retries = 12
)

# ---------------------------------------------------------------------------------------

# 2).Download the file orders.csv from s3 into the edge node , into the airflow_pipeline directory

def get_order_url():
    session = settings.Session()
    connection = session.query(Connection).filter(Connection.conn_id == "order_s3").first()
    return f'{connection.schema}://{connection.host}/orders.csv'

download_order_cmd = f'rm -rf airflow_pipeline && mkdir -p airflow_pipeline && cd airflow_pipeline && wget {get_order_url()}'

download_to_edge_node = SSHOperator(
    task_id = "download_orders",
    ssh_conn_id="itversity",
    command=download_order_cmd,
    dag=dag
)

# --------------------------------------------------------------------------------------------

# 3).Import the customers data from mysql database into hdfs using sqoop and built hive table on it
def fetch_customer_info_cmd():
    command_one = "hive -e 'DROP TABLE airflow.customers'"
    command_one_ext = "hdfs dfs -rm -R -f customers"
    command_two = "sqoop import --connect jdbc:mysql://192.168.1.24:3306/retail_db --username retail_dba --password cloudera --table customers --hive-import --create-hive-table --hive-table airflow.customers"
    command_three = "exit 0"
    return f'{command_one} && {command_one_ext} && ({command_two} || {command_three})'


import_customer_info = SSHOperator(
    task_id = "import_customers",
    ssh_conn_id="itversity",
    command=fetch_customer_info_cmd(),
    dag=dag
)

# ---------------------------------------------------------------------------------------


# 4).Move the orders.csv file downloaded from the S3 bucket into the edge node , to hdfs
upload_order_info = SSHOperator(
    task_id="upload_orders_to_hdfs",
    ssh_conn_id="itversity",
    command="hdfs dfs -rm -R -f airflow_input && hdfs dfs -mdkir -p airflow_input && hdfs dfs -put ./airflow_pipeline/orders.csv airflow_input/"
)

# -----------------------------------------------------------------------------------------
# 6).Execute the spark program to filter the orders

def get_order_filter_cmd():
    command_zero = "export SPARK_MAJOR_VERSION=2"
    command_one = "hdfs dfs -rm -R -f airflow_output"
    command_two = "spark-submit --class DataFramesExample sparkbundle.jar airflow_input/orders.csv airflow_output"

    return f" {command_zero} &&{command_one} && {command_two}"


process_order_info = SSHOperator(
    task_id="process_orders",
    ssh_conn_id="itversity",
    command=get_order_filter_cmd(),
    dag = dag
)

# -----------------------------------------------------------------------------------------

def create_order_hive_table_cmd():
    command_one = """hive -e "CREATE external table if not exists airflow.orders(order_id int,order_date string,customer_id int,status string) row format delimited fields terminated by ',' stored as textfile location '/user/bigdatabysumit/airflow_output ' " """

    return command_one


create_order_table = SSHOperator(
    task_id="create_orders_table_hive",
    ssh_conn_id="itversity",
    command=create_order_hive_table_cmd(),
    dag = dag
)

# --------------------------------------------------------------------------------------------

# Uploading data into hbase
def load_hdfs_cmd():
    command_one = "hive -e 'create table if not exists airflow.airflow_hbase(customer_id int , customer_fname string,customer_lname string,order_id int, order_date string) STORED BY \'org.apache.hadoop.hive.hbase.HbaseStorageHandler\' with SERDEPROPERTIES(\'hbase.columns.mapping\'=\'=:key.personal:customer_fname,personal:customer_lname,personal:order_id,personal:order_date\') '"

    command_two = 'hive -e "insert overwrite table airflow.airflow_hbase select c.customer_id,c.customerfname,c.customer_lname,o.order_id from airflow.customers c join airflow.orders o on (c.customer_id = o.customer_id)"'

    return f'{command_one} && {command_two}'

load_hbase = SSHOperator(
    task_id="load_hbase_table",
    ssh_conn_id="itversity",
    command=load_hdfs_cmd(),
    dag = dag
)

# 1).Create a channel in slack
# 2).This is slack airflow webhook
# 3).We have to acquire a url which contains your password
# 4).We have to make a connection with airflow

def slack_password():
    return "/TU2LPJ0LS/B01BU51PDNV/ec/NUUzA9Zdqc1Zyp4Th7Y65K"


success_notify = SlackWebhookOperator(
    task_id="success_notify",
    http_conn_id="slack_webhook",
    message="Data loaded successfully in HBase",
    username="airflow",
    webhook_token=slack_password(),
    dag=dag
)

failure_notify = SlackWebhookOperator(
    task_id="failure_notify",
    http_conn_id="slack_webhook",
    message="Data loaded failed in HBase",
    username="airflow",
    webhook_token=slack_password(),
    dag=dag,
    trigger_rule="all_failed"
)



dummy = DummyOperator(
    task_id="dummy",
    dag = dag
)



sensor >> import_customer_info >> dummy

sensor >> download_to_edge_node >> upload_order_info >> process_order_info >> create_order_table 

[import_customer_info,create_order_table] >> load_hbase
load_hbase >> [success_notify,failure_notify]