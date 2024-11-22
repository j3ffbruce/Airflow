import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

docs = """
## DAG: dag_template

"Código fonte de template para criação de Pipelines.

#### Tasks Description

`Task: <name>`
- <description>
<br/>
For any questions or concerns, please contact: @***.com.br
<br/>
© Copyright 2023. Todos os direitos reservados.
"""


airflow_environment:str = Variable.get("airflow_environment")

if airflow_environment == "dev":
    task_config ={
        "spark": {
            "environment":{
                "cluster_name": "***-spark-dev",
                "bucket_spark": "***-stg-spark",
                "bucket_stg":"***-stg-projeto",
                "bucket_raw":"***-raw-projeto",
                "bucket_ref":"***-ref-projeto",
                "subnet": "***",
                "project_id_data_hub": "e-***-data-hub-dev-82fd"
            },
            "jobs":[
                {
                    "job_name": "dataset_carga_retroativa",
                    "project_name": "pjto_higienizacao_agencia",
                    "script_name": "spark_carga_retroativa.py",
                    "dependency": False,
                    "process_type": "extract",
                    "output_file_name" : "dataset_carga_retroativa",
                    "path_origin": "files/***/logistica/higienizacao_agencia/csv",
                    "path_destin": "***/logistica/higienizacao_agencia"
                },
                {
                    "job_name": "dim_via_cartao",
                    "project_name": "pjto_fluxo_cartoes",
                    "script_name": "spark_dim_via_cartao.py",
                    "dependency": False,
                    "process_type": "transform",
                    "output_file_name" : "dim_calendar",
                    "path_destin": "projeto/pjto_fluxo_cartoes",
                },
                {
                    "job_name": "fat_historico_situacao_cartao",
                    "project_name": "pjto_fluxo_cartoes",
                    "script_name": "spark_fat_historico_situacao_cartao.py",
                    "dependency": ['dim_via_cartao'],
                    "process_type": "transform",
                    "output_file_name" : "fat_historico_situacao_cartao",
                    "path_destin": "projeto/pjto_fluxo_cartoes",
                },
                {
                    "job_name": "fat_primeira_ativacao",
                    "project_name": "pjto_fluxo_cartoes",
                    "script_name": "spark_fat_primeira_ativacao.py",
                    "dependency": ['fat_historico_situacao_cartao'],
                    "process_type": "transform",
                    "output_file_name" : "fat_primeira_ativacao",
                    "path_destin": "projeto/pjto_fluxo_cartoes",
                },
                {
                    "job_name": "fat_agg_situacao_cartao_snapshot",
                    "project_name": "pjto_fluxo_cartoes",
                    "script_name": "spark_fat_agg_situacao_cartao_snapshot.py",
                    "dependency": ['fat_historico_situacao_cartao'],
                    "process_type": "transform",
                    "output_file_name" : "fat_agg_situacao_cartao_snapshot",
                    "path_destin": "projeto/pjto_fluxo_cartoes",
                }
                ]
            },
        "big_query": {
            "environment":{
                "project_id_producer": "e-***-producer-dev-87d1",
                "bucket_ref":"***-ref-projeto"
            },
            "jobs": [
                {
                    "job_name": "fat_agg_situacao_cartao_snapshot",
                    "dataset_name":"pjto_fluxo_cartoes",
                    "table_name":"fat_agg_situacao_cartao_snapshot",
                    "file_name" : "fat_agg_situacao_cartao_snapshot",
                    "path_project": "projeto/pjto_fluxo_cartoes",
                }]
        },
        "great_expectations": {
            "environment":{
                "working_directory": "/home/airflow/gcsfuse/great_expectations",
                "dag_name": "dag_template"
            },
            "jobs": [
                {
                    "dataset_name": "dim_via_cartao",
                    "bucket_ref": '***-ref-projeto',
                    "path_location": "projeto/pjto_fluxo_cartoes/dim_via_cartao",
                    "project_name": "pjto_fluxo_cartoes",
                    "data_sample_size_limitation": 50,
                    "exceptions": {   
                        "expect_column_values_to_not_be_null": True,
                        "expect_column_values_to_be_of_type": True,
                        "expect_table_columns_to_match_ordered_list": True
                    },
                    "columns" : [
                        {
                            "name": "nr_identificacao_cartao",
                            "is_null": False,
                            "type": "objgit ect"
                        },
                        {
                            "name": "ds_modelo_cartao",
                            "is_null": False,
                            "type": "object"
                        },
                        {
                            "name": "nr_remessa",
                            "is_null": False,
                            "type": "int32"
                        },
                        {
                            "name": "nr_bin_cartao",
                            "is_null": False,
                            "type": "int32"
                        },
                        {
                            "name": "ds_tipo_cartao",
                            "is_null": False,
                            "type": "object"
                        },
                        {
                            "name": "dt_solicitacao_cartao",
                            "is_null": False,
                            "type": "datetime64[ns]"
                        }
                    ]
                },
                {
                    "dataset_name": "fat_primeira_ativacao",
                    "bucket_ref": '***-ref-projeto',
                    "path_location": "projeto/pjto_fluxo_cartoes/fat_primeira_ativacao",
                    "project_name": "pjto_fluxo_cartoes",
                    "data_sample_size_limitation": 50,
                    "exceptions": {   
                        "expect_column_values_to_not_be_null": True,
                        "expect_column_values_to_be_of_type": True,
                        "expect_table_columns_to_match_ordered_list": True
                    },
                    "columns" : [
                        {
                            "name": "cd_historico_cartao",
                            "is_null": False,
                            "type": "int32"
                        },
                        {
                            "name": "ds_modelo_cartao",
                            "is_null": False,
                            "type": "object"
                        },
                        {
                            "name": "ds_tipo_cartao",
                            "is_null": False,
                            "type": "object"
                        },
                        {
                            "name": "nr_remessa",
                            "is_null": False,
                            "type": "int32"
                        },
                        {
                            "name": "nr_dias_ativacao",
                            "is_null": False,
                            "type": "int32"
                        },
                        {
                            "name": "dt_solicitacao",
                            "is_null": False,
                            "type": "datetime64[ns]"
                        },
                        {
                            "name": "dt_historico_cartao",
                            "is_null": False,
                            "type": "datetime64[ns]"
                        }
                    ]
                },
                {
                    "dataset_name": "fat_agg_situacao_cartao_snapshot",
                    "bucket_ref": '***-ref-projeto',
                    "path_location": "projeto/pjto_fluxo_cartoes/fat_agg_situacao_cartao_snapshot",
                    "project_name": "pjto_fluxo_cartoes",
                    "data_sample_size_limitation": None,
                    "exceptions": {   
                        "expect_column_values_to_not_be_null": True,
                        "expect_column_values_to_be_of_type": True,
                        "expect_table_columns_to_match_ordered_list": True
                    },
                    "columns" : [
                        {
                            "name": "dt_referencia_solicitacao",
                            "is_null": False,
                            "type": "datetime64[ns]"
                        },
                        {
                            "name": "dt_referencia_historico",
                            "is_null": False,
                            "type": "datetime64[ns]"
                        },
                        {
                            "name": "ds_modelo_cartao",
                            "is_null": False,
                            "type": "object"
                        },
                        {
                            "name": "nr_remessa",
                            "is_null": False,
                            "type": "int32"
                        },
                        {
                            "name": "ds_tipo_cartao",
                            "is_null": False,
                            "type": "object"
                        },
                        {
                            "name": "ds_situacao",
                            "is_null": False,
                            "type": "object"
                        },
                        {
                            "name": "qt_cartao",
                            "is_null": False,
                            "type": "int32"
                        }
                    ]
                }
            ]
        }
    }

default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
}

with DAG(
    'dag_template',
    schedule_interval=None,                                 
    catchup=False,                                          
    concurrency=1,
    tags=['template',"extract", 'transform', 'test', 'load'],
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=20),
    description='Pipeline template') as dag:

    dag.doc_md = docs

    if airflow_environment == "dev":
        create_dataproc_cluster = BashOperator(
        task_id='create_dataproc_cluster',
        bash_command=f"""echo None""")
  
    elif airflow_environment == "prd":
        create_dataproc_cluster = BashOperator(
        task_id='create_dataproc_cluster',
        bash_command=f"""
        gcloud dataproc clusters create {task_config["spark"]["environment"]["cluster_name"]} \
        --enable-component-gateway \
        --bucket {task_config["spark"]["environment"]["bucket_spark"]} \
        --region southamerica-east1 \
        --subnet {task_config["spark"]["environment"]["bucket_spark"]} \
        --no-address \
        --zone southamerica-east1-a \
        --master-machine-type n1-standard-4 \
        --master-boot-disk-size 500 \
        --num-workers 2 \
        --worker-machine-type n1-standard-4 \
        --worker-boot-disk-size 500 \
        --image-version 2.0-debian10 \
        --optional-components JUPYTER \
        --scopes 'https://www.googleapis.com/auth/cloud-platform' \
        --project {task_config["spark"]["environment"]["project_id_data_hub"]} \
        --properties=^#^dataproc:dataproc.conscrypt.provider.enable=false\
        --labels aplicacao=dag_higienizacao_agencia,dag=dag_higienizacao_agencia""")

    with TaskGroup("extract_data") as extract_data:

        operators:list = []

        for job in task_config["spark"]["jobs"]:
            if ((job["process_type"] == "extract") and (job["dependency"] == False)):
                extract = BashOperator(
                task_id=f"{job['job_name']}",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark gs://{task_config["spark"]["environment"]["bucket_spark"]}/jobs/{job['project_name']}/{job['script_name']} \
                    --cluster={task_config["spark"]["environment"]["cluster_name"]} \
                    --region=southamerica-east1  \
                    --jars=gs://{task_config["spark"]["environment"]["bucket_spark"]}/jars/mssql-jdbc-6.4.0.jre8.jar \
                    --  --bucket_stage="{task_config["spark"]["environment"]["bucket_stg"]}" \
                        --path_origin="{job['path_origin']}" \
                        --bucket_raw="{task_config["spark"]["environment"]["bucket_raw"]}" \
                        --path_destin="{job['path_destin']}"
                """)

                operators.append({"task_id": job['job_name'], "operator": extract})

                extract
        
        for job in task_config["spark"]["jobs"]:
            if ((job["process_type"] == "extract") and (job["dependency"] != False)):
                extract = BashOperator(
                task_id=f"{job['job_name']}",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark gs://{task_config["spark"]["environment"]["bucket_spark"]}/jobs/{job['project_name']}/{job['script_name']} \
                    --cluster={task_config["spark"]["environment"]["cluster_name"]} \
                    --region=southamerica-east1  \
                    --jars=gs://{task_config["spark"]["environment"]["bucket_spark"]}/jars/mssql-jdbc-6.4.0.jre8.jar \
                    --  --bucket_stage="{task_config["spark"]["environment"]["bucket_stg"]}" \
                        --path_origin="{job['path_origin']}" \
                        --bucket_raw="{task_config["spark"]["environment"]["bucket_raw"]}" \
                        --path_destin="{job['path_destin']}"
                """)
                operators.append({"task_id": job['job_name'], "operator": extract})
            
                for dependency in job["dependency"]:
                    for operator in operators:
                        if dependency == operator['task_id']:
                            operator["operator"] >> extract

    with TaskGroup("transform_data") as transform_data:

        operators:list = []

        for job in task_config["spark"]["jobs"]:
            if ((job["process_type"] == "transform") and (job["dependency"] == False)):
                transform = BashOperator(
                task_id=f"{job['job_name']}",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark gs://{task_config["spark"]["environment"]["bucket_spark"]}/jobs/{job['project_name']}/{job['script_name']} \
                    --cluster={task_config["spark"]["environment"]["cluster_name"]} \
                    --region=southamerica-east1  \
                    --jars=gs://{task_config["spark"]["environment"]["bucket_spark"]}/jars/mssql-jdbc-6.4.0.jre8.jar \
                    -- --bucket_raw="{task_config["spark"]["environment"]["bucket_raw"]}" \
                        --bucket_ref="{task_config["spark"]["environment"]["bucket_ref"]}" \
                        --path_destin="{job['path_destin']}" \
                        --output_file_name="{job['output_file_name']}"
                """)
                operators.append({"task_id": job['job_name'], "operator": transform})
                transform
        
        for job in task_config["spark"]["jobs"]:
            if ((job["process_type"] == "transform") and (job["dependency"] != False)):
                transform = BashOperator(
                task_id=f"{job['job_name']}",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark gs://{task_config["spark"]["environment"]["bucket_spark"]}/jobs/{job['project_name']}/{job['script_name']} \
                    --cluster={task_config["spark"]["environment"]["cluster_name"]} \
                    --region=southamerica-east1  \
                    --jars=gs://{task_config["spark"]["environment"]["bucket_spark"]}/jars/mssql-jdbc-6.4.0.jre8.jar \
                    -- --bucket_raw="{task_config["spark"]["environment"]["bucket_raw"]}" \
                        --bucket_ref="{task_config["spark"]["environment"]["bucket_ref"]}" \
                        --path_destin="{job['path_destin']}" \
                        --output_file_name="{job['output_file_name']}"
                """)
                operators.append({"task_id": job['job_name'], "operator": transform})
            
                for dependency in job["dependency"]:
                    for operator in operators:
                        if dependency == operator['task_id']:
                            operator["operator"] >> transform
    
    with TaskGroup("data_quality") as data_quality:
        @task()
        def create_test_architecture():
            import great_expectations
            import os
            from great_expectations.core.expectation_configuration import ExpectationConfiguration
            from great_expectations.expectations.expectation import Expectation
            from great_expectations.core import ExpectationConfiguration

            working_directory = task_config["great_expectations"]["environment"]["working_directory"]
            os.chdir(working_directory)

            def add_data_source(context):
                datasource_config = {
                    "name": "pandas_datasource",
                    "class_name": "Datasource",
                    "module_name": "great_expectations.datasource",
                    "execution_engine": {
                        "module_name": "great_expectations.execution_engine",
                        "class_name": "PandasExecutionEngine",
                    },
                    "data_connectors": {
                        "default_runtime_data_connector_name": {
                            "class_name": "RuntimeDataConnector",
                            "module_name": "great_expectations.datasource.data_connector",
                            "batch_identifiers": ["default_identifier_name"],
                        },
                    },
                }

                context.add_datasource(**datasource_config)

            def add_expectation_suite(context, expectation_suite_name):
                context.create_expectation_suite(
                    expectation_suite_name=expectation_suite_name,
                    overwrite_existing=True
                )

                return context.get_expectation_suite(expectation_suite_name)

            def add_expect_column_values_to_not_be_null(expectation_suite, column_name):
                expectation_config = ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={
                        "column": column_name
                    },
                    meta={
                        "notes": f"Valida se a coluna {column_name} é NotNull"
                    }
                )
                expectation_suite.add_expectation(expectation_config)

            def add_expect_column_values_to_be_of_type(expectation_suite, column_name, type_name):
                expectation_config = ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_of_type",
                    kwargs={
                        "column": column_name,
                        "type_": type_name
                    },
                    meta={
                        "notes": f"Valida se a coluna {column_name} está com o tipo {type_name}"
                    }
                )
                expectation_suite.add_expectation(expectation_config)
            
            def add_expect_table_columns_to_match_ordered_list(expectation_suite, column_list:list):
                expectation_config = ExpectationConfiguration(
                    expectation_type="expect_table_columns_to_match_ordered_list",
                    kwargs={
                        "column_list": column_list
                    },
                    meta={
                        "notes": f"Efetua validação das colunas da existencia das colunas na ordem {column_list} do dataset"
                    }
                )
                expectation_suite.add_expectation(expectation_config)

            def add_checkpoint(context, dataset_name, expectation_suite_name):
                checkpoint_config = {
                    "name": f"checkpoint_{dataset_name}",
                    "config_version": 1,
                    "class_name": "SimpleCheckpoint",
                    "expectation_suite_name": expectation_suite_name,
                }
                context.add_checkpoint(**checkpoint_config)

            context = great_expectations.get_context()          
            
            add_data_source(context)
            
            for dataset in task_config["great_expectations"]["jobs"]:
                expectation_suite_name = f'{dataset["project_name"]}_{dataset["dataset_name"]}'
                expectation_suite = add_expectation_suite(context, expectation_suite_name)
                collumns_order_list = []
                for column in dataset['columns']:
                    collumns_order_list.append(column["name"])
                    if dataset["exceptions"]["expect_column_values_to_not_be_null"] == True and column["is_null"] == False:
                        add_expect_column_values_to_not_be_null(expectation_suite, column['name'])
                    if dataset["exceptions"]["expect_column_values_to_be_of_type"] == True:
                        add_expect_column_values_to_be_of_type(expectation_suite, column['name'], column['type'])
                if dataset["exceptions"]["expect_table_columns_to_match_ordered_list"] == True:
                    add_expect_table_columns_to_match_ordered_list(expectation_suite, collumns_order_list)
                context.save_expectation_suite(expectation_suite)
                add_checkpoint(context,dataset["dataset_name"], expectation_suite_name)

        create_test_architecture = create_test_architecture()   
        
        @task.virtualenv(
            requirements=['dask==2023.5.0', 'google-cloud-storage==2.10.0','gcsfs==2023.9.0', 'dask[dataframe]', 'fastparquet==2023.7.0'],
            system_site_packages=True,
        )
        def run_quality_test(task_config):
            from google.cloud import storage
            import dask.dataframe as dd
            import fastparquet
            import os
            from great_expectations.core.batch import RuntimeBatchRequest
            import great_expectations
            from airflow.exceptions import AirflowException

            def mb_to_byte(mb):
                bytes = mb * (1024 * 1024)
                return bytes

            def bytes_to_mb(bytes):
                mb = bytes / (1024 * 1024)
                return mb

            def generate_data_sample(dataframe, mb):
                first_line = dd.from_pandas(dataframe.head(1), npartitions=1)
                memory_size = first_line.memory_usage(deep=True).sum().compute()
                number_of_lines = int(mb_to_byte(mb)//memory_size)     
                number_of_partitions = int((number_of_lines//2000000) + 1)
                data_sample = dataframe.head(n=number_of_lines, npartitions=number_of_partitions)
                data_sample_converted = dd.from_pandas(data_sample, npartitions=1)
                memory_size = bytes_to_mb(data_sample_converted.memory_usage(deep=True).sum().compute())
                print(f"[Airflow Log]: Number of rows in the data sample =  {number_of_lines} line(s)")
                print(f"[Airflow Log]: Dataframe Size =  {int(memory_size)} mb(s)")
                
                return data_sample_converted

            def list_bucket_files(bucket_name, folder, extension):
                client = storage.Client()
                bucket = client.get_bucket(bucket_name)
                blobs = bucket.list_blobs(prefix=folder+'/')
                
                files = []
                
                for blob in blobs:
                    if blob.name.endswith(extension):
                        nome_arquivo = os.path.basename(blob.name)
                        files.append(nome_arquivo)
                
                return files
                
            working_directory = task_config["great_expectations"]["environment"]["working_directory"]
            os.chdir(working_directory)
            context = great_expectations.get_context()
            results = []

            for dataset in task_config["great_expectations"]["jobs"]:
                print(f"[Airflow Log]: Dataset Name {dataset['dataset_name']}")
                tables = []
                files = list_bucket_files(dataset['bucket_ref'], dataset['path_location'], ".parquet")
                
                for file in files:
                    table = dd.read_parquet(f"gs://{dataset['bucket_ref']}/{dataset['path_location']}/{file}", engine="fastparquet")
                    tables.append(table)           

                dataFrame = dd.concat(tables)
                
                if dataset['data_sample_size_limitation'] != None:
                    dataFrame = generate_data_sample(dataFrame,dataset['data_sample_size_limitation'])
                else:
                    memory_size = bytes_to_mb(dataFrame.memory_usage(deep=True).sum().compute())
                    print(f"[Airflow Log]: Dataframe Total Size  =  {int(memory_size)} mb(s)")

                batch_request = RuntimeBatchRequest(
                    datasource_name="pandas_datasource",
                    data_connector_name="default_runtime_data_connector_name",
                    data_asset_name=dataset["dataset_name"],
                    runtime_parameters={"batch_data": dataFrame.compute()},
                    batch_identifiers={"default_identifier_name": "default_identifier"},
                )

                result = context.run_checkpoint(
                    checkpoint_name=f"checkpoint_{dataset['dataset_name']}",
                    run_name=f'{task_config["great_expectations"]["environment"]["dag_name"]}',
                    validations=[
                        {"batch_request": batch_request}
                    ],
                )

                results.append(result)

            for result in results:
                if result["success"]:
                    print("[Airflow Log]: Data Quality - PASS!")
                else:
                    raise AirflowException("[Airflow Log] Erro: Falha no Data Quality")

        run_quality_test = run_quality_test(task_config) 
    
    with TaskGroup("load_data") as load_data:
        for job in task_config["big_query"]["jobs"]:
           load = BigQueryInsertJobOperator(
               task_id=f"{job['job_name']}",
               configuration={
                   "load": {
                       "writeDisposition": "WRITE_TRUNCATE",
                       "createDisposition": "CREATE_IF_NEEDED",
                       "destinationTable": {
                           "projectId": task_config['big_query']['environment']['project_id_producer'],
                           "datasetId": job['dataset_name'],
                           "tableId": job['table_name']
                       },
                       "sourceUris": f"gs://{task_config['big_query']['environment']['bucket_ref']}/{job['path_project']}/{job['file_name']}/*.parquet",
                       "sourceFormat": "PARQUET"
                   }
               })
        
           load

    if airflow_environment == "dev":
        drop_dataproc_cluster = BashOperator(
        task_id='drop_dataproc_cluster',
        trigger_rule='all_done',
        bash_command=f"""echo None""")
  
    elif airflow_environment == "prd":
        drop_dataproc_cluster = BashOperator(
        task_id='drop_dataproc_cluster',
        trigger_rule='all_done',
        bash_command=f"""gcloud dataproc clusters delete {task_config["spark"]["environment"]["cluster_name"]} --region southamerica-east1""")

    # Task-Flow
    create_test_architecture >> run_quality_test

    create_dataproc_cluster >> extract_data >> transform_data >> data_quality >> load_data
    data_quality >> drop_dataproc_cluster


"""

(Dev) Transfer Filecc to Airflow Bucket: 
gsutil cp -r dag_template.py gs://southamerica-east1-***-air-b2b67ece-bucket/dags/

"""