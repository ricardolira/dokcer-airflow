import json
import logging
import random

import os
from datetime import timedelta

import yaml
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.providers.amazon.aws.operators.emr_add_steps import (
    EmrAddStepsOperator,
)
from airflow.providers.amazon.aws.operators.emr_create_job_flow import (
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import (
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.utils.dates import days_ago

logger = logging.getLogger(__file__)

DAG_ID = os.path.basename(__file__).replace(".py", "")
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@weg.net"],
    "email_on_failure": False,
    "email_on_retry": False,
}
DAG_TAGS = ["emr"]

dag_definition = {
    "dag_id": DAG_ID,
    "description": "Run built-in Spark app on Amazon EMR",
    "default_args": DEFAULT_ARGS,
    "dagrun_timeout": timedelta(hours=2),
    "start_date": days_ago(1),
    "schedule_interval": "@once",
    "tags": DAG_TAGS,
}

def read_yaml_files_from_s3(path_to_s3_yaml_file: str) -> dict:
    """
    Read files from S3 bucket and parse into dict

    Reads a file from S3 bucket and parses the file as python readable dict.
    It accepts as param the s3 URI composed by the bucket and key to the yaml
    file.

    Example of s3 URI: s3://<bucket>/<path-to-file>/<dot-yaml-file>

    :param path_to_s3_yaml_file : str
        The path
    :return: data_as_dict : dict
        The python dict properly parsed.
    """
    data_as_dict = None
    try:
        s3_hook = S3Hook()
        data_as_yaml = s3_hook.read_key(path_to_s3_yaml_file)
        data_as_dict = yaml.safe_load(data_as_yaml)
    except Exception as exception:
        logger.warning(f"The file reader threw an exception: {exception}")
    return data_as_dict


def add_spark_step(step, jar_file, script_name):
    """
        Prepare the EMR payload containing a step job definition.

        Prepare the EMR payload containing a step job definition based on the data
        injestion that is performer after new data arrives into s3.

        These steps are based on the table schema and table name as dependencies
        of the python script taht will run in the EMR cluster.

        :param step : dict
            The dict containing all predefined parameters of a particular job.
            This is parsed from an s3 file reader.
        :param jar_file : str
            The jar file that should be imported as dependencies for the
            successful job run.
        :param script_name : str
            The name of the spark job that will be run in the EMR cluster.
        :return step_payload :
            The step payload that should be sent to the EMR cluster.
        """
    job_table_name = step.get("table_name")
    job_table_schema = step.get("table_schema")
    primary_keys = step.get("primary_keys")
    partition_key = step.get("partition_key")
    job_definitions = {
        "primary_keys": primary_keys,
        "table_schema": job_table_schema,
        "table_name": job_table_name,
        "partition_key": partition_key,
    }
    step_payload = {
        "Name": f"spark-job-{job_table_schema}-{job_table_name}",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--conf",
                "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                "--conf",
                "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "--packages",
                "io.delta:delta-core_2.12:0.7.0",
                "--jars",
                jar_file,
                script_name,
                json.dumps(job_definitions),
            ],
        },
    }
    return step_payload


def copy_step(source_object_path, destination_object_path, delete_on_success=False):
    """
    Prepare the EMR payload containing a step job definition.

    Prepare the EMR payload containing a step job definition for a simple s3
    copy job. If the task is sucessful, then the data source is removed from
    the source path.
    This accepts as arguments only s3 URI pointing to the key where the files
    are stored.

    :param source_object_path : str
        The source object URI in which data is currently stored upon job call.
    :param destination_object_path : str
        The destination object URI in which the data should be loaded upon
        job call.
    :param delete_on_success:
    :return:
    """
    args = [
        "s3-dist-cp",
        f"--src=s3://{source_object_path}",
        f"--dest=s3://{destination_object_path}",
        "--deleteOnSuccess",
    ]
    step_definition = {
        "Name": f"Moving data from {source_object_path} to {destination_object_path}",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": args,
        },
    }
    return step_definition


s3_path = (
    "s3://weg-lake-artifacts/spark-jobs/weg-cdc/ymls/emr_instance_groups.yml"
)
params = read_yaml_files_from_s3(s3_path)
JOB_FLOW_OVERRIDES = {
    "Name": params["job_name"],
    "LogUri": params["log_path_s3"],
    "ReleaseLabel": params["emr_version"],
    "StepConcurrencyLevel": params["step_concurrency_level"],
    "VisibleToAllUsers": params["visible_to_all_users"],
    "JobFlowRole": params["job_flow_role"],
    "ScaleDownBehavior": params["scale_down_behavior"],
    "ServiceRole": params["service_role"],
    "EbsRootVolumeSize": params["ebs_root_volume_size"],
    "Tags": params["tags"],
    "Configurations": params["configurations"],
    "Applications": params["applications"],
    "Steps": params["steps"],
    "Instances": params["instances"],
}

with DAG(**dag_definition) as dag:

    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        aws_conn_id='aws_default',
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    copy_data_from_raw_to_processing = EmrAddStepsOperator(
        task_id='copy_data_from_raw_to_processing',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=[
            copy_step("weg-lake-raw/dms2/", "weg-lake-raw/processing/"),
        ],
        retries=3,
        retry_delay=timedelta(seconds=10)
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        trigger_rule="all_done",
        retries=3,
        retry_delay=timedelta(seconds=60)
    )
    s3_path = 's3://weg-lake-artifacts/spark-jobs/weg-cdc/ymls/tables_configuration.yml'
    job_definition = read_yaml_files_from_s3(s3_path).get("batch_steps")
    steps = job_definition.get("steps")
    jar_file = job_definition.get("jars")
    script_name = job_definition.get("script_name")
    for step in steps:
        table_name = step.get("table_name")
        table_schema = step.get("table_schema")
        wait_for_retry = random.randint(1, 20)
        # wait_before_add_step = BashOperator(
        #     task_id=f'wait_until_step_{table_schema}_{table_name}_is_added',
        #     bash_command=f'sleep {random.randint(0, 60)}',
        # )
        step_adder = EmrAddStepsOperator(
            task_id=f"add_step_{table_schema}_{table_name}",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
            aws_conn_id="aws_default",
            steps=[
                add_spark_step(step, jar_file, script_name),
            ],
            retries=3,
            retry_delay=timedelta(seconds=wait_for_retry)
        )
        step_id = f"{{{{ task_instance.xcom_pull(task_ids='add_step_{table_schema}_{table_name}', key='return_value')[0] }}}}"
        step_checker = EmrStepSensor(
            task_id=f"watch_step_{table_schema}_{table_name}",
            job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
            step_id=step_id,
            aws_conn_id="aws_default",
            retries=3,
            retry_delay=timedelta(seconds=wait_for_retry)
        )
        copy_step_on_success = EmrAddStepsOperator(
            task_id=f"copy_step_on_success_{table_schema}_{table_name}",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
            aws_conn_id="aws_default",
            steps=[
                copy_step(
                    f"weg-lake-raw/processing/{table_schema}/{table_name}",
                    f"weg-lake-raw/archive/{table_schema}/{table_name}",
                ),
            ],
            trigger_rule="one_success",
            retries=3,
            retry_delay=timedelta(seconds=wait_for_retry)
        )
        step_id = f"{{{{ task_instance.xcom_pull(task_ids='copy_step_on_success_{table_schema}_{table_name}', key='return_value')[0] }}}}"
        step_copy_step_on_success_checker = EmrStepSensor(
            task_id=f"watch_copy_step_on_success_{table_schema}_{table_name}",
            job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
            step_id=step_id,
            aws_conn_id="aws_default",
            retries=3,
            retry_delay=timedelta(seconds=wait_for_retry)
        )
        copy_step_on_failure = EmrAddStepsOperator(
            task_id=f"copy_step_on_failure_{table_schema}_{table_name}",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
            aws_conn_id="aws_default",
            steps=[
                copy_step(
                    f"weg-lake-raw/processing/{table_schema}/{table_name}",
                    f"weg-lake-raw/dms2/{table_schema}/{table_name}",
                ),
            ],
            trigger_rule="one_failed",
            retries=3,
            retry_delay=timedelta(seconds=wait_for_retry)
        )
        step_id = f"{{{{ task_instance.xcom_pull(task_ids='copy_step_on_failure_{table_schema}_{table_name}', key='return_value')[0] }}}}"
        step_copy_step_on_failure_checker = EmrStepSensor(
            task_id=f"watch_copy_step_on_failure_{table_schema}_{table_name}",
            job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
            step_id=step_id,
            aws_conn_id="aws_default",
            retries=3,
            retry_delay=timedelta(seconds=wait_for_retry)
        )
        cluster_creator >> copy_data_from_raw_to_processing >> \
        step_adder >> step_checker >> [
            copy_step_on_success, copy_step_on_failure
        ]
        copy_step_on_success >> step_copy_step_on_success_checker
        copy_step_on_failure >> step_copy_step_on_failure_checker
        [step_copy_step_on_success_checker, step_copy_step_on_failure_checker] >> \
        terminate_emr_cluster