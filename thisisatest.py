"""
Code to run an image using a template
"""
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 20),
    "email": ["fhuaulme@talend.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

dag = DAG('', default_args=default_args,
          schedule_interval= '')

current = KubernetesPodOperator(namespace='default',
                                image="",
                                name="test-",
                                task_id="task-system-",
                                is_delete_operator_pod=True,
                                hostnetwork=False,
                                in_cluster=True,
                                dag=dag
                                )