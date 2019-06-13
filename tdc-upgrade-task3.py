"""
Code to run an image using a template
"""
import logging
import requests
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.subdag_operator import SubDagOperator

log_level=logging.DEBUG if os.environ.get('LOG_LEVEL') == 'DEBUG' else logging.INFO
logging.basicConfig(stream=sys.stdout, level=log_level,
                    format='%(asctime)s - %(levelname)s - %(message)s',)
logger=logging.getLogger("app")

accountServiceUrl=os.environ.get('ACCOUNT_SERVICE_URL')

"""
Utility method to list accounts from Account service
"""
def listAccounts(application):
    logger.debug("Listing account from URL {}".format(accountServiceUrl))
    headers = {"Accept": "application/json"}
    logger.info("Search all accounts with application {}".format(application))
    url=accountServiceUrl + "?application={}&count=100".format(application)
    response=requests.get(url, headers=headers)
    if response.status_code != 200:
        logger.error("Account service error, returned status={}, reason={}".format(response.status_code, response.reason))
        return []
    else:
        payload=response.json()
        accounts=payload.get("Resources")
        logger.info("{} accounts returned by account service".format(len(accounts)))
        return accounts

"""
DAG Declaration
"""
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.today().strftime("%m/%d/%Y %H:%M:%S"),
    "email": "",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1
}

configmaps = ['']

dag = DAG('', default_args=default_args,
          schedule_interval= '')

accounts = listAccounts("")
validationAccounts= accounts[0:min(5, len(accounts))]
remainingAccounts=[]
if(len(validationAccounts) >= 5):
    remainingAccounts=accounts[5:len(accounts)]

validationAccountOperator = SubDagOperator(dag=dag, parent=dag)

for account in validationAccounts:
    accountId = str(account.get("id"))
    current = KubernetesPodOperator(namespace="",
                image="",
                arguments=[accountId],
                name="" + accountId,
                task_id="" + accountId,
                is_delete_operator_pod=True,
                hostnetwork=False,
                in_cluster=True,
                dag=dag,
                configmaps=configmaps
            )
    current.set_upstream(validationAccountOperator)

remainingAccountOperator = SubDagOperator(dag=dag, parent=dag)

for account in remainingAccounts:
    accountId = str(account.get("id"))
    current = KubernetesPodOperator(namespace="",
                image="",
                arguments=[accountId],
                name="" + accountId,
                task_id="" + accountId,
                is_delete_operator_pod=True,
                hostnetwork=False,
                in_cluster=True,
                configmaps=configmaps
            )
    current.set_upstream(remainingAccountOperator)