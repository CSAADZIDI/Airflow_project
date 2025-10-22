from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.task.trigger_rule import TriggerRule

from src.model_development import (
    load_data,
    data_preprocessing,
    separate_data_outputs,
    build_model,
    load_model,
)

# ---------- Enable XCom pickling ----------
from airflow.configuration import conf
conf.set("core", "enable_xcom_pickling", "True")

# ---------- Default args ----------
default_args = {
    "start_date": pendulum.datetime(2025, 10, 22, tz="UTC"),
    "retries": 0,
}

# Define function to notify failure or success via email
def notify_success(context):
    """Send email notification on success."""
    success_email = EmailOperator(
        task_id="notify_success_email",
        to="zidisaad.chaima@gmail.com",
        subject="Airflow Task Succeeded",
        html_content=f"<p>Task succeeded.</p>",
        dag=context['dag'],
    )
    success_email.execute(context=context)

def notify_failure(context):
    """Send email notification on failure."""
    failure_email = EmailOperator(
        task_id="notify_failure_email",
        to="zidisaad.chaima@gmail.com",
        subject="Airflow Task Failed",
        html_content=f"<p>Task failed.</p>",
        dag=context['dag'],
    )
    failure_email.execute(context=context)  

# ---------- DAG ----------
with DAG(
    dag_id="Airflow_project",
    default_args=default_args,
    description="Airflow_project DAG Description",
    schedule="@daily",
    catchup=False,
    tags=["example"],
    owner_links={"Chaima SAAD": "https://github.com/CSAADZIDI/Airflow_project"},
    max_active_runs=1,
) as dag:

    # ---------- Tasks ----------
    owner_task = BashOperator(
        task_id="task_using_linked_owner",
        bash_command="echo 1",
        owner="Chaima SAAD",
    )

    send_email = EmailOperator(
        task_id="send_email",
        to="zidisaad.chaima@gmail.com",
        subject="Notification from Airflow",
        html_content="<p>This is a notification email sent from Airflow.</p>",
        on_success_callback=notify_success,
        on_failure_callback=notify_failure
    )

    load_data_task = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data,
    )

    data_preprocessing_task = PythonOperator(
        task_id="data_preprocessing_task",
        python_callable=data_preprocessing,
        op_args=[load_data_task.output],
    )

    # Define function to separate data outputs
    def separate_data_outputs(**kwargs):
        ti = kwargs['ti']
        X_train, X_test, y_train, y_test = ti.xcom_pull(task_ids='data_preprocessing_task')
        return X_train, X_test, y_train, y_test

    separate_data_outputs_task = PythonOperator(
        task_id="separate_data_outputs_task",
        python_callable=separate_data_outputs,
        op_args=[data_preprocessing_task.output],
    )

    build_save_model_task = PythonOperator(
        task_id="build_save_model_task",
        python_callable=build_model,
        op_args=[separate_data_outputs_task.output, "model.sav"],
    )

    load_model_task = PythonOperator(
        task_id="load_model_task",
        python_callable=load_model,
        op_args=[separate_data_outputs_task.output, "model.sav"],
    )

    # Fire-and-forget trigger so this DAG can finish cleanly.
    trigger_dag_task = TriggerDagRunOperator(
        task_id="my_trigger_task",
        trigger_dag_id="Airflow_project_Flask",
        conf={"message": "Data from upstream DAG"},
        reset_dag_run=False,
        wait_for_completion=False,          # don't block
        trigger_rule=TriggerRule.ALL_DONE,  # still run even if something upstream fails
    )

    # ---------- Dependencies ----------
    owner_task >> load_data_task >> data_preprocessing_task >> \
        separate_data_outputs_task >> build_save_model_task >> \
        load_model_task >> trigger_dag_task

    # # Optional: email after model loads (independent branch)
    load_model_task >> send_email
