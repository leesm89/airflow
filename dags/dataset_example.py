from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Produce 작업: 숫자 생성
def produce_number():
    number = 10  # 간단한 숫자 생성
    print(f"Produced number: {number}")
    return number  # XCom으로 전달

# Consume 작업: 숫자 소비
def consume_number(**kwargs):
    number = kwargs['ti'].xcom_pull(task_ids='produce_number')  # XCom으로 전달된 숫자 가져오기
    print(f"Consumed number: {number}")
    return number * 2  # 숫자에 2를 곱하는 예제

# DAG 정의
with DAG(
    'simple_produce_consume_example',
    default_args={'owner': 'airflow'},
    description='A simple Airflow DAG demonstrating a produce-consume pattern',
    schedule_interval=None,  # 수동 실행
    start_date=datetime(2025, 8, 10),
    catchup=False,
) as dag:

    # Produce 작업
    produce_task = PythonOperator(
        task_id='produce_number',
        python_callable=produce_number,
    )

    # Consume 작업
    consume_task = PythonOperator(
        task_id='consume_number',
        python_callable=consume_number,
        provide_context=True,  # XCom을 사용하려면 이 옵션을 활성화
    )

    # 작업 흐름 정의
    produce_task >> consume_task
