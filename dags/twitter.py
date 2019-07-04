import sys
import os
import datetime as dt
import sqlite3 as sql

if sys.platform == 'darwin' or sys.platform == 'linux':
    sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../")

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.sqlite_hook import SqliteHook
from src.scraper import TwitterScraper
from src.kafka.producer import publish_trends_message, publish_users_message
from src.kafka.consumer import get_trends_message, get_users_message

TwitterCrawler = TwitterScraper()

def publish_trends(**kwargs):
    trendings = TwitterCrawler.get_trendings()
    publish_trends_message(trendings)

def crawl_users(**kwargs):
    trendings = get_trends_message()
    user_groups = TwitterCrawler.get_trending_users(trendings)
    return user_groups

def publish_users(**kwargs):
    ti = kwargs['ti']
    user_groups = ti.xcom_pull(key=None, task_ids='crawl_users')
    publish_users_message(user_groups)

def crawl_user_tweets(**kwargs):
    user_groups = get_users_message()
    data = TwitterCrawler.crawl_users_profile(user_groups)
    return data

def push_to_db(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key=None, task_ids='crawl_user_tweets')
    TwitterCrawler.save(data)

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2019, 7, 4, 15, 10, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('twitter_crawling_dag',
         catchup=False,
         default_args=default_args,
         schedule_interval='*/0 * * * *',
         #schedule_interval=None,
         ) as dag:
    op_publish_trends = PythonOperator(task_id='publish_trends',
                                       python_callable=publish_trends,
                                       provide_context=True)
    op_crawl_users = PythonOperator(task_id='crawl_users',
                                       python_callable=crawl_users,
                                       provide_context=True)
    op_publish_users = PythonOperator(task_id='publish_users',
                                            python_callable=publish_users,
                                            provide_context=True)
    op_crawl_user_tweets = PythonOperator(task_id='crawl_user_tweets',
                                            python_callable=crawl_user_tweets,
                                            provide_context=True)
    opr_push_to_db = PythonOperator(task_id='push_to_db',
                                            python_callable=push_to_db,
                                            provide_context=True)

op_publish_trends >> op_crawl_users >> op_publish_users >> op_crawl_user_tweets >> opr_push_to_db