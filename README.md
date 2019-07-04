# Twitter Worldwide Trends Crawling
Twitter trends crawler that runs as a scheduled service

## Requirements
Firstly, you will need **Python 3** of course.

Create and activate virtual environment
```
    $ python -m venv .env
    $ source .env/bin/activate
```

Install from requirements.txt
```
    $ pip install -r requirements.txt
```

## Setup
Stacks includes:
- Apache Airflow
- Apache Kafka
- MongoDB
- Mainly Dockers

You will need to pull and run docker image of MongoDB, I use port 5000 for my MongoDB:
```
    $ docker pull mongo
    $ docker run -p 5000:5000 --name mongodb -d mongo
```

Then run docker compose for kafka, I use default port 9092 and 2181, feel free to change it:
```
    $ docker-compose -f src/kafka/docker-compose.yml up -d
```

Make sure mongo, kafka and zookeeper are all up.

The crawler uses **tweepy api** for retrieving trends, so get your key and secret. Export them into your environment, I do write them into my .env.

Also export AIRFLOW_HOME as the repo base directory. Mine is '~/twitter-trends-crawling'

Then, run initialize airflow db, server and scheduler.

## Usage
An Airflow DAG named *"twitter_crawling_dag"* will be created.

Currently, the dag:
1. Runs hourly
2. Crawl top 5 worldwide trends and retrieve top 5 tweets
3. Go into profile of each tweet, and crawl two pages (~40 latest tweets)

Feel free to change it for your own use, Cheers!