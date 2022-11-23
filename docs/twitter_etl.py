#!/usr/bin/env python
# coding: utf-8

# # End-To-End Data Engineering Project

# ### Step 1 Extract and Transform Data
# - Set up connection between my code and Twitter API
# - Extract data from Twitter using Twitter API
# - Transform data into the data dataframe using Python
# - Store it onto the local csv file

# In[2]:


# Installed tweepy, pandas, s3fs
import tweepy
import pandas as pd
import json
from datetime import datetime
# Python filesystem interface for S3
import s3fs 

def run_twitter_etl():

    access_key = "30esHxarcuNhKRrjwOE0wSDpa" # API Key
    access_secret = "bs2AFbEYPSrAp9vYMO0JyTQULmJ3bEjkVA193DO0zOTTM2YO2Y"
    consumer_key = "1323651046338555905-dfvYNXD4CPulPoPYwuF57namKPtGJ4" # Access Token
    consumer_secret = "t8gDv5HROtk8q9LcE38I5RMXPG5ZrvnDvk2hObkFH5wqA"
    # Function in tweepy package to do Twitter authentication 
    # to create connection between my code and twitter api
    auth = tweepy.OAuthHandler(access_key,access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # Create an API object to access function inside tweepy API
    api = tweepy.API(auth)

    # Passing parameters
    tweets = api.user_timeline(screen_name ='@elonmusk', # Username
                              # 200 is the maximum allowed count
                              count=200, # How many tweets to extract
                              include_rts = False, # Rts is retweets
                              # Necessary to keep full_text
                              # Otherwise only the first 140 words are extracted
                              tweet_mode = 'extended'
                              )

    # Test connection also in terminal (mac) ->  python3 twitter_etl.py
    # Returned in JSON format
    # print(tweets) 

    # Data Transformation
    # For each tweet, extract data from it.
    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                         'text': text, # Actual tweet
                        'favorite_count': tweet.favorite_count,
                        'retweet_count': tweet.retweet_count,
                        'created_at': tweet.created_at}
        # The entire dictionary appended to the list
        tweet_list.append(refined_tweet) 


    df = pd.DataFrame(tweet_list)
    #df.to_csv("elonmusk_twitter_data.csv") # CSV file created in the local folder
    df.to_csv("s3://mel-airflow-test-project-bucket/elonmusk_twitter_data.csv")


# ## Step 2 Deploy the Code on Airflow/EC2
# - Create an Amazon EC2 instance
# - Deploy it over Airflow on the EC2 instance

# - Create an Amazon EC2 instance.
# - Connect to instnace via AWS interface/via SSH client
# - Run the follwing commands 
#     - sudo apt-get update : updates all files available to ubuntu machine
#     - sudo apt install python3-pip
#     - sudo apt-get install apache2
#     - sudo pip install apache-airflow
#     - sudo pip install pandas
#     - sudo pip install s3fs
#     - sudo pip install tweepy
#     - systemctl start apache2
#     - systemctl status apache2 : to check the status
#     - pip install apache-airflow[cncf.kubernetes]
#     - pip install virtualenv
# - Run the following command to launch the airflow server
#     - airflow standalone : save username and password
#     - airflow scheduler -D
#     - airflow webserver --port 8080 
#     - lsof -i tcp:8080 : to find out the Pid then kill if needed
# - Sign in Airflow console
# - Put the entire code into the function run_twitter_etl
# - Create a new file (twitter_dag) to run the function
# - Create a S3 bucket to store the data frame
# - Update the csv file destination from local to s3.
# - On terminal
#     - cd airflow/
#     - sudo nano airflow.cfg
#       change dags_folder path from dags to twitter_dag, 
#       change scheduler_health_check_threshold from 30 to 240, ctrl + x, then enter to save it
#     - mkdir twitter_dag : to create a folder
#     - cd twitter_dag
#     - sudo nano twitter_dag.py : copy and paste the entire code from twitter_dag file
#     - sudo nano twitter_etl.py : copy and paste the entire code from twitter_etl file (code 
#       above)
# - On AWS, create a new IAM role for the EC2 instance giving full access for EC2 and S3

# ## Step 3 Trigger DAG
# - On terminal, this can be tested by 
#     - airflow tasks test twitter_dag complete_twitter_etl
# - On Airflow, a new dag (twitter_dag) can be found. Go to Graph, click on Run (play button), select Trigger DAG.
# - If Dag is not working, run `ps aux | grep airflow` and check if airflow webserver or airflow scheduler processes are   running. If they are kill them and rerun using airflow scheduler -D.
