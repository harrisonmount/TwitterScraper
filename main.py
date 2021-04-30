import pandas as pd
import numpy as np
import csv
import tweepy
import time
import os
import json
import logging
import time

import requests
from tqdm import tqdm
from newspaper import Article

"""from util.util import DataCollector
from util import Config, create_dir
from util import Constants"""


auth = tweepy.OAuthHandler("""""")
auth.set_access_token("""""")
api = tweepy.API(auth, wait_on_rate_limit=True)
'''
             180 calls per 15 minutes
            GET statuses/retweets/:id  75
            take the _json for the source tweet and put it in the source_tweets dir as tweetid.json
            for retweet in retweets write line in retweets.json in sourceid folder
            figure out who-follows-whom.dat
            annotation = {'misinformation": 0, 'true': int(label) }
'''

def buildThreads():
    DF = pd.read_csv('COVID19.csv')
    for index, item in DF.iterrows():
        tweet_id = item['tweet_id']
        label = item['label']
        try:
            source_tweet = api.get_status(tweet_id, tweet_mode='extended', wait_on_rate_limit=True)
            #reactions = api.retweets(tweet_id)
            tmp_folder = 'COVIDTHREADS/{}'.format(tweet_id)
            if not os.access(tmp_folder, os.F_OK):
                os.mkdir(tmp_folder)
            #print(source_tweet)
            source_folder = 'COVIDTHREADS/{}/source-tweets'.format(tweet_id)
            if not os.access(source_folder, os.F_OK):
                os.mkdir(source_folder)
            with open('COVIDTHREADS/{}/source-tweets/{}.json'.format(tweet_id, tweet_id), 'w') as outfile:
                json.dump(source_tweet._json, outfile)
            with open('COVIDTHREADS/{}/annotation.json'.format(tweet_id), 'w') as afile:
                json.dump({'misinformation': 0, 'true': int(label)}, afile)
            #with open('COVIDTHREADS/{}/retweets.json'.format(tweet_id), 'w') as rtfile:

            #print(len(reactions))
        except Exception as e:
            print(e)
            pass
def getRetweets():
    DF = pd.read_csv('COVID19.csv')
    #print(api.rate_limit_status)
    for index, item in DF.iterrows():
        tweet_id = item['tweet_id']
        label = item['label']
        try:
            print(tweet_id)
            retweets = api.retweeters(tweet_id)
            #print(retweets)
            #print(retweets)
            tmp_folder = 'COVIDTHREADS/{}'.format(tweet_id)
            if not os.access(tmp_folder, os.F_OK):
                os.mkdir(tmp_folder)
            rts = pd.Series(retweets)
            rts.to_csv('COVIDTHREADS/{}/retweets.csv'.format(tweet_id), index=None, header=None)
            # print(source_tweet)
            #retweets = retweets[1:]
            #rtDF = pd.DataFrame(retweets)
            #rtDF.to_json('COVIDTHREADS/{}/retweets.json'.format(tweet_id), orient='values')
            '''
            with open('COVIDTHREADS/{}/retweets.csv'.format(tweet_id), 'w') as rfile:
                writer = csv.writer(rfile)
                writer.writerows(retweets)
            '''
            # with open('COVIDTHREADS/{}/retweets.json'.format(tweet_id), 'w') as rtfile:
            # print(len(reactions))

        except Exception as e:
            print(e)
            pass

"""def whofollows(tweet_id):
    #for each line in retweets.csv
    #check all lines besides itself, if true input user a and b into new csv
    #id tab id newline
    DF = pd.read_csv('COVIDTHREADS/{}/retweets.csv'.format(tweet_id))
    #print(api.rate_limit_status)
    for index, item in DF.iterrows():
        userA = item[]
        #user = pd
        #panda series(
        #show_friendship
        #if following true, if followed by true, print both"""

def pull(filename, label, foldername):
    counter = 0
    threadcounter = 0
    rtcount=0
    DF = pd.read_csv(filename)

    for index, item in DF.iterrows():
        counter += 1
        if threadcounter > 50:
            break
        try:
            tweetsline = item['tweet_ids']
            gosid = item['id']
            url = item['news_url']
            subcounter = 0

            for tweet_id in tweetsline.split():
                subcounter += 1
                if subcounter > 10:
                    break
                retweets = api.retweeters(tweet_id)
                if len(retweets) > 0:
                    try:
                        try:
                            infojson = crawl_link_article(url)
                        except Exception as e:
                            print(e)
                            subcounter -= 1
                            break
                        rtcount += len(retweets)
                        threadcounter += 1
                        source_tweet = api.get_status(tweet_id, tweet_mode='extended', wait_on_rate_limit=True)
                        tweetjson = source_tweet._json
                        tweetjson['full_text'] = infojson['text']

                        retweets = api.retweeters(tweet_id)

                        tmp_folder = '{}/{}'.format(foldername, tweet_id)
                        if not os.access(tmp_folder, os.F_OK):
                            os.mkdir(tmp_folder)
                        source_folder = '{}/{}/source-tweets'.format(foldername, tweet_id)
                        if not os.access(source_folder, os.F_OK):
                            os.mkdir(source_folder)
                        with open('{}/{}/source-tweets/{}.json'.format(foldername, tweet_id, tweet_id), 'w') as outfile:
                            json.dump(source_tweet._json, outfile)
                        with open('{}/{}/annotation.json'.format(foldername, tweet_id), 'w') as afile:
                            if label:
                                json.dump({'misinformation': 0, 'true': 1}, afile)
                            else:
                                json.dump({'misinformation': 1, 'true': 0}, afile)
                        rts = pd.Series(retweets)
                        rts.to_csv('{}/{}/retweets.csv'.format(foldername, tweet_id), index=None, header=None)

                        with open('{}/{}/who-follows-whom.dat'.format(foldername, tweet_id), 'w') as whof:
                            pass

                    except Exception as e:
                        print(e)
                        pass
                else:
                    print(tweet_id + ' has no retweets')
        except Exception as e:
            print(e)
            pass


def crawl_link_article(url):
    result_json = None
    try:
        if 'http' not in url:
            if url[0] == '/':
                url = url[1:]
            try:
                article = Article('http://' + url)
                article.download()
                time.sleep(2)
                article.parse()
                flag = True
            except:
                logging.exception("Exception in getting data from url {}".format(url))
                flag = False
                pass
            if flag == False:
                try:
                    article = Article('https://' + url)
                    article.download()
                    time.sleep(2)
                    article.parse()
                    flag = True
                except:
                    logging.exception("Exception in getting data from url {}".format(url))
                    flag = False
                    pass
            if flag == False:
                return None
        else:
            try:
                article = Article(url)
                article.download()
                time.sleep(2)
                article.parse()
            except:
                logging.exception("Exception in getting data from url {}".format(url))
                return None

        if not article.is_parsed:
            return None

        visible_text = article.text
        title = article.title
        result_json = {'text' : visible_text,
                       'title' : title
                       }
    except:
        logging.exception("Exception in fetching article form URL : {}".format(url))

    return result_json

if __name__ == '__main__':

    #checkforretweets('politifact_real.csv', True, 'POLITIFACTTHREADS')
    #checkforretweets('politifact_fake.csv', False, 'POLITIFACTTHREADS')
    pull('gossipcop_real.csv', True, 'GOSSIPCOPTHREADS')
    #checkforretweets('gossipcop_fake.csv', False, 'GOSSIPCOPTHREADS')