import tweepy
import time
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():

    consumer_key = "1591229986727661568-1bXNw247rAW3qwKkvD3gNx3Ct07eSV"
    consumer_secret = "XENrVOiloDP5Clfc3K9UOoGyFRNST9O652fvEjYTuCUXc"
    access_key = "D9GnSayZJ7Bx3zJmuhFeXU4h7"
    access_secret = "lUlqzbLOES0Rvx4KD62dqbWgTeRS7PfqeI1OnKcoICwrtutYv0"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # # # Creating an API object
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk',
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}

        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('elonmusk_tweets.csv')


# consumer_key = "1591229986727661568-1bXNw247rAW3qwKkvD3gNx3Ct07eSV"
# consumer_secret = "XENrVOiloDP5Clfc3K9UOoGyFRNST9O652fvEjYTuCUXc"
# access_key = "D9GnSayZJ7Bx3zJmuhFeXU4h7"
# access_secret = "lUlqzbLOES0Rvx4KD62dqbWgTeRS7PfqeI1OnKcoICwrtutYv0"
#
# # Twitter authentication
# auth = tweepy.OAuthHandler(access_key, access_secret)
# auth.set_access_token(consumer_key, consumer_secret)
#
# # # # Creating an API object
# api = tweepy.API(auth)
#
# # print(f'{tweepy.Stream.}')
#
# # time.sleep(5)
#
# # mentions = api.mentions_timeline()
# tweets = api.user_timeline(screen_name='@bookpoets',
#                            # 200 is the maximum allowed count
#                            count=100,
#                            include_rts=False,
#                            # Necessary to keep full_text
#                            # otherwise only the first 140 words are extracted
#                            tweet_mode='extended'
#                            )
#
# print(tweets)