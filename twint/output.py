from datetime import datetime

from . import format
from .tweet import Tweet
from .user import User
from .storage import db, elasticsearch, write, panda
from . import output

follows_list = []
tweets_list = []
users_list = []

author_list = {''}
author_list.pop()

# used by Pandas
_follows_object = {}

def _clean_follow_list():

    global _follows_object
    _follows_object = {}

def clean_lists():
    global follows_list
    global tweets_list
    global users_list
    follows_list = []
    tweets_list = []
    users_list = []

def datecheck(datetimestamp, config):
    if config.Since and config.Until:
        d = int(datetime.strptime(datetimestamp, "%Y-%m-%d %H:%M:%S").timestamp())
        s = int(datetime.strptime(config.Since, "%Y-%m-%d %H:%M:%S").timestamp())
        if d < s:
           return False
    return True

def is_tweet(tw):
    try:
        tw["data-item-id"]
        return True
    except:
        return False

def _output(obj, result, config, **extra):

    if config.Lowercase:
        if isinstance(obj, str):
            obj = obj.lower()
        elif obj.__class__.__name__ == "user":
            pass
        elif obj.__class__.__name__ == "tweet":
            obj.username = obj.username.lower()
            author_list.update({obj.username})
            for i in range(len(obj.mentions)):
                obj.mentions[i] = obj.mentions[i].lower()
            for i in range(len(obj.hashtags)):
                obj.hashtags[i] = obj.hashtags[i].lower()
            for i in range(len(obj.cashtags)):
                obj.cashtags[i] = obj.cashtags[i].lower()
        else:

            return
    if config.Output != None:
        if config.Store_csv:
            try:
                write.Csv(obj, config)

            except Exception as e:

                print(str(e) + " [x] output._output")
        elif config.Store_json:
            write.Json(obj, config)

        else:
            write.Text(result, config.Output)


    if config.Elasticsearch:

        print("", end=".", flush=True)
    else:
        if not config.Hide_output:
            try:
                output.follows_list.append(result.split('\n'))
            except UnicodeEncodeError:
                pass

async def checkData(tweet, config, conn):
    copyright = tweet.find("div", "StreamItemContent--withheld")
    if copyright is None and is_tweet(tweet):
        tweet = Tweet(tweet, config)

        if not tweet.datestamp:

            return

        if datecheck(tweet.datestamp + " " + tweet.timestamp, config):
            output = format.Tweet(config, tweet)

            if config.Database:

                db.tweets(conn, tweet, config)

            if config.Pandas:
                panda.update(tweet, config)

            if config.Store_object:
                if hasattr(config.Store_object_tweets_list, 'append'):
                    config.Store_object_tweets_list.append(tweet)
                else:
                    tweets_list.append(tweet)

            if config.Elasticsearch:
                elasticsearch.Tweet(tweet, config)

            _output(tweet, output, config)
    else:
        pass

async def Tweets(tweets, config, conn, url=''):
    if config.Favorites or config.Profile_full or config.Location:

        for tw in tweets:
            if tw['data-item-id'] == url.split('?')[0].split('/')[-1]:
                await checkData(tw, config, conn)
    elif config.TwitterSearch:

        await checkData(tweets, config, conn)
    else:

        if int(tweets["data-user-id"]) == config.User_id or config.Retweets:
            await checkData(tweets, config, conn)

async def Users(u, config, conn):
    global users_list

    user = User(u)
    output = format.User(config.Format, user)

    if config.Database:

        db.user(conn, config, user)

    if config.Elasticsearch:

        _save_date = user.join_date
        _save_time = user.join_time
        user.join_date = str(datetime.strptime(user.join_date, "%d %b %Y")).split()[0]
        user.join_time = str(datetime.strptime(user.join_time, "%I:%M %p")).split()[1]
        elasticsearch.UserProfile(user, config)
        user.join_date = _save_date
        user.join_time = _save_time

    if config.Store_object:

        users_list.append(user) # twint.user.user
    
    if config.Pandas:
        panda.update(obj, config)

    _output(user, output, config)

async def Username(username, config, conn):
    global _follows_object
    global follows_list
    follow_var = config.Following*"following" + config.Followers*"followers"

    if config.Database:
        db.follow(conn, config.Username, config.Followers, username)
    if config.Elasticsearch:
        elasticsearch.Follow(username, config)

    if config.Store_object:
        follows_list.append(username)

    if config.Pandas:
        try:
            _ = _follows_object[config.Username][follow_var]
        except KeyError:
            _follows_object.update({config.Username: {follow_var: []}})
        _follows_object[config.Username][follow_var].append(username)
        if config.Pandas_au:
            panda.update(_follows_object[config.Username], config)
    _output(username, username, config)
