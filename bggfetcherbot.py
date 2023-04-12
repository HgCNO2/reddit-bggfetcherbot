import praw
import re
import time
import json
import pandas as pd
from thefuzz import fuzz, process

# Load authentication data
with open('reddit_secrets.json') as f:
    secrets = json.load(f)

# Initialize Reddit Class
reddit = praw.Reddit(
    client_id=secrets['client_id'],
    client_secret=secrets['client_secret'],
    username=secrets['username'],
    password=secrets['password'],
    user_agent=secrets['user_agent'],
)

# Load game database
game_data = pd.read_pickle('game_data.pickle.gz')

# Set subreddit
subreddit = reddit.subreddit("boardgames")

# Infinitely Loop comment stream
while True:
    try:
        for comment in subreddit.stream.comments(skip_existing=True):
            game_names = re.findall(r'\[\[(.*?)\]\]', comment.body)
            if game_names and comment.author.name != "BGGFetcherBot":
                reply_text = ""
                for game_name in game_names:
                    closest_match = process.extractOne(game_name, game_data['game_title'],
                                                       scorer=fuzz.token_sort_ratio)
                    if closest_match[1] < 80:
                        closest_match = process.extractOne(game_name, game_data['game_title'],
                                                           scorer=fuzz.token_set_ratio)
                    game_link = game_data[game_data['game_title'] == closest_match[0]]['url'].values[0]
                    reply_text += f"[{game_name}]({game_link})\n\n"
                reply_text += '^^[[gamename]] ^^to ^^call'
                comment.reply(reply_text)
    except praw.exceptions.APIException as e:
        if "RATELIMIT" in str(e):
            delay_time = re.search(r"(\d+) minutes?", str(e))
            if delay_time:
                delay_seconds = int(delay_time.group(1)) * 60 + 10
            else:
                delay_seconds = 10
            print(f"Rate limited. Sleeping for {delay_seconds} seconds...")
            time.sleep(delay_seconds)
        else:
            raise e
    except praw.exceptions.PRAWException as e:
        print(f"PRAW error: {e}")
        time.sleep(10)
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(10)
