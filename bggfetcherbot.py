import praw
import re
import time
import json
import pandas as pd
from thefuzz import fuzz, process
import datetime
import numpy as np


# Define logger
def log_error(exception):
    with open('bggfetcherbot.log', 'a') as log:
        log.write(f'{datetime.datetime.now()}: {exception}\n\n')


# Define closest match logic function
def find_closest_match(query, dataset):
    closest_match = process.extractOne(query, dataset, scorer=fuzz.token_sort_ratio)
    if closest_match[1] < 80:
        closest_match = process.extractOne(query, dataset, scorer=fuzz.token_set_ratio)
    return closest_match


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
date_loaded = datetime.date.today()

# Set subreddit
test_subreddit = 'BGGFetcherBot'
subreddits = ['boardgames']
subreddit = reddit.subreddit("+".join(subreddits) + f'+{test_subreddit}')

# Compile Regex
game_names_regex = re.compile(r'\\?\[\\?\[(.*?)\\?\]\\?\]')
game_year_regex = re.compile(r'(.*)\\\|(\d{4})\\?([+-])?$')
single_game_regex = re.compile(r'^(.*)\\\|')

# Infinitely Loop comment stream
while True:
    try:
        for comment in subreddit.stream.comments(skip_existing=True):
            # Ensure data stays fresh
            if (datetime.date.today() - date_loaded).days >= 7:
                game_data = pd.read_pickle('game_data.pickle.gz')
                date_loaded = datetime.date.today()
            game_names = re.findall(game_names_regex, comment.body.replace('**', ''))
            if game_names and comment.author.name != "BGGFetcherBot":
                reply_text = ""
                for game_name in game_names:
                    # Strip extra whitespace & escape regex characters
                    game_query = re.escape(game_name.strip()).replace(r'\ ', ' ')
                    # TODO Look for year in call & extract year and modifier
                    if re.match(game_year_regex, game_query):
                        year_query = float(re.match(game_year_regex, game_query).groups()[1])
                        modifier = re.match(game_year_regex, game_query).groups()[2]
                        game_query = re.match(single_game_regex, game_query).groups()[0].strip()
                    # Attempt to pull games that exactly match
                    possible_matches = game_data[game_data['game_title'].str.contains(game_query, regex=True,
                                                                                      flags=re.I)]['game_title']
                    if possible_matches.empty:
                        # Attempt to pull all games that match any word in the call
                        query = '(' + game_query + ')'
                        query = '|'.join(query.split(' '))
                        possible_matches = game_data[game_data['game_title'].str.contains(query, flags=re.I, regex=True
                                                                                          )]['game_title']
                    if not possible_matches.empty:
                        # Will run fuzzy matching on a small set of matches
                        closest_match = find_closest_match(game_query, possible_matches)
                    else:
                        # Will fuzzy match against the entire database
                        closest_match = find_closest_match(game_query, game_data['game_title'])
                    game_link = game_data[game_data['game_title'] == closest_match[0]]['url'].values[0]
                    game_year = game_data[game_data['game_title'] == closest_match[0]]['game_year'].values[0]
                    if game_year == np.nan:
                        game_year = ''
                    else:
                        game_year = f" ({game_year})"
                    reply_text += f"[{game_name} -> {closest_match[0]}{game_year}]({game_link})\n\n"
                reply_text += '^^[[gamename]] ^^or ^^[[gamename|year]] ^^to ^^call'
                comment.reply(reply_text)
    except praw.exceptions.APIException as e:
        if "RATELIMIT" in str(e):
            delay_time = re.search(r"(\d+) minutes?", str(e))
            if delay_time:
                delay_seconds = int(delay_time.group(1)) * 60 + 10
            else:
                delay_seconds = 10
            log_error(e)
            time.sleep(delay_seconds)
        else:
            raise e
    except praw.exceptions.PRAWException as e:
        log_error(e)
        time.sleep(10)
    except Exception as e:
        log_error(e)
        time.sleep(10)
