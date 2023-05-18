import praw
import re
import time
import json
import pandas as pd
from rapidfuzz import fuzz, process, distance
import datetime
from typing import Union


# Define logger
def log_error(exception):
    with open('bggfetcherbot.log', 'a') as log:
        log.write(f'{datetime.datetime.now()}: {exception}\n\n')


# Define closest match logic function
def find_closest_match(query, dataset):
    jaro_match = process.extractOne(query, dataset, scorer=distance.JaroWinkler.similarity)
    ratio_match = process.extractOne(query, dataset, scorer=fuzz.ratio)
    return jaro_match if jaro_match[1] * 100 >= ratio_match[1] else ratio_match


# Define lookup with years and modifiers
def find_possible_matches(query: str, data_set: pd.DataFrame, year_query: Union[float, tuple] = None, modifier: str =
None):
    refined_data = data_set[data_set['game_title'].str.contains(query, regex=True, flags=re.I)]
    if year_query and modifier:
        if modifier == '+':
            refined_data = refined_data[refined_data['game_year'] >= year_query]
        elif modifier == '-':
            refined_data = refined_data[refined_data['game_year'] <= year_query]
    elif year_query and not modifier:
        if type(year_query) == tuple:
            refined_data = refined_data[refined_data['game_year'].between(float(min(year_query)), float(max(
                year_query)))]
        else:
            refined_data = refined_data[refined_data['game_year'] == year_query]
    return refined_data['game_title']


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

# Set subreddit for production
subreddits = ['boardgames', 'soloboardgaming']
subreddit = reddit.subreddit("+".join(subreddits))

# Set subreddit for testing
# subreddit = reddit.subreddit('BGGFetcherBot')

# Compile Regex
game_names_regex = re.compile(r'\\?\[\\?\[(.*?)\\?\]\\?\]')
game_year_regex = re.compile(r'(.*)\\\|(\d{4})\\?([+-])?$')
game_year_range_regex = re.compile(r'\\\|(\d{4})\\-(\d{4})$')
single_game_regex = re.compile(r'^(.*)\\\|')
fetch_regex = re.compile(r'!fetch')
game_names_bold = re.compile(r'\*\*(.*)\*\*')

# Infinitely Loop comment stream
while True:
    try:
        for comment in subreddit.stream.comments(skip_existing=True):
            # Ensure data stays fresh
            if (datetime.date.today() - date_loaded).days >= 7:
                game_data = pd.read_pickle('game_data.pickle.gz')
                date_loaded = datetime.date.today()
            game_names = []
            if re.search(fetch_regex, comment.body, flags=re.I):
                game_names.extend(re.findall(game_names_bold, comment.body))
            if re.search(game_names_regex, comment.body.replace('**', '')):
                game_names.extend(re.findall(game_names_regex, comment.body.replace('**', '')))
            if game_names and comment.author.name != "BGGFetcherBot":
                reply_text = ""
                game_names = list(dict.fromkeys(game_names))
                for game_name in game_names:
                    if len(game_name) >= 200:
                        continue
                    # Strip extra whitespace & escape regex characters
                    game_query = re.escape(game_name.strip()).replace(r'\ ', ' ')
                    year_query = None
                    modifier = None
                    # Look for year in call & extract year and modifier
                    if re.match(game_year_regex, game_query):
                        year_query = float(re.match(game_year_regex, game_query).groups()[1])
                        modifier = re.match(game_year_regex, game_query).groups()[2]
                    elif re.search(game_year_range_regex, game_query):
                        year_query = re.findall(game_year_range_regex, game_query)[0]
                    if "|" in game_query:
                        game_query = re.match(single_game_regex, game_query).groups()[0].strip()
                    # Attempt to pull games that exactly match
                    possible_matches = find_possible_matches(game_query, game_data, year_query, modifier)
                    if possible_matches.empty:
                        # Attempt to pull all games that match any word in the call
                        query = '(' + game_query + ')'
                        query = '|'.join(query.split(' '))
                        possible_matches = find_possible_matches(query, game_data, year_query, modifier)
                    if not possible_matches.empty:
                        # Will run fuzzy matching on a small set of matches
                        closest_match = find_closest_match(game_query, possible_matches)
                    else:
                        # Will fuzzy match against the entire database
                        closest_match = find_closest_match(game_query, game_data['game_title'])
                    game_link = game_data.loc[closest_match[-1]]['url']
                    game_year = game_data.loc[closest_match[-1]]['game_year']
                    try:
                        game_year = f" ({int(game_year)})"
                    except ValueError:
                        game_year = ""
                    reply_text += f"[{game_name} -> {closest_match[0]}{game_year}]({game_link})\n\n"
                if reply_text:
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
