import gzip
import re
from collections import defaultdict
from time import sleep

import pandas as pd
import numpy as np
from lxml.etree import XMLSyntaxError
from bs4 import BeautifulSoup
import requests


# Define Reliable Sitemap Parser
def parse_sitemap(sitemap: str, **kwargs) -> "None | pd.DataFrame":
    urls = pd.DataFrame()
    sm_urls = pd.DataFrame()
    resp = requests.get(sitemap, **kwargs)
    if not resp.ok:
        return None
    if resp.headers['Content-Type'] == 'application/x-gzip' or re.search(r'\.gz$', sitemap):
        content = gzip.decompress(resp.content)
    else:
        content = resp.content
    soup = BeautifulSoup(content, 'xml')
    if soup.select('sitemapindex'):
        sitemaps = pd.read_xml(content)
        for each_sitemap in sitemaps['loc'].tolist():
            if re.search(r'_boardgame_', each_sitemap):
                resp = requests.get(each_sitemap, **kwargs)
                if resp.ok:
                    if resp.headers['Content-Type'] == 'application/x-gzip' or re.search(r'\.gz$', each_sitemap):
                        content = gzip.decompress(resp.content)
                    else:
                        content = resp.content
                    try:
                        sm_urls = pd.read_xml(content)
                        sm_urls['sitemap'] = each_sitemap
                    except ValueError as e:
                        print(f'{each_sitemap} unparsable.\n{e}')
                    except XMLSyntaxError as e:
                        print(f'{each_sitemap} unparsable.\n{e}')
                else:
                    print(f'Unable to fetch {each_sitemap}. Request returned HTTP Response {resp.status_code}.')
                urls = pd.concat([urls, sm_urls])
    else:
        urls = pd.read_xml(content)
        urls['sitemap'] = sitemap
    return urls


# Pull all board game URLS
live_urls = parse_sitemap('https://boardgamegeek.com/sitemapindex')[['loc']]
live_urls.rename(columns={'loc': 'url'}, inplace=True)
live_urls['game_id'] = live_urls['url'].str.extract(r'/boardgame/(\d+)/')

# Pull existing game URLs into a DataFrame
existing_table = pd.read_pickle('game_data.pickle.gz').dropna(subset="game_title")

# Pull out just newly discovered URLS and crawl for title and year
new_entries = pd.concat([live_urls, existing_table]).drop_duplicates(subset='game_id', keep=False)

# Set up defaultdict to build dataframe
game_data = defaultdict(list)

# crawl new_entries urls for title and year, push update to gzipped pickle file.
if not new_entries.empty:
    # Initiate retry counter
    retry_counter = 0

    for i, row in new_entries.iterrows():
        if retry_counter >= 35:
            print("Too many retries.")
            break
        print(f"Row {i} of {len(new_entries)}")
        resp = requests.get(f"https://api.geekdo.com/api/market/products?ajax=1&nosession=1&"
                            f"objectid={row['game_id']}"
                            f"&objecttype=thing&pageid=1&showcount=1&stock=instock")

        # To mitigate stoppage from rate limits or server errors, sleep then move on.
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            if resp.status_code == 429:
                print(resp.status_code)
                sleep(60 * 20)  # Sleep for 20 minutes for every 429
                continue
            else:
                print(resp.status_code)
                retry_counter += 1
                sleep(60)
                continue

        try:
            json_resp = resp.json()
        except requests.exceptions.JSONDecodeError:
            print(f"JSON decoder failed for game_id {row['game_id']}.")
            sleep(15)
            continue

        # Get game_year
        game_year = np.nan
        for item in json_resp.get('linkeditem').get('descriptors'):
            if item.get('name') == "yearpublished":
                game_year = float(item.get('displayValue'))

        # Get game_title
        game_title = json_resp.get('linkeditem').get('name')

        # Append data
        game_data['url'].append(row['url'])
        game_data['game_id'].append(row['game_id'])
        game_data['game_title'].append(game_title)
        game_data['game_year'].append(game_year)

    # Create game_data DataFrame
    game_data_df = pd.DataFrame(game_data)

    # Build existing table and push to database
    existing_table = pd.concat([existing_table, game_data_df])
    existing_table.sort_values('game_year', ascending=False, inplace=True)
    existing_table.reset_index(drop=True, inplace=True)
    existing_table.to_pickle('game_data.pickle.gz')
