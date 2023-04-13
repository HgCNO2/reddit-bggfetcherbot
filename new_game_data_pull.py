import pandas as pd
import numpy as np
from lxml.etree import XMLSyntaxError
from bs4 import BeautifulSoup
import requests
import gzip
import re
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
import math


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
            if re.search(r'_boardgame(expansion)?_', each_sitemap):
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


# Define Game Data Scrape Function
def retrieve_game_data(browser, url):
    try:
        browser.get(url)
        WebDriverWait(browser, 4).until(
            EC.presence_of_element_located((By.XPATH, "//h1/a"))
        )
    except TimeoutException:
        return np.nan, np.nan
    try:
        # extract the text from the specified xpaths
        game_title = browser.find_element(By.XPATH, '//h1/a').text.strip()
    except NoSuchElementException:
        game_title = np.nan
    try:
        game_year = browser.find_element(By.XPATH, '//h1/span').text.strip()
    except NoSuchElementException:
        game_year = np.nan
    return game_title, game_year


# Pull all board game URLS
live_urls = parse_sitemap('https://boardgamegeek.com/sitemapindex')[['loc']]
live_urls.rename(columns={'loc': 'url'}, inplace=True)

# Pull existing game URLs into a DataFrame
existing_table = pd.read_pickle('game_data.pickle.gz')\
    .dropna(subset="game_title")

# Pull out just newly discovered URLS and crawl for title and year
new_entries = pd.concat([live_urls, existing_table])\
    .drop_duplicates(subset='url', keep=False)

# crawl new_entries urls for title and year, push update to gzipped pickle file.
if not new_entries.empty:
    entries_split = np.array_split(new_entries, math.ceil(len(new_entries) / 250))
    for df in entries_split:
        options = Options()
        options.add_argument('-headless')
        options.add_argument("start-maximized")
        options.add_argument("disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-application-cache')
        options.add_argument('--disable-gpu')
        options.add_argument("--disable-dev-shm-usage")
        browser = webdriver.Firefox(options=options, service=FirefoxService(GeckoDriverManager(
            path='.\\driver').install()))
        df['game_details'] = df.apply(lambda row: retrieve_game_data(browser, row['url']), axis=1)
        df['game_title'] = df['game_details'].apply(lambda row: row[0])
        df['game_year'] = df['game_details'].apply(lambda row: row[1])
        try:
            df['game_year'] = df['game_year'].str.replace(r'\(|\)', '', regex=True)
        except AttributeError:
            pass
        df.drop(columns='game_details', inplace=True)
        existing_table = pd.concat([existing_table, df])
        existing_table.to_pickle('game_data.pickle.gz')
        browser.quit()
