import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import re
import time
from dateutil.parser import parse

# only uses liquipedia wikis as these are guaranteed to be esports-related
liquipedia_wikis = [
    'counterstrike',
    'leagueoflegends',
    'dota2',
    'valorant',
    'rocketleague',
    'mobilelegends',
    'apexlegends',
    'rainbowsix',
    'starcraft2',
    'overwatch',
    'pubgmobile',
    'ageofempires',
    'pubg',
    'smash',
    'warcraft',
    'brawlstars',
    'wildrift',
    'starcraft',
    'fifa',
    'heroes',
    'artifact',
    'hearthstone',
    'fighters',
    'arenaofvalor',
    'callofduty',
    'fortnite',
    'freefire',
    'pokemon',
    'tft',
    'clashroyale',
    'halo',
    'worldofwarcraft',
    'arenafps',
    'tetris',
    'teamfortress',
    'paladins',
    'sideswipe',
    'crossfire',
    'brawlhalla',
    'zula',
    'simracing',
    'splatoon',
    'omegastrikers',
    'naraka',
    'clashofclans',
    'splitgate',
    'criticalops',
    'battalion',
    'runeterra',
    'autochess',
    'magic',
    'squadrons',
    'underlords',
    'battlerite'
]

# required header for Liquipedia API
header = {
    'User-Agent': 'ProsInMultipleEsports/1.0 (XXXXXXXXX)', 
    'Accept-Encoding': 'gzip'
    }

def query_player_list(url, params):
    """Query the API for 'url' using the parameters in 'params', continuing where necessary."""
    lastContinue = {}
    while True:
        # clone original request
        par = params.copy()
        # modify it with the values returned in the 'continue' section of the last result.
        par.update(lastContinue)
        # call API
        result = requests.get(url, params=par, headers=header).json()
        time.sleep(4)
        if 'error' in result:
            raise ValueError(result['error'])
        if 'warnings' in result:
            print(result['warnings'])
        if 'query' in result:
            yield result['query']
        if 'continue' not in result:
            break
        lastContinue = result['continue']

def query_player_page(url, params):
    """Query the API for 'url' using the parameters in 'params'."""
    result = requests.get(url, params=params, headers=header).json()
    time.sleep(4)
    if 'error' in result:
        raise ValueError(result['error'])
    if 'warnings' in result:
        print(result['warnings'])
    if 'query' in result:
        return result

def set_player_page_params(pageids):
    """Set the parameters for the API query of a player's page."""
    params = {
                'action': 'query',
                'pageids': pageids,
                'format': 'json',
                'prop': 'revisions',
                'rvprop': 'content',
                'rvslots': 'main',
                'rvsection': 0,
    }

    return params

def set_player_list_params(subject):
    """Set the parameters for the API query of a wiki's player list."""
    params = {
                'action': 'query',
                'titles': subject,
                'format': 'json',
                'generator': 'categorymembers',
                'gcmtitle': subject,
                'gcmprop': 'ids|title',
                'gcmlimit': 50,
    }

    return params

def remove_brackets(string):
    """Remove brackets and their contents from a string."""
    return re.sub("[\(\[].*?[\)\]]", "", string).strip()

def scrape_game_players(wiki):
    """Scrape the players and their names, ids, and dobs from a wiki's player list."""
    player_array = []
    subject = 'Category:Players'
    url = 'https://liquipedia.net/' + wiki + '/api.php'
    params = set_player_list_params(subject)
    for result in query_player_list(url, params):
        # query all player pages at once using pageids
        pageids = ''
        for page in result['pages']:
            if pageids == '':
                pageids += page
                continue
            pageids += '|' + page
        player_params = set_player_page_params(pageids)
        player_pages = query_player_page(url, player_params)
        # search page_content for id, name, and dob
        for player in iter(player_pages['query']['pages'].values()):
            content = player['revisions'][0]['slots']['main']['*']
            id_loc, name_loc, dob_loc = content.find('|id='), content.find('|name='), content.find('|birth_date=')
            
            if id_loc != -1:
                id = content[id_loc + 4:id_loc + content[id_loc:].find('\n')]
                if id == '':
                    id = None
            else:
                id = None

            if name_loc != -1:
                name = content[name_loc + 6:name_loc + content[name_loc:].find('\n')]
                if name == '':
                    name = None
            else:
                name = None

            if dob_loc != -1:
                dob = remove_brackets(content[dob_loc + 12:dob_loc + content[dob_loc:].find('\n')])
                if dob == '':
                    dob = None
                else:
                    try:
                        dob = datetime.strptime(dob, '%Y-%m-%d')
                    except ValueError:
                        try:
                            dob = parse(dob)
                        except:
                            dob = None
            else:
                dob = None

            if id != None or name != None:
                player_array.append([id, name, dob])

    # convert to pandas dataframe and save to file
    player_array = pd.DataFrame(player_array, columns=['id', 'name', 'dob'])
    player_array.to_csv('data/%s_players.csv' % wiki, index=False)

# scrape every wiki on liquipedia
for wiki in liquipedia_wikis:
    scrape_game_players(wiki)

# find all players who have pages on multiple liquipedias
trackmania_players = pd.read_csv('data/trackmania_players.csv', keep_default_na=False)
column_names=['track_id', 'game_id', 'track_name', 'game_name', 'track_dob', 'game_dob', 'matching_game']
matching_players = pd.DataFrame(columns=column_names)
for wiki in liquipedia_wikis:
    print(wiki)
    esport_players = pd.read_csv('data/%s_players.csv' % wiki, keep_default_na=False)
    for track_player in trackmania_players.itertuples():
        for esport_player in esport_players.itertuples():
            # name and id may not match depending on how the player is listed
            matches = []
            if track_player.name != '' and esport_player.name != '':
                matches.append(track_player.name.lower() == esport_player.name.lower())
            if track_player.id != '' and esport_player.id != '':
                matches.append(track_player.id.lower() == esport_player.id.lower())
            if track_player.dob != '' and esport_player.dob != '':
                matches.append(track_player.dob == esport_player.dob)
            # can also try matching only one to be more lenient
            if sum(matches) >= 2:
                df = pd.DataFrame([track_player.id, esport_player.id, track_player.name, 
                                   esport_player.name, track_player.dob, esport_player.dob, wiki]).T
                df.columns = column_names
                matching_players = pd.concat([matching_players, df], ignore_index=True)