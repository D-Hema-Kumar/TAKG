#!/usr/bin/env python
# coding: utf-8

SPARQL_ENDPOINTS = {'wikidata':'https://query.wikidata.org/sparql'}
DATABASE = "./kgdb/TAKG.db"
TABLES = {"base_KG_table":"KG",
          "temporal_meta_table":"Temporal",
         "meta_table":"Metadata",
         }
SOURCES = {'wiki_source':'Wiki',
          'transfer_market':'TFM'}

RAW_DATA_FILE_PATH = './data/playerRawData.csv'

STANDARD_TRIPLES = ['subject','predicate','object']
KG_TABLE_COLUMNS = ['subject','predicate','object','statement_id']
TEMPORAL_TABLE_COLUMNS = ['statement_id', 'start', 'end', 'time_point', 'retrieval_id' ]
META_TABLE_COLUMNS = ['retrieval_id','source','insertion_time']


PLAYER_COLUMNS = ['player_uri','player','fullName', 'transferMarketID', 'DOB', 'sex']
TEAM_COLUMNS = ['team_uri','team', 'transferMarketTeamID','teamCountry', 'teamLabel']
TEMPORAL_DATA_COLUMNS = ['player_uri','team_uri','start','end']

COLUMN_CLASS_MAPPING = {
    'player_uri':'TAKG.Player',
    'player':'TAKG.WikiID',
    'transferMarketID':'TAKG.TFMID',
    'transferMarketTeamID':'TAKG.TFMID',
    'sex':'TAKG.Sex',
    'team':'TAKG.WikiID',
    'team_uri':'TAKG.FootballTeam',
    'teamCountry':'TAKG.Country',
    'event_uri':'TAKG.Event',
    'award_uri':'TAKG.Award',
    'event':'TAKG.WikiID',
    'award':'TAKG.WikiID',
    
    }

PREDICATES = {'player_played_for':'playedFor','player_participation':'participatedIn','player_award':'wasAwarded'}

PLAYER_COLUMN_PREDICATE_MAPPING = {'fullName':'hasFullName','player':'hasWikiID',
        'transferMarketID':"hasTransferMarketID",'DOB':'hasBirthDay','sex' : 'hasSex' }

TEAM_COLUMN_PREDICATE_MAPPING = {'team':'hasWikiID','teamCountry':'isInCountry','transferMarketTeamID':'hasTransferMarketID',                                  'teamLabel':'RDFS.label'}

PLAYED_FOR_SATEMENT_ID_COLUMNS = ['player_uri','team_uri', 'start'] #columns required for calculating statement ID
MARKET_VALUE_STATEMENT_ID_COLUMNS = ['player_uri',]
RETRIEVAL_ID_COLUMNS = ['insertion_time','source']


TIME_COLUMN_MAPPING = {'startTime':'start','endTime':'end'}

TRANSFER_MARKET_PLAYER_DATA_MAPPING = {'player_transfer_market_ID':"transferMarketID",
                                'transferMarketTeamID':'transferMarketTeamID',
                                'clubJoined_name':'teamLabel',
                                'startTime':'start','endTime':'end'
                                 }

PLAYER_MARKET_VALUE_PREDICATE_MAPPING = {'max_market_value':'hasMaximunMarketValue', 
                                         #'max_value_date':'hasMaximumMarketValueOnDate',
                                         'current_market_value':'hasCurrentMarketValue',
                                         #'transferMarketID':"hasTransferMarketID"
                                        }

EVENT_COLUMNS = ['event_uri','event','eventLabel']
EVENT_COLUMN_PREDICATE_MAPPING = {'event': 'hasWikiID','eventLabel': 'RDFS.label'}
EVENT_TEMPORAL_COLUMNS = ['player_uri', 'event_uri', 'start', 'end']
EVENT_PARTICIPATED_SATEMENT_ID_COLUMNS = ['player_uri', 'event_uri', 'start']

AWARD_COLUMNS = ['award_uri','award','awardLabel']
AWARD_COLUMN_PREDICATE_MAPPING = {'award': 'hasWikiID','awardLabel': 'RDFS.label'}
AWARD_TEMPORAL_COLUMNS = ['player_uri', 'award_uri', 'time_point']
