#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import numpy as np
import ctypes
import datetime
import sqlite3 as sql
import requests
import re
import bs4
from bs4 import BeautifulSoup
from SPARQLWrapper import SPARQLWrapper, JSON
from constants import *


# In[5]:


def merge_two_dictionaries(dict1,dict2):
    result = {**dict1,**dict2}
    return result


# In[6]:


def drop_table(table_name:str):
    q = f'DROP TABLE {table_name};'
    conn = create_connection(DATABASE)
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    conn.close()
    return print(f'{table_name} table dropped') 

def delete_table(table_name:str):
    q = f'DELETE FROM {table_name};'
    conn = create_connection(DATABASE)
    cur = conn.cursor()
    cur.execute(q)
    q = f'DELETE FROM SQLITE_SEQUENCE WHERE name="{table_name}";'
    conn.commit()
    conn.close()
    return print(f'{table_name} table truncated')


# In[7]:


def create_table(create_query):
    
    """Create a table in the database
    param conn: connection object to a specifi database
          create_query: The create table query
    Output: sucessfully create the table     
    """
    conn=create_connection(DATABASE)
    cur = conn.cursor()
    cur.execute(create_query)


# In[8]:


def create_connection(dbName:str):
    
    """Creates a connection to the database specified by dbName and return a connection object
    param dbName: The name of the databse name
    :return: connection object or none
    
    """
    
    conn = None
    
    try:
        conn = sql.connect(dbName)
    except:
        print("Connection failed: Check if database exists or active")
    return conn


# In[9]:


def select_data(table_name:str):
    q = f'SELECT * FROM {table_name} limit 2'
    conn = create_connection(DATABASE)
    df = pd.read_sql_query(q,conn)
    conn.close()
    return df

def query_TAKG(query:str):
    """Function to select data from the database and return a dataframe"""
    conn = create_connection(DATABASE)
    df = pd.read_sql_query(query,conn)
    conn.close()
    return df


# In[10]:


def get_row_count(table_name:str):
    q = f'SELECT COUNT(*) as row_count FROM {table_name}'
    conn = create_connection(DATABASE)
    df = pd.read_sql_query(q,conn)
    conn.close()
    return df


# In[11]:


def create_uri(colData:pd.Series):
    return 'TAKG.'+ colData


# In[12]:


def replace_invalid_time(rawData:pd.DataFrame):
    """The function checks for the invalid start Or end times and replaces them with np.nan"""
    raw_data = rawData.copy()
    mask_invalid_endtime = raw_data.end.str.len()>20
    raw_data.loc[mask_invalid_endtime,'end']=np.nan
    mask_invalid_start_time = raw_data.start.str.len()>20
    raw_data.loc[mask_invalid_start_time,'start']=np.nan
    return raw_data
    
def get_all_data(query:str):
    
    """Takes in the SPARQL query and gets all the data from the defined endpoint"""
    
    sparql = SPARQLWrapper(SPARQL_ENDPOINTS['wikidata'])
    sparql.setReturnFormat(JSON)
    sparql.setQuery(query)
    results = sparql.query().convert()
    return results

def transform_all_player_data(rawData:dict):
    """This function transforms the raw bulk data of the players obtained from the function get_all_data
    param rawDict: Input dictionary output: pandas dataframe """
    
    #try column list
    try:
        cols = list(rawData["results"]["bindings"][1].keys())
        #print(cols)
        #creating a data dictionary
        data = {key:[] for key in cols}
        #print(data)

        #for loop to append each row into the dictionary
        for item in rawData["results"]["bindings"]:
            for col in cols:
                if col in item:
                    data[col].append(item[col]['value'])
                else:
                    data[col].append("")
        #print("Length of column ",col,':',len(data[col]))
        ###
        raw_data = pd.DataFrame(data)
        #only keeping the football club types
        raw_data = raw_data[raw_data.teamType=="http://www.wikidata.org/entity/Q476028"].copy()
        raw_data = raw_data.drop_duplicates()
        #convert transfer market IDs into string tye
        raw_data['transferMarketID'] = raw_data['transferMarketID'].astype(str)
        raw_data['transferMarketTeamID'] = raw_data['transferMarketTeamID'].astype(str)
        #add player and team uri for the TAKG domain
        raw_data['player_uri'] = raw_data.transferMarketID.apply(create_uri)
        raw_data['team_uri'] = raw_data.transferMarketTeamID.apply(create_uri)
        raw_data = raw_data.rename(columns=TIME_COLUMN_MAPPING)
        raw_data = replace_invalid_time(raw_data)
        print('Total records in the dataframe:',len(raw_data))
        
        return raw_data
    except:
        return print('something went wrong')

# In[13]:


def triples_dataframe():
    """Creates an empty data frame with triples"""
    return pd.DataFrame(columns=['subject','predicate','object'])


# In[14]:


def entity_to_parent(raw_data:pd.DataFrame):
    """
    The function adds the (entity, RDF:type, parent) riples to a pandas dataframe
    Parameters:
    
        
        raw_data:pd.DataFrame -> input raw data
        col_class_mapping:dict -> dictionary to map whihc feature contains values belonging to whihc class
    
    Output:
        
        returns dataframe after adding the triples
    
    """
    classes_to_parent = triples_dataframe()
    #iterate through each key in the COLUMN_CLASS_MAPPING dictioanry
    for label, parent in COLUMN_CLASS_MAPPING.items():
        
        if label in list(raw_data.columns):
        
            intermediate_frame =  triples_dataframe()
            intermediate_frame['subject'] = raw_data[label].unique()
            intermediate_frame['predicate'] = 'RDF.type'
            intermediate_frame['object'] = parent
            classes_to_parent = pd.concat([classes_to_parent,intermediate_frame], ignore_index=True)
    
    return classes_to_parent


# In[15]:


def get_data(rawData:pd.DataFrame,data_columns:list):
    """Function to read raw data from a CSV file and filter the data frame witht e passed column list
    param rawData: input pandas dataframe from the CSV file
    param data_columns: columns to be kept
    
    Output : Return a dataframe"""
    
    filtered_data = rawData[data_columns].copy()
    filtered_data = filtered_data.drop_duplicates()
    filtered_data = filtered_data.fillna('')
    filtered_data = filtered_data.reset_index(drop=True)
    return filtered_data
    


# In[16]:


def entity_data_to_triples(entity_data:pd.DataFrame,sub_col:str,class_column_predicate_mapping:dict):
    """
    The function adds triples to the table iteratively based on the input dataframe and the property mappings
    
    
    entity_data:pd.DataFrame : input pandas dataframe : The column names are property names except for the subject clumn
    sub:col : This is the name of the column in the dataframe whose values are considered to be subjects.
    class_column_predicate_mapping:dict : column name and its equivalent predicate names : The column names are predicates.
    
    """
    #reset the index of dataframe to avoid indexing errors
    entity_data = entity_data.reset_index()
    row = 0
    num_row = len(entity_data.index)
    entity_data_triples = triples_dataframe()
    #for each row in the dataframe
    while row<num_row:
        
        #get the record
        data_row = entity_data.loc[row,]
        #setting subject value
        subject = data_row[sub_col]
        intermediate_frame =  triples_dataframe()
        #loop for predicates and their corresponding values
        for label, predicate in class_column_predicate_mapping.items():
            # appending s,p,o to the intermediate frame
            if label in data_row.keys():
                triple_row = pd.DataFrame({'subject':subject,'predicate':predicate,                                                           'object':data_row[label]},index=[0])
                intermediate_frame= pd.concat([intermediate_frame,triple_row], ignore_index=True)
        entity_data_triples = pd.concat([entity_data_triples,intermediate_frame], ignore_index=True)
        row+=1

    entity_data_triples = entity_data_triples.reset_index(drop=True)
    return entity_data_triples


# In[17]:


def sum_string_columns(data:pd.DataFrame,colList:list):
    """concatenates the columns of a given dataframe and column list and returns a series object"""
    data_ = data[colList[0]].astype(str).copy()
    for i in range(1,len(colList)):
        data_ = data_+data[colList[i]].astype(str)
    
    return data_


# In[18]:


def id_generator(statement:str):
    """Given a value generate unique id"""
    return str(ctypes.c_size_t(hash(statement)).value)


# In[19]:


def add_statement_id(inputData:pd.DataFrame,colList:list):
    input_data = inputData.copy()
    input_data['statement_id'] = sum_string_columns(input_data,colList)
    input_data['statement_id'] = input_data.statement_id.apply(id_generator)
    return input_data


# In[20]:


def player_played_for_data(playedForData:pd.DataFrame,source:str):
    """This function takes in player, team, start & end time columns as input dataframe
    
    output : 'player',team', 'start','end', 'timePioint', 'statementId', 'retrievalID','insertion_time','source'"""
    played_for_data = playedForData.copy()
    
    #add retrieval source and insertion time
    played_for_data['source']=source
    played_for_data['insertion_time']= datetime.datetime.now()
    
    #column filling and renaming
    played_for_data['predicate'] = 'playedFor'
    played_for_data['time_point'] = ''
    played_for_data = played_for_data.rename(columns={'player_uri':'subject','team_uri':'object'})

    #calculate statementID based on subject predicate object & retrievalID based 'insertion_time &'source'
    played_for_data = add_statement_idX(played_for_data,STANDARD_TRIPLES,'statement_id')
    played_for_data = add_statement_idX(played_for_data,RETRIEVAL_ID_COLUMNS,'retrieval_id')
    
    kg_data = played_for_data[KG_TABLE_COLUMNS].drop_duplicates()
    temporal_data = played_for_data[TEMPORAL_TABLE_COLUMNS].drop_duplicates()
    metadata = played_for_data[META_TABLE_COLUMNS].drop_duplicates()
    
    return kg_data,temporal_data,metadata


# In[21]:


def insert_data_to_table(playerData:pd.DataFrame,tableName:str):
    
    """Inserts the player's data, downloaded from wikidata OR Transfer market, into the table passed as param
    Parameter
        playerData: pandas dataframe with players data
        tableName: table name that the data to be iserted
    
    Output
        retun none
    """
    conn = create_connection(DATABASE)
    playerData.to_sql(tableName, conn, schema=None, if_exists='append', index=False, index_label=None,
                    chunksize=None, dtype=None, method=None)
    conn.commit()
    conn.close()
   


# In[22]:


def get_player_profile(tfmid:str):
    headers = {'User-Agent': 
           'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

    page = "https://www.transfermarkt.com/transfermarkt/profil/spieler/"+tfmid
    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
    print(page)
    
    return pageSoup


# In[23]:


def get_transfer_history(page:bs4.BeautifulSoup):
    
    playerTransferDataRows = page.find_all('div',class_='grid tm-player-transfer-history-grid')
    playerTransferData = {key:[] for key in ['date','clubLeft_name','clubLeft_href','clubJoined_name','clubJoined_href']}
    
    for row in playerTransferDataRows:
        date = row.find('div',class_='grid__cell grid__cell--center tm-player-transfer-history-grid__date').text.replace('\n','').replace('  ','')
        #club left
        clubLeft = row.find('div',class_='grid__cell grid__cell--center tm-player-transfer-history-grid__old-club')
        clubLeft_name = clubLeft.text.replace('\n','').replace('  ','')
        clubLeft_href = clubLeft.find('a',href=True)
        #club joined
        clubJoined = row.find('div',class_="grid__cell grid__cell--center tm-player-transfer-history-grid__new-club")
        clubJoined_name = clubJoined.text.replace('\n','').replace('  ','')
        clubJoined_href = clubJoined.find('a',href=True)


        playerTransferData['date'].append(date)
        playerTransferData['clubLeft_name'].append(clubLeft_name)
        playerTransferData['clubLeft_href'].append(clubLeft_href['href'])
        playerTransferData['clubJoined_name'].append(clubJoined_name)
        playerTransferData['clubJoined_href'].append(clubJoined_href['href'])
    
    return pd.DataFrame(playerTransferData ) 


# In[24]:


def transform_player_history(playerHistory:pd.DataFrame):
    playerHistory['endTime'] = playerHistory['date'].shift(1,axis=0)
    playerHistory['startTime'] = playerHistory['date']
    #get the club's tfmid
    playerHistory['transferMarketTeamID'] = playerHistory['clubJoined_href'].apply(lambda row: re.split(r'/',row)[4])
    
    return playerHistory


# In[25]:


def get_market_value(page:bs4.BeautifulSoup):
    market_value_details = page.find('div' ,class_="tm-player-market-value-development__current-and-max")
    max_value_details = market_value_details.find('div' ,class_="tm-player-market-value-development__max").text
    current_value_details = market_value_details.find('div' ,class_="tm-player-market-value-development__current-value")     .text.replace('\n','').replace(" ",'')
    max_value_details_re = re.search(r'\n(.*?)\n(.*?)\n(.*?)\n',max_value_details)
    return {'max_market_value':max_value_details_re.group(2),'max_value_date':max_value_details_re.group(3),            'current_market_value':current_value_details} 


# In[26]:


def date_format_converter(dateString):
    
    """Takes in the date of format "Jul 23, 1999" and converts it into "1999-07-23T00:00:00Z" format """
    
    if type(dateString)==str:
        date_obj = datetime.datetime.strptime(dateString, "%b %d, %Y")
        standard_format = date_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
        return standard_format
    else:
        return dateString


# In[27]:


def get_player_transfer_market_data(tfmid:str):
    """Get the players transfer market details by taking in the TFMID
    param tfmID : Player's transfer market ID
    return Output a dataframe with players transfer history
    """
    #required columns
    temporal_data_columns = list(TRANSFER_MARKET_PLAYER_DATA_MAPPING.keys())
    #get the html data from player profile page
    print("getting player data from transfer market ...")
    playerProfile = get_player_profile(tfmid)
    
    #get player market value
    player_market_value = get_market_value(playerProfile)
    player_market_value['transferMarketID']=tfmid
    
    #get transfer history from the html page
    playerTransferHistory = get_transfer_history(playerProfile)
    #print("Player Transfer History extracted ...")
    #print(playerTransferHistory)

    #transform the player data
    TransformedPlayerTransferHistory = transform_player_history(playerTransferHistory)
    TransformedPlayerTransferHistory['player_transfer_market_ID']=tfmid
    #print("Player Transfer History transformed.")
    
    TransformedPlayerTransferHistory = TransformedPlayerTransferHistory[temporal_data_columns]     .rename(columns=TRANSFER_MARKET_PLAYER_DATA_MAPPING).copy()
    
    #transfor the time format of start and end
    TransformedPlayerTransferHistory['start'] = TransformedPlayerTransferHistory.start.apply(date_format_converter)
    TransformedPlayerTransferHistory['end'] = TransformedPlayerTransferHistory.end.apply(date_format_converter)
    
    return TransformedPlayerTransferHistory,player_market_value


# In[28]:


def intersecting_columns(colList1:list,colList2:list):
    resList = [value for value in colList1 if value in colList2]
    return resList


# In[29]:


def process_player_transfer_market_data(transfer_market_data:pd.DataFrame):
    """Takes in the player_transfer_market_data and return two separate dataframes for insertion into
    KG and Temporal table"""
    player_transfer_market_data = transfer_market_data.copy()
    # add player_uri & team-uri
    player_transfer_market_data['player_uri'] = 'TAKG.'+ player_transfer_market_data['transferMarketID']
    player_transfer_market_data['team_uri'] = 'TAKG.'+ player_transfer_market_data['transferMarketTeamID']
    
    #team KG Data
    tm_team_columns = intersecting_columns(list(player_transfer_market_data.columns),TEAM_COLUMNS)
    tm_team_data = get_data(player_transfer_market_data,tm_team_columns)
    tm_team_triples = entity_data_to_triples(tm_team_data,'team_uri',TEAM_COLUMN_PREDICATE_MAPPING)
    tm_team_KG_data = add_statement_id(tm_team_triples,STANDARD_TRIPLES)
    
    #player played for temporal data
    tm_player_data = get_data(player_transfer_market_data,TEMPORAL_DATA_COLUMNS)
    tm_player_kg_data,tm_player_temporal_data,tm_player_metadata = player_played_for_data(tm_player_data,'TFM')
    
    return tm_team_KG_data,tm_player_kg_data,tm_player_temporal_data,tm_player_metadata


# In[30]:


def add_statement_idX(inputData:pd.DataFrame,colList:list,idColName:str):
    """This function generates a unique id based on column list
    Input
        inputData : A panadas dataframe to generate the id on
        colList : The list of columns on whihc the id needs to be calculates
        idColName : The name of the desired id column
    Return returns the input dataframe with an extra idColName column"""
    input_data = inputData.copy()
    input_data[idColName] = sum_string_columns(input_data,colList)
    input_data[idColName] = input_data[idColName].apply(id_generator)
    return input_data


# In[31]:


def get_player_market_value_insertion_data(inputDict:dict): 
    
    """Function to take in the player's market value details and output data to be inserted into the KG, 
    temporal and Meta tables
    parameter inputDict: The input dictionary comes from the output of get_player_transfer_market_data('XYZ')
    
    Output returns 3 dataframes to be inserted into the tables.
    
    
    """
    
    tm_market_data = pd.DataFrame(inputDict, index=[0])
    
    #construct the dataframe
    tm_market_data['player_uri'] = 'TAKG.'+ tm_market_data['transferMarketID']
    tm_market_data = entity_data_to_triples(tm_market_data,'player_uri',PLAYER_MARKET_VALUE_PREDICATE_MAPPING)
    tm_market_data['start']=''
    tm_market_data['end']=''
    tm_market_data['source']=SOURCES['transfer_market']
    tm_market_data['insertion_time']= datetime.datetime.now()
    mask = tm_market_data.predicate==PLAYER_MARKET_VALUE_PREDICATE_MAPPING['max_market_value']
    tm_market_data.loc[mask,'time_point']=date_format_converter(inputDict['max_value_date'])
    mask=tm_market_data.predicate==PLAYER_MARKET_VALUE_PREDICATE_MAPPING['current_market_value']
    tm_market_data.loc[mask,'time_point']=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # add statement id & Retrieval id
    tm_market_data = add_statement_idX(tm_market_data,STANDARD_TRIPLES,'statement_id')
    tm_market_data = add_statement_idX(tm_market_data,RETRIEVAL_ID_COLUMNS,'retrieval_id')
    
    # Dataframes for the three tables
    mv_kg_data = tm_market_data[KG_TABLE_COLUMNS].drop_duplicates()
    mv_temporal_data = tm_market_data[TEMPORAL_TABLE_COLUMNS].drop_duplicates()
    mv_metadata = tm_market_data[META_TABLE_COLUMNS].drop_duplicates()

    return mv_kg_data,mv_temporal_data,mv_metadata


# In[32]:


def transform_player_concept_results(player_concept_results:dict,concept:str):
    
    """Function to handle the SPARQL query results
    param player_concept_results: takes in the SPARQL query dictionary output
          concept: The concept with which the player is associated
    output player_concept_results_data_frame: Outputs a dataframe of the player and the concept he/she is 
    related with by creating the player_uri and concept_uri """
    
    #try column list
    try:
        cols = list(player_concept_results["results"]["bindings"][1].keys())
        #print(cols)
        #creating a data dictionary
        data = {key:[] for key in cols}
        #print(data)

        #for loop to append each row into the dictionary
        for item in player_concept_results["results"]["bindings"]:
            for col in cols:
                if col in item:
                    data[col].append(item[col]['value'])
                else:
                    data[col].append("")
        #print("Length of column ",col,':',len(data[col]))

        #create a dataframe out of the data dictioanry and save it to a csv
        player_concept_results_data_frame = pd.DataFrame(data)
        #print('There were',len(data_frame),'records in the dataframe')
        player_concept_results_data_frame = player_concept_results_data_frame.drop_duplicates()

        player_concept_results_data_frame['player_uri'] = player_concept_results_data_frame.transferMarketID.apply(create_uri)
        player_concept_results_data_frame[concept+'_uri'] = player_concept_results_data_frame[concept].str.lstrip("http://www.wikidata.org/entity/").apply(create_uri)
        #print('There are',len(data_frame),'records in the dataframe AFTER dropping duplicates')

        player_concept_results_data_frame = player_concept_results_data_frame.rename(columns=TIME_COLUMN_MAPPING)
        return player_concept_results_data_frame
    except:
        return print('something went wrong')
    


# In[33]:


def transform_player_SPARQL_results(player_results:dict):
    """Function to transform the SPARQL results of player played for data"""
    
    #column list
    cols = list(player_results["results"]["bindings"][1].keys())
    #print(cols)

    #creating a data dictionary
    data = {key:[] for key in cols}
    #print(data)

    #for loop to append each row into the dictionary
    for item in player_results["results"]["bindings"]:
        for col in cols:
            if col in item:
                data[col].append(item[col]['value'])
            else:
                data[col].append("")
    #print("Length of column ",col,':',len(data[col]))

    #create a dataframe out of the data dictioanry and save it to a csv
    player_results_data_frame = pd.DataFrame(data)
    #print('There were',len(data_frame),'records in the dataframe')
    player_results_data_frame = player_results_data_frame.drop_duplicates()
    
    player_results_data_frame['player_uri'] = player_results_data_frame.transferMarketID.apply(create_uri)
    player_results_data_frame['team_uri'] = player_results_data_frame.transferMarketTeamID.apply(create_uri)
    #print('There are',len(data_frame),'records in the dataframe AFTER dropping duplicates')
    
    player_results_data_frame = player_results_data_frame.rename(columns=TIME_COLUMN_MAPPING)
    return player_results_data_frame


# In[34]:


def get_player_event_participation_data(playerEventData:pd.DataFrame,source:str,predicate:str):
    """This function takes in player, event, start & end time columns as input dataframe
    
    output : 'player',event', 'start','end', 'timePioint', 'statementId', 'retrievalID','insertion_time','source'"""
    event_participation_data = playerEventData.copy()
    
    #column filling and renaming
    event_participation_data = event_participation_data.rename(columns={'player_uri':'subject','event_uri':'object'})
    event_participation_data['predicate'] = predicate
    event_participation_data['time_point'] = ''
    
    #add retrieval source and insertion time
    event_participation_data['source']=source
    event_participation_data['insertion_time']= datetime.datetime.now()
    
    # add statement id & Retrieval id
    event_participation_data = add_statement_idX(event_participation_data,STANDARD_TRIPLES,'statement_id')
    event_participation_data = add_statement_idX(event_participation_data,RETRIEVAL_ID_COLUMNS,'retrieval_id')
    
    #KG, TEMPORAL & METADATA Fields
    kg_data = event_participation_data[KG_TABLE_COLUMNS].drop_duplicates()
    temporal_data = event_participation_data[TEMPORAL_TABLE_COLUMNS].drop_duplicates()
    metadata = event_participation_data[META_TABLE_COLUMNS].drop_duplicates()
    
    return kg_data,temporal_data,metadata


# In[35]:


def get_player_award_insertion_data(playerAwardData:pd.DataFrame,source:str,predicate:str): 
    
    """Function to take in the player's award details and output data to be inserted into the KG, 
    temporal and Meta tables
    parameter inputDict: The input dataframe comes from the output of functiontransform_player_concept_results()
    
    Output returns 3 dataframes to be inserted into the tables.
    
    """
    
    player_award_data = playerAwardData.copy()
    
    #construct the dataframe
    player_award_data = player_award_data.rename(columns={'player_uri':'subject','award_uri':'object'})
    player_award_data['predicate'] = predicate
    player_award_data['start']=''
    player_award_data['end']=''
    player_award_data['source']=source
    player_award_data['insertion_time']= datetime.datetime.now()
    
    # add statement id & Retrieval id
    player_award_data = add_statement_idX(player_award_data,STANDARD_TRIPLES,'statement_id')
    player_award_data = add_statement_idX(player_award_data,RETRIEVAL_ID_COLUMNS,'retrieval_id')
    
    # Dataframes for the three tables
    award_kg_data = player_award_data[KG_TABLE_COLUMNS].drop_duplicates()
    award_temporal_data = player_award_data[TEMPORAL_TABLE_COLUMNS].drop_duplicates()
    award_metadata = player_award_data[META_TABLE_COLUMNS].drop_duplicates()

    return award_kg_data,award_temporal_data,award_metadata

