import pandas as pd
from util import entity_to_parent, get_data, entity_data_to_triples, player_played_for_data, add_statement_id, insert_data_to_table, get_row_count
from constants import RAW_DATA_FILE_PATH, PLAYER_COLUMNS, PLAYER_COLUMN_PREDICATE_MAPPING, TEAM_COLUMNS, TEAM_COLUMN_PREDICATE_MAPPING, \
TEMPORAL_DATA_COLUMNS, SOURCES, STANDARD_TRIPLES, TABLES


#process static data
raw_data = pd.read_csv(RAW_DATA_FILE_PATH)
classes_to_parent_triples = entity_to_parent(raw_data)
player_data = get_data(raw_data,PLAYER_COLUMNS)
player_triples = entity_data_to_triples(player_data,'player_uri',PLAYER_COLUMN_PREDICATE_MAPPING)
team_data = get_data(raw_data,TEAM_COLUMNS)
team_triples = entity_data_to_triples(team_data,'team_uri',TEAM_COLUMN_PREDICATE_MAPPING)

#process temporal data
player_played_for_temporal_data = get_data(raw_data,TEMPORAL_DATA_COLUMNS)
player_played_for_kg_data,played_for_temporal_data,played_for_metadata = player_played_for_data(player_played_for_temporal_data,SOURCES['wiki_source'])

#Making Quintiples for insertion into KG table
classes_KG_data = add_statement_id(classes_to_parent_triples,STANDARD_TRIPLES)
player_KG_data = add_statement_id(player_triples,STANDARD_TRIPLES)
team_KG_data = add_statement_id(team_triples,STANDARD_TRIPLES)

#insertion
print('#####Counts BEFORE Insertion#####\n')
print('Total KG row count: ',get_row_count(TABLES['base_KG_table'])['row_count'][0])
print('Total Temporal row count: ',get_row_count(TABLES['temporal_meta_table'])['row_count'][0])
print('Total Metadata row count: ',get_row_count(TABLES['meta_table'])['row_count'][0])

print('\ninsertion in progress...\n')

#Bulk Data Insertion into KG, Temporal & Metadata tables
insert_data_to_table(classes_KG_data,TABLES['base_KG_table'])
insert_data_to_table(player_KG_data,TABLES['base_KG_table'])
insert_data_to_table(team_KG_data,TABLES['base_KG_table'])
insert_data_to_table(player_played_for_kg_data,TABLES['base_KG_table'])
insert_data_to_table(played_for_temporal_data,TABLES['temporal_meta_table'])
insert_data_to_table(played_for_metadata,TABLES['meta_table'])

print('\ninsertion complete.\n')

print('\n#####Counts AFTER Insertion#####\n')
print('Total KG row count: ',get_row_count(TABLES['base_KG_table'])['row_count'][0])
print('Total Temporal row count: ',get_row_count(TABLES['temporal_meta_table'])['row_count'][0])
print('Total Metadata row count: ',get_row_count(TABLES['meta_table'])['row_count'][0])