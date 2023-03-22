from constants import DATABASE, TABLES, SOURCES
from util import create_connection,create_table,drop_table,select_data

create_base_KG_query = """CREATE TABLE IF NOT EXISTS KG(
        subject TEXT,
        predicate TEXT,
        object TEXT,
        statement_id TEXT
        );"""

create_temporal_query = """CREATE TABLE IF NOT EXISTS Temporal(
        statement_id TEXT,
        start TEXT,
        end TEXT,
        time_point TEXT,
        retrieval_id TEXT
        );"""

create_meta_query = """CREATE TABLE IF NOT EXISTS Metadata(
        retrieval_id TEXT,
        source TEXT,
        insertion_time TEXT
        );"""

#Table creation
create_table(create_base_KG_query)
print(select_data("KG"))
create_table(create_temporal_query)
print(select_data("Temporal"))
create_table(create_meta_query)
print(select_data("Metadata"))