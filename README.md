# TAKG
The project can be directly set up locally using the environment.yml file. You can just use the functions.ipynb file for executing different functionalities specified there. If you want to set the database from scrach, please follow below steps.<br>

### Set up from scratch<br>
1. Delete the TAKG Database in the ./kgdb folder and the playerRawData.csv file from ./data, and create an SQLite database 'TAKG.db'. <br>
2. Execute create_tables.py file for creating the tables with desired schema.<br>
3. Execute get_all_data.py file. This will download the playerRawData.csv file to ./data/ <br>
4. Execute insert_all_data.py file for bulk insertion.<br>

