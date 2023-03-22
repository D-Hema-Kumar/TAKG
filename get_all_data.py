 
from constants import SPARQL_ENDPOINTS, TIME_COLUMN_MAPPING
from util import get_all_data, transform_all_player_data

player_data_query = """


PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 

SELECT ?fullName ?player ?DOB ?sex ?transferMarketID ?team ?teamLabel ?teamType  ?startTime ?endTime  ?teamCountry ?transferMarketTeamID
WHERE {
   ?player wdt:P106 wd:Q937857; # Football association player
            p:P569 ?DOB_statement;
            p:P21 ?sex_statement;
            p:P2446 ?transferMarket_statement;
            p:P54 ?team_statement ;
            p:P1559 ?fullName_statement.
  
   ?fullName_statement ps:P1559 ?fullName.         
   OPTIONAL{?DOB_statement ps:P569 ?DOB}.
   OPTIONAL{?sex_statement ps:P21 ?sex}.
   OPTIONAL{?transferMarket_statement  ps:P2446 ?transferMarketID}.
   
   ?team_statement ps:P54 ?team ;
   OPTIONAL{?team_statement pq:P580 ?startTime} .
   OPTIONAL{?team_statement pq:P582 ?endTime} .
                   
   ?team rdfs:label ?teamLabel;
         p:P17 ?country_statement;
         p:P7223 ?transferMarketTeam_statement;
         p:P31 ?teamType_Statement. # whether fotball club(https://www.wikidata.org/wiki/Q476028) or national team
   ?teamType_Statement ps:P31 ?teamType.
   ?transferMarketTeam_statement ps:P7223 ?transferMarketTeamID.
   ?country_statement ps:P17 ?teamCountry.
   
   #VALUES ?player {wd:Q615}.
   # VALUES ?relation {p:P54}.
   # https://www.wikidata.org/wiki/Property:P569 #DOB
   # https://www.wikidata.org/wiki/Property:P21  #Sex
   # https://www.wikidata.org/wiki/Property:P735 #Given Name
   # https://www.wikidata.org/wiki/Property:P734 #Family Name
   # https://www.wikidata.org/wiki/Property:P118 #League
   # https://www.wikidata.org/wiki/Property:P17  #Country
   # https://www.wikidata.org/wiki/Property:P2446 #transferMarketID
   # https://www.transfermarkt.com/transfermarkt/profil/spieler/  #transfermarket URL
  
  FILTER EXISTS {?player p:P2276 ?UEFA_id} #to limit the data size filter only UEFA players
  FILTER (langMatches( lang(?teamLabel), "EN" ) ) #
  
  }
  
"""

player_results = get_all_data(player_data_query)
player_results_transformed = transform_all_player_data(player_results)
#write data to a csv
player_results_transformed.to_csv('./data/playerRawData.csv', index=False)



