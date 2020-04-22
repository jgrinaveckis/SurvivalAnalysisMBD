import os
import pandas as pd
import numpy as np
from scipy import spatial
from sklearn.preprocessing import MinMaxScaler
from pubg_python import Telemetry
import resources as r


class TelemetryDataReader:

    def __init__(self, telemetry):
        self.telemetry = telemetry

    #parsing match id
    def MatchDefinition(self):
        match_id = telemetry.events_from_type('LogMatchDefinition')[0].match_id
        match_name_list = match_id.split(".")
        try:
            match_id_parsed = match_name_list[-1]
        except:
            print("Naming format changed!")
        return match_id_parsed
    
    #Taking player names from other events ends up in nonsensical data (f.e. 101 players in game)
    def PlayerNameReader(self):
        characters = telemetry.events_from_type('LogMatchStart')
        namesList = [ch.name
                    for char in characters
                    for ch in char.characters]
        return namesList
        
    #get position events by player name
    def PlayerPositionEvents(self, player_names_list, match_id):
        player_data = pd.DataFrame()
        player_positions = telemetry.events_from_type('LogPlayerPosition')
        for player_position in player_positions:
            for player_name in player_names_list:
                if player_position.character.name == player_name:
                    # zone = player_position.character.zone
                    # Change zone numerical value from -1 (changed to that if categorical value is NaN)?
                    # if not zone:
                    #     zone.append(None)
                    # else:
                    #     zone = zone[0]
                    player_data = player_data.append(pd.DataFrame({
                        'health': player_position.character.health,
                        'name': player_position.character.name,
                        'game_state': player_position.common.is_game,
                        'x_position': player_position.character.location.x,
                        'y_position': player_position.character.location.y,
                        'z_position': player_position.character.location.z,
                        'ranking': player_position.character.ranking,
                        'is_in_blue_zone': player_position.character.is_in_blue_zone,
                        'is_in_red_zone': player_position.character.is_in_red_zone,
                        'timestamp': pd.to_datetime(player_position.timestamp, format="%Y-%m-%dT%H:%M:%S.%fZ")
                        },
                        index=[player_position.timestamp+'_'+player_position.character.name+'_'+match_id]))
        return player_data

    def Attacker_Events(self, player_names_list, match_id):
        player_data = pd.DataFrame()
        attacker_events = telemetry.events_from_type('LogPlayerTakeDamage')
        for attacker_info in attacker_events:
            for player_name in player_names_list:
                if attacker_info.attacker.name == player_name:
                    zone = attacker_info.attacker.zone
                    # Change zone numerical value from -1 (changed to that if categorical value is NaN)?
                    # if not zone:
                    #     zone.append(None)
                    # else:
                    #     zone = zone[0]
                    player_data = player_data.append(pd.DataFrame({
                        'health': attacker_info.attacker.health,
                        'name': attacker_info.attacker.name,
                        'game_state': attacker_info.common.is_game,
                        'x_position': attacker_info.attacker.location.x,
                        'y_position': attacker_info.attacker.location.y,
                        'z_position': attacker_info.attacker.location.z,
                        'ranking': attacker_info.attacker.ranking,
                        'is_in_blue_zone': attacker_info.attacker.is_in_blue_zone,
                        'is_in_red_zone': attacker_info.attacker.is_in_red_zone,
                        'timestamp': pd.to_datetime(attacker_info.timestamp, format="%Y-%m-%dT%H:%M:%S.%fZ")
                        },
                        index=[attacker_info.timestamp+'_'+attacker_info.attacker.name+'_'+match_id]))
        return player_data

    def Victim_Events(self, player_names_list, match_id):
        player_data = pd.DataFrame()
        events = ['LogPlayerKill', 'LogPlayerTakeDamage']

        for ie in events:
            victim_events = telemetry.events_from_type(ie)
            for victim_info in victim_events:
                for player_name in player_names_list:
                    if victim_info.victim.name == player_name:
                        # zone = victim_info.victim.zone
                        # Change zone numerical value from -1 (changed to that if categorical value is NaN)?
                        # if not zone:
                        #     zone.append(None)
                        # else:
                        #     zone = zone[0]
                        player_data = player_data.append(pd.DataFrame({
                            'health': victim_info.victim.health,
                            'name': victim_info.victim.name,
                            'game_state': victim_info.common.is_game,
                            'x_position': victim_info.victim.location.x,
                            'y_position': victim_info.victim.location.y,
                            'z_position': victim_info.victim.location.z,
                            'ranking': victim_info.victim.ranking,
                            'is_in_blue_zone': victim_info.victim.is_in_blue_zone,
                            'is_in_red_zone': victim_info.victim.is_in_red_zone,
                            'timestamp': pd.to_datetime(victim_info.timestamp, format="%Y-%m-%dT%H:%M:%S.%fZ")
                            },
                            index=[victim_info.timestamp+'_'+victim_info.victim.name+'_'+match_id]))
        
        return player_data

    
    #player ranking dict
    def char_ranking(self):
        characters = telemetry.events_from_type('LogMatchEnd')
        char_rank = {ch.name: ch.ranking
                     for char in characters
                     for ch in char.characters}
        return char_rank
 

    #combine dataframes from different types of events
    def combine_dataframes(self, player_names_list, match_id):

        df_1 = self.Attacker_Events(player_names_list, match_id)
        df_2 = self.Victim_Events(player_names_list, match_id)
        df_3 = self.PlayerPositionEvents(player_names_list, match_id)
        pd_list = [df_1, df_2, df_3]
        common_df = pd.concat(pd_list)
        player_char = self.char_ranking()

        #adding ranking places to players
        ranking_list = []
        for index, row in common_df.iterrows():
            if row['name'] in player_char:
                ranking_list.append(player_char[row['name']])
        common_df['ranking'] = ranking_list

        #remove duplicates when health == 0
        common_df = pd.concat([common_df.loc[common_df.health > 0],
                                common_df.loc[common_df.health == 0].drop_duplicates(['name'], keep="first")])

        #converting categorical data to numerical
        #common_df['map_zone'] = common_df['map_zone'].astype('category').cat.codes

        #normalizing coordinates from 0 to 100
        pos_list = ['x_position', 'y_position', 'z_position']
        for item in pos_list:
            common_df[common_df[item] < 0] = np.nan
            common_df[item] = ((common_df[item]-common_df[item].min())/(common_df[item].max()-common_df[item].min()))*100
            common_df[item] = common_df[item].fillna(0).round(2)

        #calculating cumulative distance
        dx = (common_df['x_position'] - common_df.groupby('name')['x_position'].shift(-1)).fillna(0)
        dy = (common_df['x_position'] - common_df.groupby('name')['x_position'].shift(-1)).fillna(0)
        dz = (common_df['x_position'] - common_df.groupby('name')['x_position'].shift(-1)).fillna(0)
        common_df['dist'] = np.sqrt(dx**2 + dy**2 + dz**2)
        common_df['dist'] = common_df['dist'].fillna(0).round(2)
        common_df['cumdist'] = common_df.groupby('name')['dist'].cumsum()

        common_df = common_df.sort_values(by=['timestamp'])
        common_df['elapsed_time'] = common_df['timestamp'] - common_df['timestamp'].iloc[0]
        common_df['elapsed_time'] = common_df['elapsed_time']/ np.timedelta64(1, 's')

        #duplicates for elapsed time  == 0 removed
        common_df = pd.concat([common_df.loc[common_df.elapsed_time > 0],
                                common_df.loc[common_df.elapsed_time == 0].drop_duplicates(['name'], keep="first")]).sort_values(by='timestamp')
        
        common_df = common_df.drop(columns=['timestamp']).reset_index()

        #remove censored (who didn't die) players
        #main_df = common_df.groupby('name').filter(lambda x: x['health'].iloc[-1] == 0)
        #main_df.reset_index(drop=True, inplace=True)
        main_df = common_df[common_df.groupby('name')['health'].apply(lambda x: x.shift().eq(0).cumsum().eq(0))]
        

        #time series data (t-1, t)
        #main_df['start'] = main_df['elapsed_time'] - main_df.groupby('name')['elapsed_time'].transform('first')
        #main_df['end'] = main_df['start']
        #main_df['start'] = main_df.groupby('name')['start'].shift()
        #main_df = main_df.groupby('name', group_keys=False).apply(lambda x:x.iloc[1:])

        #calculate time to death
        #main_df['time_to_death'] = main_df.groupby('name')['end'].transform('last')
        main_df = main_df.groupby('name', group_keys=False).apply(lambda x:x.iloc[1:])
        main_df['time_to_death'] = main_df.groupby('name')['elapsed_time'].transform('last') - main_df['elapsed_time']
        
        #add event flag in death moment
        flags = [1 if row['health'] == 0 else 0 for index, row in main_df.iterrows()]
        main_df['event'] = flags

        #sort by columns and drop unnecessary
        main_df.sort_values(by=['name', 'elapsed_time'])
        #Need to leave for random forests
        #main_df.drop(columns=['elapsed_time'], inplace=True)

        main_df = main_df.set_index('index')
        return main_df

    #Player picked items list
    def player_item_events(self, player_names_list, match_id):
        values_list = list(r.ITEM_MAP.values())
        player_item_data = pd.DataFrame(columns=['name', 'item', 'item_stack_count'].extend(values_list))
        item_events_list = ['LogItemPickup', 'LogItemPickupFromCarepackage', 'LogItemPickupFromLootBox']

        for ie in item_events_list:
            player_item_events = telemetry.events_from_type(ie)
            for item_event in player_item_events:
                for pn in player_names_list:
                    for item_name in values_list:
                        if item_event.character.name == pn and item_event.item.name == item_name:
                            player_item_data = player_item_data.append(pd.DataFrame({
                                'name' : item_event.character.name,
                                'item' : item_event.item.name,
                                'item_stack_count' : item_event.item.stack_count,
                                item_name : item_event.item.stack_count,
                                'timestamp': pd.to_datetime(item_event.timestamp, format="%Y-%m-%dT%H:%M:%S.%fZ")
                            },
                            index=[item_event.timestamp+'_'+item_event.character.name+'_'+match_id]))
        #fill NaN values
        player_item_data.fillna(0, inplace=True)
        player_item_data = player_item_data.sort_values(by=['timestamp'])
        #cumulative sum of items if the same item (or ammo) was picked by same player
        for item_name in values_list:
            if item_name in player_item_data.columns:
                player_item_data[item_name] = player_item_data.groupby(['name'])[item_name].cumsum()
        return player_item_data
    
if __name__ == "__main__":
    telemetry = Telemetry.from_json('C:\\Users\\grina\\Desktop\\test_folder\\58ad8697-5f4e-4f6f-9cb2-534f5db6dee3.json')
    tdm = TelemetryDataReader(telemetry)
    names = tdm.PlayerNameReader()
    match_id = tdm.MatchDefinition()
    final_df = tdm.combine_dataframes(names, match_id)
    
    #testing appending multiple dfs
    # final_df = pd.DataFrame()
    # path = 'C:\\Users\\grina\\Desktop\\test_folder'
    # for filename in os.listdir(path):
    #     telemetry = Telemetry.from_json(path+'\\'+filename)
    #     tdm = TelemetryDataReader(telemetry)
    #     names = tdm.PlayerNameReader()
    #     match_id = tdm.MatchDefinition()
    #     test_df = tdm.combine_dataframes(names, match_id)
    #     final_df = final_df.append(test_df)
    
    # with open(r'C:\Users\grina\Desktop\VGTU\test_pos_feat2.csv', 'w', newline='') as ref:
    #     final_df.to_csv(ref, sep=',')
   
