import os
import pandas as pd
import numpy as np
from functools import reduce
from pubg_python import Telemetry
import resources as r


class TelemetryDataReader:
    players_names = []
    def __init__(self, telemetry):
        self.telemetry = telemetry
        

    #parsing match id
    def match_definition(self):
        match_id = telemetry.events_from_type('LogMatchDefinition')[0].match_id
        match_name_list = match_id.split(".")
        try:
            match_id_parsed = match_name_list[-1]
        except:
            print("Naming format changed!")
        return match_id_parsed

    #Players names
    def player_name_reader(self):
        characters = telemetry.events_from_type('LogMatchStart')
        namesList = [ch.name
                    for char in characters
                    for ch in char.characters]
        return namesList

    def add_missing_names(self, data_df, names):
        names_orig = data_df['name'].tolist()
        missing_names = [name for name in names if name not in names_orig]
        df_names = pd.DataFrame({'name': missing_names})
        data_df = pd.concat([data_df, df_names], sort=False)
        data_df.reset_index(level=0, inplace=True)
        data_df = data_df.drop(columns=['index'])
        data_df = data_df.fillna(0)
        return data_df


    def victim_events(self, player_names_list, match_id):
        player_data = pd.DataFrame()
        player_assist_data = pd.DataFrame()
        kill_events = telemetry.events_from_type('LogPlayerKill')
        start_obj = telemetry.events_from_type('LogMatchDefinition')[0]
        start_time = pd.to_datetime(start_obj.timestamp, format="%Y-%m-%dT%H:%M:%S.%fZ")
        for victim_info in kill_events:
            for player_name in player_names_list:
                if victim_info.victim.name == player_name:
                    player_data = player_data.append(pd.DataFrame({
                        'name': victim_info.victim.name,
                        'rank': victim_info.victim_game_result.rank,
                        'kills': victim_info.victim_game_result.stats.kill_count,
                        "is_in_blue_zone": victim_info.victim.is_in_blue_zone,
                        "is_in_red_zone": victim_info.victim.is_in_red_zone,
                        'dist_on_foot': victim_info.victim_game_result.stats.distance_on_foot,
                        'dist_on_swim': victim_info.victim_game_result.stats.distance_on_swim,
                        'dist_on_vehicle': victim_info.victim_game_result.stats.distance_on_vehicle,
                        'dist_on_parachute': victim_info.victim_game_result.stats.distance_on_parachute,
                        'dist_on_freefall': victim_info.victim_game_result.stats.distance_on_freefall,
                        'groggy': victim_info.dbno_id,
                        'timestamp': pd.to_datetime(victim_info.timestamp, format="%Y-%m-%dT%H:%M:%S.%fZ")
                        },
                        index=[victim_info.timestamp+'_'+victim_info.victim.name+'_'+match_id]))
                elif victim_info.assistant.name == player_name:
                    player_assist_data = player_assist_data.append(pd.DataFrame({
                        'name': player_name,
                        'assist' : 1
                    },
                    #cant pass scalars other way
                    index=[0]))
        player_data.loc[player_data['groggy'] > 0, 'groggy'] = 0
        player_data.loc[player_data['groggy'] == -1, 'groggy'] = 1
        player_assist_data.reset_index(level=0, inplace=True)
        player_assist_data = player_assist_data.groupby('name').apply(lambda x: x.sum()).drop(columns=['name', 'index'])
        player_assist_data.reset_index(level=0, inplace=True)
        player_data['death_time'] = player_data['timestamp'] - start_time
        player_data['death_time'] = player_data['death_time']/ np.timedelta64(1, 's')
        player_data = player_data.drop(columns=['timestamp'])
        self.players_names = player_data['name'].tolist()

        player_assist_data = self.add_missing_names(player_assist_data, self.players_names)
        return player_data, player_assist_data

    # Cum amount of healing player did in a match
    def player_heal_events(self, player_names_list, match_id):
        player_heal_data = pd.DataFrame()
        heal_events = telemetry.events_from_type('LogHeal')
        for event in heal_events:
            for pn in player_names_list:
                if event.character.name == pn:
                    player_heal_data = player_heal_data.append(pd.DataFrame({
                        'name': pn,
                        'heal_amount': event.heal_amount,
                    },
                    index=[0]))
        player_heal_data = player_heal_data.groupby('name').apply(lambda x: x.sum()).drop(columns=['name'])
        player_heal_data.reset_index(level=0, inplace=True)
        names = self.players_names
        player_heal_data = self.add_missing_names(player_heal_data, names)
        return player_heal_data

    #player ranking dict
    def char_ranking(self):
        characters = telemetry.events_from_type('LogMatchEnd')
        char_rank = {ch.name: ch.ranking
                     for char in characters
                     for ch in char.characters}
        return char_rank


    #Player picked items list
    def player_item_events(self, player_names_list, match_id):
        values_list = list(r.ITEM_MAP.values())
        player_item_data = pd.DataFrame()
        item_events_list = ['LogItemPickup', 'LogItemPickupFromCarepackage', 'LogItemPickupFromLootBox']

        for ie in item_events_list:
            player_item_events = telemetry.events_from_type(ie)
            for item_event in player_item_events:
                for pn in player_names_list:
                    for item_name in values_list:
                        if item_event.character.name == pn and item_event.item.name == item_name:
                            player_item_data = player_item_data.append(pd.DataFrame({
                                'name' : item_event.character.name,
                                'item_stack_count' : item_event.item.stack_count,
                                item_name : item_event.item.stack_count
                            },
                            index=[item_event.timestamp+'_'+item_event.character.name+'_'+match_id]))
        #fill NaN values
        player_item_data.fillna(0, inplace=True)
        player_item_data = player_item_data.groupby('name').apply(lambda x: x.sum()).drop(columns=['name'])
        names = self.players_names
        player_item_data.reset_index(level=0, inplace=True)
        player_item_data = player_item_data[player_item_data['name'].isin(names)]
        return player_item_data

    def player_damage_events(self, player_names_list, match_id):
        player_dmg_events = telemetry.events_from_type('LogPlayerTakeDamage')

        player_dmg_data = pd.DataFrame()
        for event in player_dmg_events:
            for pn in player_names_list:
                if event.attacker.name == pn:
                    player_dmg_data = player_dmg_data.append(pd.DataFrame({
                        'name': event.attacker.name,
                        'damage': event.damage
                    },
                    index=[0]))
        #sum player damage
        player_dmg_data = player_dmg_data.groupby('name').apply(lambda x: x.sum()).drop(columns=['name'])
        player_dmg_data.reset_index(level=0, inplace=True)
        # patikrint, kurie zaidejai nedare dmg ir pridet ju nickus su dmg == 0
        player_dmg_data = self.add_missing_names(player_dmg_data, self.players_names)

        return player_dmg_data

    def combine_dfs(self, player_names_list, match_id):
        df_1, df_2 = self.victim_events(player_names_list, match_id)
        df_3 = self.player_item_events(player_names_list, match_id)
        df_4 = self.player_damage_events(player_names_list, match_id)
        df_5 = self.player_heal_events(player_names_list, match_id)
        df_list = [df_1, df_2, df_3, df_4, df_5]
        common_df = reduce(lambda left,right: pd.merge(left,right,on='name'), df_list)

        #adding missing rankings for players
        player_char = self.char_ranking()
        ranking_list = []
        for index, row in common_df.iterrows():
            if row['name'] in player_char:
                ranking_list.append(player_char[row['name']])
        common_df['rank'] = ranking_list

        common_df['distance_sum'] = common_df.loc[:,['dist_on_foot', 'dist_on_swim', 'dist_on_vehicle', 'dist_on_parachute', 'dist_on_freefall']].sum(axis=1)
        common_df['dmg_per_distance'] = common_df['damage']/common_df['distance_sum']
        common_df['dmg_per_distance'].replace(to_replace=np.inf, value=0, inplace=True)
        common_df['dmg_per_distance'].fillna(value=0, inplace=True)
        common_df = common_df.round(3)
        return common_df

if __name__ == "__main__":

    final_df = pd.DataFrame()
    path = 'C:\\Users\\grina\\Desktop\\squad_official_matches'
    for filename in os.listdir(path):
        if filename.endswith(".json"):
            telemetry = Telemetry.from_json(path+'\\'+filename)
            tdm = TelemetryDataReader(telemetry)
            names = tdm.player_name_reader()
            match_id = tdm.match_definition()
            test_df = tdm.combine_dfs(names, match_id)
            final_df = final_df.append(test_df)
    with open(r'C:\Users\grina\Desktop\VGTU\squad_data2.csv', 'w', newline='') as ref:
        final_df.to_csv(ref, sep=',')
   
