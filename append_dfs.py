import pandas as pd
import numpy as np
import collections

def add_col():
    squad_df = pd.read_csv("C:/Users/grina/Desktop/VGTU/squad_data.csv", engine='python', index_col=0)
    squad_df['playing_type'] = 3
    squad_miss = squad_df.columns[squad_df.isnull().any()].tolist()

    squad_df = squad_df.drop(columns=squad_miss)
    duo_df = pd.read_csv("C:/Users/grina/Desktop/VGTU/duo_data.csv", engine='python', index_col=0)
    duo_df['playing_type'] = 2
    duo_miss = duo_df.columns[duo_df.isnull().any()].tolist() 
    duo_df = duo_df.drop(columns=duo_miss)
    solo_df = pd.read_csv("C:/Users/grina/Desktop/VGTU/solo_data.csv", engine='python', index_col=0)
    solo_df['playing_type'] = 1
    solo_miss = solo_df.columns[solo_df.isnull().any()].tolist()
    solo_df = solo_df.drop(columns=solo_miss)
    list_df = [squad_df, duo_df, solo_df]
    final_df = pd.DataFrame()
    for df in list_df:
        final_df = final_df.append(df)

    final_miss = final_df.columns[final_df.isnull().any()].tolist()
    final_df = final_df.drop(columns=final_miss)
    with open(r'C:\Users\grina\Desktop\VGTU\final_data.csv', 'w', newline='') as ref:
        final_df.to_csv(ref, sep=',')
add_col()