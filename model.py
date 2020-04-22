import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pysurvival.models.semi_parametric import CoxPHModel
from lifelines import WeibullAFTFitter
from sklearn.model_selection import train_test_split
from pysurvival.utils.metrics import concordance_index
from pysurvival.utils.display import integrated_brier_score
from lifelines import CoxPHFitter

#estimating time-to-event using random forests
def model_data():
    train_df = pd.read_csv(r'C:\Users\grina\Desktop\VGTU\survival_data.csv', index_col=0)
    train_df.reset_index(inplace=True)
    #replace empty values with 0
    train_df.fillna(value=0, inplace=True)
    train_df.drop(columns=['name', 'Parachute'], inplace=True)
    #change T, F with 1,0
    train_df['is_in_blue_zone'] = train_df['is_in_blue_zone'].replace({True:1, False:0})
    train_df['is_in_red_zone'] = train_df['is_in_red_zone'].replace({True:1, False:0})
    train_df.rename(columns={'event':'groggy'}, inplace=True)
    train_df['event'] = 1

    # plt.hist(df['death_time'], bins=100)
    # plt.ylabel('Įvykių skaičius')
    # plt.xlabel('Laikas t(s)')
    # plt.show()


    return train_df

def cox_ph(df):
    # columns = df.columns
    # features = columns.remove(['death_time', 'event'])
    # index_train, index_test = train_test_split(range(df.shape[0]), test_size=0.2)
    # data_train = df.loc[index_train].reset_index(drop=True)
    # data_test = df.loc[index_test].reset_index(drop=True)

    # X_train, X_test = data_train[features], data_test[features]
    # T_train, T_test = data_train['death_time'], data_test['death_time']
    # E_train, E_test = data_train['event'], data_test['event']

    # coxph = CoxPHModel()
    # coxph.fit(X_train, T_train, E_train, )

    # coxph = CoxPHFitter()

    # coxph.fit(df, duration_col='death_time', event_col='event', show_progress=True, step_size=0.1)

    # with open(r'C:\Users\grina\Desktop\VGTU\coxph.txt', 'w') as f:
    #     print(coxph.print_summary(), file=f)
    sns.set(style="white")
    corr = df.corr()
    corr.style.background_gradient(cmap='coolwarm').set_precision(2)
    
    # f, ax = plt.subplots(figsize=(11, 9))
    # cmap = sns.diverging_palette(220, 10, as_cmap=True)
    # sns.heatmap(corr, mask=mask, cmap=cmap, vmax=.3, center=0,
    #         square=True, linewidths=.5, cbar_kws={"shrink": .5})

if __name__ == "__main__":
    df = model_data()
    cox_ph(df)
    # with open(r'C:\Users\grina\Desktop\VGTU\test_pos_feat5.csv', 'w', newline='') as ref:
    #     df.to_csv(ref, sep=',', index=False)