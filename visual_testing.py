def direction_heatmap(dir_df):
    import seaborn as sns
    import pandas as pd
    dir_df['HOUR'] = dir_df['TimeSnap'].apply(lambda x: x.hour)
    dir_df['WIND_DIR_GROUP'] = pd.cut(dir_df['Dir'], bins=range(0, 364, 5))
    dir_df.reset_index(drop = True, inplace = True)
    heatmap_df = dir_df.groupby(['HOUR','WIND_DIR_GROUP']).size().reset_index()
    heatmap1_data = pd.pivot_table(heatmap_df, values=0,
                     index=['WIND_DIR_GROUP'],
                     columns='HOUR')
    sns.heatmap(heatmap1_data, cmap="YlGnBu")