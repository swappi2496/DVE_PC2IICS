import pandas as pd
import logging

import datacompy
import numpy as np
import os
from log import get_logger

#Logging
logger = get_logger('FULL')
logging.getLogger('datacompy.core').setLevel(logging.WARNING)



def full_comparison(tid, src,tgt, source_table, target_table,src_pk, tgt_pk):
    # print('Hello')
    amax = src.max(numeric_only= True) == (tgt.max(numeric_only= True))
    # print(src.max(numeric_only=True))
    # print(tgt.max(numeric_only= True))
    amin = src.min(numeric_only= True) == (tgt.min(numeric_only= True))
    asum = src.sum(numeric_only= True) == (tgt.sum(numeric_only= True))
    amean = src.mean(numeric_only= True) == (tgt.mean(numeric_only= True))
    # print('Hello again')
    # src = src.astype(str)
    # tgt = tgt.astype(str)
     # condition for primary and target primary key    
    if src_pk == tgt_pk:
        primary_column = src_pk
    else:  
        primary_column = f'{src_pk}-{tgt_pk}'
        src.columns[0] = primary_column
        tgt.columns[0] = primary_column

    compare = datacompy.Compare(src, tgt, join_columns=primary_column, df1_name=source_table,
                                        df2_name=target_table)
    # print(compare.report())

    acompare = compare.matches(ignore_extra_columns=False)
    mis = compare.all_mismatch()
    source_col = src.columns
    target_col = tgt.columns
    second = [col for col in mis.columns if col.endswith('_df2')]
    first = [col for col in mis.columns if col.endswith('_df1')]
    miss = mis.copy()
    miss.reset_index(inplace=True)
    miss.drop('index', axis=1, inplace=True)

    for c in range(mis.shape[0]):
        for (f, s) in zip(first, second):
            if mis[f].iloc[c] == mis[s].iloc[c]:
                miss.loc[c, f] = np.nan
                miss.loc[c, s] = np.nan
            else:
                pass
    excel_name = []
    excel_df = []
    miss.columns = miss.columns.str.replace("_df1", "_source")
    miss.columns = miss.columns.str.replace("_df2", "_target")
    if miss.shape[0] != 0:
        miss.dropna(axis='columns', how='all', inplace=True)
        excel_df.append(miss)
        excel_name.append('Mismatch')
    else:
        pass
    # print("miss", miss)
    logger.info("Miss-Match....")
    logger.info(miss)
    df = src.merge(tgt, on=primary_column.lower(), how='outer', indicator='join')
    # print("df", df)
    logger.info('DataFrame....')
    logger.info(df)
    if df[df['join'] == 'right_only'].shape[0] == 0:
        right = pd.DataFrame()
    else:
        right = df[df['join'] == 'right_only'].drop('join', axis=1).dropna(axis=1)
        right.columns = target_col
        excel_name.append('Only in Target')
        excel_df.append(right)

    if df[df['join'] == 'left_only'].shape[0] == 0:
        left = pd.DataFrame()
    else:
        left = df[df['join'] == 'left_only'].drop('join', axis=1).dropna(axis=1)
        left.columns = source_col
        excel_name.append('Only in Source')
        excel_df.append(left)
    # print("right",right)
    # print("left", left)
    summary = pd.DataFrame(
        {
            "Table_Name": [source_table, target_table],
            "Rows": [src.shape[0], tgt.shape[0]],
        }
    )
    print(summary)
    logger.info('Summary....')
    logger.info(summary)

    su = summary.to_string(index = False)
    only_summary = pd.DataFrame(
        {
            "Summary": ['Only in Source Table', 'Only in Target Table'],
            "Row_Count": [left.shape[0], right.shape[0]],
        }
    )
    print('===================================')
    # print("only", only_summary)
    logger.info('Only Summary....')
    logger.info(only_summary)
    os1 = only_summary.to_string(index = False)
    stats = [summary, only_summary]
    miss_new = miss.copy()
    miss_new.columns = miss_new.columns.str.replace(r"_source", "")
    miss_new.columns = miss_new.columns.str.replace(r"_target", "")
    # print(miss_new)
    colst = [primary_column]
    joinn = [i.lower() for i in colst]
    if miss_new.shape[0] != 0:
        new_df = miss_new.loc[:, ~miss_new.columns.duplicated()].drop(joinn, axis=1)
        # print("new df", new_df)
        miss_summary = pd.DataFrame(new_df.count(), columns=['Row Count'])
        miss_summary.index.name = 'Mismatch Column'
        miss_summary.reset_index(inplace=True)
        # miss_summary.drop(['index'], axis=1, inplace=True)
        stats.append(miss_summary)
    else:
        miss_summary = pd.DataFrame(
            {
                "Mismatch": ['None'],
                "Row_Count": [0],
            }
        )
        stats.append(miss_summary)
    # print(miss_summary)
    logger.info('Miss-Match Summary....')
    logger.info(miss_summary)
    ms = miss_summary.to_string(index = False)
    if not os.path.exists('output'):
        os.makedirs('output')    
    fileDir = os.getcwd()

    def multiple_dfs(df_list, sheets, file_name, spaces):
        # print('startstst')
        writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
        row = 0
        for dataframe in df_list:
            # print(dataframe)
            dataframe.to_excel(writer, sheet_name=sheets, startrow=row, startcol=0, index=False)
            row = row + len(dataframe.index) + spaces + 1
            # print(row)
        # print('Suc')
        writer.save()

    multiple_dfs(stats, 'Summary', fileDir + '\\output\\' + str(tid) + 'Detail_Report_for_Full_Check' + '.xlsx',3)

    with pd.ExcelWriter(fileDir + '\\output\\' + str(tid) + 'Detail_Report_for_Full_Check' + '.xlsx',
                        mode='a', engine='openpyxl',if_sheet_exists='new') as writer:
        print('Creating Report.....')
        logger.info('Creating Report.....')

        for df, df_name in zip(excel_df, excel_name):
            df.to_excel(writer, sheet_name=df_name, index=False)
        print('Success>>>>>>')
        logger.info('Success>>>>>>')
    afull = amin.all() == amax.all()==amean.all()==asum.all()==acompare == True
    
    return afull, amin, amax, amean, asum, acompare, os1, ms