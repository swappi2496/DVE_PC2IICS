import pandas as pd
import numpy as np
import datacompy
from database import *
from full import full_comparison
import os
import warnings
warnings.filterwarnings("ignore")
from log import get_logger
from File_DB import flat_file

#Logging
logger = get_logger('APP')



#path = config.get('filepath','inputpath')
#insheet = pd.read_csv(f'{path}')
insheet = pd.read_csv('input_file.csv')
nr = insheet.shape[0]
df_report = pd.DataFrame(columns = ['Mapping', 'Test_CaseId', 'testid', 'Test_Type', 'PC_targettable', 'IICS_targettable', 'Result', 'PC_targettable_count', 'IICS_targettable_count', 'min_test_result', 'max_test_result', 'mean_test_result', 'sum_test_result', 'Full_comp_test_result', 'Info'])
for i in range(0, nr):
    print('=====================================')
    logger.info('=====================================')
    # print(i)

    y = i*2
    mapping = insheet['Mapping_Name'][i]
    tid = insheet['TestCaseID'][i]
    print("Executing TestCaseID - " + tid)
    logger.info("Executing TestCaseID - " + tid)

    source_conn = insheet['PowerCenter_Connection'][i]
    print('------------------------------------')
    logger.info('------------------------------------')

    print(f'Source Connection Type: {source_conn}')
    logger.info(f'Source Connection Type: {source_conn}')

    source_table = insheet['PowerCentre_targettable'][i]
    logger.info('source_table')

    target_conn = insheet['IICS_Connection'][i]
    print('------------------------------------')
    logger.info('------------------------------------')

    print(f'Target Connection Type: {target_conn}')

    logger.info(f'Target Connection Type: {target_conn}')
    print('------------------------------------')

    target_table = insheet['IICS_targettable'][i]
    logger.info('target_table')

    ## new column to be added in input testcase.csv and reading in 25 and 26 line 
    src_pk = insheet['Source_Primary_Column'][i]

    tgt_pk = insheet['Target_Primary_Column'][i]

    count_check =  insheet['Count(Y/N)'][i]

    recon_check = insheet['Data_Recon (Y/N)'][i]

    try:
        if source_table.lower() == 'flatfile':
            src = flat_file(source_conn)
            # src = src.astype(str)
            # print(src)
            # print('SOURCE_DF: ',src.info())
        else:
            sconn = source_connection(source_conn) # co3

            src = pd.read_sql_query(f'select * from {source_table}', sconn)
            sconn.close()
    except Exception as e:
        print('Source Connection Error:\n',e)
    try:
        if target_table.lower() == 'flatfile':
            tgt = flat_file(target_conn)
        else:
            tconn = target_connection(target_conn)
            # print(tconn)    # c04
            tgt = pd.read_sql_query(f'select * from {target_table}', tconn)
            # tgt.sort_values(by = 'ID', inplace= True)
            # tgt.reset_index(
            # tgt = tgt.astype(str)
            tconn.close()
    except Exception as e:
        print('Target Connection Error:\n',e)


    if source_table.lower() == 'flatfile' or target_table.lower() == 'flatfile':
        src = src.astype(str)
        tgt = tgt.astype(str)


    if count_check == 'Y' and recon_check == 'N':
        print('Count Checking....')
        logger.info('Count Checking....')
        add_row = [mapping, tid, str(1), 'Count_Check',source_table,target_table,src.shape[0] == tgt.shape[0],src.shape[0],tgt.shape[0],np.nan,np.nan,np.nan,np.nan,np.nan,np.nan ]
        df_report.loc[y] = add_row
    elif count_check == 'N' and recon_check == 'Y':
        print('Recon_Checking....')
        logger.info('Recon_Checking....')
        afull,amin,amax,amean,asum,acompare,os1,ms = [i for i in full_comparison(tid, src,tgt, source_table, target_table, src_pk, tgt_pk)]
        if not acompare:
            add_row = [mapping, tid, str(1), 'Recon_Check',source_table,target_table,afull,np.nan,np.nan,amin.all(), amax.all(),amean.all(), asum.all(), acompare, f'{os1}\n\n{ms}']
        else:
            add_row = [mapping, tid, str(1), 'Recon_Check',source_table,target_table,afull,np.nan,np.nan,amin.all(), amax.all(),amean.all(), asum.all(), acompare,np.nan]
        df_report.loc[y] = add_row

    elif count_check == 'Y' and recon_check == 'Y':
        print('Checking Count and Recon....')
        logger.info('Checking Count and Recon....')
        add_row = [mapping, tid, str(1), 'Count_Check',source_table,target_table,src.shape[0] == tgt.shape[0],src.shape[0],tgt.shape[0],np.nan,np.nan,np.nan,np.nan,np.nan,np.nan ]
        df_report.loc[y] = add_row
        afull,amin,amax,amean,asum,acompare,os1,ms = [i for i in full_comparison(tid, src,tgt, source_table, target_table, src_pk, tgt_pk)]
        # report = full_comparison(tid, src,tgt, source_table, target_table, src_pk, tgt_pk)[7]
        # print(report)
        if not acompare:
            new_row = [mapping, tid, str(2), 'Recon_Check',source_table,target_table,afull,np.nan,np.nan,amin.all(), amax.all(),amean.all(), asum.all(), acompare, f'{os1}\n\n{ms}']
        else:
            new_row = [mapping, tid, str(2), 'Recon_Check',source_table,target_table,afull,np.nan,np.nan,amin.all(), amax.all(),amean.all(), asum.all(), acompare, np.nan]

        df_report.loc[y+1] = new_row
    else:
        pass
    # except Exception as e:
    #     print('Error in the Count and Reconn Check:\n',e)
    #     logger.error(e)

    print('Exporting the files....')
    logger.info('Exporting the files....')
    df_report.replace({False: 'Fail', True: 'Pass'}, inplace=True)
    df_report.iloc[:9] = df_report.iloc[:9].apply(lambda x: x.replace(1,'Pass').replace(0 , 'Fail'))
    df_report.iloc[:10] = df_report.iloc[:10].apply(lambda x: x.replace(1,'Pass').replace(0 , 'Fail'))
    df_report.iloc[:11] = df_report.iloc[:11].apply(lambda x: x.replace(1,'Pass').replace(0 , 'Fail'))
    df_report.iloc[:12] = df_report.iloc[:12].apply(lambda x: x.replace(1,'Pass').replace(0 , 'Fail'))
    print('=====================================')
    logger.info('=====================================')
    fileDir =os.getcwd()
    if not os.path.exists('output'):
        os.makedirs('output')

    #df_report.to_excel(fileDir+'/output.xlsx', index=False)
    df_report.to_excel(fileDir+'\\output\\output.xlsx', index=False)
    #print(os.getcwd()+'\\'+'/output.xlsx', index=False)


        
        
        
            
        
