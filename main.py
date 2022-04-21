import jaydebeapi
import pandas as pd
import os
from sql_scripts import init_tables as init
from sql_scripts import load_data as load
from sql_scripts import hist_data as hist
from sql_scripts import fraud_report as fraud


connect = jaydebeapi.connect(
    'oracle.jdbc.driver.OracleDriver',
    'jdbc:oracle:thin:link',
    ['login', 'pass'],
    'ojdbc7.jar')
cursor = connect.cursor()


path = './'
path_new = 'archive/'
blklst_files = []
term_files = []
trans_files = []
file_list = sorted(os.listdir(path))
print(file_list)
for item in file_list:
    if item[0:8] == 'passport':
        blklst_files.append(item)
    if item[0:9] == 'terminals':
        term_files.append(item)
    if item[0:12] == 'transactions':
        trans_files.append(item)


def ETL_process():
    init.create_STG_tables()
    init.create_META_tables()
    # Загружаем файлы в таблицы. Переменовываем файлы и перемещаем в архив.
    for i in range(len(trans_files)):
        load.data_to_STG(os.path.join(path, trans_files[i]), os.path.join(
            path, term_files[i]), os.path.join(path, blklst_files[i]))
        os.renames(os.path.join(path, blklst_files[i]), os.path.join(
            path_new, blklst_files[i])+'.backup')
        os.renames(os.path.join(path, term_files[i]), os.path.join(
            path_new, term_files[i])+'.backup')
        os.renames(os.path.join(path, trans_files[i]), os.path.join(
            path_new, trans_files[i])+'.backup')
    init.create_FACT_tables()
    load.STG_to_FACT()
    init.create_HIST_tables()
    hist.new_FACT()
    hist.deleted_FACT()
    hist.updated_FACT()
    hist.update_tables()
    init.create_REP_fraud()
    fraud.find_fraud_trans()


ETL_process()
