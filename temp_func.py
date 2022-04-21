import jaydebeapi
import pandas as pd


connect = jaydebeapi.connect(
    'oracle.jdbc.driver.OracleDriver',
    'jdbc:oracle:thin:link',
    ['login', 'pass'],
    'ojdbc7.jar')
cursor = connect.cursor()

# Тут я находил размер строк
# def line_size(source):
#     list = ['terminal_id', 'terminal_type',
#             'terminal_city', 'terminal_address']
#     for i in list:
#         cursor.execute(f'select max(LENGTHB({i})) from {source}')
#         for row in cursor.fetchall():
#             print(row)


# show_data('s_29_STG_term')

def drop_tables():
    cursor.execute('DROP TABLE de3hd.s_29_STG_trans')
    cursor.execute('DROP TABLE de3hd.s_29_STG_term')
    cursor.execute('DROP TABLE de3hd.s_29_STG_blklst')

    cursor.execute('DROP TABLE de3hd.s_29_DWH_FACT_trans')
    cursor.execute('DROP TABLE de3hd.s_29_DWH_FACT_term')
    cursor.execute('DROP TABLE de3hd.s_29_DWH_FACT_blklst')

    cursor.execute('DROP TABLE de3hd.s_29_DWH_DIM_term_HIST')
    cursor.execute('DROP TABLE de3hd.s_29_DWH_DIM_blklst_HIST')

    cursor.execute('DROP VIEW de3hd.s_29_v_term_HIST')
    cursor.execute('DROP VIEW de3hd.s_29_v_blklst_HIST')

    cursor.execute('DROP TABLE de3hd.s_29_META_UPL')
    cursor.execute('DROP TABLE de3hd.s_29_REP_FRAUD')

    cursor.execute('DROP VIEW de3hd.s_29_v_new_rows_term')
    cursor.execute('DROP VIEW de3hd.s_29_v_new_rows_blklst')
    cursor.execute('DROP VIEW de3hd.s_29_v_dltd_rows_term')
    cursor.execute('DROP VIEW de3hd.s_29_v_dltd_rows_blklst')
    cursor.execute('DROP VIEW de3hd.s_29_v_upd_rows_term')
    cursor.execute('DROP VIEW de3hd.s_29_v_upd_rows_blklst')
    cursor.execute('DROP TABLE de3hd.s_29_TMP_REP_FRAUD')


# init.create_STAGE_tables()
# load.data_to_STG('transactions_01032021.txt',
#                  'terminals_01032021.xlsx',
#                  'passport_blacklist_01032021.xlsx')
# init.create_FACT_tables()
# load.STG_to_FACT()
# init.create_HIST_tables()
# init.create_meta_tables()
# init.create_rep_fraud()


drop_tables()

# cursor.execute('''
#    SELECT
# 				min(hi.transaction_date) fr_date,
# 				bcl.passport_num fr_passp
# 			FROM de3hd.s_29_DWH_FACT_trans hi
# 			inner join bank.cards bc
# 			on hi.card_num = TRIM(TRAILING ' ' FROM bc.card_num)
# 			inner join bank.accounts ba
# 			on  bc.account = ba.account
# 			inner join bank.clients bcl
# 			on ba.client = bcl.client_id and bcl.passport_num in (
# 				SELECT
#                     passport_num
# 				FROM bank.clients
# 				WHERE passport_valid_to < systimestamp
# 				UNION
# 				SELECT
#                     passport
# 				FROM de3hd.s_29_DWH_DIM_blklst_HIST
# 				)
# 			group by bcl.passport_num
# ''')
# for row in cursor.fetchall():
#     print(row)

# for i in cursor.description:
#     print(i[0])
# print('_-'*40)
