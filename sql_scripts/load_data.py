import jaydebeapi
import pandas as pd

connect = jaydebeapi.connect(
    'oracle.jdbc.driver.OracleDriver',
    'jdbc:oracle:thin:link',
    ['login', 'pass'],
    'ojdbc7.jar')
cursor = connect.cursor()


def data_to_STG(file_trans, file_term, file_blklst):
    '''
    Загружаем данные с файлов в таблицы STAGE
    '''
    try:
        df = pd.read_csv(file_trans, sep=';')
        cursor.executemany('''INSERT INTO de3hd.s_29_STG_trans
            VALUES(?,?,?,?,?,?,?)''', df.values.tolist())
        cursor.execute('''INSERT INTO de3hd.s_29_META_UPL (
             upload_path,
            status
            ) VALUES(?,'Accepted')
        ''', [file_trans])
    except:
        cursor.execute('''
                INSERT INTO de3hd.s_29_META_UPL (
                    upload_path,
                    status
                    ) VALUES(?,'Rejected')
            ''', [file_trans])

    try:
        df = pd.read_excel(file_term)
        cursor.executemany('''
            INSERT INTO de3hd.s_29_STG_term
            VALUES(?,?,?,?)''', df.values.tolist())
        cursor.execute('''
            INSERT INTO de3hd.s_29_META_UPL (
                upload_path,
                status
            ) VALUES(?,'Accepted')
        ''', [file_term])
    except:
        cursor.execute('''
            INSERT INTO de3hd.s_29_META_UPL (
                upload_path,
                status
            ) VALUES(?,'Rejected')
        ''', [file_term])

    try:
        df = pd.read_excel(file_blklst, dtype={'date': str})
        cursor.executemany('''
            INSERT INTO de3hd.s_29_STG_blklst
            VALUES(?,?)''', df.values.tolist())
        cursor.execute('''
            INSERT INTO de3hd.s_29_META_UPL (
                upload_path,
                status
            ) VALUES(?,'Accepted')
        ''', [file_blklst])
    except:
        cursor.execute('''
            INSERT INTO de3hd.s_29_META_UPL (
                upload_path,
                status
            ) VALUES(?,'Rejected')
        ''', [file_blklst])


def STG_to_FACT():
    '''
    Перекидываем STG в FACT и преобразуем дату в нужный timestamp
    '''
    cursor.execute('''INSERT INTO de3hd.s_29_DWH_FACT_trans
        SELECT transaction_id, TO_TIMESTAMP(transaction_date, 'YYYY-MM-DD HH24:MI:SS'), amount, card_num, oper_type, oper_result, terminal
        FROM de3hd.s_29_STG_trans
    ''')
    cursor.execute('''INSERT INTO de3hd.s_29_DWH_FACT_blklst
        SELECT TO_TIMESTAMP(passport_date, 'YYYY-MM-DD HH24:MI:SS'), passport
        FROM de3hd.s_29_STG_blklst
    ''')
    cursor.execute('''INSERT INTO de3hd.s_29_DWH_FACT_term
        SELECT terminal_id, terminal_type, terminal_city, terminal_address
        FROM de3hd.s_29_STG_term
    ''')
