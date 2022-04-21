import jaydebeapi
import pandas as pd

connect = jaydebeapi.connect(
    'oracle.jdbc.driver.OracleDriver',
    'jdbc:oracle:thin:link',
    ['login', 'pass'],
    'ojdbc7.jar')
cursor = connect.cursor()


def create_STG_tables():
    '''
    Создаем STAGE таблицы для первоночальной загрузки файлов.
    Перед загрузкой делаем удаление.
    '''

    # Список транзакций за текущий день.
    try:
        cursor.execute('DROP TABLE de3hd.s_29_STG_trans')
        print('# Таблица "de3hd.s_29_STG_trans" удалена')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_STG_trans', e)
    try:
        cursor.execute('''
            CREATE TABLE de3hd.s_29_stg_trans(
                transaction_id varchar(15),
				transaction_date varchar(25),
				amount number(10, 2),
				card_num varchar(20),
				oper_type varchar(20),
				oper_result varchar(20),
				terminal varchar(15)
            )
        ''')
        print('# Таблица "de3hd.s_29_STG_trans" создана')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_STG_trans', e)

    # Список терминалов полным срезом
    try:
        cursor.execute('DROP TABLE de3hd.s_29_STG_term')
        print('# Таблица "de3hd.s_29_STG_term" удалена')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_STG_term', e)
    try:
        cursor.execute('''
            CREATE TABLE de3hd.s_29_STG_term(
                terminal_id varchar(10),
				terminal_type varchar(5),
				terminal_city varchar(40),
				terminal_address varchar(80)
            )
        ''')
        print('# Таблица "de3hd.s_29_STG_term" создана')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_STG_term', e)

    # Список паспортов, включенных в «черный список»
    try:
        cursor.execute('DROP TABLE de3hd.s_29_STG_blklst')
        print('# Таблица "de3hd.s_29_STG_blklst" удалена')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_STG_blklst', e)
    try:
        cursor.execute('''
            CREATE TABLE de3hd.s_29_stg_blklst(
                passport_date varchar(20),
				passport varchar(15)
            )
        ''')
        print('# Таблица "de3hd.s_29_STG_blklst" создана')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_STG_blklst', e)


def create_FACT_tables():
    '''
    Создаем FACT таблицы для инкримента и преобразований.
    '''

    # Список транзакций за текущий день.
    try:
        cursor.execute('DROP TABLE de3hd.s_29_DWH_FACT_trans')
        print('# Таблица "de3hd.s_29_DWH_FACT_trans" удалена')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_DWH_FACT_trans', e)
    try:
        cursor.execute('''
            CREATE TABLE de3hd.s_29_dwh_fact_trans(
                transaction_id varchar(15),
				transaction_date timestamp,
				amount number(10, 2),
				card_num varchar(20),
				oper_type varchar(20),
				oper_result varchar(20),
				terminal varchar(15)
            )
        ''')
        print('# Таблица "de3hd.s_29_DWH_FACT_trans" создана')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_DWH_FACT_trans', e)

    # Список терминалов полным срезом
    try:
        cursor.execute('DROP TABLE de3hd.s_29_DWH_FACT_term')
        print('# Таблица "de3hd.s_29_DWH_FACT_term" удалена')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_DWH_FACT_term', e)
    try:
        cursor.execute('''
            CREATE TABLE de3hd.s_29_DWH_FACT_term(
                terminal_id varchar(10),
				terminal_type varchar(5),
				terminal_city varchar(40),
				terminal_address varchar(80)
            )
        ''')
        print('# Таблица "de3hd.s_29_DWH_FACT_term" создана')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_DWH_FACT_term', e)

    # Список паспортов, включенных в «черный список»
    try:
        cursor.execute('DROP TABLE de3hd.s_29_DWH_FACT_blklst')
        print('# Таблица "de3hd.s_29_DWH_FACT_blklst" удалена')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_DWH_FACT_blklst', e)
    try:
        cursor.execute('''
            CREATE TABLE de3hd.s_29_DWH_fact_blklst(
                passport_date timestamp,
				passport varchar(15)
            )
        ''')
        print('# Таблица "de3hd.s_29_DWH_FACT_blklst" создана')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_DWH_FACT_blklst', e)


def create_HIST_tables():
    '''
    Создаем исторических таблицы для терминалов и черного списка паспортов.
    Создаем представления для актуальных записей.
    '''

    # Создаем таблицу и представление для терминалов
    try:
        cursor.execute('''
            CREATE TABLE de3hd.s_29_DWH_DIM_term_HIST(
                terminal_id varchar(10),
				terminal_type varchar(5),
				terminal_city varchar(40),
				terminal_address varchar(80),
                deleted_flg integer default 0,
				effective_from timestamp default SYSDATE,
				effective_to timestamp default timestamp '2999-12-31 23:59:59'
            )
        ''')
        print('# Таблица "de3hd.s_29_DWH_DIM_term_HIST" создана')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_DWH_DIM_term_HIST', e)

    try:
        cursor.execute('''
            CREATE view de3hd.s_29_v_term_HIST as
            SELECT
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address
            FROM de3hd.s_29_DWH_DIM_term_HIST
            WHERE current_timestamp BETWEEN effective_from AND effective_to AND deleted_flg = 0
        ''')
        print('# Представление "de3hd.s_29_v_term_HIST" создано')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_term_HIST', e)

    # Создаем таблицу и представление для черного списка паспортов
    try:
        cursor.execute('''
            CREATE TABLE de3hd.s_29_DWH_DIM_blklst_HIST(
                passport_date timestamp,
                passport varchar(15),
                deleted_flg integer default 0,
                effective_from timestamp default SYSDATE,
                effective_to timestamp default timestamp '2999-12-31 23:59:59'
            )
        ''')
        print('# Таблица "de3hd.s_29_DWH_DIM_blklst_HIST" создана')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_DWH_DIM_blklst_HIST', e)

    try:
        cursor.execute('''
            CREATE view de3hd.s_29_v_blklst_HIST as
            SELECT
                passport_date,
                passport
            FROM de3hd.s_29_DWH_DIM_blklst_HIST
            WHERE current_timestamp BETWEEN effective_from AND effective_to AND deleted_flg = 0             
        ''')
        print('# Представление "de3hd.s_29_v_blklst_HIST" создано')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_blklst_HIST', e)


def create_META_tables():
    '''
    Создаем таблицу для ведения лога по отработке загрузки файлов
    '''
    try:
        cursor.execute('''
            CREATE TABLE de3hd.s_29_META_UPL(
                upload_date timestamp default SYSDATE,
                upload_path varchar(128),
                status varchar(128)
            )
        ''')
        print('# Таблица "de3hd.s_29_META_UPL" создана')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_META_UPL', e)


def create_REP_fraud():
    '''
    Создаем таблицу с отчетом
    '''
    try:
        cursor.execute('''
            CREATE TABLE de3hd.s_29_REP_FRAUD(
                event_dt timestamp,
				passport varchar(15),
				fio varchar(60),
				phone varchar(20),
				event_type varchar(80),
				report_dt timestamp default SYSDATE
            )
        ''')
        print('# Таблица "de3hd.s_29_REP_FRAUD" создана')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_REP_FRAUD', e)
