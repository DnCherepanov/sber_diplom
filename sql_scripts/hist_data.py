import jaydebeapi
import pandas as pd

connect = jaydebeapi.connect(
    'oracle.jdbc.driver.OracleDriver',
    'jdbc:oracle:thin:link',
    ['login', 'pass'],
    'ojdbc7.jar')
cursor = connect.cursor()


def new_FACT():
    '''
    Новые записи для исторических таблиц
    '''
    try:
        cursor.execute('DROP VIEW de3hd.s_29_v_new_rows_term')
        print('# Представление "de3hd.s_29_v_new_rows_term" удалено')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_new_rows_term', e)

    try:
        cursor.execute('DROP VIEW de3hd.s_29_v_new_rows_blklst')
        print('# Представление "de3hd.s_29_v_new_rows_blklst" удалено')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_new_rows_blklst', e)

    try:
        cursor.execute('''CREATE VIEW de3hd.s_29_v_new_rows_term as
        SELECT
            t1.terminal_id,
            t1.terminal_type,
            t1.terminal_city,
            t1.terminal_address
        FROM de3hd.s_29_DWH_FACT_term t1
        LEFT JOIN de3hd.s_29_DWH_DIM_term_HIST t2
        on t1.terminal_id = t2.terminal_id
        where t2.terminal_id is null
    ''')
        print('# Представление "de3hd.s_29_v_new_rows_term" создано')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_new_rows_blklst', e)

    try:
        cursor.execute(''' CREATE VIEW de3hd.s_29_v_new_rows_blklst as
        SELECT
            t1.passport_date,
            t1.passport
        FROM de3hd.s_29_DWH_FACT_blklst t1
        LEFT JOIN de3hd.s_29_v_blklst_HIST t2
        ON t1.passport = t2.passport
        WHERE t2.passport is null
    ''')
        print('# Представление "de3hd.s_29_v_new_rows_blklst" создано')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_new_rows_blklst', e)


def deleted_FACT():
    '''
    Удаленные записи для исторических таблиц
    '''

    try:
        cursor.execute('DROP VIEW de3hd.s_29_v_dltd_rows_term')
        print('# Представление "de3hd.s_29_v_dltd_rows_term" удалено')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_dltd_rows_term', e)

    try:
        cursor.execute('DROP VIEW de3hd.s_29_v_dltd_rows_blklst')
        print('# Представление "de3hd.s_29_v_dltd_rows_blklst" удалено')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_dltd_rows_blklst', e)

    try:
        cursor.execute('''CREATE VIEW de3hd.s_29_v_dltd_rows_term as
        SELECT
            t1.terminal_id,
            t1.terminal_type,
            t1.terminal_city,
            t1.terminal_address
        FROM de3hd.s_29_v_term_HIST t1
        LEFT JOIN de3hd.s_29_DWH_DIM_term_HIST t2
        on t1.terminal_id = t2.terminal_id
        where t2.terminal_id is null
    ''')
        print('# Представление "de3hd.s_29_v_dltd_rows_term" создано')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_dltd_rows_term', e)

    try:
        cursor.execute(''' CREATE VIEW de3hd.s_29_v_dltd_rows_blklst as
        SELECT
            t1.passport_date,
            t1.passport
        FROM de3hd.s_29_v_blklst_HIST t1
        LEFT JOIN de3hd.s_29_v_blklst_HIST t2
        ON t1.passport = t2.passport
        WHERE t2.passport is null
    ''')
        print('# Представление "de3hd.s_29_v_dltd_rows_blklst" создано')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_dltd_rows_blklst', e)


def updated_FACT():
    '''
    Обновленные записи для исторических таблиц
    '''

    try:
        cursor.execute('DROP VIEW de3hd.s_29_v_upd_rows_term')
        print('# Представление "de3hd.s_29_v_upd_rows_term" удалено')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_upd_rows_term', e)

    try:
        cursor.execute('DROP VIEW de3hd.s_29_v_upd_rows_blklst')
        print('# Представление "de3hd.s_29_v_upd_rows_blklst" удалено')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_upd_rows_blklst', e)

    try:
        cursor.execute('''
        CREATE VIEW de3hd.s_29_v_upd_rows_term as
        SELECT
            t1.terminal_id,
            t1.terminal_type,
            t1.terminal_city,
            t1.terminal_address
        FROM de3hd.s_29_DWH_FACT_term t1
        INNER JOIN de3hd.s_29_v_term_HIST t2
        ON t1.terminal_id = t2.terminal_id AND
        (t1.terminal_type <> t2.terminal_type OR
		t1.terminal_city <> t2.terminal_city OR
		t1.terminal_address <> t2.terminal_address)
    ''')
        print('# Представление "de3hd.s_29_v_upd_rows_term" создано')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_upd_rows_term', e)

    try:
        cursor.execute('''
        CREATE VIEW de3hd.s_29_v_upd_rows_blklst AS
        SELECT
            t1.passport_date,
            t1.passport
        FROM de3hd.s_29_DWH_FACT_blklst t1
        INNER JOIN de3hd.s_29_v_blklst_HIST t2
        ON t1.passport = t2.passport AND
        (t1.passport_date <> t2.passport_date)
    ''')
        print('# Представление "de3hd.s_29_v_upd_rows_blklst" создано')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_v_upd_rows_blklst', e)


def update_tables():
    '''
    Вносим данные в исторические таблицы терминалов и черного списка паспортов
    '''
    # ПАСПОРТА
    cursor.execute('''
		UPDATE de3hd.s_29_DWH_DIM_blklst_HIST
		SET effective_to = SYSTIMESTAMP - NUMTODSINTERVAL(1, 'SECOND')
		where passport in (select passport from de3hd.s_29_v_upd_rows_blklst)
		and effective_to = timestamp '2999-12-31 23:59:59'
	''')
    cursor.execute('''
		UPDATE de3hd.s_29_DWH_DIM_blklst_HIST
		SET effective_to = SYSTIMESTAMP - NUMTODSINTERVAL(1, 'SECOND')
		where passport in (select passport from de3hd.s_29_v_dltd_rows_blklst)
		and effective_to = timestamp '2999-12-31 23:59:59'
	''')
    cursor.execute('''
		INSERT INTO de3hd.s_29_DWH_DIM_blklst_HIST (passport_date, passport)
		SELECT passport_date, passport
		FROM de3hd.s_29_v_new_rows_blklst
	''')
    cursor.execute('''
		INSERT INTO de3hd.s_29_DWH_DIM_blklst_HIST (passport_date, passport)
		SELECT passport_date, passport
		FROM de3hd.s_29_v_upd_rows_blklst
	''')
    cursor.execute('''
		INSERT INTO de3hd.s_29_DWH_DIM_blklst_HIST (passport_date, passport, deleted_flg)
		SELECT passport_date, passport, 1
		FROM de3hd.s_29_v_dltd_rows_blklst
	''')

    # ТЕРМИНАЛЫ
    cursor.execute('''
		UPDATE de3hd.s_29_DWH_DIM_term_HIST
		SET effective_to = SYSTIMESTAMP - NUMTODSINTERVAL(1, 'SECOND')
		where terminal_id in (select terminal_id from de3hd.s_29_v_upd_rows_term)
		and effective_to = timestamp '2999-12-31 23:59:59'
	''')
    cursor.execute('''
		UPDATE de3hd.s_29_DWH_DIM_term_HIST
		SET effective_to = SYSTIMESTAMP - NUMTODSINTERVAL(1, 'SECOND')
		where terminal_id in (select terminal_id from de3hd.s_29_v_dltd_rows_term)
		and effective_to = timestamp '2999-12-31 23:59:59'
	''')
    cursor.execute('''
		INSERT INTO de3hd.s_29_DWH_DIM_term_HIST (terminal_id, terminal_type, terminal_city, terminal_address)
		SELECT terminal_id, terminal_type, terminal_city, terminal_address
		FROM de3hd.s_29_v_new_rows_term
	''')
    cursor.execute('''
		INSERT INTO de3hd.s_29_DWH_DIM_term_HIST (terminal_id, terminal_type, terminal_city, terminal_address)
		SELECT terminal_id, terminal_type, terminal_city, terminal_address
		FROM de3hd.s_29_v_upd_rows_term
	''')
    cursor.execute('''
		INSERT INTO de3hd.s_29_DWH_DIM_term_HIST (terminal_id, terminal_type, terminal_city, terminal_address, deleted_flg)
		SELECT terminal_id, terminal_type, terminal_city, terminal_address, 1
		FROM de3hd.s_29_v_dltd_rows_term
	''')
    print('>>> Историческая таблица обновлена')
