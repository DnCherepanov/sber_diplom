import jaydebeapi
import pandas as pd


connect = jaydebeapi.connect(
    'oracle.jdbc.driver.OracleDriver',
    'jdbc:oracle:thin:link',
    ['login', 'pass'],
    'ojdbc7.jar')
cursor = connect.cursor()


def find_fraud_trans():
    # Создаем таблицу для сохранения найденных машеннических операций
    try:
        cursor.execute('''
			CREATE TABLE de3hd.s_29_TMP_REP_FRAUD(
				event_dt timestamp,
				passport varchar(15),
				fio varchar(60),
				phone varchar(20),
				event_type varchar(80),
				report_dt timestamp default SYSDATE
			)
		''')
        print('# Таблица "de3hd.s_29_TMP_REP_FRAUD" успешно создана')
    except jaydebeapi.DatabaseError as e:
        print('de3hd.s_29_TMP_REP_FRAUD', e)

    # Ищем операции совершенные с заблокированными и просроченными паспортами
    cursor.execute('''
		INSERT INTO de3hd.s_29_TMP_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
		SELECT
			s1.fr_date,
			s1.fr_passp,
			bank.clients.last_name||' '||bank.clients.first_name||' '||bank.clients.patronymic,
			bank.clients.phone,
			'ПРОСРОЧЕН ИЛИ ЗАБЛОКИРОВАН ПАСПОРТ',
			systimestamp
		FROM bank.clients
		inner join (
			SELECT
				min(hi.transaction_date) fr_date,
				bcl.passport_num fr_passp
			FROM de3hd.s_29_DWH_FACT_trans hi
			inner join bank.cards bc
			on hi.card_num = TRIM(TRAILING ' ' FROM bc.card_num)
			inner join bank.accounts ba
			on  bc.account = ba.account
			inner join bank.clients bcl
			on ba.client = bcl.client_id and bcl.passport_num in (
				SELECT 
                    passport_num
				FROM bank.clients
				WHERE passport_valid_to < systimestamp
				UNION
				SELECT 
                    passport
				FROM de3hd.s_29_DWH_DIM_blklst_HIST
				)
			group by bcl.passport_num
			) s1
		on bank.clients.passport_num = s1.fr_passp
	''')

    # Ищем операции совершенные с недействующим договором
    cursor.execute('''
		INSERT INTO de3hd.s_29_TMP_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
		SELECT
			s1.fr_date,
			s1.fr_passp,
			bank.clients.last_name||' '||bank.clients.first_name||' '||bank.clients.patronymic,
			bank.clients.phone,
			'НЕДЕЙСТВУЮЩИЙ ДОГОВОР',
			systimestamp
		FROM bank.clients
		inner join (
			SELECT
				min(hi.transaction_date) fr_date,
				bcl.passport_num fr_passp
			FROM de3hd.s_29_DWH_FACT_trans hi
			inner join bank.cards bc
			on hi.card_num = TRIM(TRAILING ' ' FROM bc.card_num)
			inner join bank.accounts ba
			on ba.account = bc.account and ba.valid_to < sysdate
			inner join bank.clients bcl
			on ba.client = bcl.client_id
			group by bcl.passport_num
			) s1
		on bank.clients.passport_num = s1.fr_passp
	''')

    # Ищем операции cовершенные в разных городах в течение одного часа
    cursor.execute('''
		INSERT INTO de3hd.s_29_TMP_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
		SELECT
			s3.fr_date,
			bcl.passport_num,
			bcl.last_name||' '||bcl.first_name||' '||bcl.patronymic,
			bcl.phone,
			'ОПЕРАЦИИ В РАЗНЫХ ГОРОДАХ В ТЕЧЕНИЕ ЧАСА',
			systimestamp
		FROM bank.clients bcl
		inner join bank.accounts ba
		on ba.client = bcl.client_id
		inner join bank.cards bc
		on ba.account = bc.account
		inner join (
			SELECT
				distinct s2.cnum fr_card,
				FIRST_VALUE(s2.tdate) OVER(PARTITION BY s2.cnum ORDER BY s2.tdate) AS fr_date
			FROM
				(SELECT
					tr.card_num cnum,
					tr.transaction_date tdate,
					teh.terminal_city city,
					LAG(tr.transaction_date) OVER(PARTITION BY tr.card_num ORDER BY tr.transaction_date) AS prv_dttm,
					LAG(teh.terminal_city) OVER(PARTITION BY tr.card_num ORDER BY tr.transaction_date) AS prv_city
				FROM de3hd.s_29_DWH_FACT_trans tr
				inner join (
					SELECT
						hi.card_num cnum,
						count(distinct ter.terminal_city)
					FROM de3hd.s_29_DWH_DIM_term_HIST ter
					inner join de3hd.s_29_DWH_FACT_trans hi
					on ter.terminal_id = hi.terminal
					group by hi.card_num
					having count(distinct ter.terminal_city) > 1
					) s1
				on tr.card_num = s1.cnum
				inner join de3hd.s_29_DWH_DIM_term_HIST teh
				on tr.terminal = teh.terminal_id
				order by tr.card_num, tr.transaction_date, teh.terminal_city
				) s2
			WHERE s2.city <> s2.prv_city and (s2.prv_dttm + NUMTODSINTERVAL(1,'HOUR')) > s2.tdate
			) s3
		on s3.fr_card = TRIM(TRAILING ' ' FROM bc.card_num)
	''')

    # Ищем операции cовершенные в течение 20 минут с подбором суммы
    cursor.execute('''
		INSERT INTO de3hd.s_29_TMP_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
		SELECT
			s3.fr_date,
			bcl.passport_num,
			bcl.last_name||' '||bcl.first_name||' '||bcl.patronymic,
			bcl.phone,
			'ПОДБОР СУММЫ',
			systimestamp
		FROM bank.clients bcl
		inner join bank.accounts ba
		on ba.client = bcl.client_id
		inner join bank.cards bc
		on ba.account = bc.account
		inner join
			(select
			    s2.cn fr_card,
			    s2.td fr_date
			FROM
				(select
				    s1.card_num cn,
				    s1.transaction_date td,
				    s1.amount am1,
				    s1.transaction_id,
				    s1.OPER_RESULT or1,
				    LAG(s1.amount,1) OVER(partition by card_num order by s1.transaction_id) am2,
				    LAG(s1.amount,2) OVER(partition by card_num order by s1.transaction_id) am3,
				    LAG(s1.amount,3) OVER(partition by card_num order by s1.transaction_id) am4,
				    LAG(s1.OPER_RESULT,1) OVER(partition by card_num order by s1.transaction_id) or2,
				    LAG(s1.OPER_RESULT,2) OVER(partition by card_num order by s1.transaction_id) or3,
				    LAG(s1.OPER_RESULT,3) OVER(partition by card_num order by s1.transaction_id) or4,
				    LAG(s1.transaction_date,3) OVER(partition by card_num order by s1.transaction_id) td4
				FROM
					(select card_num,transaction_date, amount, transaction_id, OPER_RESULT
					FROM s_29_DWH_FACT_TRANS
					order by card_num, transaction_date
					) s1
				) s2
			where s2.am1 < s2.am2 and s2.am2 < s2.am3 and s2.am3 < s2.am4 and s2.or1 = 'SUCCESS'
			and s2.or2 = 'REJECT' and s2.or3 = 'REJECT' and s2.or4 = 'REJECT'
			and (s2.td4 + NUMTODSINTERVAL(20,'MINUTE')) > s2.td
			) s3
		on s3.fr_card = TRIM(TRAILING ' ' FROM bc.card_num)
	''')

    # Добавляем строки отчета, полученные за день, в таблицу отчетов
    cursor.execute('''
		INSERT INTO de3hd.s_29_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
		SELECT
			t1.event_dt,
			t1.passport,
			t1.fio,
			t1.phone,
			t1.event_type,
			t1.report_dt
		FROM de3hd.s_29_TMP_REP_FRAUD t1
		left join de3hd.s_29_REP_FRAUD t2
		on t1.event_dt = t2.event_dt and t1.passport = t2.passport and t1.event_type = t2.event_type
		WHERE t2.event_dt is NULL
	''')

    print('>>> Витрина отчетности по мошенническим операциям построена')

    # Сбрасываем временную таблицу обнаруженных операций
    cursor.execute('DROP TABLE de3hd.s_29_TMP_REP_FRAUD')
