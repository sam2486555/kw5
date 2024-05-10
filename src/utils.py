import psycopg2
from classes import HHGetData


def create_database(params):
    """
    Создаем базу данных coursework5
    """
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""DROP DATABASE IF EXISTS coursework5""")
    cur.execute("""CREATE DATABASE coursework5""")

    cur.close()
    conn.close()


def creating_tables(params):
    conn = psycopg2.connect(dbname='coursework5', **params)
    with conn.cursor() as cur:
        """
        Создаем таблицы employers и vacancies
        """
        cur.execute("""
                    CREATE TABLE employers (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        url VARCHAR(50)
                    )
                """)

        cur.execute("""
                    CREATE TABLE vacancies (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        salary INTEGER,
                        currency VARCHAR(30),
                        url VARCHAR(50),
                        employer_id INTEGER REFERENCES employers(id) 
                    )
                """)

    conn.commit()
    conn.close()


def data_entry(params, emp_id):
    hh = HHGetData(emp_id)
    employers = hh.get_employers_list()
    vacancies = hh.filter_salary()
    conn = psycopg2.connect(dbname='coursework5', **params)
    with conn:
        with conn.cursor() as cur:
            for employer in employers:
                """ 
                Заполняем созданные таблицы
                """
                cur.execute("""
                                INSERT INTO employers VALUES (%s, %s, %s)
                            """, (employer["id"], employer["name"], employer["url"]))
            for vacancy in vacancies:
                cur.execute("""INSERT INTO vacancies VALUES (%s, %s, %s, %s, %s, %s)
                                    """, (vacancy["id"], vacancy["name"],
                                          vacancy["salary"], vacancy["currency"], vacancy["url"],
                                          vacancy["employer"]))

    conn.commit()
    conn.close()
