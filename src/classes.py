import psycopg2
import requests

class DBManager:
    def get_companies_and_vacancies_count(self, params):
        """
        Получает список всех компаний и количество вакансий у каждой из них
        """
        conn = psycopg2.connect(dbname='coursework5', **params)
        with conn:
            with conn.cursor() as cur:
                cur.execute('''SELECT employers.name, COUNT(vacancies.employer_id) AS vacancies_count 
                FROM employers 
                LEFT JOIN vacancies ON employers.id = vacancies.employer_id 
                GROUP BY employers.name''')
                companies_and_vacs_count = cur.fetchall()
                return companies_and_vacs_count
        conn.close()

    def get_all_vacancies(self, params):
        """
        Получаем список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию
        """
        conn = psycopg2.connect(dbname='coursework5', **params)
        with conn:
            with conn.cursor() as cur:
                cur.execute('''SELECT employers.name, vacancies.name, vacancies.salary, vacancies.url 
                FROM employers JOIN vacancies ON employers.id = vacancies.employer_id ''')
                all_vacancies = cur.fetchall()
                return all_vacancies
        conn.close()

    def get_avg_salary(self, params):
        """
        Получает среднюю зарплату по вакансиям
        """
        conn = psycopg2.connect(dbname='coursework5', **params)
        with conn:
            with conn.cursor() as cur:
                cur.execute('''SELECT ROUND(AVG(salary)) AS salary_avg FROM vacancies''')
                avg_salary = cur.fetchone()[0]
                return avg_salary
        conn.close()

    def get_vacancies_with_higher_salary(self, params):
        """
        Получает список всех вакансий, зарплата которых выше средней по всем вакансиям
        """
        conn = psycopg2.connect(dbname='coursework5', **params)
        with conn:
            with conn.cursor() as cur:
                cur.execute('''SELECT * FROM vacancies WHERE salary > (select AVG(salary) 
                            FROM vacancies)''')
                vacancies_with_higher_salary = cur.fetchall()
                return vacancies_with_higher_salary
        conn.close()

    def get_vacancies_with_keyword(self, keyword, params):
        """
        Получает список всех вакансий, в названии которых содержатся переданные в метод слова
        """
        conn = psycopg2.connect(dbname='coursework5', **params)
        with conn:
            with conn.cursor() as cur:
                cur.execute(f'''SELECT * FROM vacancies WHERE name ILIKE '%{keyword}%' ''')
                vacancies_with_keyword = cur.fetchall()
                return vacancies_with_keyword

        conn.close()


class HHGetData():
    """
    Создаем класс для подключения к HH и получения данных по компаниям и вакансиям
    """

    def __init__(self, emp_id):
        self.emp_id = emp_id

    def job_employers(self, id):
        url = f'https://api.hh.ru/employers/{id}/'
        params = {'per_page': 10, "sort_by": "by_vacancies_open", 'area': 4}
        response = requests.get(url, params=params)
        employer = response.json()
        return {
            'id': employer['id'],
            'name': employer['name'],
            'url': employer['alternate_url']
        }

    def job_vacancies(self, id):
        """
        Подключаемся
        """
        url = 'https://api.hh.ru/vacancies/'
        params = {'per_page': 10, "employer_id": id, 'area': 4}
        response = requests.get(url, params=params)
        vacancies = response.json()['items']
        return vacancies

    def get_employers_list(self):
        """
        Получаем данные о компаниях
        """
        employers_list = []

        for id in self.emp_id:
            employers_list.append(self.job_employers(id))
        return employers_list

    def get_vacancies_list(self):
        """
        Получаем данные о вакансиях
        """
        emp = self.get_employers_list()
        vacancies_list = []
        for employer in emp:
            vacancies_list.extend(self.job_vacancies(employer["id"]))
        return vacancies_list

    def filter_salary(self):
        """
        Готовим данные по вакансиям для таблицы
        """
        vacancies = self.get_vacancies_list()
        filter_vacancies = []
        for vac in vacancies:
            if not vac["salary"]:
                vac["salary"] = 0
                vac["currency"] = "Валюта не определена"
            else:
                if vac["salary"] is None:
                    vac["salary"] = 0
                else:
                    if vac["salary"]["currency"]:
                        vac["currency"] = vac["salary"]["currency"]
                    else:
                        vac["currency"] = "Валюта не определена"
                    if vac["salary"]["from"] is None and vac["salary"]["to"] is None:
                        vac["salary"] = 0
                    else:
                        if vac["salary"]["from"] is None and vac["salary"]["to"] is not None:
                            vac["salary"] = vac["salary"]["to"]
                        else:
                            if vac["salary"]["from"] is not None and vac["salary"]["to"] is None:
                                vac["salary"] = vac["salary"]["from"]
                            else:
                                if vac["salary"]["from"] is not None and vac["salary"]["to"] is not None:
                                    vac["salary"] = vac["salary"]["to"]
            filter_vacancies.append({
                "id": vac["id"],
                "name": vac["name"],
                "salary": vac["salary"],
                "currency": vac["currency"],
                "url": vac["alternate_url"],
                "employer": vac["employer"]["id"],
            })
        return filter_vacancies
