from config import config
from classes import HHGetData, DBManager
from utils import create_database, creating_tables, data_entry


def main():
    emp_id = [2255720, 678191, 10789921, 2104558, 2967080, 9618600, 2198765, 2842036, 924205, 3978265]
    print('Подгружаются данные с сайта Head Hunter')
    head = HHGetData(emp_id)
    head.get_employers_list()
    head.filter_salary()
    params = config()
    print('Пожалуйста подождите, происходит обработка данных')
    create_database(params)
    creating_tables(params)
    data_entry(params, emp_id)
    dbm = DBManager()
    print('В каком виде вы предпочитаете получить информацию о вакансиях?')
    option = input('1 - список всех компаний и кол-во вакансий у каждой компании\n'
                   '2 - список всех вакансий с указанием названия компании, вакансии, зарплаты и ссылки на вакансию\n'
                   '3 - средняя зарплата по вакансиям\n'
                   '4 - список всех вакансий, у которых зарплата выше средней по всем вакансиям\n'
                   '5 - список всех вакансий, в названии которых есть ключевое слово\n')

    if option == '1':
        print(dbm.get_companies_and_vacancies_count(params))
    elif option == '2':
        print(dbm.get_all_vacancies(params))
    elif option == '3':
        print(dbm.get_avg_salary(params))
    elif option == '4':
        print(dbm.get_vacancies_with_higher_salary(params))
    elif option == '5':
        keyword = str(input('Введите слово по которому будет выполнен поиск\n'))
        print(dbm.get_vacancies_with_keyword(keyword, params))


if __name__ == '__main__':
    main()
