# Скрипт пробегается по всем приказам на команду проекта и формирует датафрейм с участниками команд проектов.
# Данные выгружает из папки: S:\\22. Офис управления проектами\\Общая документация\\1. Приказы\\Приказы на команды проектов
# Затем сохраняет их в БД: anodb.dwh_data.ano_project_team.

import pandas as pd
import sqlalchemy as sqlalchemy
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import time
from datetime import datetime
import os
import glob
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# Создаем соединение с базой данных
conn = psycopg2.connect(dbname="anodb", host="192.168.89.32",
                        user="a.tretyakov", password="admin")

# Слова исключения из путей к папкам
words = ["Архив", "архив", "иные", "комисси", "справочн", "!", "$"]

# Путь к папке с файлами Excel
folder_path = r'S:\\22. Офис управления проектами\\Общая документация\\1. Приказы\\Приказы на команды проектов'

# Поиск кода ДС
keys = ["013", "014", "015", "016", "017", "018",
        "019", "020", "021", "022", "023", "024"]

# Список с путями к приказам на команду проекта по объектно
list_project_team = []


# Функция поиска слов исключений в путях
def fx_puth(str_, words):
    for word in words:
        if word.lower() in str_.lower():
            return True
    return False


# Наполнение списка путями к папкам по всем проектам

# Пробегаю по папке "Приказы на команды проектов"
for name in os.listdir(folder_path):
    if fx_puth(name, words):
        continue
    else:
        # Пробегаю по папке "Приказы на команды проектов\Здрав...(обр, уник и т.д)". Проверяю на слова исключения, оставляю только нужную подпапку.
        folder_path_1 = f"{folder_path}\{name}"
        for name1 in os.listdir(folder_path_1):
            if fx_puth(name1, words):
                continue
            else:
                # Пробегаю по папке "Приказы на команды проектов\\Здравоохранение\\! ДП_Измайловский проезд - приостановлены\\...". Если в папке есть xlsx файл, добавляю его в список. Проверяю на слова исключения, оставляю только нужную подпапку.
                folder_path_2 = f"{folder_path_1}\{name1}"
                for name2 in os.listdir(folder_path_2):
                    if fx_puth(name2, words):
                        continue
                    elif name2.endswith('.xlsx'):
                        file_path_object = f"{folder_path_2}\{name2}"
                        list_project_team.append(file_path_object)
                        break
                    else:
                        # Пробегаю по папке "Приказы на команды проектов\\Здравоохранение\\! ДП_Измайловский проезд - приостановлены\\от 01.09.2022 с изм. 20.03.2023 +", далее ищу xlsx файл и добавляю его в список. Проверяю на слова исключения, оставляю только нужную подпапку.
                        folder_path_3 = f"{folder_path_2}\{name2}"
                        for name3 in os.listdir(folder_path_3):
                            if fx_puth(name3, words):
                                continue
                            elif name3.endswith('.xlsx'):
                                file_path_object2 = f"{folder_path_3}\{name3}"
                                list_project_team.append(file_path_object2)

# Пробегаю по итоговому списку и удаляю элементые, которые редактируются.
for i in list_project_team:
    if "~$" in i.lower():
        list_project_team.remove(i)


# Функция обработки excel приказа, возвращает df с 3 колонками: кодДС, ФИО, Роль.
def read_excel_file(file_path):
    # Пустой лист для добавления названий колонок
    list_columns = []
    try:
        df = pd.read_excel(file_path, header=None)
        obj_key = '000-0000'
        # Изменяю тип данных первого столбца. Пробегаюсь по нему, если нахожу значение из списка "keys" то сохраняю его в переменную код_ДС
        df = df.astype({0: 'str'})
        for value in df[0]:
            for key in keys:
                if key in value:
                    obj_key = value.strip()[0:8]
                    break
        # Создаю новую колонку
        df['obj_key'] = obj_key

        # Удаляю пустые строки по столбцу "ФИО"
        df = df.dropna(subset=[3])

        # Пробегаю по столбцам (кроме последнего с кодом ДС) и вытаскиваю из 1 строки названия, добавляю в список list_columns. А затем добавляю название последнего столбца "obj_key"
        for i in range(len(df.columns)-1):
            list_columns.append(df.iloc[0, i])
        list_columns.append('obj_key')

        # Меняю названия столбцов
        df.columns = list_columns

        # Удаляю 1-ую строку
        df = df.iloc[1:, :]

        # Оставляю только необходимые столбцы
        selected_columns = ['obj_key', 'ФИО', 'Роль в проекте']
        df = df[selected_columns]

        # Переименовываем колонок
        df.rename(columns={'ФИО': 'Full_name',
                  'Роль в проекте': 'role'}, inplace=True)

        df['Full_name'] = df['Full_name'].str.replace('ё', 'е')

        # Добавление колонки "Фамилия И.О."
        df['Short_name'] = df['Full_name'].str.split().str[0] + ' ' + df['Full_name'].str.split(
        ).str[1].str.split('').str[1] + '.' + df['Full_name'].str.split().str[-1].str.split('').str[1] + '.'

        # Добавление колонки Дата создания df
        df = df.assign(created_date=datetime.now().strftime("%Y-%m-%d"))

        return df
    except Exception as e:
        print(f"Ошибка при чтении файла '{file_path}': {e}")
        return None

    # Функция проверки excel приказа, возвращает df с путем и кодом-дс


def Check_obj_key(file_path):
    # Пустой лист для добавления названий колонок
    list_columns = []
    df = {'Puth': [''],
          'obj_key': ['']
          }
    data = pd.DataFrame(df)

    try:
        df1 = pd.read_excel(file_path, header=None)
        obj_key = '000-0000'
        # Изменяю тип данных первого столбца. Пробегаюсь по нему, если нахожу значение из списка "keys" то сохраняю его в переменную код_ДС
        df1 = df1.astype({0: 'str'})
        for value in df1[0]:
            for key in keys:
                if key in value:
                    obj_key = value.strip()[0:8]
                    break

        data.loc[len(data.index)] = [file_path, obj_key]

        return data

    except Exception as e:
        print(f"Ошибка при чтении файла '{file_path}': {e}")
        return None

    # Функция для обработки списка файлов Excel и объединения их в один DataFrame.


def process_excel_files(list_project_team):

    all_dfs = []
    for file_path in list_project_team:
        # Проверяем, существует ли файл
        if not os.path.exists(file_path):
            print(f"Файл '{file_path}' не найден.")
            continue

        # Читаем данные из Excel файла
        df = read_excel_file(file_path)

        if df is not None:
            all_dfs.append(df)

    # Объединяем DataFrames
    if len(all_dfs) > 0:
        combined_df = pd.concat(all_dfs)
        return combined_df
    else:
        return None


# Функция для обработки списка файлов Excel и объединения их в один DataFrame для проверки кодов ДС.
def process_Check_obj_key(list_project_team):

    all_dfs = []
    for file_path in list_project_team:
        # Проверяем, существует ли файл
        if not os.path.exists(file_path):
            print(f"Файл '{file_path}' не найден.")
            continue

        # Читаем данные из Excel файла
        df = Check_obj_key(file_path)
        if df is not None:
            all_dfs.append(df)

    # Объединяем DataFrames
    if len(all_dfs) > 0:
        combined_df = pd.concat(all_dfs)
        return combined_df
    else:
        return None


combined_df = process_excel_files(list_project_team)
combined_df.reset_index(drop=True, inplace=True)
combined_df['index'] = combined_df.index
check_obj_key_df = process_Check_obj_key(list_project_team)
if combined_df is not None:
    print('Объединенный DataFrame с приказами на команду проектов успешно создан.')
    rows_to_insert = [tuple(x) for x in combined_df.to_numpy()]
    # Формирование SQL-запроса для вставки данных
    insert_query = "INSERT INTO anodb.dwh_data.ano_project_team (obj_key, Full_name, role, Short_name, Created_date, index) VALUES %s;"
    cursor = conn.cursor()
    execute_values(cursor, insert_query, rows_to_insert)
    conn.commit()
    cursor.close()
    conn.close()
    print('\n Объединенный DataFrame с приказами на команду проектов успешно загружен в БД: anodb.dwh_data.ano_project_team')
    # print("Объединенный DataFrame с приказами на команду проектов сохранен. Путь: r'S:\22. Офис управления проектами\Общая документация\10. Аналитика\Прочие отчеты\46. Приказы на команду проекта\project_team.xlsx")
    # combined_df.to_excel(
    #     r'S:\\22. Офис управления проектами\\Общая документация\\10. Аналитика\\Прочие отчеты\\46. Приказы на команду проекта\\project_team.xlsx', index=False)
else:
    print("Ни один из файлов не был успешно обработан.")


if check_obj_key_df is not None:
    path = "S:\\22. Офис управления проектами\\Общая документация\\10. Аналитика\\Прочие отчеты\\46. Приказы на команду проекта\\Проверка приказов\\check_obj_key_" + \
        datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".xlsx"
    print("Проверка корректности кодов ДС в приказах проведена. Результат сохранен. Путь: r'S:\\22. Офис управления проектами\\Общая документация\\10. Аналитика\\Прочие отчеты\\46. Приказы на команду проекта\\Проверка приказов\\check_obj_key.xlsx'")
    check_obj_key_df.to_excel(path, index=False)
else:
    print("Ни один из файлов не был успешно обработан.")
