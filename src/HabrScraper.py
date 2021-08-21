###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################
# Для работы с ОС 
import os

# Для работы с HTTP-запросами 
import requests 
# Для работы с ошибками proxy
from requests.exceptions import ConnectTimeout, ProxyError, ConnectionError, ReadTimeout
# Для генерации поддельного User agent
from fake_useragent import UserAgent
# Обработка HTML
from bs4 import BeautifulSoup

# Для работы с табличными данными
import pandas as pd

# Для работы с регулярными выражениями
import re

# Для работы с SQL
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
# Для работы с Postgre
import psycopg2

# Для работы с tor
import stem
from stem.control import Controller
from stem.process import launch_tor_with_config

# Для работы с airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

# Для работы с датой-временем
import datetime

# Для произведения синтаксического анализа (лемматизации)
import pymorphy2 as pm
# Загрузим словарь русского языка
morph = pm.MorphAnalyzer()

# Для параллельной работы кода
from multiprocessing.dummy import Pool as ThreadPool

# Для мониторинга выполнения циклов
from tqdm import tqdm
from tqdm.notebook import tqdm as tqdm_notebook 

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################
###############################################################################################################################################

class HabrScraper:
    
    def __init__(self 
                 ,pg_password
                 ,pg_login
                 ,host
                 ,password_tor
                 ,url = 'https://habr.com/ru/post/'
                ):
        """
        Функция для инциализации объекта класса.
        Вход:
            pg_password(str) - пароль для postgre.
            pg_login(str) - логин для postgre.
            host(str) - ip адрес.
            password_tor(str) - пароль для TOR.
            url(str) - url. 
        Выход: 
            нет.
        """
        self.host = host
        self.engine = create_engine(f'postgres://{pg_login}:{pg_password}@{self.host}:5432/{pg_login}')
        self.password_tor = password_tor
        self.conn = psycopg2.connect(f"host={self.host} dbname={pg_login} user={pg_login} password={pg_password}")
        self.url = url
        self.dic_month = {'января' : '01', 'февраля' : '02', 'марта' : '03', 'апреля' : '04', 'мая' : '05', 'июня' : '06', 'июля' : '07','августа' : '08',
                          'сентября' : '09', 'октября' : '10', 'ноября' : '11', 'декабря' : '12'}

    def change_ip(self): 
        """
        Функция для смены IP.
        Вход: 
            нет.
        Выход:
            нет.
        """
        with Controller.from_port(address = self.host, port = 9051) as controller:
                controller.authenticate(password = self.password_tor)
                controller.signal(stem.Signal.NEWNYM)
                
    def data_page(self, url): 
        """
        Функция для получения данных через tor. 
        Вход: 
            url(str) - url.
        Выход: 
            (tuple) - BeautifulSoup и страница. 
        """
# Меняем IP 
        self.change_ip()
# Делаем запрос
        s = requests.session()
        s.proxies = {}
        s.proxies['http'] = f'socks5h://{self.host}:9050'
        s.proxies['https'] = f'socks5h://{self.host}:9050'
        while True:             
            try: 
                r = s.get(url = url, headers={'User-Agent': UserAgent(cache = True).chrome}, timeout = (20, 50)) 
                break
            except (ProxyError, ConnectionError, ReadTimeout):
                continue            
        return (BeautifulSoup(r.text, 'html5lib'), r)
    
    def html_write_habr(self, pid, schema = 'article', table_name = 'html'): 
        """
        Функция для получения и записи html. 
        Вход: 
            pid(int) - id статьи.
            schema(str) - название схемы.
            table_name(str) - название таблицы.
        Выход: 
            result(bool) - булево значение для ненайденных старниц.
        """
        while True:
            url = self.url + str(pid)
            try:                 
# Получаем html
                html, r = self.data_page(url) 
                break
            except (ConnectTimeout, ConnectionError, ReadTimeout, ProxyError) as e: 
                continue 
        metadata = MetaData(schema = schema)
        metadata.bind = self.engine
        table = Table(
            table_name
            ,metadata
            ,schema = schema
            ,autoload = True
        )
        primary_keys = [key.name for key in inspect(table).primary_key]
        records = [{
            'id' : pid
            ,'link' : url
            ,'html' : f'{html}'
            ,'date_load' : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }]
# Производим запись данных
        stmt = pg_insert(table).values(records)

        update_dict = {
            c.name: c
            for c in stmt.excluded
            if not c.primary_key and c.name not in ['date_load']
        }
# Обновляем поля, если строка существует 
        update_stmt = stmt.on_conflict_do_update(
            index_elements = primary_keys,
            set_ = update_dict
        )

        with self.engine.connect() as conn:
            conn.execute(update_stmt)
        
        if re.findall('Страница не найдена', html.text):
            result = True
        else: 
            result = False
        return result
    
    def mass_html_write_habr(self, start, stop): 
        """
        Массовое получение статей с сайта habr. 
        Вход: 
            start - стартового индекс.
            stop - конечный индекс.
        Выход: 
            нет.
        """
        with ThreadPool(10) as p:
            p.map(self.html_write_habr, range(start, stop))  
    
    def pg_descriptions(self, schema = "article"): 
        """
        Функция для возвращения таблицы с описанием таблиц в pg. 
        Вход: 
            schema(str) - наименование схемы.
        Выход: 
            desription_df(DataFrame) - таблица с описанием таблиц pg.
        """
            
        query_table_description = f"""
        with 
             tables as (
             select distinct
                    table_name
                    ,table_schema
            from 
                    information_schema.columns 
            where 
        -- Отбираем схему
                    table_schema in ('{schema}')
             )
        select 
                nspname as scheme_name, 
                obj_description(n.oid) as scheme_description,
                relname as table_name, 
                attname as column_name, 
                format_type(atttypid, atttypmod), 
                obj_description(c.oid) as table_description, 
                col_description(c.oid, a.attnum) as column_description 
        from 
                pg_class as c 
        join pg_attribute as a on (a.attrelid = c.oid) 
        join pg_namespace as n on (n.oid = c.relnamespace)
        join tables on tables.table_name = c.relname and tables.table_schema = n.nspname
        where
                format_type(atttypid, atttypmod) not in ('oid', 'cid', 'xid', 'tid', '-')
        """

        desription_df = pd.read_sql(
            query_table_description 
            ,con = self.engine
        )
        return desription_df

    def date_text(self, raw_date):
        """
        Преобразуем сырую дату в дату.
        Вход: 
            raw_date(ыек) - сырая дата.
        Выход:
            date(str) - даат в ФОРМАТЕ '%Y-%m-%d %H:%M'
        """
        # Если получаем дату в формате 'сегодня'
        if len(re.findall('сегодня', raw_date)) > 0:
            time = re.search(r'\d{2}:\d{2}', raw_date).group(0).strip()
            date = (datetime.datetime.now()).strftime("%Y-%m-%d") + ' ' + time
        # Если получаем дату в формате 'вчера'
        elif len(re.findall('вчера', raw_date)) > 0:
            time = re.search(r'\d{2}:\d{2}', raw_date).group(0).strip()
            date = (datetime.datetime.now() - datetime.timedelta(days = 1)).strftime("%Y-%m-%d") + ' ' + time
        # Иначе
        else:
            year = re.search(r'\d{4}', raw_date).group(0)
            day = re.search(r'^\d{1,2}', raw_date).group(0)
            month = re.search(r'\s\w.+?\s', raw_date).group(0).strip()
            time = re.search(r'\d{2}:\d{2}', raw_date).group(0).strip()
            if len(day) == 1:
                day = '0' + re.search(r'^\d{1,2}', raw_date).group(0)
            date = year + '-' + self.dic_month[month] + '-' + day + ' ' + time
        return date
    
    def clean_text(self, text):
        """
        Очистка текста. 

        Вход: 
            text(str) - сырой текст. 
        Выход:
            clean_text(str) - очищенный текст
        """
        # Переводим в нижний регистр
        lower_text = text.lower()
        # Заменяем все кроме буквы или цифры
        clean_text = re.sub(r'\W', ' ', lower_text)
        # Удаляем все пробелы, кроме между словами
        clean_text = ' '.join(clean_text.split())
        return clean_text

    def lem_text(self, text):
        """
        Нормализация(лемматизация) текста.
        Вход:
            text(str) - очищенный текст.
        Выход:
            finish_text(str) - лемматизированный тест.
        """
        # Лемматизируем каждое слово
        word_lem = [morph.parse(item)[0].normal_form for item in text.split()]
        # Склеиваем слово через пробел
        finish_text = ' '.join(word_lem)
        return finish_text
    
    def new_data(self):
        """
        Новые данные для обработки.
        Вход:
            нет.
        Выход:
            posts_df(DataFrame) - таблица с необработанными данными.
        """
        query = """
        select 
                html.link 
                ,html.html 
                ,html.id
        from 
                article.html as html
        left join article.habr_posts as posts on html.id = posts.id 
        where 
                posts.id is null
        """

        posts_df = pd.read_sql(
            query
            ,con = self.engine
        )
        return posts_df
    
    def write_new_data(self, schema = 'article', table_name = 'habr_posts'): 
        """
        Запись новых данных в pg.
        Вход: 
            schema(str) - название схемы.
            table_name(str) - название таблицы.
        Выход: 
            нет.
        """
# Получаем новые данные 
        posts_df = self.new_data()

# Разбираем и записываем новые данные 
        for index, row in posts_df.iterrows(): 
            doc_dic = {}
            soup = BeautifulSoup(row['html'], 'html5lib')
            title = soup.find("h1", {"tm-article-snippet__title tm-article-snippet__title_h1"})
            if title:   
# Получаем идентификатор
                doc_dic['id'] = row['id']

# Получаем заголовок
                title = title.text
                doc_dic['title'] = title

# Получаем текст 
                text = soup.find("div", {"id": "post-content-body"})
                if text:
                    text = soup.find("div", {"id": "post-content-body"}).text
                else: 
                    text = ''
                doc_dic['text'] = text
                
# Получаем дату-время                
                doc_dic['date_time'] = self.date_text(soup.find("span", {"class": "tm-article-snippet__datetime-published"}).text)
                tags = soup.find_all("span", {"class": "tm-article-body__tags-item"}) 
        
# Получаем теги
                if tags: 
                    tags = [i.text.lower().strip() for i in tags]
                else: 
                    tags = ''
                doc_dic['tags'] = str(tags)
                full_text = title + ' ' + text + ' ' + ' ' + ' '.join(tags) 
                doc_dic['lem_text'] = self.lem_text(self.clean_text(full_text))
                
# Получаем лемматизированный текст
                doc_dic['user_id'] = soup.find("span", {"class": "tm-user-info__user"}).text.strip()
    
# Получаем признак понравившейся статьи   
                doc_dic['is_like'] = False   
    
# Получаем дату загрузки
                doc_dic['date_load'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                metadata = MetaData(schema = schema)
                metadata.bind = self.engine
                table = Table(
                    table_name
                    ,metadata
                    ,schema = schema
                    ,autoload = True
                )
                primary_keys = [key.name for key in inspect(table).primary_key]
                records = [doc_dic]
# Производим запись данных
                stmt = pg_insert(table).values(records)

                update_dict = {
                    c.name: c
                    for c in stmt.excluded
                    if not c.primary_key and c.name not in ['date_load']
                }
# Обновляем поля, если строка существует 
                update_stmt = stmt.on_conflict_do_update(
                    index_elements = primary_keys,
                    set_ = update_dict
                )

                with self.engine.connect() as conn:
                    conn.execute(update_stmt)
                
    def max_id(self):
        """
        Максимальный id в таблицах: habr_posts и html.
        Вход:
            нет.
        Выход:
            (list) - лист с максимумами для таблиц: habr_posts и html.
        """
        
        query1 = """
        select max(id) from article.html
        """
        max_html = pd.read_sql(
            query1
            ,con = self.engine
        )
        max_html = max_html.loc[0, 'max']
        
        query2 = """
        select max(id) from article.habr_posts
        """
        max_habr_posts = pd.read_sql(
            query2
            ,con = self.engine
        )        
        max_habr_posts = max_habr_posts.loc[0, 'max']

# Если таблицы пустые, то проставляем 0, иначе считаем разницу между нимим
        if max_html is None and max_habr_posts is None:
            difference, max_html, max_habr_posts = 0, 0, 0
        else:
            difference = max_html - max_habr_posts
        
        return [max_html, max_habr_posts, difference]
    
    def all_new(self): 
        """
        Получение нового html и его разбор. 
        Вход: 
            нет.
        Выход: 
            нет. 
        """
# Если разница между html и разобранным кодом составляет более 99, то прерываем процесс   
        while True: 
            max_id = self.max_id()
            if max_id[2] < 100: 
                max_id_start = max_id[0] + 1 
                max_id_stop = max_id_start + 99
                self.mass_html_write_habr(max_id_start, max_id_stop)
                self.write_new_data()
                print(max_id_start, max_id_stop)
            else: 
                break