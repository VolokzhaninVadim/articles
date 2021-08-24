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
from sqlalchemy import create_engine, Table, MetaData, update
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker

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
                 ,yandex_mail 
                 ,habr_password
                 ,url = {
                     'main' : 'https://habr.com/ru/post/'
                     ,'login' : 'https://account.habr.com/login'
                 }
                 ,schema = 'article'
                 ,table_list = ['habr_posts', 'habr_html']
                ):
        """
        Функция для инциализации объекта класса.
        Вход:
            pg_password(str) - пароль для postgre.
            pg_login(str) - логин для postgre.
            host(str) - ip адрес.
            password_tor(str) - пароль для TOR.
            yandex_mail(str) - ящик на yandex как логин в Habr.
            habr_password(str) - пароль для Habr.
            url(dict) - url.  
            schema() - схема pg.
            table_list(list) - список таблиц в pg.
        Выход: 
            нет.
        """
        self.host = host
        self.engine = create_engine(f'postgres://{pg_login}:{pg_password}@{self.host}:5432/{pg_login}')
        self.password_tor = password_tor
        self.yandex_mail = yandex_mail
        self.habr_password = habr_password
        self.url = url
        self.dic_month = {'января' : '01', 'февраля' : '02', 'марта' : '03', 'апреля' : '04', 'мая' : '05', 'июня' : '06', 'июля' : '07','августа' : '08',
                          'сентября' : '09', 'октября' : '10', 'ноября' : '11', 'декабря' : '12'}
        self.schema = schema
        self.table_list = table_list
        
    
    def metadata_db(self): 
        """
        Создаем схему данных.
        Вход: 
            нет.
        Выход: 
            (MetaData) - объект схемы pg.
        """
        
# Создаем объект схемы 
        metadata = MetaData(schema = self.schema)
        metadata.bind = self.engine
# Добавляем к схеме таблицы 
        for table in self.table_list: 
            Table(
                table
                ,metadata
                ,schema = self.schema
                ,autoload = True
            )
        return metadata

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
    
    def html_write_habr(self, pid, schema = 'article', table_name = 'habr_html'): 
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
            url = self.url['main'] + str(pid)
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
    
    def mass_write_habr(self, start, stop): 
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
        self.write_new_data(start, stop)
    
    def pg_descriptions(self): 
        """
        Функция для возвращения описания таблиц в pg. 
        Вход: 
            нет.
        Выход: 
            (MetaData) - описание таблиц в pg.
        """
        
# Получаем объект pg
        inspector = inspect(self.engine)
        schemas = inspector.get_schema_names()
        
# Получаем описание объектов pg 
        for schema in schemas:            
            if schema == self.schema: 
                for table_name in inspector.get_table_names(schema = schema):
                    for column in inspector.get_columns(table_name, schema=schema):
                        print(
                            f'Схема: {schema}'
                            ,f'Таблица: {table_name}'
                            ,f"Колонка: {column}"
                        )       

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
    
    def new_data(self, start, stop):
        """
        Новые данные для обработки.
        Вход:
            start - стартового индекс.
            stop - конечный индекс.
        Выход:
            posts_df(DataFrame) - таблица с необработанными данными.
        """
        query = f"""
        select 
                html.link 
                ,html.html 
                ,html.id
        from 
                article.habr_html as html
        left join article.habr_posts as posts on html.id = posts.id 
        where 
                posts.id is null
            and html.id  between {start} and {stop}
        """

        posts_df = pd.read_sql(
            query
            ,con = self.engine
        )
        return posts_df
    
    def write_new_data(self, start, stop, schema = 'article', table_name = 'habr_posts'): 
        """
        Запись новых данных в pg.
        Вход: 
            start - стартового индекс.
            stop - конечный индекс.
            schema(str) - название схемы.
            table_name(str) - название таблицы.
        Выход: 
            нет.
        """
# Получаем новые данные 
        posts_df = self.new_data(start, stop)

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
        select max(id) from article.habr_html
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
                self.mass_write_habr(max_id_start, max_id_stop)                
                print(max_id_start, max_id_stop)
            else: 
                break
                
    def bookmarks(self): 
        """
        Получение id статей, которые я положил в закладки. 
        Вход: 
            нет. 
        Выход: 
            id_list(list) - лист id.
        """

# Зарегестрируемся
        url = 'https://account.habr.com/login'
        data = {'email_field': self.yandex_mail, 'password_field': self.habr_password}
        requests.post(self.url['login'], data = data)

# Получим ссылки
        id_list = []
        page = 0
# Пишем цикл, если несколько страниц
        while True:
            page += 1
            r = requests.get(f'https://habr.com/ru/users/Volokzhanin/favorites/posts/page{page}/')
            soup = BeautifulSoup(r.text, 'html5lib')
            bookmark_links = soup.find_all("a", {"class": "tm-article-snippet__title-link"})
            if len(bookmark_links) > 0:
                for id_post in bookmark_links:
                    current_link = re.search(r'/\d{1,}/', str(id_post)).group(0)
                    current_link = re.sub(r'\D', '', current_link)
                    id_list.append(int(current_link))
            else:
                break
        return id_list
    
    def mark_bookmarks(self, table_name = 'article.habr_posts'): 
        """
        Отметка закладок. 
        Вход: 
            table_name(str) - название таблицы для отметки.
        Выход: 
            нет.
        """
        
# Получаем ссылки на id статей в моих закладках
        id_list = self.bookmarks()

# Получаем объект схемы pg
        metadata = self.metadata_db()

# Обвновляем запись
        table = metadata.tables[table_name]
        for post_id in tqdm_notebook(id_list): 
            stmt = table.update().where(table.c.id == post_id and table.c.is_like == False).values(is_like = True)
            with self.engine.connect() as conn:    
                conn.execute(stmt)