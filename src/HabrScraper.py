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
from sqlalchemy import create_engine, Table, MetaData, update, select, or_, and_
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import func

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

# Для работы с БД
from src.DBResources import DBResources

# Для работы с параметрами
from src.Params import Params

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################
###############################################################################################################################################

class HabrScraper:    
    def __init__(self):
        """
        Функция для инциализации объекта класса.
        Вход:   
            нет.
        Выход: 
            нет.
        """
        self.params = Params()
        self.db_resources = DBResources() 
        self.inspector = inspect(self.db_resources.engine)
        self.metadata = MetaData(schema = self.inspector.get_schema_names(), bind = self.db_resources.engine)

    def change_ip(self): 
        """
        Функция для смены IP.
        Вход: 
            нет.
        Выход:
            нет.
        """
        with Controller.from_port(address = self.params.pg_host, port = 9051) as controller:
                controller.authenticate(password = self.params.password_tor)
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
        s.proxies['http'] = f'socks5h://{self.params.pg_host}:9050'
        s.proxies['https'] = f'socks5h://{self.params.pg_host}:9050'
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
            url = self.params.url['main'] + str(pid)
            try:                 
# Получаем html
                html, r = self.data_page(url) 
                break
            except (ConnectTimeout, ConnectionError, ReadTimeout, ProxyError) as e: 
                continue 
        table = Table(
            table_name
            ,self.metadata
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

        with self.db_resources.engine.connect() as conn:
            conn.execute(update_stmt)
        
        if re.findall('Страница не найдена', html.text):
            result = True
        else: 
            result = False
        return result
    
    def mass_write_habr(self, start = None, stop = None): 
        """
        Массовое получение статей с сайта habr. 
        Вход: 
            start(int) - стартового индекс.
            stop(int) - конечный индекс.
            update_list_id(list) - список id. 
        Выход: 
            нет.
        """
# Есди нет start и stop, то делаем update, иначе используем start и stop
        if start and stop:            
            with ThreadPool(10) as p:
                p.map(self.html_write_habr, range(start, stop)) 
            self.write_new_data(start, stop)
        else: 
            df = self.new_data()
            list_id = [int(i) for i in df['id'].values]
            with ThreadPool(10) as p:
                p.map(self.html_write_habr, list_id)  
            self.write_new_data()     

    def date_text(self, raw_date):
        """
        Преобразуем сырую дату в дату.
        Вход: 
            raw_date(ыек) - сырая дата.
        Выход:
            date(str) - даат в ФОРМАТЕ '%Y-%m-%d %H:%M'
        """
# Словарь месяцев        
        dic_month = {'января' : '01', 'февраля' : '02', 'марта' : '03', 'апреля' : '04', 'мая' : '05', 'июня' : '06', 'июля' : '07','августа' : '08',
                          'сентября' : '09', 'октября' : '10', 'ноября' : '11', 'декабря' : '12'} 
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
            year = re.search(r'\d{4}', raw_date).group(0) if re.findall(r'\d{4}', raw_date) else str(datetime.datetime.now().year)
            day = re.search(r'^\d{1,2}', raw_date).group(0)
            month = re.search(r'\s\w.+?\s', raw_date).group(0).strip()
            time = re.search(r'\d{2}:\d{2}', raw_date).group(0).strip()
            if len(day) == 1:
                day = '0' + re.search(r'^\d{1,2}', raw_date).group(0)
            date = year + '-' + dic_month[month] + '-' + day + ' ' + time
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
    
    def new_data(self, start = None, stop = None):
        """
        Новые данные для обработки.
        Вход:
            start - стартового индекс.
            stop - конечный индекс.
        Выход:
            rersult_df(DataFrame) - таблица с необработанными данными.
        """
# Получаем таблицы для объединения 
        inspector = inspect(self.db_resources.metadata)
        habr_posts_table = inspector.tables['article.habr_posts']
        habr_html_table = inspector.tables['article.habr_html']

# Объединяем таблицы 
        join_tables = habr_html_table.join(
            habr_posts_table
            ,habr_html_table.c.id == habr_posts_table.c.id
            ,isouter = True
        )

# Получаем ссылки на неразобранные страницы
        if start and stop: 
            stmt = select([join_tables.c.article_habr_html_id, join_tables.c.article_habr_html_link, join_tables.c.article_habr_html_html]).\
            select_from(join_tables).\
            where(and_(join_tables.c.article_habr_html_id.between(start, stop), join_tables.c.article_habr_posts_title == None))

# Выполняем запрос 
            with self.engine.connect() as conn:   
                id_list = conn.execute(stmt)

# Получаем id статей 
            rersult_list = id_list.fetchall()
            rersult_df = pd.DataFrame([dict(row) for row in rersult_list])
        else: 
            stmt = select([join_tables.c.article_habr_html_id, join_tables.c.article_habr_html_link, join_tables.c.article_habr_html_html]).\
            select_from(join_tables).where(join_tables.c.article_habr_posts_title == None).\
            order_by(func.random()).\
            limit(500)

# Выполняем запрос 
            with self.db_resources.engine.connect() as conn:   
                id_list = conn.execute(stmt)

# Получаем id статей 
            rersult_list = id_list.fetchall()
            rersult_df = pd.DataFrame([dict(row) for row in rersult_list])
        return rersult_df
    
    def write_new_data(self, start = None, stop = None, schema = 'article', table_name = 'habr_posts'): 
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
                user_id = soup.find("span", {"class": "tm-user-info__user"}).text.strip()
                doc_dic['tags'] = str(tags)
                full_text = title + ' ' + text + ' ' + ' '.join(tags) + ' ' + user_id
                doc_dic['lem_text'] = self.lem_text(self.clean_text(full_text))
                
# Получаем лемматизированный текст
                doc_dic['user_id'] = user_id
    
# Получаем признак понравившейся статьи   
                doc_dic['is_like'] = False   
    
# Получаем дату загрузки
                doc_dic['date_load'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                table = Table(
                    table_name
                    ,self.db_resources.metadata
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

                with self.db_resources.engine.connect() as conn:
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
            ,con = self.db_resources.engine
        )
        max_html = max_html.loc[0, 'max']
        
        query2 = """
        select max(id) from article.habr_posts
        """
        max_habr_posts = pd.read_sql(
            query2
            ,con = self.db_resources.engine
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
        data = {'email_field': self.params.yandex_mail, 'password_field': self.params.habr_password}
        requests.post(self.params.url['login'], data = data)

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

# Обвновляем запись
        table = self.db_resources.metadata.tables[table_name]
        for post_id in id_list: 
            stmt = table.update().where(table.c.id == post_id and table.c.is_like == False).values(is_like = True)
            with self.db_resources.engine.connect() as conn:    
                conn.execute(stmt)
