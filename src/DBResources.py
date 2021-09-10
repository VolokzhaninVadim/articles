###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################
# Для работы с SQL 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect
from sqlalchemy import (Column, String, Text, ForeignKey, Integer, TIMESTAMP, Text,\
                create_engine, MetaData, Table, update, select, or_, and_, DATETIME, Boolean)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import func

# Для работы с параметрами
from src.Params import Params

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################
###############################################################################################################################################

class DBResources:    
    def __init__(self):
        """
        Функция для инциализации объекта класса.
        Вход:
            нет.
        Выход: 
            нет.
        """
        self.params = Params()
        self.engine = create_engine(f'postgres://{self.params.pg_login}:{self.params.pg_password}@{self.params.pg_host}:5432/{self.params.pg_login}')
        self.inspector = inspect(self.engine)
        self.metadata = MetaData(schema = self.inspector.get_schema_names(), bind = self.engine)
        self.schema = 'article'
        self.table_list = ['habr_posts', 'habr_html']   
            
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

    def create_db_scheme(self, schemas = ['article']): 
        """
        Создание схемм данных. 
        Вход: 
            schemas(dict) - словарь схем с описанием.
        Выход: 
            нет. 
        """   
        for schema in schemas: 
    # Если нет схемы, то создаем ее 
            if schema not in self.metadata.schema: 
                engine.execute(CreateSchema(schema))

    def truncate_db_tables(self, tables_list = ['article.habr_html', 'article.habr_posts']): 
        """
        Очистка необходимых объектов. 
        Вход: 
            tables_list(list) - лист с наименованиями таблиц.
        Выход: 
            нет.
        """
    # Очищаем все необходимые таблицы
        for table in tables_list: 
            with self.engine.connect() as connection:
                connection.execute(f'truncate {table}')

    def create_db_tables(self, tables_list = ['habr_html', 'habr_posts']):
        """
        Создание таблиц.
        Вход: 
            tables_list(list) - лист таблиц для создания.
        Выход: 
            нет.
        """
        Base = declarative_base()

        class HabrHtml(Base):
            __tablename__ = 'habr_html'
            __table_args__ = {'schema': self.schema, 'comment': 'html код'}    
            link = Column('link', String(), primary_key = True, comment = 'Ссылка на post')
            html = Column('html', Text(),  nullable = False, comment = 'html')
            date_load = Column('date_load', TIMESTAMP(), nullable = False, comment = 'Дата загрузки')
            id = Column('id', Integer(), nullable = False, comment = 'Идентифкатор post')    

        class HabrPosts(Base):
            __tablename__ = 'habr_posts'
            __table_args__ = {'schema': self.schema, 'comment': 'Данные с habr'}     
            id = Column('id', Integer(), primary_key = True, nullable = False, comment = 'Идентифкатор post')    
            title = Column('title', String(), comment = 'Тема поста')
            text = Column('text', Text(),  nullable = False, comment = 'Текст поста')
            lem_text = Column('lem_text', Text(),  nullable = False, comment = 'Лемматизированная тема, текст, теги и автор поста') 
            tags_text = Column('tags_text', Text(),  nullable = False, comment = 'Теги') 
            user_id = Column('user_id', String(),  nullable = False, comment = 'Идентификатор пользователей')
            is_like = Column('is_like', Boolean(),  nullable = False, comment = 'Признак того, что пост мне нравится') 
            date_load = Column('date_load', TIMESTAMP(), nullable = False, comment = 'Дата размещения поста')

        for table in tables_list: 
            if table not in self.inspector.get_table_names(schema = self.schema): 
    # Создаем таблицу для новинок
                HabrHtml.metadata.bind = self.engine
                HabrHtml.metadata.create_all(self.engine)
                HabrPosts.metadata.bind = self.engine
                HabrPosts.metadata.create_all(self.engine)   