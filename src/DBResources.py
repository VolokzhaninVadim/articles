###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################
# Для работы с SQL 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect
from sqlalchemy import (Column, String, Text, ForeignKey, Integer, TIMESTAMP, Text,\
                create_engine, MetaData, DATETIME, Boolean)
from sqlalchemy.schema import CreateSchema

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################
###############################################################################################################################################

engine = create_engine(f'postgres://{pg_login}:{pg_password}@{pg_host}:5432/{pg_login}')
inspector = inspect(engine)
metadata = MetaData(schema = inspector.get_schema_names(), bind = engine)

Base = declarative_base()

class HabrHtml(Base):
    __tablename__ = 'habr_html'
    __table_args__ = {'schema': 'article', 'comment': 'html код'}    
    link = Column('link', String(), primary_key = True, comment = 'Ссылка на post')
    html = Column('html', Text(),  nullable = False, comment = 'html')
    date_load = Column('date_load', TIMESTAMP(), nullable = False, comment = 'Дата загрузки')
    id = Column('id', Integer(), nullable = False, comment = 'Идентифкатор post')    
# HabrHtml.metadata.bind = engine
# HabrHtml.metadata.create_all(engine)

class HabrPosts(Base):
    __tablename__ = 'habr_posts'
    __table_args__ = {'schema': 'article', 'comment': 'Данные с habr'}     
    id = Column('id', Integer(), primary_key = True, nullable = False, comment = 'Идентифкатор post')    
    title = Column('title', String(), comment = 'Тема поста')
    text = Column('text', Text(),  nullable = False, comment = 'Текст поста')
    lem_text = Column('lem_text', Text(),  nullable = False, comment = 'Лемматизированная тема, текст, теги и автор поста') 
    tags_text = Column('tags_text', Text(),  nullable = False, comment = 'Теги') 
    user_id = Column('user_id', String(),  nullable = False, comment = 'Идентификатор пользователей')
    is_like = Column('is_like', Boolean(),  nullable = False, comment = 'Признак того, что пост мне нравится') 
    date_load = Column('date_load', TIMESTAMP(), nullable = False, comment = 'Дата размещения поста')
# HabrPosts.metadata.bind = engine
# HabrPosts.metadata.create_all(engine)

def create_db_scheme(schemas = ['article']): 
    """
    Создание схемм данных. 
    Вход: 
        schemas(dict) - словарь схем с описанием.
    Выход: 
        нет. 
    """   
    for schema in schemas: 
# Если нет схемы, то создаем ее 
        if schema not in metadata.schema: 
            engine.execute(CreateSchema(schema))
            
def truncate_db_tables(tables_list = ['article.habr_html', 'article.habr_posts']): 
    """
    Очистка необходимых объектов. 
    Вход: 
        tables_list(list) - лист с наименованиями таблиц.
    Выход: 
        нет.
    """
# Очищаем все необходимые таблицы
    for table in tables_list: 
        with data_loader.pg_engine.connect() as connection:
            connection.execute(f'truncate {table}')
            
def create_db_tables(tables_list = ['habr_html', 'habr_posts']):
    """
    Создание таблиц.
    Вход: 
        tables_list(list) - лист таблиц для создания.
    Выход: 
        нет.
    """
    for table in tables_list: 
        if table not in inspector.get_table_names(schema = 'article1'): 
# Создаем таблицу для новинок
            HabrHtml.metadata.bind = engine
            HabrHtml.metadata.create_all(engine)
            HabrPosts.metadata.bind = engine
            HabrPosts.metadata.create_all(engine)   