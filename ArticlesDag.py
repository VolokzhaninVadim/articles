###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################
# Для работы с Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator import BashOperator
import datetime

# Для получения данных с Habr
from articles.src.HabrScraper import HabrScraper

# Для работы с операционной сисемой 
import os

# Получаем переменные окружения
pg_password = os.environ['PG_PASSWORD']
pg_login = os.environ['LOGIN_NAME']
pg_host = os.environ['PG_HOST']
password_tor = os.environ['PASSWORD_TOR']

# Создаем функцию для получения новых данных с Habr
def article_scraper():
    """
    Получаем новые данные с Habr.
    Вход: 
        нет.
    Выход: 
        нет.
    """
    article_scraper = HabrScraper(
        pg_password = pg_password
        ,pg_login = pg_login
        ,host = pg_host
        ,password_tor = password_tor
    )
    return article_scraper


# Вводим по умолчанию аргументы dag
default_args = {
    'owner': 'Volokzhanin Vadim',
    'start_date': datetime.datetime(2021, 8, 21),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes = 2)
}

##############################################################################################################################################
############################################### Создадим DAG и поток данных ##################################################################
############################################################################################################################################## 
with DAG(
    "article_loader", 
    description = "Получение статей"
    ,default_args = default_args
    ,catchup = False
    ,schedule_interval = "@once"
    ,tags=['article_loader']) as dag:

# Получаем статьи с Habr
    all_new_habr = PythonOperator(
        task_id = "habr_loader", 
        python_callable = all_new_habr().all_new, 
        dag = dag
        ) 

# Порядок выполнения задач
all_new_habr  
