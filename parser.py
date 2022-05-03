from cgitb import text
from datetime import datetime
from multiprocessing import connection
from os import link
from lxml import html
from urllib import response
import requests
import mysql.connector
from bs4 import BeautifulSoup
import dateparser
from urllib.parse import urlparse

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
}

WHITE = '\033[0;37m'
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[0;33m'
BLUE = '\033[0;34m'



def main(): # отправка данных в бд
    resourcedb = select_items("SELECT * FROM resource")

    for row in resourcedb:
        items = parsing(row)
        send_to_db("INSERT INTO `items` (res_id, link, title, content, nd_date, s_date, not_date) VALUES (%s, %s, %s, %s, %s, UNIX_TIMESTAMP(CURRENT_TIMESTAMP), %s)", items)
        

def select_items(query): # подключение к базе и взятие из таблиц resource тегов
    db = mysql.connector.connect(host='localhost', user='dima', password='dima', db='bd', charset='utf8mb4', autocommit=True, buffered=True)                 
    cursor = db.cursor()

    try:
        cursor.execute(query)
        answer = cursor.fetchall()
        return answer
    except Exception as ex:
        print(ex)
    finally:
        print("Successful SELECT")
        cursor.close()
        db.close()

def send_to_db(query, params): # подключение к бд и и делает отправку спарсенных данных в бд
    db = mysql.connector.connect(host='localhost', user='dima', password='dima', db='bd', charset='utf8mb4', autocommit=True, buffered=True)                 
    cursor = db.cursor()

    try:
        cursor.executemany(query, params)
    except Exception as ex:
        print(ex)
    finally:
        print("Successful SELECT")
        cursor.close()
        db.close()
    
def correct_url(base_url, url): # испрвляет битые ссылки типо: /article/sport/....
    base_url = str(urlparse(base_url).scheme) + "://" + str(urlparse(base_url).netloc)
    
    if (str(url).startswith("http")):
        return url
    else:
        return str(base_url) + str(url)

# def check(query):
#     db = mysql.connector.connect(host='localhost', user='dima', password='dima', db='bd', charset='utf8mb4', autocommit=True, buffered=True)                 
#     cursor = db.cursor()
    
#     try:
#         cursor.execute(query)
#         answer = cursor.fetchall()
#         return answer
#     except Exception as ex:
#         print(ex)
#     finally:
#         print("Successful SELECT")
#         cursor.close()
#         db.close()



def parsing(row):
    resource_id, resource_name, resource_url, top_tags, bottom_tags, title_cuts, date_cut = row
    r = requests.get(resource_url)
    soup = BeautifulSoup(r.text, 'lxml')
    links = [correct_url(resource_url, l.select_one('*').get('href')) for l in soup.select(top_tags)] # берет ссылки с главной страницы перебирает их и фиксит битые ссылки
    
    items = []
    for link in links:
        response1 = requests.get(link)
        soup1 = BeautifulSoup(response1.text, 'lxml')

        contents = ''.join(text.text.strip() for text in soup1.find_all(class_= bottom_tags)) # текст новости из ссылки
        title = soup1.find(title_cuts).text # берет тайтл новости из новостной ссылки
        date_news = soup1.find(date_cut).get('datetime') # берет дату и время из новостной ссылки
        date_times = dateparser.parse(date_news) # парсит дату в нормальный объект для дальнейшей работы и конвератции
        date_times_YMD = datetime.date(date_times).strftime('%Y:%m:%d') # дата новости в формате Год-Месяц-День.
        date_times_UNIX = date_times.timestamp() # дата новости в формате UNIX
        item = (resource_id, link, title, contents, date_times_UNIX, date_times_YMD) #словарь в массиве
        items.append(item) # помещает список в список
        
    return items
if __name__ == "__main__":
    main()