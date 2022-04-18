from datetime import datetime
from multiprocessing import connection
from os import link
from lxml import html
from urllib import response
import requests
import mysql.connector
from bs4 import BeautifulSoup
import dateparser


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

def send_to_db(query, params):
    db = mysql.connector.connect(host='localhost', user='dima', password='dima', db='bd', charset='utf8mb4', autocommit=True, buffered=True)                 
    cursor = db.cursor()

    try:
        cursor.executemany(query, params)
    except Exception as ex:
        print(ex)
    finally:
        print("Successful INSERT")
        cursor.close()
        db.close()
    
    #itembd = mycursor.fetchall()

itembd = select_items("SELECT * FROM resource")

for row in itembd:
    resource_id = row[0]    
    resource_name = row[1]
    resource_url = row[2]
    top_tags = row[3]
    bottom_tags = row[4]
    title_cuts = row[5]
    date_cut = row[6]

#print(resource_url)
    
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
}

WHITE = '\033[0;37m'
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[0;33m'
BLUE = '\033[0;34m'

def parsing():
    r = requests.get(resource_url)
    soup = BeautifulSoup(r.text, 'lxml')
    links = soup.find_all(class_= top_tags) # берет ссылки с главной страницы
    
    items = []

    for link in links:
        link = link.get('href')
        if link[:18] == "https://www.nur.kz":
             # собирает все ссылки на новости
            response1 = requests.get(link)
            soup1 = BeautifulSoup(response1.content, 'lxml')
            title = soup1.find(class_= title_cuts).text # берет тайтл новости из новостной ссылки
            contents = [text.text for text in soup1.find_all(class_= bottom_tags)] # текст новости из ссылки
            date_news = soup1.find(class_= date_cut).get('datetime') # берет дату и время из новостной ссылки
            date_times = dateparser.parse(date_news)
            date_times_YMD = datetime.date(date_times).strftime('%Y:%m:%d') # дата новости в формате Год-Месяц-День.
            date_times_UNIX = date_times.timestamp() # дата новости в формате UNIX
            date_in_unix_db = "UNIX_TIMESTAMP(CURRENT_TIMESTAMP)"

            item = [resource_id, link, title, contents, date_times_UNIX, date_in_unix_db, date_times_YMD] #словарь в массиве
            items.append(item)
    send_to_db("INSERT INTO `items` (res_id, link, title, content, nd_date, s_date, not_date) VALUES (%s, %s, %s, %s, %s, %s, %s)", items)
    

        #print(f'{WHITE} Ссылка: {RED} {item[0]} \n {WHITE} Тайтл: {BLUE} {item[1]} \n {WHITE} Контент: \n {YELLOW} {item[2][0]}')



parsing()
