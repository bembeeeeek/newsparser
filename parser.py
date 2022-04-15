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

def send_to_db(query):
    db = mysql.connector.connect(host='localhost', user='dima', password='dima', db='bd', charset='utf8mb4', autocommit=True, buffered=True)                 
    cursor = db.cursor()

    try:
        cursor.execute(query)
        answer = cursor.fetchall()
    except Exception as ex:
        print(ex)
    finally:
        print("Successful SELECT")
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
    
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
}

WHITE = '\033[0;37m'
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[0;33m'
BLUE = '\033[0;34m'

r = requests.get(resource_url)
soup = BeautifulSoup(r.text, 'lxml')



def parsing():
    # url = "https://www.nur.kz/latest/"
    # response = requests.get(url)
    # soup = BeautifulSoup(response.content, 'lxml')
    links = soup.find_all('a', class_= top_tags) # берет ссылки с главной страницы

    items = []

    for link in links:
        link = link.get('href')
        if link[:18] == "https://www.nur.kz":
             # собирает все ссылки на новости
            response1 = requests.get(link)
            soup1 = BeautifulSoup(response1.content, 'lxml')
            title = soup1.find('h1',class_= title_cuts).text # берет тайтл новости из новостной ссылки
            contents = [text.text for text in soup1.find_all('div', class_= bottom_tags)] # текст новости из ссылки
            date_news = soup1.find('time', class_= date_cut).get('datetime') # берет дату и время из новостной ссылки
            date_times = dateparser.parse(date_news)
            date_times_YMD = datetime.date(date_times).strftime('%Y:%m:%d') # дата новости в формате Год-Месяц-День.
            date_times_UNIX = date_times.timestamp() # дата новости в формате UNIX
            items.append((link, title, contents, date_times, date_times_YMD, date_times_UNIX))

    for item in items:
        # print(item[0], item[1], item[2])
        send_to_db()
        

        cursor.execute("INSERT INTO `items` (res_id, link, title, content, nd_date, s_date, not_date) VALUES (%s, %s, %s, %s, %s, %s)", (link, title, contents, date_times_UNIX, date_times_YMD))


















        #print(f'{WHITE} Ссылка: {RED} {item[0]} \n {WHITE} Тайтл: {BLUE} {item[1]} \n {WHITE} Контент: \n {YELLOW} {item[2][0]}')



        # mycursor.execute("INSERT INTO `items` (link, title) VALUES (%s, %s)", (item[0], item[1]))
        
        
    #     mydb.commit()  

    #     mycursor.execute("INSERT INTO `items` (link, title) VALUES (%s, %s)", (item[0], item[1]))
          
    #         # mycursor.execute("INSERT INTO `items` (res_id, link, title, content, nd_date, s_date, not_date) VALUES (%s, %s, %s, %s, %s, %s)", (link, title, contents, date_times_UNIX, date_times_YMD))
    # mydb.close()
        #mycursor.execute("INSERT INTO `items` (res_id, link, title, content, nd_date, s_date, not_date) VALUES (%s, %s, %s, %s, UNIX_TIMESTAMP(CURRENT_TIMESTAMP), %s)",(id, link, title, contents, date_times_UNIX, date_times_YMD))
# params = (id, name, more_urlss, more_titles, text_news, date_times_UNIX, date_times_YMD)
parsing()