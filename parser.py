from datetime import datetime
from multiprocessing import connection
from os import link
from lxml import html
from urllib import response
import requests
import mysql.connector
from bs4 import BeautifulSoup
import dateparser

# подключение к бд
mydb = mysql.connector.connect(
    host="localhost",
    user="dima",
    password="dima",
    database="bd"
)

mycursor=mydb.cursor()


mycursor.execute("SELECT * FROM resource")
itembd = mycursor.fetchall()



headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
}

url = "https://www.nur.kz/latest/"

r = requests.get(url)
soup = BeautifulSoup(r.text, 'lxml')


for row in itembd:
    id = row[0]
    name = row[1]
    urls = row[2]
    top_tags = row[3]
    bottom_tags = row[4]
    title_cuts = row[5]
    date_cut = row[6]

def parsing():
    url = "https://www.nur.kz/latest/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'lxml')
    links = soup.find_all('a',class_='article-preview-category__content') # берет ссылки с главной страницы
    
    # titles = soup.find_all('h2', class_='article-preview-category__subhead') берет тайтлы с главной страницы 
    nurkz = "https://www.nur.kz"
    res_name = "Информационный портал nur.kz"

    items = []

    for link in links:
        link = link.get('href') # собирает все ссылки на новости
        response1 = requests.get(link)
        soup1 = BeautifulSoup(response1.content, 'lxml')
        title = soup1.find('h1',class_='main-headline js-main-headline').text # берет тайтл новости из новостной ссылки
        contents = [text.text for text in soup1.find_all('div', class_='formatted-body io-article-body')] # текст новости из ссылки
        date_news = soup1.find('time', class_='datetime datetime--publication').get('datetime') # берет дату и время из новостной ссылки
        date_times = dateparser.parse(date_news)
        date_times_YMD = datetime.date(date_times).strftime('%Y:%m:%d') # дата новости в формате Год-Месяц-День.
        date_times_UNIX = date_times.timestamp() # дата новости в формате UNIX
        items.append((link, title, contents, date_news, date_times_YMD, date_times_UNIX))

    for item in items:
        print(item[0], item[1], item[])


    # for datestr in date_news: # берет дату и время из новостной ссылки
    #     date_times = datestr.get('datetime')
    #     date_times = dateparser.parse(date_times)
    #     date_times_YMD = datetime.date(date_times).strftime('%Y:%m:%d') # дата новости в формате Год-Месяц-День.
    #     date_times_UNIX = date_times.timestamp() # дата новости в формате UNIX

        # params = (id, name, more_urlss, more_titles, text_news, date_times_UNIX, date_times_YMD)

        # send = ("INSERT INTO `items` (res_id, link, title, content, nd_date, s_date, not_date) VALUES (%s, %s, %s, %s, UNIX_TIMESTAMP(CURRENT_TIMESTAMP), %s)"), params
        # mycursor.execute(send)

        
    # mydb.commit()        
    # mydb.close()
        #mycursor.execute("INSERT INTO `items` (res_id, link, title, content, nd_date, s_date, not_date) VALUES (%s, %s, %s, %s, UNIX_TIMESTAMP(CURRENT_TIMESTAMP), %s)",(id, more_urlss, more_titles, text_news, date_times_UNIX, date_times_YMD))

parsing()