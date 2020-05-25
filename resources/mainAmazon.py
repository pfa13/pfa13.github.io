#from urllib import robotparser
#import requests

import datetime
from bs4 import BeautifulSoup
import subprocess
import pandas as pd
import urllib2
import time
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark_llap import HiveWarehouseSession

header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

# def getRobots():
#     queue = ["https://www.amazon.es/gp/bestsellers/electronics/ref=zg_bs_nav_0"]
#     robotsUrl = "https://www.amazon.es/gp/bestsellers/electronics/ref=zg_bs_nav_0/robots.txt"
#     parse = robotparser.RobotFileParser()
#     parse.set_url(robotsUrl)
#     parse.read()
#     if parse.can_fetch('*',queue[0]):
#             return True

def scraping():
    product_num = 0
    productList = []
    now = datetime.datetime.now()
    current_date = now.strftime("%Y-%m-%d %H:%M:%S")
    mainpage = "https://www.amazon.es/gp/bestsellers/electronics/ref=zg_bs_nav_0"
    settings = [
        ('spark.sql.hive.hiveserver2.jdbc.url',
        'jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default'), 
    ]
    
    for i in range(2):
        #content = requests.get(mainpage)#, headers=self.header)
        req = urllib2.Request(mainpage)
        content = urllib2.urlopen(req).read()
        #bs = BeautifulSoup(content.text, 'html.parser')
        bs = BeautifulSoup(content, 'html.parser')
        data = bs.find('div', {"id": "zg-center-div"})
        bs = BeautifulSoup(str(data), 'html.parser')
        products = bs.find_all('li', {"class":"zg-item-immersion"})        

        for product in products: 
            print("------------------------------")
            product_id = product.find('span', {"class":"zg-badge-text"}).text 
            product_id = product_id[1:] # Hace falta quitarle el #
            print(product_id)            
            product_link_info = product.find('a', {"class":"a-link-normal"})
            product_url = "https://www.amazon.es"
            if product_link_info is not None:
                product_url = product_url + product_link_info['href']
            print(product_url)              
            product_image_info = product.find('span', {"class":"zg-text-center-align"}).find('img')
            if product_image_info is not None:
                product_image = product_image_info['src']
                product_title_text = product_image_info['alt'].encode('utf-8').decode('ascii', 'ignore')
                product_title = product_title_text.replace("'", "")
            else: 
                product_image = "None"
                product_title = "None"
            print(product_image)         
            # product_title = product.find('span', {"class":"zg-text-center-align"}).find('img')['alt'] 
            # product_title_text = product_title.encode('utf-8').decode('ascii', 'ignore')
            # product_title = product_title_text.replace("'", "")
            print(product_title)
            product_price_info = product.find('span', {"class":"p13n-sc-price"})
            if product_price_info is not None:
                product_price_text = product.find('span', {"class":"p13n-sc-price"}).text
                product_price = product_price_text[:-2].replace(',', '.')
            else:
                product_price = "0"
            print(product_price)
            product_stars_info = product.find('div', {"class":"a-icon-row"})
            if product_stars_info is not None:
                product_stars_text = product_stars_info.find('span', {"class":"a-icon-alt"}).text
                product_stars = product_stars_text[:3].replace(',', '.')
                product_stars_count_text = product_stars_info.find('a', {"class":"a-size-small"}).text
                product_stars_count = product_stars_count_text.replace('.', '')
            else:
                product_stars = "0"
                product_stars_count = "0"
            print(product_stars)
            print(product_stars_count)
            product_prime_info = product.find('i', {"class":"a-icon-prime"})
            if product_prime_info is not None:
                product_prime = "1"
            else:
                product_prime = "0"
            print(product_prime)
                        
            #productList[product_num] = [current_date, product_id, product_title, product_url, product_image, product_stars, product_stars_count, product_price, product_prime]
            productList.append([current_date, product_id, product_title, product_url, product_image, product_stars, product_stars_count, product_price, product_prime])
            product_num += 1  
            
        next_page_info = bs.find('ul', {"class":"a-pagination"}).find('li', {"class":"a-last"}).find('a')
        if next_page_info is not None:
            next_page = next_page_info['href']
            mainpage = next_page
            print(next_page)
            time.sleep(60)
        else:
            print("THE END - MANAGEMENT OF DATA")

            conf = SparkConf().setAppName("Pyspark and Hive!").setAll(settings)
            #Spark 2: use SparkSession instead of SparkContext.
            spark = (
                SparkSession
                .builder
                .config(conf=conf)
                # There is no HiveContext anymore either.
                .enableHiveSupport()
                .getOrCreate()
            )
            hive = HiveWarehouseSession.session(spark).userPassword('hive','hive').build()

            df = pd.DataFrame.from_records(productList)
            df.columns = ['ScrapDate', 'Id', 'Title', 'Url', 'Image', 'Stars', 'StarsCount', 'Price', 'Prime']
            df.to_csv('AmazonProducts' + now.strftime("%m%d%Y%H%M%S") + '.csv', index=False)

            subprocess.call('hdfs dfs -copyFromLocal ./AmazonProducts' + now.strftime("%m%d%Y%H%M%S") + '.csv hdfs://sandbox-hdp.hortonworks.com:8020/tmp', shell=True)

            hive.executeUpdate("CREATE DATABASE IF NOT EXISTS amazontfm")
            hive.setDatabase("amazontfm")
            hive.executeUpdate("CREATE TABLE IF NOT EXISTS bestsellers(ScrapDate timestamp, Id int, Title string, Url string, Image string, Stars decimal(6,2), StarsCount int, Price decimal(6,2), Prime int)")
            for index, row in df.iterrows(): 
                print("Inserting row: " + str(index))
                print("INSERT INTO bestsellers VALUES('" + row["ScrapDate"] + "', " + row["Id"] + ", '" + row["Title"] + "', '" + row["Url"] + "', '" + row["Image"] + "', " + row["Stars"] + ", " + row["StarsCount"] + ", " + row["Price"] + ", " + row["Prime"] + ")")
                hive.executeUpdate("INSERT INTO bestsellers (ScrapDate, Id, Title, Url, Image, Stars, StarsCount, Price, Prime) VALUES('" + row["ScrapDate"] + "', " + row["Id"] + ", '" + row["Title"] + "', '" + row["Url"] + "', '" + row["Image"] + "', " + row["Stars"] + ", " + row["StarsCount"] + ", " + row["Price"] + ", " + row["Prime"] + ")")
                print("Row inserted: " + str(index))
            
            print("THE END")

# if(getRobots()):
print("Init WebScraping against Amazon")
scraping()
# else:
#     print("Amazon WebScraping not supported")