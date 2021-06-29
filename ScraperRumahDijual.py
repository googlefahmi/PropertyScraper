#!/usr/bin/env python
# coding: utf-8

# In[517]:


import requests
import re
import json
import os
import csv
import pandas as pd
from bs4 import BeautifulSoup as bs
import logging
import time
import threading
import lxml
import unicodedata
import pandas as pd
import datetime as dt
import sys
from threading import Thread
from time import sleep
from timeit import default_timer as timer
from datetime import timedelta
import multiprocessing
from itertools import product
from concurrent.futures import ThreadPoolExecutor

# In[604]:

def clean_tab(teks):
    pattern = r'[\t\r]'
    teks = re.sub(pattern, '', teks)
    return teks

def clean_all(teks):
    pattern = r'[\t\r\n]'
    teks = re.sub(pattern, ' ', teks)
    return teks

def kamar_tidur(teks):
    pattern = r'Kamar tidur: ([0-9+]*)'
    kamar = re.search(pattern, str(teks)).group(1)
    return kamar

def kamar_mandi(teks):
    pattern = r'Kamar mandi: ([0-9+]*)'
    kamar_mandi = re.search(pattern, str(teks)).group(1)
    return kamar_mandi

def kamar_pembantu(teks):
    pattern = r'Kamar pembantu: ([0-9+]*)'
    kamar_pembantu = re.search(pattern, str(teks)).group(1)
    return kamar_pembantu

def luas_tanah(teks):
    pattern = r'Luas tanah: (\d*)'
    luas_tanah = re.search(pattern, str(teks)).group(1)
    return luas_tanah

def luas_bangunan(teks):
    pattern = r'Luas bangunan: (\d*)'
    luas_bangunan = re.search(pattern, str(teks)).group(1)
    return luas_bangunan

def extract_price(teks):
    pattern = r'[Rp. a-zA-Z]'
    price = re.sub(pattern, '', teks)
    return price

def extract_person(teks):
    person = re.search(r'([a-zA-Z ]*)', teks).group(1)
    return person.strip()

def extract_phone(teks):
    phone = re.search(r'(\d+)', teks).group(1)
    return phone.strip()

def extract_config(filename='Scraper Config.txt'):
    with open(filename) as c:
        text = c.readlines()
    f_pattern = r'filename: ([a-zA-Z0-9.txt]*)'
    f_c = re.search(f_pattern, text[0]).group(1)
    
    start_pattern = r'start: ?(\d*)'
    start_c = re.search(start_pattern, text[1]).group(1)
    
    end_pattern = r'end: ?(\d*)'
    end_c = re.search(end_pattern, text[2]).group(1)
    
    return f_c, start_c, end_c


# In[608]:


def save_error_id(property_id, filename='error_id.txt'):
    '''
    Parameter:
    filename = property id will be saved in default filename named error_id.txt, 
                but you can change it to other filename. (Recommend = keep in default filename)
    property_id = a property id that failed to be written by parse_html function
    
    Return:
    A list of error id that saved in file named 'error_id.txt' (if the config keep in default)
    '''
    
    id_error_file = 'error_id.csv'
    if os.path.exists(f'Database/{id_error_file}'):
        method = 'a'
        e = open(f'Database/{id_error_file}', method, encoding='utf-8')
        writer = csv.writer(e)
        
    else:
        fieldname = 'id'
        method = 'w'
        e = open(f'Database/{id_error_file}', method)
        writer = csv.writer(e)
        writer.writerow(fieldname)
        
    writer.writerow([*property_id])
    logging.info(f'{property_id} successfully saved in {id_error_file}.')
    e.close()

def save_items(items_list, filename='001.txt'):
    '''
    Parameter:
    filename =  property id will be saved in default filename named 001.csv, 
                but you can change it to other filename. (Recommendation = Change filename if previous
                file already inserted in admintopia website)
    item_list = contains of necessary items as a requirements to fulfill www.admintopia.com form of new property.
    These necessary items are: id, judul, user, nohp, nohp2, kamar tidur, kamar mandi, kamar pembantu, luas tanah
    luas bangunan, lantai, harga, kota, sertifikat, deskripsi, img (list of urls)
    
    Return:
    A list of items that saved in file named '001.txt' (if the config keep in default)
    '''
    if os.path.exists(f'Raw Data/{filename}'):
        method = 'a'
        f = open(f'Raw Data/{filename}', method, encoding='utf-8')
        writer = csv.writer(f)

    else:
        fieldnames = ['id', 'judul', 'user', 'nohp_alt', 'nohp', 'kamar tidur', 'kamar mandi',
                       'kamar pembantu', 'luas tanah', 'luas bangunan', 'lantai', 'harga',
                       'kota', 'sertifikat', 'deskripsi', 'img']
        method = 'w'
        f = open(f'Raw Data/{filename}', method, encoding='utf-8')
        writer = csv.writer(f)
        writer.writerow(fieldnames)
    
    writer.writerow(items_list)
    f.close()

def thread_start(i):
    filename = outputfile
    logging.info(f'Parsing and writing property id: {i}.....')
    #print(f'Parsing and writing property id: {i}.....done')
    url = f'https://rumahdijual.com/{i}'
    page = requests.get(url)
    soup = bs(page.content, 'lxml')
    prop_info = ''
    sleep(3)

#   try:
    if bool(soup.find('td', text=re.compile('Properti ini sudah laku'))):
        logging.info(f'Parsing and writing property id: {i}.....sold, skip')
        #print(f'Parsing and writing property id: {i}.....sold, skip')

    else:
        ### Title ###
        title = soup.title.text

        ### Description ###
        try:
            desc_raw = soup.find_all('td', class_='alt2')
            desc = clean_tab(desc_raw[2].text)
        except:
            desc = ''

        ### Other infos ###
        try:
            prop_info_raw = soup.find('div', id=re.compile('post_message_'))
            prop_info = clean_all(prop_info_raw.text)
        except:
            try:
                prop_info_raw = soup.find('div', id=re.compile('post_message_'))
                prop_info = clean_all(prop_info_raw.text)
            except:
                logging.info(f'{i} Error')
                save_error_id(str(i))
                
        try:
            bedrooms = kamar_tidur(prop_info)
        except:
            bedrooms = ''
        try:
            bathrooms = kamar_mandi(prop_info)
        except:
            bathrooms = ''
        try:
            assistrooms = kamar_pembantu(prop_info)
        except AttributeError:
            assistrooms = ''
        try:
            luastanah = luas_tanah(prop_info)
        except:
            luastanah = ''
        try:
            luasbangunan = luas_bangunan(prop_info)
        except:
            luasbangunan = ''
        
        kota = soup.find('link', href=True)['href'].split('/')[3]
        try:
            sertifikat_raw = soup.find_all('b', text=re.compile('Sertifikat', re.IGNORECASE))
            sertifikat = sertifikat_raw[0].text
        except:
            sertifikat = ''

        ### HARGA ###
        try:
            harga_raw = soup.find('font', color='green').text
            hargas = extract_price(harga_raw)
            harga = re.sub('\D', '', hargas)
        except:
            harga = '0'
        lantai = ''

        ### Contact Person ###
        contact_person = soup.find('div', style="font-size:120%;margin-top:6px;").b.text
        person = extract_person(contact_person)
        try:
            phone = extract_phone(contact_person)
        except:
            phone = ''

        try:
            phone_raw = soup.find('span', class_='nohp').text
            phone_2 = unicodedata.normalize("NFKD", phone_raw)
        except:
            phone_2 = ''

        ### IMAGES ###
        img_url = []
        for images in soup.find_all('a', id=re.compile('attachment'), href=True):
            img_url.append(images['href'])
        img_url = [*set(img_url)]

        items = [str(i), title, person, str(phone), str(phone_2), str(bedrooms), str(bathrooms), str(assistrooms), 
                    str(luastanah), str(luasbangunan), lantai, str(harga), kota, sertifikat, desc, str(img_url)]
        save_items(items, filename=filename)
            
#         except:
#             logging.error(f'Error at id {i}, please check the website.')
#             save_error_id(str(i))

def parse_html(start=4002000, end=4003000, filename='002.txt'):
    logging.info('Starting ...')
    ends = (start + end + 1)
    pool = ThreadPoolExecutor(max_workers=20)
    
    for i in range(start, ends):
        #Thread(target=thread_start, args=(i,filename,)).start()
        pool.submit(thread_start, i)
    
    pool.shutdown(wait=True) 
    logging.info("Scraping ID within range: " + str(start) + " - " + str(i) + " Done. Thank me later")
    #print("Scraping ID within range: " + str(start) + " - " + str(i) + " Done. Thank me later")
    
if __name__ == '__main__':
    global outputfile
    start_time = timer()
    # Change root logger level from WARNING (default) to NOTSET in order for all messages to be delegated.
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(format='[%(process)d - %(threadName)-9s] %(asctime)s - %(levelname)s-%(message)s',handlers=[logging.FileHandler(str(sys.argv[0])+".log"), logging.StreamHandler()])

    #filename = file_c
    file_c, start_c, end_c = extract_config()
    #parse_html(int(start_c), int(end_c), filename=filename)

    #editbyfahmi
    
    startme = sys.argv[1]
    endme = sys.argv[2]
    filename = str(sys.argv[3])
    
    outputfile = str(sys.argv[3])

    parse_html(int(startme), int(endme), filename=filename)
    end = timer()
    #logging.info("--- Total elapsed time: %s minutes(s) ---" % ((time.time() - start_time)/60))
    logging.info("--- Total elapsed time: "+str(timedelta(seconds=end-start_time))+" ---")
    
    





