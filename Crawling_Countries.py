#!/usr/bin/env python
# coding: utf-8

# In[1]:


import urlparse
import urllib
import requests
import csv
import json
import time
from pprint import pprint
from bs4 import BeautifulSoup
import Queue
import threading


# In[2]:



def perform_act(q_in, q_out):
    while True:
        try:
            line = q_in.get(block=True, timeout=3)
        except:
            break

        print line
        complain = ""
        link = str(line).replace('-','')
        phone_number = link.replace('\n','')
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                             '(KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
               'cookie': 'split=ver~4&DIA_PRICE~CONTROL; locale=ver~2&country~USA&currency~USD&language~en-us&productSet~BN; clrdsearch=ver~2&view~grid; devconfig=ver~4&debugmode~false&force_serve_awesome~false&force_serve_non_awesome~false&force_serve_non_pre_launch~false&force_serve_non_solr~false&force_serve_pre_launch~false&force_serve_solr~false; GUID=BF7725DE_BDD8_47C2_A8D0_149E29104983; browserCheckCookie=true; migrationstatus=ver~2&redirected~false; sitetrack=ver~3&jse~1; bnper=ver~6&NIB~0&DM~-&GUID~BF7725DE_BDD8_47C2_A8D0_149E29104983&SESS-CT~1&STC~CF9WDJ&FB_MINI~false&SUB~false; bnses=ver~2&cdclosed~false&ace~false&isbml~false&livechat~false&fbcs~false&imeu~false&ss~0&mbpop~false&sswpu~true&deo~false&nogtm~false; pop=ver~3&belpop~false&china~false&emailpop~false&french~false&ie~false&internationalSelect~false&iphoneApp~false&survey~false&uae~false&webroompop~false&sweeps_count~1&sweeps_date~20170702; bncat=ver~5&currency~USD&id~Preset+Engagement+Rings+Catalog&oids~50890&productSet~BN&sort~RBS; device=ver~2&device_type~Desktop&orientation~Landscape&resolution~1519x735; _ga=GA1.2.1646956438.1498838145; _gid=GA1.2.1651108146.1498962238; mp_blue_nile_mixpanel=%7B%22distinct_id%22%3A%20%2215cf9b719fb7a4-0024103b1926d-8383667-144000-15cf9b719fc4da%22%7D; dsearch=ver~7&visible~800040000&newUser~false&state~---------------------1------0%2Cnull%2Cnull-tablepercent-asc-USD-1%2C2%2C3%2C4%2C5%2C6%2C7-3%2C4%2C5%2C6-1%2C2%2C3%2C4%2C5%2C6%2C7%2C8--false; _bcvm_vid_3855064483794580490=618350636304221973T50799A8A8C3AF3B2210499F5B9EFDC3D830C2972CDD93D424C74D0EEC939236998BC4A876D9000C6B6FD6A97D0F4CA97FEB7D1D2F67A19559C0615B7F9071764; _bcvm_vrid_3855064483794580490=618349395160811315T3BC553462AF75E3F824DFE056C8257177F2F4C826C4ABA8EED7256FF88BF18C905D680DBA0AE005E08C6940DE225787C15E35D2360B3166388A08092E045F7E7'}
        url = "https://scamnumbers.info/shownumber/" + link
        try:
            html = requests.get(url, headers=headers)
        except:
            print "Connection refused by the server.. Continuing after 5 seconds"
            time.sleep(5)
        soup = BeautifulSoup(html.content, 'html.parser')

        for row in soup.find_all('div',attrs={"class" : "col-xs-10"}):
            record = {}
            for strong_tag in row.contents[3].find_all('strong'):

                record.update({  
                strong_tag.text: strong_tag.next_sibling,
                })
        for scam in soup.find_all('div',attrs={"class" : "panel-heading"}):
            for scam_type in scam.find_all('h3',attrs={"class" : "panel-title"}):
                record.update({  
                "Date": scam_type.text,
                })    

                #Update the imformation of type of spam
        for comment in soup.find_all("div",attrs={"id" : "collapseTwo"}):
            for comment_type in comment.find_all('p', limit=2):
                record.update({  
                "Information": comment_type.text,      
                })
            record.update({  
                    "Phone Number": phone_number,
                    })
            q_out.put(record)
    print "Quiting thread"



print "Processing ............." 

url = "https://scamnumbers.info/countries.php"
try:
    html = requests.get(url)
except:
    print
    "Connection refused by the server.. Continuing after 5 seconds"
    time.sleep(5)
soup = BeautifulSoup(html.content, 'html.parser')

urls = []
for links in soup.find_all('div',attrs={"class" : "col-sm-10 col-xs-12"}):
        for link in links.find_all('a', href=True):
            new = link.get('href')
            urls.insert(0,new)


for i in range(0, len(urls)):
    data = []
    text=str(urls[i])
    code=text[text.find("=")+1:]
    url1 = "https://scamnumbers.info/" + urls[i]
    try:
        html1 = requests.get(url1)
    except:
        print
        "Connection refused by the server.. Continuing after 5 seconds"
        time.sleep(5)

    soup1 = BeautifulSoup(html1.content, 'html.parser')
    coun = soup1.find("div", attrs={"class", "col-sm-10 col-xs-12"})
    test = coun.find('h1')
    test = test.text
    country = test[test.find("from")+5:]
    print "Processing for country " + country

    code_urls = []
    q_in = Queue.Queue()
    q_out = Queue.Queue()
    threads_arr = []

    all_links=soup1.find_all("a")
    for link in all_links:
        number = link.get_text()
        if number.isdigit():
            input_num = number
            q_in.put(input_num)

    for i in range(0, 8):
        t = threading.Thread(target=perform_act, args = (q_in, q_out))
        threads_arr.append(t)
        threads_arr[-1].daemon = True
        threads_arr[-1].start()
    for t in threads_arr:
        t.join()

    data = list(q_out.queue)		   
    dest_file_name = 'datasample_' + country + ".json" #Generates the json file with the details of spam number for every country
    with open(dest_file_name, 'w') as outfile:  
        json.dump(data, outfile, indent=1)

    print "Processing Complete for " + country



