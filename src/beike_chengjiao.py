import time
import redis
from selenium import webdriver
from selenium.webdriver import ChromeOptions
import datetime
from elasticsearch import elasticsearch


es = Elasticsearch(hosts="10.147.20.96:9200")
collection = db['beike_house_chengjiao']
from lxml import etree


pool = redis.ConnectionPool(host='10.147.20.96', port=6379, db=7,decode_responses=True)
option = ChromeOptions()
option.add_argument('--disable-infobars')  # 禁用浏览器正在被自动化程序控制的提示
# 反爬机制代码开始，采用此代码在F12控制台输入window.navigator.webdriver结果不是True，而是undefined就成功了
option.add_experimental_option('excludeSwitches', ['enable-automation'])
option.add_argument('--no-sandbox')
option.add_argument('--disable-dev-shm-usage')
# option.add_argument('--headless')
option.add_argument('blink-settings=imagesEnabled=false')
# option.add_argument('--disable-gpu')
driver = webdriver.Chrome(options=option)
driver.get('https://sh.ke.com/')
time.sleep(2)
# driver.find_element_by_xpath('/html/body/div[20]/div[4]').click()  # 关闭弹出框
# time.sleep(1)

# /html/body/div[1]/div/div[2]/div/div/div/span/a[1]/span
driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/span/a[1]/span').click()  # 选择登录
time.sleep(1)
driver.find_element_by_xpath('//*[@id="loginModel"]/div[2]/div[2]/form/div[8]/a').click()  # 选择账号密码登录
time.sleep(1)

# 输入自己已经注册好的账号（最好是手机号哟）
driver.find_element_by_xpath('//*[@id="loginModel"]/div[2]/div[2]/form/ul/li[1]/input').send_keys('13163301778')

time.sleep(1)
# 输入密码
driver.find_element_by_xpath('//*[@id="loginModel"]/div[2]/div[2]/form/ul/li[3]/input').send_keys('python4house')
time.sleep(2)

# 点击登录
driver.find_element_by_xpath('//*[@id="loginModel"]/div[2]/div[2]/form/div[7]').click()
time.sleep(3)


# self.driver = webdriver.PhantomJS() # 无界面浏览已停止更新，建议使用headless
# 反爬机制代码结束
# 窗口最大化
driver.maximize_window()
# 隐式等待
# self.


def fetch_details(url):
    logger.info(url)
    driver.get(url )
    driver.implicitly_wait(10)
    data = driver.page_source
    html = etree.HTML(data)
    for district in html.xpath('//*[@id="beike"]/div[1]/div[3]/div[1]/dl[2]/dd/div/div/a'):
        district_name =district.xpath("text()")[0]
        district_url = url.split('/chengjiao')[0] + district.xpath("@href")[0]

        print(district.xpath("text()")[0])
        print(district_url)
        # /html/body/div[1]/div[3]/div[1]/dl[2]/dd/div/div/a[1]
        # //*[@id="beike"]/div[1]/div[3]/div[1]/dl[2]/dd/div/div/a[1]
        # district_name = district.xpath("div/a/text()")[0]
        # district_url = url.split('/chengjiao')[0]  + district.xpath("div/a/@href")[0]
        # print(district_name)
        # print(district_url)
        r.sadd("chengjiao_district3",str({"city_url":url.split('/chengjiao')[0],"district_url":district_url.split('/pg')[0],'district_name':district_name}))
    houses = html.xpath("/html/body/div[1]/div[5]/div[1]/div[4]/ul/li")
    for house in houses:
        house_url =  house.xpath("div/div[1]/a/@href")[0]
        title = house.xpath("div/div[1]/a/text()")[0]
        # /html/body/div[1]/div[5]/div[1]/div[4]/ul/li[4]/
        property = house.xpath("div/div[2]/div[1]/text()")[0]
        complete_time = house.xpath("div/div[2]/div[2]/text()")[0]
        unit =house.xpath("div/div[2]/div[3]/text()")[0]
        average = house.xpath("div/div[3]/div[2]/span/text()")[0]
        address = house.xpath("div/div[4]/span[2]/span/text()")[0]
        # /html/body/div[1]/div[5]/div[1]/div[4]/ul/li[4]/div/div[5]/span[2]/span[1]/text()
        want_sell = house.xpath("div/div[5]/span[2]/span[1]/text()")[0] if house.xpath("div/div[5]/span[2]/span[1]/text()") != [] else ""
        days_to_sell = house.xpath("div/div[5]/span[2]/span[2]/text()")[0] if house.xpath("div/div[5]/span[2]/span[2]/text()") != [] else ""
        price = house.xpath("div/div[2]/div[3]/span/text()")[0]
        property2 = house.xpath("div/div[3]/div[1]/text()")[0]
        dic = {
            "house_url":house_url.strip(),
            "title":title.strip(),
            "price":int(price.strip()),
            "average":int(average.strip()),
            "property":property.strip(),
            "property2":property2.strip(),
            "unit":unit.strip(),
            "complete_time":complete_time.strip(),
            "address":address.strip(),
            "want_sell":want_sell.strip(),
            "days_to_sell":days_to_sell.strip(),
            "fetch_time": datetime.datetime.now()
        }
        logger.info(dic)
        # producer('beike_chengjiao', dic)
        es.index(index="beike_chengjiao2", doc_type="doc", id=house_url.split("/")[-1].replace(".html",''), body=dic)
        db['ershou_chengjiao'].update_one({"_id":house_url.split("/")[-1].replace('.html','')},{"$set":dic},upsert=True)
        collection.insert_one(dic)
        #
    #     /html/body/div[1]/div[5]/div[1]/div[5]/div[2]/div/a[1]
    next_urls = html.xpath("/html/body/div[1]/div[5]/div[1]/div[5]/div[2]/div/a/@href")
    logger.info(next_urls)
    next_url = next_urls[-1]
    if next_urls[0] == next_urls[-2]:
        raise EOFError
    fetch_details(url.split(".com")[0] + ".com" + next_url)


if __name__ == "__main__":

    r = redis.Redis(connection_pool=pool)
    while True:
        url = r.spop("chengjiao_district2")
        if url is None:
            print(url)
            break
        try:
            url = eval(url)['city_url'] + "/chengjiao/ddo41/"

            logger.info(url)
            fetch_details(url)
            url = eval(url)['city_url'] + "/chengjiao/ddo11/"
            logger.info(url)
            fetch_details(url)

            url = eval(url)['city_url'] + "/chengjiao/ddo21/"
            logger.info(url)
            fetch_details(url)
        except Exception as e:
            logger.info(e)


