from selenium import webdriver
import time
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from tianyancha.core import tokens
desired_capabilities = DesiredCapabilities.FIREFOX  # 修改页面加载策略
desired_capabilities["pageLoadStrategy"] = "eager"  # 注释这两行会导致最后输出结果的延迟，即等待页面加载完成再输出


def format_name(s):
    upper_index = []
    for i in range(len(s)):
        if s[i].isupper():
            upper_index.append(i)
    k = list(s)
    for i in range(len(upper_index)):
        k.insert(upper_index[i]+i, '_')
    return ''.join(k).upper()


if __name__ == '__main__':
    dr = webdriver.Firefox()  # 初始化一个火狐浏览器实例：driver
    dr.maximize_window()  # 最大化浏览器
    for i in tokens:
        mob = i['mobile']
        passwd = i['cdpassword']
        dr.get("https://www.tianyancha.com/login")
        ser = dr.find_element_by_xpath(
            '//div[@class="module module1 module2 loginmodule collapse in"]/div[@class="over-hide f18 point"]/div[@onclick="changeCurrent(1);"]')
        ser.click()
        time.sleep(0.2)
        phone_input = dr.find_element_by_xpath(
            '//div[@class="modulein modulein1 mobile_box pl30 pr30 f14 collapse in"]/div/input[@class="_input input_nor contactphone"]')
        phone_input.clear()
        phone_input.send_keys(mob)
        time.sleep(0.2)
        passwd_input = dr.find_element_by_xpath(
            '//div[@class="modulein modulein1 mobile_box pl30 pr30 f14 collapse in"]/div/input[@class="_input input_nor contactword"]')
        passwd_input.clear()
        passwd_input.send_keys(passwd)
        time.sleep(0.2)
        submit = dr.find_element_by_xpath(
            '//div[@class="modulein modulein1 mobile_box pl30 pr30 f14 collapse in"]/div[@class="c-white b-c9 pt8 f18 text-center login_btn"]')
        submit.click()
        dr.get('https://hub.tianyancha.com/search?key=td')
        text = dr.find_elements_by_xpath('//div[@class="content"]/div')[0].text
        if u'我们只是确认一下你不是机器人，' in text:
            time.sleep(10)
        print(f'finish{mob}')
