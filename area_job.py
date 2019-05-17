from lxml import etree, html
import redis
from requests.exceptions import RequestException
import requests
import happybase as hconn
import json
import traceback
from itertools import product
from scrapy.core import DynamicIpAgent, logger, NeedValidException, OraclePersist, CookieListEmptyException
from scrapy.cache import HangYeRedisQueue


r = redis.StrictRedis(host="133.0.189.11", port=6379, decode_responses=True, password='general999')

search_condi = {"os": [2, 1],
                "la": [1, 2, 3, 4],
                "ss": ["050", "5099", "100499", "500999", "4999", "9999", "10000"]}

kk = list()
for k, v in search_condi.items():
    kk.append(["{}{}".format(k, x) for x in v])

condi = ["-".join(x) for x in product(*kk)]

CONTENT_XPATH = ''


class ScrapyHangYe(object):
    def __init__(self, sleep, max_ip=-1):
        self.city = 'hub'
        self.agent = DynamicIpAgent(sleep=sleep, max_ip=max_ip)
        self.orc = OraclePersist()
        self.id_pool = HangYeRedisQueue(redis_client=r, task_name='AREA_TASK_ID')

    def generate_company_id(self, hangye, area):
        logger.info(f'开始获取{area}地区{hangye}行业ID')
        #
        for cond in condi:
            # url = f"https://{self.city}.*********.com/search/{cond}?areaCode={area}"
            url = f"https://{self.city}.*********.com/search/{cond}-la1?key={hangye}&areaCode={area}"
            id_response = self.agent.req_agent_get(url)
            if not id_response:
                self.id_pool.add_job(hangye, area)
                return
            selector = etree.HTML(id_response.text)
            num_flag = selector.xpath('//*[@id="search"]/span[@class="tips-num"]/text()')
            num = 0
            if num_flag:
                num = num_flag[0]
            else:
                no_result = selector.xpath(
                    '//div[@class="result-list no-result"]/div[@id="hideSearching"]/div/img[@alt="无结果"]')
                if no_result:
                    return
                is_robot = selector.xpath('//div[@class="content"]/div/text()')
                if is_robot and u'我们只是确认一下你不是机器人，' in is_robot[0]:
                    if self.agent.drop_cookie() < 1:
                        raise CookieListEmptyException()
                else:
                    return
            import re
            num = re.match(r'\d+',str(num)).group(0)
            pagenum = 5 if int(num) > 100 else (int(num) // 20 + 1)
            for page_no in range(2, pagenum + 2):
                if not selector:
                    continue
                corplist = selector.xpath('//div[@class="result-list"]/div/div[@class="search-result-single  "]')
                for item in corplist:
                    id_ = item.xpath("./@data-id")[0]
                    print(id_)
                    self.id_pool.add_id((area, id_))

                if page_no <= pagenum:
                    # url = f"https://{self.city}.*********.com/search/{cond}/p{page_no}?areaCode={area}"
                    url = f"https://{self.city}.*********.com/search/{cond}-la1/p{page_no}?key={hangye}&areaCode={area}"
                    per_page_response = self.agent.req_agent_get(url)
                    selector = etree.HTML(per_page_response.text if per_page_response else '')

    def generate_id(self):
        while self.id_pool.id_size < 1000 and self.id_pool.job_size > 0:
            hy, code = self.id_pool.get_job()
            self.generate_company_id(hy, code)
        return self.id_pool.id_size > 0

    def get_detail_html_selector(self, id_):
        url = f"https://www.*********.com/company/{id_}"
        resp = self.agent.req_agent_get(url)
        if not resp:
            raise RequestException("请求失败！")
        selector = etree.HTML(resp.text)
        is_robot = selector.xpath('//div[@class="content"]/div/text()')
        if is_robot and u'我们只是确认一下你不是机器人，' in is_robot[0]:
            raise NeedValidException()
        is_detail = selector.xpath('//*[@id="company_web_top"]/div[@class="box"]/div[@class="content"]')
        if not is_detail:
            raise RequestException("未找到对应的xpath")
        return selector

    def get_summary(self, selector):
        data = {}
        Info = selector.xpath('//div[@class="content -shadow"]/div')
        for item in Info:
            h = item.xpath('./div[2]/div[@class="item  "]')
            for hitem in h:
                info_num = hitem.xpath("./span/text()")
                if info_num:
                    value = info_num[0]
                    name = hitem.xpath("./@onclick")[0][10:-2]
                    name = name.split('-')[-1]
                    if 'past' not in name:
                        data[name] = value
        return data

    def get_html_content(self, selector):
        webcontent = selector.xpath('//*[@id="web-content"]')[0]
        return html.tostring(webcontent)

    def save_hbase(self, id_, selector):
        summary = self.get_summary(selector)
        html = self.get_html_content(selector)
        try:
            conn = hconn.Connection(host='133.0.6.89')
            ht = conn.table(b'vip:tian_yan')
            ht.put(id_, {b'data:html': html, b'data:summary': json.dumps(summary)})
        except Exception as e:
            logger.exception(e)
        else:
            conn.close()

    def get_detail(self, id_, selector):
        SQL_NULL = 'NULL'
        # url = f"https://www.*********.com/company/{id_}"
        # selector = etree.HTML(self.agent.req_agent_get(url).text)

        # 公司信息
        corpInfo_dict = {}
        corp = selector.xpath('//*[@id="company_web_top"]/div[@class="box"]/div[@class="content"]')
        # if not corp:
        #     is_robot = selector.xpath('//div[@class="content"]/div/text()')
        #     if is_robot and u'我们只是确认一下你不是机器人，' in is_robot[0]:
        #         raise NeedValidException()
        #     else:
        #         raise RequestException()
        corpInfo = corp[0]
        corp_name = corpInfo.xpath('./div[@class="header"]/h1/text()')[0]
        corpInfo_dict["name"] = corp_name

        item = corpInfo.xpath('./div[@class="detail "]/div[@class="f0"]')
        phone = item[0].xpath('./div[@class="in-block"][1]/span[2]/text()')[0]
        email = item[0].xpath('./div[@class="in-block"][2]/span[2]/text()')[0]

        corpInfo_dict["phone"] = phone
        corpInfo_dict["email"] = email

        try:
            reg_mount = selector.xpath('//div[@id="_container_baseInfo"]/table[1]/tbody/tr[1]/td[2]/div[2]/@title')
        except:
            reg_mount = SQL_NULL
        else:
            if isinstance(reg_mount, list):
                reg_mount = reg_mount[0] if reg_mount else SQL_NULL
        corpInfo_dict['reg_mount'] = reg_mount[0] if isinstance(reg_mount, list) else reg_mount
        try:
            reg_name = selector.xpath(
                '//div[@id="_container_baseInfo"]/table[1]/tbody/tr[1]/td[1]/div/div/div[2]/div/a/@title')
        except:
            reg_name = SQL_NULL
        else:
            if isinstance(reg_name, list):
                reg_name = reg_name[0] if reg_name else SQL_NULL
        corpInfo_dict['reg_name'] = reg_name
        try:
            website = item[1].xpath('./div[@class="in-block"][1]/a/@href')[0]
        except:
            website = SQL_NULL

        try:
            address = item[1].xpath('./div[@class="in-block"][2]/span[2]/@title')
            if not address:
                address = item[1].xpath('./div[@class="in-block"][2]/text()')[0]
        except:
            address = item[1].xpath('./div[@class="in-block"][2]/text()')

        corpInfo_dict["website"] = website
        corpInfo_dict["address"] = address

        baseinfo_dict = dict()
        baseinfo = selector.xpath(
            '//div[@id="_container_baseInfo"]/table[@class="table -striped-col -border-top-none"]/tbody/tr')
        for item in baseinfo:
            tdr = item.xpath("./td")
            item_name = tdr[0].xpath('string(.)').strip().split("：")[0]
            item_value = tdr[1].xpath('string(.)').strip()
            baseinfo_dict[item_name] = item_value

            if len(tdr) > 2:
                item_name = tdr[2].xpath('string(.)').strip().split("：")[0]
                item_value = tdr[3].xpath('string(.)').strip()
                baseinfo_dict[item_name] = item_value

        webcatinfo_list = list()
        webcatinfo = selector.xpath('//*[@id="_container_wechat"]/div/div[@class="wechat"]')
        if webcatinfo:
            for item in webcatinfo:
                webcatinfo_list.append([item.xpath('./div[@class="content"]/div[1]/text()'),
                                        item.xpath('./div[@class="content"]/div[2]/span/text()'),
                                        item.xpath('./div[@class="content"]/div[3]/span/text()')])

        icpinfo_list = list()
        icpinfo = selector.xpath('//*[@id="_container_icp"]/table/tbody/tr')
        if icpinfo:
            for item in icpinfo:
                net_data = item.xpath('./td[2]/span/text()')
                net_name = item.xpath('./td[3]/span/text()')
                net_index = item.xpath('./td[4]/text()')
                net_domain = item.xpath('./td[5]/text()')
                net_no = item.xpath('./td[6]/span/text()')
                net_state = item.xpath('./td[7]/span/text()')
                net_type = item.xpath('./td[8]/span/text()')
                icpinfo_list.append([net_data if net_data else '',
                                     net_name if net_name else '',
                                     net_index if net_index else '',
                                     net_domain if net_domain else '',
                                     net_no if net_no else '',
                                     net_state if net_state else '',
                                     net_type if net_type else ''])
        brach_num_list = selector.xpath('//*[@id="nav-main-branchCount"]/span/text()')
        branch_list = list()
        if brach_num_list:
            if brach_num_list and int(brach_num_list[0]) > 0:
                url = f"https://dis.*********.com/dis/timeline.json?id={id_}"
                kwargs1 = self.agent.agent.get_cookie_param()
                headers1 = kwargs1['headers']
                headers1["Accep"] = "application/json, text/plain, */*"
                headers1["Referer"] = "https://dis.*********.com/dis/old"
                response = requests.request("GET", url, **kwargs1)
                data_ = response.json()["data"]
                for item in data_["relationships"]:
                    if "BRANCH" in item["type"]:
                        branch_list.append(item["endNode"])
        return id_, corpInfo_dict, baseinfo_dict, webcatinfo_list, icpinfo_list, branch_list

    def run(self):
        try:
            while True:
                data = self.id_pool.get_id()
                if not data:
                    if not self.generate_id():
                        break
                    continue
                
                try:
                    area, id_ = eval(data)
                except:
                    print(data)
                    area, id_ = "00001", data
                if not self.orc.check_exist(id_):
                    logger.info(f'scrapy {area} :  {id_}')
                    try:
                        selector = self.get_detail_html_selector(id_)
                        id_, corpInfo_dict, baseinfo_dict, webcatinfo_list, icpinfo_list, branch_list = self.get_detail(
                            id_, selector)
                        self.orc.save(id_, corpInfo_dict, baseinfo_dict, webcatinfo_list, icpinfo_list, branch_list,
                                      area=area)
                        self.save_hbase(id_, selector)
                    except NeedValidException as e:
                        if self.agent.drop_cookie() < 1:
                            raise CookieListEmptyException()
                    except RequestException as e:
                        logger.info(f'请求id={id_}企业失败！')
                        self.id_pool.add_id(id_)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            print('本次更新企业数量为: %d' % self.agent.agent.request_cnt)
            return False
        return True


class ScrapyHangYeV2(ScrapyHangYe):
    def __init__(self,sleep,max_ip=-1):
        super(ScrapyHangYeV2, self).__init__(sleep,max_ip)


if __name__ == '__main__':
    s = ScrapyHangYe(sleep=15)
    s.run()
