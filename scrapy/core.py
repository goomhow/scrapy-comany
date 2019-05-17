import cx_Oracle
import json
import traceback
import os
import warnings
import pandas as pd
import logging
import requests
import time
import hashlib
from fake_useragent import UserAgent
import redis
import happybase as hbase
warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('tian_yan_cha')
#    {"mobile": "18907181731", "cdpassword": "Cqkdczy4"},


def get_tokens():
    r = redis.StrictRedis(host="133.0.189.11", port=6379, decode_responses=True, password='general999')


tokens = [
    {"mobile": "18907191059", "cdpassword": "Cqkdczy4"},
    {"mobile": "15342284844", "cdpassword": "Cqkdczy3"},
    {"mobile": "18995502882", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907152103", "cdpassword": "1234abcd"},
    {"mobile": "18907182077", "cdpassword": "abcde12345"},
    {"mobile": "18907187801", "cdpassword": "zz654312"},
    {"mobile": "18995603679", "cdpassword": "zz654312"},
    {"mobile": "18907195332", "cdpassword": "lhl19760309"},
    {"mobile": "18907192073", "cdpassword": "Cqkdczy4"},
    {"mobile": "18908602302", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181267", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181532", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907182129", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907191189", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181282", "cdpassword": "Cqkdczy4"},
    {"mobile": "17354320422", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181275", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907192156", "cdpassword": "Cqkdczy4"},
    {"mobile": "18171288419", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907193287", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181363", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181549", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907157115", "cdpassword": "Cqkdczy4"},
    {"mobile": "15926295116", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907183279", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181240", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181273", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181261", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907151655", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907191060", "cdpassword": "1qaz2wsx3edc"},
    {"mobile": "18907191371", "cdpassword": "a1234567"},
    {"mobile": "18995588323", "cdpassword": "1234abcd"},
    {"mobile": "18986025402", "cdpassword": "1234abcd"},
    {"mobile": "18907193523", "cdpassword": "aaa1234567890"},
    {"mobile": "18907192310", "cdpassword": "zqdw123456"},
    {"mobile": "18907181583", "cdpassword": "1qaz2wsx"},
    {"mobile": "18907192552", "cdpassword": "huizhi2018"},
    {"mobile": "18907191207", "cdpassword": "yyh112233"},
    {"mobile": "13207142180", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907183256", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181265", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181113", "cdpassword": "Cqkdczy4"},
    {"mobile": "15337196881", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907199716", "cdpassword": "a12345678"},
    {"mobile": "18907196310", "cdpassword": "Cqkdczy3"},
    {"mobile": "15337135288", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181239", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181257", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907182137", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181731", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907118989", "cdpassword": "Cqkdczy4"},
    {"mobile": "18007181308", "cdpassword": "Cqkdczy4"},
    {"mobile": "18907181309", "cdpassword": "1qazxsw2"},
    {"mobile": "18907186409", "cdpassword": "abcd1234"},
    {"mobile": "18907189156", "cdpassword": "chenyan123456"},
    {"mobile": "18907192116", "cdpassword": "hujian123456"},
    {"mobile": "18907186192", "cdpassword": "abcd1234"},
    {"mobile": "18907189399", "cdpassword": "abcd1234"},
    {"mobile": "18907187878", "cdpassword": "abcd1234"},
    {"mobile": "18907188787", "cdpassword": "abcd1234"},
]



def set_ip_addresses(ip_addresses, interface_name='eno4'):
    """
    设置某网络接口的IP地址
    """
    os.system(f'ifconfig  {interface_name}  {ip_addresses}')
    os.system(f'route add default gw {ip_addresses} {interface_name}')
    logger.info('IP切换为{}'.format(ip_addresses))
    time.sleep(0.5)


def login(mobile, cdpassword):
    logger.info(f'{mobile}开始登陆。。。')
    if len(cdpassword) != 32:
        cdpassword = hashlib.md5(cdpassword.encode()).hexdigest()
    data = json.dumps({"mobile": mobile, "cdpassword": cdpassword, "loginway": "PL", "autoLogin": True})
    headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0",
               "Content-Type": "application/json; charset=utf-8",
               "X-Requested-With": "XMLHttpRequest"
               }
    kwargs = {"headers": headers,
              "verify": False,
              "timeout": 500,
              "data": data
              }
    login_url = "https://www.tianyancha.com/cd/login.json"
    ck = {}
    retry = 0
    while retry < 3:
        try:
            resp = requests.post(url=login_url, **kwargs)
            token_ = resp.json().get('data')
            ck = resp.cookies.get_dict()
            ck['tyc-user-info'] = json.dumps(token_)
            ck['auth_token'] = token_.get("token")
            ck['bannerFlag'] = "true"
            ck['mobile'] = mobile
        except Exception as e:
            logger.exception(e)
            retry += 1
        else:
            retry = 3
    return ck


class OraclePersist(object):

    def __init__(self):
        self.table = 'TM_TY_FINAL'
        self.branch_table = 'TM_TY_ID'
        self.summary_table = 'TM_TY_SUMMARY'
        self.conn = cx_Oracle.Connection('anti_fraud', 'at_87654',
                                         cx_Oracle.makedsn('133.0.176.69', 1521, sid='htxxsvc2'))

    def check_exist(self, id):
        cur = self.conn.cursor()
        cnt1 = cur.execute(f'select count(*) from TM_TY where id = {id}').fetchone()[0]
        cnt2 = cur.execute(f'select count(*) from TM_TY_SED where id = {id}').fetchone()[0]
        cnt3 = cur.execute(f'select count(*) from TM_TY_FINAL where id = {id}').fetchone()[0]
        cnt = cnt1 + cnt2 + cnt3
        cur.close()
        if cnt > 1:
            print(f'存在重复的id：{id}')
        return cnt > 0

    def persist_info(self, id_, corpInfo, baseinfo, webcatinfo, icpinfo,area):
        corp_name = corpInfo['name']
        nbr = corpInfo['phone']
        area_code = ''
        acc_nbr = nbr
        if nbr.startswith('0'):
            if '-' in nbr:
                area_code = nbr.split('-')[0]
                acc_nbr = nbr.split('-')[-1]
            else:
                if nbr.startswith('027'):
                    area_code = '027'
                    acc_nbr = nbr[3:]
                else:
                    area_code = nbr[:4]
                    acc_nbr = nbr[4:]
        elif nbr.startswith('('):
            area_code = nbr.split(')')[0][1:]
            acc_nbr = nbr.split(')')[-1]
        certification_no = baseinfo.get(u'纳税人识别号') if u'纳税人识别号' in baseinfo else ''
        email = corpInfo['email']
        website = corpInfo['website']
        address = corpInfo['address']
        if address:
            address = address[0] if isinstance(address, list) else address
            address = address.replace('•', '')
        else:
            address = ''
        if baseinfo and u'经营范围经营范围' in baseinfo:
            business_scope = baseinfo.pop(u'经营范围经营范围')
        else:
            business_scope = ''
        base_info = json.dumps(baseinfo).replace('\'', ' ')
        webcat_info = json.dumps([webcatinfo[0][:3]] if webcatinfo else webcatinfo).replace('\'', ' ')
        icp_info = json.dumps(icpinfo[:3]).replace('\'', ' ')
        reg_mount = corpInfo['reg_mount']
        reg_name = corpInfo['reg_name']
        sql = f"""
        insert into {self.table} 
        (id,corp_name,area_code,acc_nbr,cert_no,email,website,address,base_info,webcat_info,icp_info,reg_mount,reg_name,business_scope,area) 
        values 
        ({id_},'{corp_name}','{area_code}','{acc_nbr}','{certification_no}','{email}','{website}','{address}','{base_info}','{webcat_info}','{icp_info}','{reg_mount}','{reg_name}','{business_scope}',{area})
        """
        return sql

    def persist_branch(self, cur, id_, branch_list):
        if len(branch_list) > 0:
            sql = 'insert into ' + self.branch_table + '(parent_id,child_id) values (:1,:2)'
            cur.executemany(sql, [(int(id_), int(i)) for i in branch_list])

    def save(self, id_, corpInfo_dict, baseinfo_dict, webcatinfo_list, icpinfo_list, branch_list, area='NULL'):
        cur = None
        try:
            self.conn.begin()
            cur = self.conn.cursor()
            logger.debug('persist info start')
            info_sql = self.persist_info(id_, corpInfo_dict, baseinfo_dict, webcatinfo_list, icpinfo_list, area)
            cur.execute(info_sql)
            logger.debug('persist info finish')
            logger.debug('persist branch start')
            self.persist_branch(cur, id_, branch_list)
            logger.debug('persist branch finish')
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(e)
            print(info_sql)
            traceback.print_exc()
        finally:
            if cur:
                cur.close()

    def save_finish_job(self, name, keys):
        name = name.split(' ')
        area_name = name[0]
        cust_name = name[1]
        sql = F"INSERT INTO TM_TY_FINISH (AREA_NAME,CUST_NAME,TY_ID) VALUES (f'{area_name}',f'{cust_name}',:1)"
        cur = None
        try:
            self.conn.begin()
            cur = self.conn.cursor()
            cur.executemany(sql, [(int(key)) for key in keys])
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(e)
            logger.error(sql)
            traceback.print_exc()
        else:
            if cur:
                cur.close()

    def is_name_match(self, region_name, name, certificate_no):
        sid = '' if region_name == '武汉' else '2'
        sql = f'''SELECT DISTINCT CUST_NAME FROM ls65_sid{sid}.cust_t@to_sc_sid WHERE CUST_ID IN(
                    SELECT DISTINCT CUST_ID FROM ls65_sid{sid}.cust_identification_t@to_sc_sid a 
                    WHERE CERTIFICATE_NO='{certificate_no}'  
                    AND STATE='70A') AND STATE='70A' '''
        with self.conn.cursor() as cur:
            cust_names = {i[0] for i in cur.execute(sql).fetchall()}
        print(cust_names)
        return name in cust_names

    def parse(self, data, is_final=False):
        if isinstance(data, pd.DataFrame):
            base_info = pd.DataFrame(list(data.BASE_INFO.apply(lambda x: eval(str(x))).values))
            data.drop(columns='BASE_INFO', inplace=True)
            result = data.join(base_info)
            if not is_final:
                result['business_scope'] = result[u'经营范围经营范围']
            print(result)
            return result
        else:
            raise Exception("传入的参数data必须为DataFrame")

    def export(self, path='qichacha.csv'):
        data1 = self.parse(pd.read_sql(f'select * from TM_TY', self.conn))
        data2 = self.parse(pd.read_sql(f'select * from TM_TY_SED', self.conn))
        data3 = self.parse(pd.read_sql(f'select * from TM_TY_FINAL', self.conn), is_final=True)
        data1.append(data2).append(data3).to_csv(path, index=False)

    def exprot_final(self, path='qichacha-v2.csv'):
        data = self.parse(pd.read_sql(f'select * from TM_TY_FINAL', self.conn), is_final=True)
        data.to_csv(path, index=False, doublequote=True, quotechar='"')
        return data

    def export_format_final(self):
        sql = 'SELECT ID,CORP_NAME,AREA_CODE,ADDRESS,REG_MOUNT,REG_NAME,BUSINESS_SCOPE,BASE_INFO FROM TM_TY_FINAL'
        data = self.parse(pd.read_sql(sql, self.conn), is_final=True)
        return data
    def get_hbase_info(self):
        conn = hbase.Connection(host='133.0.6.90')
        conn.open()
        table = conn.table('vip:tian_yan')
        data = table.scan(columns=['data:summary'], batch_size=10000)
        def decode_col(k):
            row_key = k[0].decode()
            row_data = json.loads(k[1][b'data:summary']).get('branchCount')
            if not row_data:
                row_data = 0
            return [row_key, row_data]
        r = [decode_col(i) for i in data]
        conn.close()
        return r


class DynamicUserAgent(object):
    def __init__(self, cookie, sleep=0):
        self.ua = UserAgent(path="/tmp/fake_useragent_0.1.10.json")
        if isinstance(cookie, str):
            self.cookie = {i[0].strip(): i[1] for i in [i.split("=") for i in cookie.split(';')]}
        elif isinstance(cookie, dict):
            self.cookie = cookie
        else:
            raise Exception("cookie格式错误，只能为字典或者字符串")
        self.request_cnt = 1
        self.request_interval = sleep

    def set_cookie(self, cookie):
        if isinstance(cookie, str):
            self.cookie = {i[0].strip(): i[1] for i in [i.split("=") for i in cookie.split(';')]}
        elif isinstance(cookie, dict):
            self.cookie = cookie
        else:
            raise Exception("cookie格式错误，只能为字典或者字符串")

    def get_proxy_param(self, redis):
        headers = {
            "user-agent": self.ua.random
        }
        proxy = redis.srandmember('good_proxy', 1)[0]
        proxies = {"http": proxy}
        params = {"headers": headers, "verify": False, "proxies": proxies, "cookies": self.cookie}
        self.request_cnt += 1
        if self.request_interval > 0:
            time.sleep(self.request_interval)
        return params

    def get_cookie_param(self):
        headers = {
            "user-agent": self.ua.random
        }
        params = {"headers": headers, "verify": False, "cookies": self.cookie}
        self.request_cnt += 1
        if self.request_interval > 0:
            time.sleep(self.request_interval)
        return params

    def get_plain_param(self):
        headers = {
            "user-agent": self.ua.random
        }
        params = {"headers": headers, "verify": False, "allow_redirects": False}
        self.request_cnt += 1
        if self.request_interval > 0:
            time.sleep(self.request_interval)
        return params

    def req_agent_get(self, url):
        retry = 3
        response = None
        while retry > 0:
            try:
                response = requests.request('get', url, **self.get_cookie_param())
            except Exception as e:
                retry -= 1
                time.sleep(0.3)
            else:
                break
        return response

    def req_plain_get(self, url):
        retry = 3
        response = None
        while retry > 0:
            try:
                response = requests.request('get', url, **self.get_plain_param())
            except Exception as e:
                retry -= 1
                time.sleep(0.3)
            else:
                break
        return response


class DynamicCookieAgent(object):
    def __init__(self, cookie_list=None, sleep=0, max_ip=-1):
        tk = tokens if max_ip<0 else tokens[:max_ip]
        self.cookie_list = cookie_list if cookie_list else [login(**params) for params in tk]
        self.agent = DynamicUserAgent(self.cookie_list[0], sleep=sleep)
        self.ck_index = 0

    def add_cookie(self, ck):
        self.cookie_list.append(ck)

    def next_cookie(self):
        self.ck_index = 0 if self.ck_index >= len(self.cookie_list) - 1 else self.ck_index + 1
        self.agent.set_cookie(self.cookie_list[self.ck_index])

    def drop_cookie(self):
        if self.cookie_list.__len__() > 1:
            mobile = self.agent.cookie['mobile']
            logger.warning(f'删除mobile:{mobile}')
            self.cookie_list.pop(self.ck_index)
            if self.ck_index > self.cookie_list.__len__() - 1:
                self.ck_index = 0
            self.agent.set_cookie(self.cookie_list[self.ck_index])
            return 1
        else:
            return 0

    def req_agent_get(self, url):
        if self.agent.request_cnt % 10 == 0:
            self.next_cookie()
        return self.agent.req_agent_get(url)

    def req_plain_get(self, url):
        if self.agent.request_cnt % 10 == 0:
            self.next_cookie()
        return self.agent.req_plain_get(url)


class DynamicIpAgent(DynamicCookieAgent):
    def __init__(self, sleep=0, max_ip=-1):
        cookie_list = []
        tk = tokens if max_ip < 0 else tokens[:max_ip]
        self.ip_list = ['202.103.17.%d' % (i + 66) for i in range(tk.__len__())]
        for i in range(tk.__len__()):
            set_ip_addresses(ip_addresses=self.ip_list[i])
            cookie_list.append(login(**tk[i]))
        super(DynamicIpAgent, self).__init__(cookie_list, sleep)
        self.ip = self.ip_list[0]
        set_ip_addresses(self.ip)

    def next_cookie(self):
        self.ck_index = 0 if self.ck_index >= len(self.cookie_list) - 1 else self.ck_index + 1
        self.agent.set_cookie(self.cookie_list[self.ck_index])
        self.ip = self.ip_list[self.ck_index]
        set_ip_addresses(self.ip)
        logger.info(f'ip切换为{self.ip}')

    def drop_cookie(self):
        if self.cookie_list.__len__() > 1:
            mobile = self.agent.cookie['mobile']
            logger.warning(f'删除ip：{self.ip}    mobile:{mobile}')
            self.cookie_list.pop(self.ck_index)
            self.ip_list.pop(self.ck_index)
            if self.ck_index > self.cookie_list.__len__() - 1:
                self.ck_index = 0
            self.agent.set_cookie(self.cookie_list[self.ck_index])
            self.ip = self.ip_list[self.ck_index]
            set_ip_addresses(self.ip)
            return 1
        else:
            return 0


class NeedValidException(Exception):
    def __init__(self, msg=None):
        msg = msg if msg else "需要点击验证码！"
        super(NeedValidException, self).__init__(msg)


class CookieListEmptyException(Exception):
    def __init__(self, msg=None):
        msg = msg if msg else "cookie已经用完，请激活cookie！"
        super(CookieListEmptyException, self).__init__(msg)

if __name__ == '__main__':
    h = OraclePersist()
    h.exprot_final()
