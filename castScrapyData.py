import re
import json
import pandas as pd
import cx_Oracle
from lxml import etree
import happybase as hconn
import jieba
# ---------------------------------------------TM_TYC_POS 数据生成及入库------------------------------------------------
address_reg = r'([\u4e00-\u9fa5]*\u7701)?([\u4e00-\u9fa5]*\u5e02)?([\u4e00-\u9fa5]*\u533a)?([\u4e00-\u9fa5]*\u53bf)?'
ADD_REG = re.compile(address_reg)
with open('division_code.json', 'rb') as f:
    divide_code = json.load(f)
    hb_code = {k: v for k, v in divide_code.items() if k.startswith('42')}
    hb_reverse_code = {v: k for k, v in hb_code.items()}
    hb_reverse_code_simple = {v[:2]: k for k, v in hb_code.items()}


def export_system_data(data):
    if isinstance(data, str):
        data = pd.read_csv(data)
    if not isinstance(data, pd.DataFrame):
        raise Exception("data")
    data = data[
        ['ID', 'CORP_NAME', 'AREA', 'CERT_NO', '工商注册号注册号', 'ACC_NBR', 'ADDRESS', '注册地址注册地址', 'REG_MOUNT', '实缴资本',
         '人员规模', '参保人数', '核准日期', '公司类型', 'BUSINESS_SCOPE', '行业行业']
    ]
    data.columns = ['ID', 'CORP_NAME', 'AREA', 'CERT_NO', 'GS_REG_NO', 'ACC_NBR', 'ADDRESS', 'REG_ADDRESS', 'REG_MOUNT',
                    'SHI_JIAO_MOUNT',
                    'STAFF_RANGE', 'STAFF_CNT', 'HE_ZUN_DATE', 'COMPANY_TYPE', 'BUSSNESS_SCOPE', 'SCOPE']
    data['AREA'] = data.AREA.apply(lambda x: str(int(x)) if not pd.isna(x) else '') + '\t' + data.ADDRESS
    data['AREA'] = data.AREA.apply(decode_address)
    data['CITY'] = data.AREA.apply(lambda x: hb_code.get(x[:-2] + '00') if isinstance(x, str) else None)
    data['DISTRICT'] = data.AREA.apply(lambda x: hb_code.get(x) if isinstance(x, str) else None)
    x = [tuple([str(j).replace('•', '.') for j in i]) for i in data.fillna("").values]
    conn = cx_Oracle.Connection('anti_fraud', 'at_87654', cx_Oracle.makedsn('133.0.176.69', 1521, sid='htxxsvc2'))
    conn.begin()
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE TM_TYC_POS")
        cur.prepare("INSERT INTO TM_TYC_POS VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18)")
        cur.executemany(None, x)
    except Exception as e:
        conn.rollback()
        print(e)
    else:
        conn.commit()
    finally:
        cur.close()
        conn.close()
    return data


def decode_address(data):
    data = str(data)
    data = data.split('\t')
    if data[0]:
        return data[0]
    address = data[1]
    return address_decoder(address)


def address_decoder(address):
    g = jieba.cut_for_search(address)
    code = None
    for addr in g:
        code = hb_reverse_code.get(addr) or hb_reverse_code_simple.get(addr) or code
        print('{}：{}'.format(addr,code))
        if code and int(code) % 100 > 0:
            return code
    return code


def address_decoder_re(address):
    group = ADD_REG.match(address).groups()
    if group:
        group = [i for i in group if i]
        group.reverse()
        for i in group:
            code = hb_reverse_code.get(i)
            if code:
                return code
    return None

#  ----------------------------------------------物联网需求-------------------------------------------------------------
target_scope = {'OBD',
                'T-BOX',
                '一般工业设备监控',
                '井盖监控',
                '产品溯源',
                '人员定位器',
                '仓储监控',
                '儿童定位手表',
                '充电桩设备管理',
                '充电桩设备管理(含wifi,视频等)',
                '光伏监控',
                '公共自行车',
                '公网对讲',
                '共享单车',
                '其它智能家电',
                '其它智能穿戴',
                '农业机械',
                '冰箱',
                '冷柜',
                '净水器',
                '卫生洁具',
                '危化品生产环境监控',
                '厨房用具',
                '叉车', '起重机', '推土机', '挖掘机', '泵车',
                '叫号机',
                '排队机',
                '可燃性气体生产环境监控',
                '喷淋及光照控制器',
                '土壤监测设备',
                '地下水监控',
                '场地直播系统',
                '天翼对讲',
                '安防监控',
                '定位终端',
                '部标车机',
                '宠物跟踪',
                '家庭安防',
                '家庭网关',
                '导航监控终端',
                '工业自动化生产线',
                '广告屏',
                '建筑能耗监测',
                '手持医疗终端',
                '数控机床',
                '无人机',
                '无线血压计',
                '无线血糖仪',
                '智能ETC',
                '智能停车场',
                '智能后视镜',
                '智能垃圾箱',
                '智能快递柜',
                '智能电控柜',
                '智能锁',
                '智能门禁',
                '服装/眼镜/鞋等',
                '机器人',
                '机床监控',
                '村村响',
                '检测仪',
                '气表',
                '水库监控',
                '水表',
                '水质监测',
                '河道水文监控',
                '油井环境监控',
                '泵站监控',
                '洗衣机',
                '消防栓监控',
                '温湿度监测设备',
                '烟感',
                '热水器',
                '热表',
                '照片打印机',
                '物流手持终端',
                '牲畜定位跟踪',
                '环境监控终端',
                '冷链温湿度监控',
                '环境监测',
                '电动乘用车监控终端',
                '电动特用车监控终端',
                '电动自行车',
                '电子秤',
                '电梯监控',
                '电气防火监控',
                '电池监控',
                '电表',
                '畜禽体征监控',
                '矿井环境监控',
                '移动执法仪',
                '空气净化器',
                '空调',
                '空调监控',
                '管网监控',
                '手表',
                '手环',
                '考勤机',
                '能耗监测',
                '照明监控',
                '智能插座',
                '自动售货机',
                '自动点唱机',
                '自动雨量站监测',
                '船载WiFi',
                '色选机监控',
                '蓄电池监控',
                '行业PAD',
                '行车记录仪',
                '视频监控终端',
                '资产定位',
                '货单定位终端',
                '路灯监控',
                '车载WiFi',
                '车辆刷卡机',
                '过塑机监控',
                '运动手环手表',
                '远程诊断',
                '道路停车',
                '配电设施监控',
                '金融POS',
                '铁塔动环监控',
                '锅炉监控',
                '阀门监控'}


def to_number(i):
    reg = r'\D*(\d+(\.\d+)?)\D*'
    NUMBER_REG = re.compile(reg)
    r = 0
    if i:
        n = NUMBER_REG.match(i)
        if n:
            r = n.group(1)
    return r


def parse_scope(scope):
    contain_scope = [i for i in target_scope if i in scope]
    corp_scope_cnt = len(scope.split("；"))
    return [';'.join(contain_scope), corp_scope_cnt, len(contain_scope)]


def get_data():
    pass


def search_corp(data):
    if isinstance(data, str):
        data = pd.read_csv(data)
    if not isinstance(data, pd.DataFrame):
        raise Exception("data")
    data = data.fillna('')
    corp = CorpInfo()
    scope_data = data[['ID', 'BUSSNESS_SCOPE']].values.tolist()
    df = pd.DataFrame(
        [i + parse_scope(i[1]) for i in scope_data],
        columns=['ID', 'BUSSNESS_SCOPE', 'TARGET_SCOPE', 'SCOPE_CNT', 'TARGET_SCOPE_CNT']
    )
    df = df[df.TARGET_SCOPE_CNT > 0]
    df['hbase'] = df.ID.apply(corp.get_year_port_and_product)
    df['REPORT_CNT'] = df.hbase.apply(lambda x: x[0]).astype(int)
    df['JING_PING_CNT'] = df.hbase.apply(lambda x: x[1]).astype(int)
    df.to_csv('tmp_corp_info.csv')
    df = df[df['REPORT_CNT'] > 0]
    df.drop(columns='BUSSNESS_SCOPE', inplace=True)
    df.set_index('ID', inplace=True)
    data.set_index('ID', inplace=True)
    result = df.join(data)
    result.to_csv('corp_result.csv', index=False)
    return result


class CorpInfo(object):
    def __init__(self, ):
        self.conn = hconn.ConnectionPool(size=8, host='133.0.6.89')
        self.table = b'vip:tian_yan'
        self.html_col = b'data:html'
        self.summary_col = b'data:summary'

    def get_data(self, key, column):
        data = {}
        print('*' * 10 + key + '*' * 10)
        try:
            with self.conn.connection() as conn:
                ht = conn.table(self.table)
                data = ht.row(key, columns=[column])
                print(data)
        except Exception as e:
            data[column] = ''
            print(e)
        return data.get(column)

    def get_html(self, key):
        return self.get_data(key, self.html_col)

    def get_summary(self, key):
        r = self.get_data(key, self.summary_col)
        return None if r == '' else json.loads(r or '{}')

    def get_year_port_and_product(self, key):
        if not isinstance(key, str):
            key = str(key)
        s = self.get_summary(key)
        return [s.get('reportCount', 0), s.get('companyJingpin', 0)] if s else [-1, -1]


class CorpHtmlParser(object):
    def __init__(self, html):
        self.selector = etree.HTML(html)

    def get_year_report_cnt(self):
        data = {}
        info = self.selector.xpath('//div[@class="content -shadow"]/div')
        for item in info:
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


if __name__ == '__main__':
    from tianyancha.core import OraclePersist
    o = OraclePersist()
    data = o.exprot_final("qichacha-v2.csv")
    data = export_system_data(data)
    search_corp(data)