import redis


class RedisJobs(object):
    def __init__(self, job_name, redis_client=None):
        self.r = redis_client if redis_client else redis.StrictRedis(host="133.0.189.11", port=6379, decode_responses=True, password='general999')
        self.job_name = job_name

    def get_id(self, **kwargs):
        pass

    def add_id(self, **kwargs):
        pass


class RedisJobSet(RedisJobs):
    def __init__(self, job_name, redis_client=None):
        super(RedisJobSet, self).__init__(job_name=job_name, redis_client=redis_client)

    def get_id(self):
        self.r.spop(self.job_name)

    def add_id(self, element):
        self.r.sadd(self.job_name, element)


HANG_YE_JOB = 'HANG_YE_JOB'
HANG_YE_ID = 'HANG_YE_ID'
AREA_CODE = {
    "wuhan": ['420101',
              '420102',
              '420103',
              '420104',
              '420105',
              '420106',
              '420107',
              '420111',
              '420112',
              '420113',
              '420114',
              '420115',
              '420116',
              '420117'],
    "huangshi": ['420201', '420202', '420203', '420204', '420205', '420222', '420281'],
    "shiyan": ['420301',
               '420302',
               '420303',
               '420304',
               '420322',
               '420323',
               '420324',
               '420325',
               '420381'],
    "yichang": ['420501',
                '420502',
                '420503',
                '420504',
                '420505',
                '420506',
                '420525',
                '420526',
                '420527',
                '420528',
                '420529',
                '420581',
                '420582',
                '420583'],
    'xiangyang': ['420601', '420602', '420606', '420607', '420624', '420625', '420626', '420682', '420683',
                  '420684'],
    "ezhou": ['420701', '420702', '420703', '420704'],
    'jingmen': ['420801', '420802', '420804', '420821', '420822', '420881'],
    'xiaogan': ['420901', '420902', '420921', '420922', '420923', '420981', '420982', '420984', ],
    'jingzhou': ['421001', '421002', '421003', '421022', '421023', '421024', '421081', '421083', '421087'],
    'huanggang': ['421101', '421102', '421121', '421122', '421123', '421124', '421125', '421126', '421127',
                  '421181', '421182'],
    'xianning': ['421201', '421202', '421221', '421222', '421223', '421224', '421281'],
    'suizhou': ['421301', '421303', '421321', '421381'],
    'estjz': ['422801', '422802', '422822', '422823', '422825', '422826', '422827', '422828'],
    'hubzx': ['429004', '429005', '429006', '429021']
}
HANG_YE = [
    "制造",
    "集团", "科技", "中心",
    "重工", "酒店",
    "教育", "装备",
    "交通", "金融", "学校",
    "电力", "电视",
    "银行", "电信",
    "水务",
    "医药", "医院",
    "建筑", "商贸", "管理", "汽车", "投资", "合作"
]


class HangYeRedisQueue(RedisJobs,):
    def __init__(self, redis_client=None, task_name=HANG_YE_ID):
        super(HangYeRedisQueue, self).__init__(job_name=HANG_YE_JOB, redis_client=redis_client)
        self.task_name = task_name

    def init_job(self):
        if self.job_size > 0:
            return
        for hy in HANG_YE:
            for area_name, area_codes in AREA_CODE.items():
                for code in area_codes:
                    self.add_job(hy, code)

    def get_job(self):
        e = self.r.lpop(HANG_YE_JOB)
        return eval(e) if e else None

    def add_job(self, hangye, area_code):
        self.r.rpush(HANG_YE_JOB, (hangye, area_code))

    def add_emergence_job(self,hangye, area_code):
        self.r.lpush(HANG_YE_JOB, (hangye, area_code))

    def add_all_area_job(self, hy):
        for area_name, area_codes in AREA_CODE.items():
            for code in area_codes:
                self.add_emergence_job(hy, code)

    def get_id(self):
        return self.r.lpop(self.task_name)

    def add_id(self, web_id):
        self.r.rpush(self.task_name, web_id)

    @property
    def job_size(self):
        return self.r.llen(HANG_YE_JOB)

    @property
    def id_size(self):
        return self.r.llen(self.task_name)


if __name__ == '__main__':
    cache = HangYeRedisQueue(task_name='AREA_TASK_ID')
    for hy in ['中国联合网络通信集团有限公司',
               '中国移动通信集团',
               '中国电信集团']:
        cache.add_all_area_job(hy)
