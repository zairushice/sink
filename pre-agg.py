from paho.mqtt.client import Client
from sqlalchemy import create_engine, text
import requests
import logging
import json
import datetime
from threading import Thread
import time

# 定义日志信息
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

# 可配置项
######################################
topic = "nup/system/group0/attr/0/#"
dmc_host = "isc-dmc-service"
mqtt_host = "emqx-service"
mqtt_port = 1883
mysql_host = "mysql-service"
mysql_port = 3306
mysql_username = ""
mysql_password = ""
database = ""
######################################

# 初始化引擎
engine = create_engine(
    'mysql://{}:{}@{}:{}/{}?charset=utf8&connect_timeout=5'.format(mysql_username, mysql_password, mysql_host,
                                                                   mysql_port, database))
v = {}


def parse_time(t):
    try:
        d_time = datetime.datetime.fromtimestamp(float(t) / 1000)
        # mill_time = datetime.datetime.strftime(d_time, "%Y-%m-%d %H:%M:%S.%f")[:-3]
        return d_time
    except Exception as e:
        logging.error("parse time error: {}".format(e))
        raise e


def get_hour(t):
    return t.strftime("%Y%m%d%H")


# 定义回调函数
# 当代理响应连接请求时调用
def on_connect(client, userdata, flags, rc):
    logging.info('mqtt connected')
    # 订阅topic，设定服务质量等级，默认为0
    client.subscribe([(topic, 0)])


# 当收到订阅的主题消息时调用
def on_message(client, userdata, msg):
    # logging.info('on message {}: {}'.format(msg.topic, msg.payload))
    try:
        # 数据解析部分
        data = json.loads(msg.payload)
        device_id = data.get('devId')
        dev_dict = v.get(device_id)
        d_time = parse_time(data.get('time'))
        hour = get_hour(d_time)
        params = data.get('params')
        if params:
            for attr_id, value in params.items():
                if dev_dict:
                    attr_dict = dev_dict.get(attr_id)
                    if attr_dict:
                        # 当时间跨越整点时进行入库，并清除缓存，当前小时数据计算结束
                        if hour > attr_dict.get('hour'):
                            ks = ','.join(['`{}`'.format(k) for k in attr_dict.keys()])
                            vs = ','.join([':{}'.format(vl) for vl in attr_dict.keys()])
                            stmt = text("""replace into `{}` ({}) values ({})""".format('device_1_{}'.format(device_id),
                                                                                        ks, vs))
                            engine.execute(stmt, attr_dict)
                            dev_dict.pop(attr_id, 1)
                        else:
                            attr_dict['max'] = max(float(value), attr_dict.get('max'))
                            attr_dict['min'] = min(float(value), attr_dict.get('min'))
                            attr_dict['last'] = float(value)
                            attr_dict['count'] = attr_dict.get('count') + 1
                            attr_dict['sum'] = attr_dict.get('sum') + float(value)
                            attr_dict['mean'] = attr_dict.get('sum') / attr_dict.get('count')
                    if not dev_dict.get(attr_id):
                        dev_dict[attr_id] = {'attributeIdentifier': attr_id,
                                             'max': float(value), 'min': float(value),
                                             'last': float(value), 'first': float(value),
                                             'count': 1, 'sum': float(value), 'mean': float(value),
                                             'hour': hour}
                else:
                    v[device_id] = {attr_id: {'attributeIdentifier': attr_id,
                                              'max': float(value), 'min': float(value),
                                              'last': float(value), 'first': float(value),
                                              'count': 1, 'sum': float(value), 'mean': float(value),
                                              'hour': hour}}
    except Exception as e:
        pass


# 定义了入库函数
def sink(d):
    try:
        # 如果没有表则创建表
        # 将字典key转为list进行遍历，以防止遍历字典时子线程修改字典内容导致冲突
        for device_id in list(d):
            table_name = 'device_1_{}'.format(device_id)
            engine.execute('''create table if not exists `{}` (
              `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
              `attributeIdentifier` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
              `max` float NULL DEFAULT 0,
              `min` float NULL DEFAULT 0,
              `last` float NULL DEFAULT 0,
              `first` float NULL DEFAULT 0,
              `count` int(11) NULL DEFAULT 0,
              `sum` float NULL DEFAULT 0,
              `mean` float NULL DEFAULT 0,
              `hour` int(11) NULL DEFAULT 0,
              `state` int(11) NULL DEFAULT 0,
              `last_scan_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
              `latest_point_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
              `create_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
              `update_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
              PRIMARY KEY (`id`) USING BTREE,
              UNIQUE INDEX `u__attr_hour`(`attributeIdentifier`, `hour`) USING BTREE,
              INDEX `i__attr`(`attributeIdentifier`) USING BTREE,
              INDEX `i__hour`(`hour`) USING BTREE
            ) ENGINE = InnoDB AUTO_INCREMENT = 10591 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;'''.format(
                table_name))
            rows = d.get(device_id)
            for attr in list(rows):
                row = rows.get(attr)
                ks = ','.join(['`{}`'.format(k) for k in row.keys()])
                vs = ','.join([':{}'.format(v) for v in row.keys()])
                stmt = text("""replace into `{}` ({}) values ({})""".format(table_name, ks, vs))
                engine.execute(stmt, row)
    except Exception as e:
        raise e


# 主函数
def main(dmc_host_name, mqtt_host_name, port):
    # 通过OpenAPI获取mqtt连接信息
    r = requests.get("http://{}:40000/api/device/dmc/mqttAuth".format(dmc_host_name))
    if r.ok:
        data = r.json().get('data')
    else:
        logging.error("获取连接信息出错")
        return
    # 创建mqtt客户端实例
    client = Client()
    # 设置用户名密码
    client.username_pw_set(data.get('username'), data.get('password'))
    # 将回调函数指派给客户端实例
    client.on_connect = on_connect
    client.on_message = on_message
    # 将客户端连接到代理
    client.connect(mqtt_host_name, port)
    # 循环调用处理收到的消息实例
    client.loop_forever()


# 运行主函数
th_recv = Thread(target=main, daemon=True, args=(dmc_host, mqtt_host, mqtt_port))
th_recv.start()
while True:
    time.sleep(60)
    sink(v)
    logging.info("sink {} records".format(len(v)))
