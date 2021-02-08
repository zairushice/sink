from sqlalchemy import create_engine
from sqlalchemy import text
import logging
from influxdb import InfluxDBClient

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %('
                                                'message)s')
prev_hour = None
engine = create_engine(
    'mysql://username:password@mysql-service:23306/bap_data_warehouse?charset=utf8&connect_timeout=5')


def beat(prev_hour):
    try:
        client = InfluxDBClient('influxdb-service', 28086, database='device1')
        lst = [int(p['value']) for p in client.query(
            'show tag values with key=hour').get_points()]
        lst.sort(reverse=True)
        if not lst:
            logging.info('there is no tag values with key hour')
            return
        if lst[0] == prev_hour:
            hours = lst[:1]
        else:
            hours = lst[:2]
        prev_hour = lst[0]
        inserts = []
        for h in hours:
            logging.info('get series on {}'.format(h))
            for s in client.get_list_series(database='device1',
                                            measurement='device_value', tags={'hour': h}):
                d = dict(t.split('=') for t in s.split(',')[1:])
                d['table'] = 'device_1_{}'.format(d['deviceIdentifier'])
                inserts.append(d)
        logging.info('insert {} records'.format(len(inserts)))
        return inserts, prev_hour
    except Exception as e:
        raise e
    finally:
        logging.info("close influxdb client connection")
        client.close()


def sink(inserts):
    for row in inserts:
        table_name = row.get('table')
        v = {'attributeIdentifier': row.get('attributeIdentifier'), 'hour': row.get('hour')}
        ks = ','.join(['`{}`'.format(k) for k in v.keys()])
        vs = ','.join([':{}'.format(v) for v in v.keys()])
        stmt = text("""insert ignore into `{}` ({}) values ({})""".format(table_name, ks, vs))
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
  `last_scan_time` datetime(0) NULL DEFAULT NULL,
  `latest_point_time` datetime(0) NULL DEFAULT NULL,
  `create_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `u__attr_hour`(`attributeIdentifier`, `hour`) USING BTREE,
  INDEX `i__attr`(`attributeIdentifier`) USING BTREE,
  INDEX `i__hour`(`hour`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 10591 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;'''.format(
            table_name))
        engine.execute(stmt, v)


def main(prev_hour):
    inserts, prev_hour = beat(prev_hour)
    sink(inserts)


main(prev_hour)
