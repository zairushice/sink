import re
from datetime import datetime
from influxdb import InfluxDBClient
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

engine = create_engine(
    'mysql://username:password@mysql-service:23306/bap_data_warehouse?charset=utf8&connect_timeout=5')


def beat():
    reo = re.compile('device_(\d+)_(\S+)$')
    tables = engine.table_names()
    for t in tables:
        m = reo.match(t)
        if not m:
            continue
        projectIdentifier, deviceIdentifier = m.group(1), m.group(2)
        stmt = text("""SELECT * FROM `{}` a WHERE state!=3 AND
                2>(SELECT COUNT(*) FROM `{}`
                    WHERE attributeIdentifier=a.attributeIdentifier AND
                    HOUR>a.hour AND state!=3)
                ORDER BY a.attributeIdentifier,a.hour DESC""".format(t, t))
        attrs = set()
        for r in engine.execute(stmt).fetchall():
            d = dict(r.items())
            d['table'] = t
            d['projectIdentifier'] = projectIdentifier
            d['deviceIdentifier'] = deviceIdentifier
            if d['attributeIdentifier'] in attrs:
                d['state'] = 2
            else:
                attrs.add(d['attributeIdentifier'])
            logging.info('beat {}'.format(d))
            yield d


def sink(msg):
    try:
        client = InfluxDBClient('influxdb-service', 28086, database='device1')
        query = ('SELECT value FROM device_value WHERE deviceIdentifier=$deviceIdentifier AND '
                 'attributeIdentifier=$attributeIdentifier AND hour=$hour')
        bind_params = {'deviceIdentifier': str(msg['deviceIdentifier']),
                       'attributeIdentifier': str(msg['attributeIdentifier']),
                       'hour': str(msg['hour'])}
        logging.info('read points for {}'.format(bind_params))
        r = client.query(query, bind_params=bind_params)
        if r.error:
            logging.error(r.error)
            return
        values, timestamps = [], []
        for p in r.get_points():
            values.append(p['value'])
            timestamps.append(p['time'])
        if not values:
            logging.info('there is no points')
            return

        logging.info('get agg values')
        args = {'id': msg['id']}
        args['min'] = min(values)
        args['max'] = max(values)
        args['first'] = values[0]
        args['last'] = values[-1]
        args['count'] = len(values)
        args['sum'] = sum(values)
        args['mean'] = sum(values) / len(values)
        args['latest_point_time'] = datetime.strptime(timestamps[-1],
                                                      '%Y-%m-%dT%H:%M:%SZ')
        if msg['state'] == 0:
            args['state'] = 1
        elif msg['state'] == 1:
            args['state'] = 1
        elif msg['state'] == 2:
            args['state'] = 3
        now = datetime.now()
        args['last_scan_time'] = now
        if msg['latest_point_time'] and msg['last_scan_time']:
            t = msg['latest_point_time'] + (now - msg['last_scan_time'])
            if t.hour != msg['latest_point_time'].hour:
                args['state'] = 3

        if args['count'] == msg['count'] and args['state'] == msg['state']:
            logging.info('data unchanged')
            return
        logging.info('update')
        stmt = text("""UPDATE `{}` SET `min`=:min, `max`=:max, `first`=:first,
                `last`=:last, `count`=:count, `sum`=:sum, `mean`=:mean,
                `state`=:state, `latest_point_time`=:latest_point_time,
                `last_scan_time`=:last_scan_time WHERE id=:id""".format(
            msg['table']))
        engine.execute(stmt, args)
    except Exception as e:
        raise e
    finally:
        logging.info("close influxdb client connection")
        client.close()


def main():
    g = beat()
    for msg in g:
        sink(msg)


main()
