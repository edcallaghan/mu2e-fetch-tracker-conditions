# Ed Callaghan
# Collate hardware db and midas configuration info into Offline conditions tbls
# May 2025

import argparse
import json
import lz4.frame
import os.path
import psycopg2 as pg
import re
import subprocess as sp

def fetch_compressed_midas_configuration(host, base, pattern, run):
    path = os.path.join(base, pattern % run)
    tokens = ['ssh', host, 'cat %s' % path]
    rv = sp.check_output(tokens)
    return rv

def fetch_midas_configuration(host, base, pattern, run):
    # fetch compressed representation, and decompress
    buff = fetch_compressed_midas_configuration(host, base, pattern, run)
    buff = lz4.frame.decompress(buff)

    # find the first complete json chunk
    # assumes no brace appears inside of a key or value
    plus = b'{'[0]
    mnus = b'}'[0]
    start = 16
    if buff[start] != plus:
        raise Exception('malformed midas configuration')
    idx = start
    score = 0
    end = len(buff)
    while idx < end:
        if buff[idx] == plus:
            score += 1
        elif buff[idx] == mnus:
            score -= 1
        else:
            pass
        idx += 1
        if score < 1:
            break
    buff = buff[start:idx]
    buff = buff.decode()

    # interpret as json
    # assumes only ascii content
    rv = json.loads(buff)
    return rv

def get_midas_conditions(host, base, pattern, run):
    configuration = fetch_midas_configuration(host, base, pattern, run)
    configuration = configuration['Mu2e']
    configuration = configuration['RunConfigurations']
    configuration = configuration['train_station']
    configuration = configuration['Tracker']

    rv = {}
    for kst,vst in configuration.items():
        if re.match('Station_0{2}', kst):
            for kpl,vpl in vst.items():
                if re.match('Plane_[0-9]{2}', kpl):
                    for kpn,vpn in vpl.items():
                        if re.match('Panel_[0-9]{2}', kpn):
                            label = vpn['Name']
                            if label in rv.keys():
                                msg = 'encountered duplicate panel %s' % label
                                raise Exception(msg)
                            try:
                                enabled = vpn['ch_mask']
                                if len(enabled) != 96:
                                    raise Exception('malformed readout mask')
                                disabled = [i for i in range(len(enabled))
                                                if not enabled[i]]
                                rv[label] = {'readout_disabled': disabled}
                            except Exception as e:
                                msg = 'exception: %s: %s' % (label, str(e))
                                raise Exception(msg)
    return rv

def build_hardware_query(panels=[]):
    rv = 'SELECT panel_id,missing_straws,missing_wires from qc.panels'
    if 0 < len(panels):
        rv += ' WHERE'
    for i,panel in enumerate(panels):
        if 0 < i:
            rv += ' OR'
        rv += ' panel_id = %d' % panel
    rv += ' ORDER BY panel_id ASC;'
    return rv

def get_hardware_conditions(host, port, user, db, run):
    dsn = 'host=%s port=%d user=%s dbname=%s' % (host, port, user, db)
    connection = pg.connect(dsn)
    query = build_hardware_query()
    rv = {}
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                label = 'MN%03d' % row[0]
                rv[label] = {
                    'missing_straws': row[1],
                     'missing_wires': row[2],
                }
    connection.close()
    return rv

def get_conditions(run, config, allowed=None):
    midas_host = config['midas']['host']
    midas_base = config['midas']['base']
    midas_pattern = config['midas']['pattern']
    midas_conditions = get_midas_conditions(midas_host,
                                            midas_base,
                                            midas_pattern,
                                            run)

    pg_host = config['hardware']['host']
    pg_port = config['hardware']['port']
    pg_user = config['hardware']['user']
    pg_db = config['hardware']['db']
    hardware_conditions = get_hardware_conditions(pg_host,
                                                  pg_port,
                                                  pg_user,
                                                  pg_db,
                                                  run)

    if allowed is None:
        allowed = set(['MN%03d' % i for i in range(1,999+1)])
    midas_keys = set(midas_conditions.keys())
    hardware_keys = set(hardware_conditions.keys())
    keys = midas_keys.union(hardware_keys).intersection(allowed)

    rv = {}
    for key in keys:
        tmp = {}
        if key in midas_keys:
            tmp.update(midas_conditions[key])
        if key in hardware_keys:
            tmp.update(hardware_conditions[key])
        rv[key] = tmp

    return rv

def load_geographic_map(path):
    with open(path, 'r') as f:
        js = json.load(f)
    rv = {j['minnesota']: (j['plane'], j['panel']) for j in js}
    return rv

def load_config(path):
    with open(path, 'r') as f:
        rv = json.load(f)
    return rv

def write_offline_table(conditions, geography, scheme, write):
    for level,criteria in scheme.items(): 
        write(level)
        for label,lookup in criteria.items():
            for minnesota,collections in conditions.items():
                plane, panel = geography[minnesota]
                for straw in collections[lookup]:
                    write('%d_%d_%d, %s' % (plane, panel, straw, label))

def main(args):
    config = load_config(args.cpath)
    geography = load_geographic_map(args.mpath)
    conditions = get_conditions(args.run, config, allowed=args.panels)
    #print(json.dumps(conditions, indent=4, sort_keys=True))

    status_levels = {
        'TrkStrawStatusLong': {
            'Absent': 'missing_straws',
            'NoWire': 'missing_wires',
        },
        'TrkStrawStatusShort': {
            'Disabled': 'readout_disabled',
        },
    }
    write_offline_table(conditions, geography, status_levels, print)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--run', dest='run', type=int, required=True)
    parser.add_argument('-p', '--panels', dest='panels', nargs='+', default=None)
    parser.add_argument('-m', '--mapping', dest='mpath', type=str, required=True)
    parser.add_argument('-c', '--config', dest='cpath', type=str, required=True)

    args = parser.parse_args()
    main(args)
