import glob, os, sys
import argparse
import pandas as pd
from tqdm import tqdm
from subprocess import call
import sys
import click
from line_protocol_parser import parse_line
from time import sleep

assert sys.version[:1] == "3"

import paho.mqtt.client as mqtt
import json
import threading

data = {"flag": False}


def on_connect(self, client, userdata, rc):
    global data
    if rc == 0:
        data['flag'] = True
        print("connected OK Returned code=", rc)
    else:
        print("Bad connection Returned code=", rc)


@click.group()
def cli():
    """A CLI wrapper for Line2Pub-lish"""


if __name__ == '__main__':
    cli()


def filename(path):
    return os.path.basename(path).split('.csv')[0]


def loop(data):
    while not data['flag']:
        pass

    print(data['file'])

    file = data['file']
    model = data['model']
    client = data['client']

    num_lines = sum(1 for line in open(file, 'r'))
    pbar = tqdm(total=num_lines, leave=False, unit='lines')
    # telegraf/mart-ubuntu-s-1vcpu-1gb-sgp1-01/Model-PRO
    with open(file, 'r') as f:
        for line in f:
            sleep(0.00066)
            parsed = parse_line(line)
            topic = parsed['tags']['topic'].split("/gearname/")
            topic = "DUSTBOY/{}/{}".format(model, topic[1])
            parsed['timestamp'] = str(parsed['time']) + '000'
            # print(parsed['timestamp'])
            parsed['tags']['topic'] = topic
            pub_topic = 'etl/x/{}'.format(model)
            client.publish(pub_topic, json.dumps(parsed, sort_keys=True))
            pbar.update(1)
        pbar.close()


t = threading.Thread(target=loop, args=(data,))
t.start()


# @click.option('--output-dir', required=True, type=str, help='Output directory')
@click.option('--file', required=True, type=str, help='Text file with line protocol format')
@click.option('--model', required=True, type=str, help='Model ID')
@click.option('--username', required=False, type=str, help='')
@click.option('--password', required=False, type=str, help='')
@click.option('--host', required=True, type=str, help='')
@click.option('--port', required=False, type=int, help='')
@cli.command("publish")
def cc(file, model, username, password, host, port):
    """publish influx line protocol to mqtt !!!"""

    print(file, model, username, password, host, port)

    client = mqtt.Client()
    if username:
        client.username_pw_set(username, password)
    client.on_connect = on_connect

    data['file'] = file
    data['model'] = model
    data['client'] = client

    # client.on_message = on_message
    client.connect(host, port)
    client.loop_forever()

def to_line(row):
    time = row['time']
    name = row['name']
    topic = row['topic']
    if 'host' in row:
        row = row.drop(labels=['time', 'name', 'topic', 'host'])
    else:
        row = row.drop(labels=['time', 'name', 'topic'])

    row = row[row != 0]
    s = "{},topic={} ".format(name, topic)
    for (key, val) in row.iteritems():
        s += "{}={},".format(key, val)
    s = s[:-1] + ' ' + str(time) + '\n'
    return s

# def write_meta(db, output_dir):
# 	lines = [
# 		"# DDL",
# 		"CREATE DATABASE {}".format(db),
# 		"",
# 		"# DML",
# 		"# CONTEXT-DATABASE: {}".format(db),
# 		""
# 	]
# 	with open(os.path.abspath(output_dir) + '/../meta.txt', 'w') as meta:
# 		meta.write("\n".join(lines))