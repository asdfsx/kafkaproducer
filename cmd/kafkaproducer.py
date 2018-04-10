# -*- coding: utf-8 -*-

"""kafka producer"""

import ConfigParser
import glob
import json
import logging
import logging.config
import os
import signal
import sys
import time
import traceback
import urlparse

import kafka

logging.config.fileConfig("etc/producer_logging.conf")

DEFAULT_CONFIG = {"kafkaservers":"127.0.0.1:9020",
                  "producerid":"kp911",
                  "nginxlog":"etl-kafka:/data/etllogs/%s/etl_log_%s.log",
                  "batchsize":"200"}

def read_config(config_file):
    """read configurations"""
    result = {}
    cf_obj = ConfigParser.ConfigParser()
    cf_obj.read(config_file)
    result["kafkaservers"] = cf_obj.get("kafkaproducer", "servers")
    result["producerid"] = cf_obj.get("kafkaproducer", "producerid")
    result["nginxlog"] = cf_obj.get("kafkaproducer", "nginxlog")
    result["batchsize"] = cf_obj.getint("kafkaproducer", "batchsize")
    result["reconnect_interval"] = cf_obj.getint("kafkaproducer", "reconnect_interval")

    tmp = result["nginxlog"].split(",")
    result["topic_ngxlog"] = {}

    for t in tmp:
        (topic, logpattern) = t.split(":")
        result["topic_ngxlog"][topic] = logpattern

    return result

def getlogfiles(logpattern):
    """get logfiles"""
    result = []
    for pattern in logpattern:
        result.extend(glob.glob(pattern))
    return result

def readseekfile(seekfile):
    """read seek file"""
    if not os.path.exists(seekfile):
        seekposition = 0
    else:
        istream = open(seekfile, "r")
        tmp = istream.readline().strip()
        if tmp != "":
            try:
                seekposition = int(tmp)
            except:
                logging.error(traceback.format_exc())
                seekposition = 0
        else:
            seekposition = 0
        istream.close()
    return seekposition

def ipstr_2_int(ip_str):
    """parse ip to int"""
    ip_items = ip_str.split(".")
    ip_int = 0
    for item in ip_items:
        ip_int = ip_int * 256 + int(item)
    return ip_int

def timeformat(timestr):
    """calculate a timestamp from the date"""
    timeobj = time.strptime(timestr[:19], "%Y-%m-%dT%H:%M:%S")
    return int(time.mktime(timeobj))

"""
sample data
2017-08-21T07:56:01+00:00	127.0.0.1	curl/7.29.0	POST /app HTTP/1.1	{\x22object_kind\x22:\x22merge_request\xE4\xB8\xAD\xE6\x96\x87\x22}	0.000	-	-	-
"""
def logreformat(log):
    """reformat the nginx log"""
    try:
        t = log.strip().split("\t")
        ipstr = ""
        if t[3] == "-" or "ELB-HealthChecker" in t[-1]:
            return (-1, None)

        if t[1] == "-" or t[1].startswith("192.168") or t[1].startswith("10.0.0"):
            ipstr = t[-1]
        else:
            ipstr = t[1]

        timestamp = timeformat(t[0])
        ipaddr = ipstr_2_int(ipstr)
        url = t[3].split(" ")[1]
        # json_decode = t[4].decode("string_escape")
        json_decode = urlparse.unquote(t[4])

        jsons = jsonflat(timestamp, ipaddr, url, json_decode)

        return jsons
    except:
        logging.error("reformat failed:%s, reason:%s", log, traceback.format_exc())
        return None

def jsonflat(timestamp, ipaddr, url, json_str):
    """extract packData"""
    result = []

    json_obj = json.loads(json_str)

    json_tmp = {"ts":timestamp,
                "ipaddr":ipaddr,
                "endpoint":url}

    for key in json_obj:
        if key != "packData":
            json_tmp[key] = json_obj[key]

    if "packData" in json_obj:
        for obj in json_obj["packData"]:
            tmp = json_tmp.copy()
            for key in obj:
                tmp[key] = obj[key]
            result.append(json.dumps(tmp))
    return result

class KafkaProducer(object):
    """log producer"""
    def __init__(self, config_file=""):
        self.config_file = config_file
        self.quit_flag = False
        self.kafka_producer_obj = None

        self.current = 0
        self.last = 0
        self.counter = 0
        self.logconfigs = {}

        signal.signal(signal.SIGINT, self.signal_handler)

        config = read_config(self.config_file)
        self.kafkaservers = config["kafkaservers"]
        self.producerid = config["producerid"]
        self.nginxlog = config["nginxlog"]
        self.batchsize = config["batchsize"]
        self.topic_ngxlog = config["topic_ngxlog"]
        self.reconnect_interval = config["reconnect_interval"]
        self.kafka_connect_time = int(time.time())

    def signal_handler(self, signum, frame):
        """handle signals"""
        logging.info("Signal handler called with signal %s", signum)
        self.quit_flag = True

    def run(self):
        """run process"""
        while True:
            try:
                current_second = int(time.time())
                if self.kafka_producer_obj is None:
                    self.kafka_producer_obj = kafka.KafkaProducer(bootstrap_servers=self.kafkaservers,
                                                                  client_id=self.producerid,
                                                                  batch_size=self.batchsize,)
                else current_second - self.kafka_connect_time > self.reconnect_interval:
                    self.kafka_producer_obj.close()
                    self.kafka_producer_obj = None
                    self.kafka_producer_obj = kafka.KafkaProducer(bootstrap_servers=self.kafkaservers,
                                                                  client_id=self.producerid,
                                                                  batch_size=self.batchsize,)

                self.current = int(time.time())
                lasthour = self.current - 3600
                currenthour_str = time.strftime("%Y%m%d%H", time.localtime(self.current))
                lasthour_str = time.strftime("%Y%m%d%H", time.localtime(lasthour))

                if self.current - self.last > 5:
                    self.last = self.current
                    print "producer counter:%s" % (self.counter,)
                    logging.info("producer counter:%s", self.counter)
                else:
                    time.sleep(1)
                    continue

                for topic in self.topic_ngxlog:
                    if "%s" in self.topic_ngxlog[topic]:
                        logpattern_current = self.topic_ngxlog[topic] % (
                            currenthour_str[0:8], currenthour_str)
                        logpattern_last = self.topic_ngxlog[topic] % (
                            lasthour_str[0:8], lasthour_str)
                        logs = getlogfiles((logpattern_current, logpattern_last))
                        self.logconfigs[topic] = logs
                    else:
                        self.logconfigs[topic] = (self.topic_ngxlog[topic],)

                for topic in self.logconfigs:
                    for log in self.logconfigs[topic]:
                        seekfile = log+".seek"
                        seekposition = readseekfile(seekfile)
                        filesize = os.path.getsize(log)
                        if filesize < seekposition:
                            seekposition = 0
                        elif filesize == seekposition:
                            continue

                        istream = open(log, "r")
                        if seekposition != 0:
                            istream.seek(seekposition)

                        for line in istream:
                            jsons = logreformat(line)
                            if jsons is None or not jsons:
                                continue
                            else:
                                for json in jsons:
                                    self.kafka_producer_obj.send(topic,
                                                                 value=json,
                                                                 key=None,)
                                self.counter += 1

                        position = istream.tell()
                        ostream = open(seekfile, "w")
                        ostream.write("%d" % position)
                        ostream.close()
                        istream.close()

                self.kafka_producer_obj.flush()
                # if self.quit_flag:
                #     self.kafka_producer_obj.flush()
                #     return
            except kafka.errors.KafkaError:
                print traceback.format_exc()
                logging.error(traceback.format_exc())
                self.kafka_producer_obj.close
                self.kafka_producer_obj = None
            except:
                print traceback.format_exc()
                logging.error(traceback.format_exc())

def main():
    """main entry"""
    config_file = ""
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        print "usage: python kafkaproducer.py <config-file>\n" \
              "need <config-file> parameter\n"
        sys.exit(-1)
    producer = KafkaProducer(config_file)
    producer.run()

if __name__ == "__main__":
    main()
