# -*- coding: utf-8 -*-

"""kafka producer"""

import ConfigParser
import glob
import logging
import logging.config
import os
import signal
import sys
import time
import traceback

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
        if t[3] == "-":
            return (-1, None)
        if t[1] == "-" or t[1].startswith("192.168"):
            ipstr = t[-1]
        else:
            ipstr = t[1]
        t[0] = str(timeformat(t[0]))
        t[1] = str(ipstr_2_int(ipstr))
        t[3] = t[3].split(" ")[1]
        return (None, "\t".join(t))
    except:
        logging.error("reformat failed:%s, reason:%s", log, traceback.format_exc())
        return (None, None)

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

    def signal_handler(self, signum, frame):
        """handle signals"""
        logging.info("Signal handler called with signal %s", signum)
        self.quit_flag = True

    def run(self):
        """run process"""
        while True:
            try:
                if self.kafka_producer_obj is None:
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
                            (partition_key, newline) = logreformat(line)
                            if newline is None:
                                continue
                            else:
                                self.kafka_producer_obj.send(topic,
                                                             value=newline,
                                                             key=partition_key,)
                                self.counter += 1
                        
                        position = istream.tell()
                        ostream = open(seekfile, "w")
                        ostream.write("%d" % position)
                        ostream.close()
                        istream.close()

                if self.quit_flag:
                    self.kafka_producer_obj.flush()
                    return
            except kafka.errors.KafkaError:
                print traceback.format_exc()
                logging.error(traceback.format_exc())
                raise
            except:
                print traceback.format_exc()
                logging.error(traceback.format_exc())

def main():
    """main entry"""
    config_file = ""
    if len(sys.argv) > 0:
        config_file = sys.argv[1]
    else:
        print "usage: python kafkaproducer.py <config-file>\n" \
              "need <config-file> parameter\n"
        sys.exit(-1)
    producer = KafkaProducer(config_file)
    producer.run()

if __name__ == "__main__":
    main()
