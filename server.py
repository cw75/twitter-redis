import logging
import sys

from rediscluster import RedisCluster
import cloudpickle as cp
import numpy as np
import time
import uuid

import random

import zmq

logging.basicConfig(filename='log_benchmark.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

logging.info(sys.argv[1])
logging.info(sys.argv[2])

tid = int(sys.argv[2])

startup_nodes = [{"host": "kvs-debug.kvm9la.clustercfg.use1.cache.amazonaws", "port": "6379"}]
logging.info('Connecting to Redis')
redis = RedisCluster(startup_nodes=startup_nodes, decode_responses=False, skip_full_coverage_check=True)
logging.info('Connected')

f = open('/graph', "rb")
users, follow = cp.load(f)


def print_latency_stats(data, ident, log=False, epoch=0):
    npdata = np.array(data)
    tput = 0

    if epoch > 0:
        tput = len(data) / epoch

    mean = np.mean(npdata)
    median = np.percentile(npdata, 50)
    p75 = np.percentile(npdata, 75)
    p95 = np.percentile(npdata, 95)
    p99 = np.percentile(npdata, 99)
    mx = np.max(npdata)

    p25 = np.percentile(npdata, 25)
    p05 = np.percentile(npdata, 5)
    p01 = np.percentile(npdata, 1)
    mn = np.min(npdata)

    output = ('%s LATENCY:\n\tsample size: %d\n' +
              '\tTHROUGHPUT: %.4f\n'
              '\tmean: %.6f, median: %.6f\n' +
              '\tmin/max: (%.6f, %.6f)\n' +
              '\tp25/p75: (%.6f, %.6f)\n' +
              '\tp5/p95: (%.6f, %.6f)\n' +
              '\tp1/p99: (%.6f, %.6f)') % (ident, len(data), tput, mean,
                                           median, mn, mx, p25, p75, p05, p95,
                                           p01, p99)

    if log:
        logging.info(output)
    else:
        print(output)

def run(create, sckt):
    if create:
        logging.info('Create mode')
        tweets = ['Today is a beautifle day', 'I love cat!', 'I watched an interesting movie', 'What is wrong with this?', 'I cannot believe it!']
        for i, uid in enumerate(users):
            if i % 100 == 0:
                logging.info(uid)
            others = users.copy()
            others.remove(uid)
            tids = set()
            user_tweets = np.random.choice(tweets, size=3, replace=False)
            for tweet in user_tweets:
                tid = str(uuid.uuid1())
                tids.add(tid)
                value = cp.dumps(uid + ': ' + tweet)
                redis.put(tid, value)
            reply_uids = np.random.choice(others, size=2, replace=False)
            for j, reply_uid in enumerate(reply_uids):
                tid = str(uuid.uuid1())
                tids.add(tid)
                value = cp.dumps(reply_uid + ' reply: this is great!')
                redis.put(tid, value)

            kvs.put(uid, cp.dumps(tids))

        return []
    else:
        logging.info('Benchmark mode')
        total_time = []

        log_start = time.time()

        log_epoch = 0
        epoch_total = []

        for i in range(10000):
            if i % 100 == 0:
                logging.info('request %s' % i)
            uid = np.random.choice(users, size=1, replace=False).tolist()[0]
            followers = follow[uid]
            target_uid = np.random.choice(followers, size=1, replace=False).tolist()[0]

            tids = cp.loads(redis.get(target_uid))
            tid = np.random.choice(tids, size=1, replace=False).tolist()[0]

            start = time.time()
            if random.random() < 0.1:
                tweet = cp.loads(redis.get(tid))
                result = 'read ' + tweet + ' no extra dependency'
                payload = cp.dumps(result)
                key = str(uuid.uuid1())

                redis.put(key, payload)
                tids.append(key)

                end = time.time()
                redis.put(target_uid, cp.dumps(tids))
            else:
                #logging.info('read')
                tweet = cp.loads(redis.get(tid))
                result = 'read ' + tweet + ' no extra dependency'
                end = time.time()

            epoch_total.append(end - start)
            total_time.append(end - start)

            log_end = time.time()
            if (log_end - log_start) > 5:
                throughput = len(epoch_total) / 5
                sckt.send(cp.dumps((throughput, epoch_total)))
                logging.info('EPOCH %d THROUGHPUT: %.2f' % (log_epoch, throughput))
                print_latency_stats(epoch_total, 'EPOCH %d E2E' %
                                          (log_epoch), True)

                epoch_total.clear()
                log_epoch += 1
                log_start = time.time()

        return total_time

ctx = zmq.Context(1)

benchmark_start_socket = ctx.socket(zmq.PULL)
benchmark_start_socket.bind('tcp://*:' + str(3000 + tid))

while True:
    msg = benchmark_start_socket.recv_string()
    splits = msg.split(':')

    resp_addr = splits[0]
    if len(splits) > 1:
        create = bool(splits[1])
    else:
        create = False

    sckt = ctx.socket(zmq.PUSH)
    sckt.connect('tcp://' + resp_addr + ':5000')
    total = run(create, sckt)

    if len(total) == 0:
        sckt.send(b'END')
        logging.info('*** Benchmark finished. It returned no results. ***')
    else:
        sckt.send(b'END')
        logging.info('*** Benchmark finished. ***')

    if len(total) > 0:
        logging.info('Total computation time: %.4f' % (sum(total)))
        print_latency_stats(total, 'E2E', True)