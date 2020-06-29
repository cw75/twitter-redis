#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import sys
import time
import zmq

import cloudpickle as cp

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

logging.basicConfig(filename='log_trigger.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

NUM_THREADS = 4

ips = []
with open('bench_ips.txt', 'r') as f:
    line = f.readline()
    while line:
        ips.append(line.strip())
        line = f.readline()

msg = sys.argv[1]
ctx = zmq.Context(1)

recv_socket = ctx.socket(zmq.PULL)
recv_socket.bind('tcp://*:3000')

sent_msgs = 0

if 'create' in msg:
    sckt = ctx.socket(zmq.PUSH)
    sckt.connect('tcp://' + ips[0] + ':3000')

    sckt.send_string(msg)
    sent_msgs += 1
else:
    for ip in ips:
        for tid in range(NUM_THREADS):
            sckt = ctx.socket(zmq.PUSH)
            sckt.connect('tcp://' + ip + ':' + str(3000 + tid))

            sckt.send_string(msg)
            sent_msgs += 1

epoch_total = []
total = []
end_recv = 0

epoch_recv = 0
epoch = 1
epoch_thruput = 0
epoch_start = time.time()

total_start = time.time()

while end_recv < sent_msgs:
    msg = recv_socket.recv()

    if b'END' in msg:
        end_recv += 1
    else:
        msg = cp.loads(msg)

        if type(msg) == tuple:
            epoch_thruput += msg[0]
            new_tot = msg[1]
        else:
            new_tot = msg

        epoch_total += new_tot
        total += new_tot
        epoch_recv += 1

        if epoch_recv == sent_msgs:
            epoch_end = time.time()

            logging.info('\n\n*** EPOCH %d ***' % (epoch))
            logging.info('\tTHROUGHPUT: %.2f' % (epoch_thruput))
            print_latency_stats(epoch_total, 'E2E', True)

            epoch_recv = 0
            epoch_thruput = 0
            epoch_total.clear()
            epoch_start = time.time()
            epoch += 1

total_end = time.time()

logging.info('*** END ***')
#throughput = len(total) / (total_end - total_start)
#logging.info('Overall throughput is %.2f' % throughput)
if len(total) > 0:
    print_latency_stats(total, 'E2E', True)
