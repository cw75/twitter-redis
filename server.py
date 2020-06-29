import logging
import sys

logging.basicConfig(filename='log_benchmark.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

logging.info(sys.argv[1])
logging.info(sys.argv[2])

while True:
    a = 1