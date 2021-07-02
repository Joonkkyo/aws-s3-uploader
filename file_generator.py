import logging
import os
import calendar
import time
import shutil
import threading
from queue import LifoQueue
import argparse
import datetime
from datetime import timedelta


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dircount', type=int, required=True, default=10, help="Dir Count")
    parser.add_argument('--target', type=str, default=None, help="Dir path to copy")
    parser.add_argument('--depth', type=int, required=True, default=2, help="Depth ")
    parser.add_argument('--thread-num', type=int, default=1, help="the number of threads (default: 5)")
    parser.add_argument('--filecount', type=int, default=10000, help="file count")
    parser.add_argument('--rate', nargs='+', required=True, help="file count rate")
    parser.add_argument('--append', type=str, default='N', choices=['Y', 'N'], help="Append or not")
    parser.add_argument('--csv', type=str, default='N', choices=['Y', 'N'], help="make csv file or not")
    return parser.parse_args()


class FileCopyWorker(threading.Thread):
    def __init__(self, queue, thread_id, file_count, rate, file_name, is_append, csv_file, start_date, is_csv):
        super().__init__()
        self.queue = queue
        self.thread_id = thread_id
        self.file_count = file_count
        self.rate = rate
        self.file_name = file_name
        self.dst_file_name = []
        self.dst_file_name_extension = []
        self.is_append = is_append
        self.LPG_idx = 0
        self.dst_name = file_name
        self.csv_file = csv_file
        self.start_date = start_date
        self.is_csv = is_csv

        for i in range(0, len(self.file_name)):
            file = self.file_name[i].split('/')
            file = file[-1].split('.')
            self.dst_file_name.append(file[0])
            self.dst_file_name_extension.append(file[-1])


    def run(self):
        while True:
            try:
                day = timedelta(days=1)
                path = self.queue.get()
                self.start_date = datetime.date(2021, 1, 19)
                parsed_path = path.split('/')
                word_idx = 0
                for word in parsed_path:
                    word_idx += 1
                    if word.startswith('D_'):
                        break

                log_path_list = parsed_path[word_idx - 1:]
                log_path = '/'.join(log_path_list)

                for i in range(0, 3):
                    for j in range(1, self.file_count * self.rate[i] // 100 + 1):
                        self.dst_name = path + '/' + 'L' + str((self.LPG_idx // 100) % 10) + '_' + \
                                'P' + str((self.LPG_idx // 10) % 10) + '_' + 'G' + str(self.LPG_idx % 1000) + '_'\
                                    + str(calendar.timegm(time.gmtime())) + '_' + str(self.LPG_idx) + '.' + \
                                self.dst_file_name_extension[i]

                        shutil.copy(src=self.file_name[i], dst=self.dst_name)
                        self.LPG_idx += 1

                        img_name = self.dst_name.split('/')[-1]
                        if self.file_count < 90:
                            self.start_date += day
                        elif self.LPG_idx % (self.file_count // 90) == 0:
                            self.start_date += day

                        # log_text = 'C ' + str(int(time.time())) + img_name + ', ' + log_path + ','
                        ts = datetime.datetime.now().strftime("T%H:%M:%S%fZ")
                        new_ts = ts[:-7] + '.' + ts[-4:]

                        if is_csv:
                            self.csv_file.write(img_name + ', ' + log_path + ',' + str(self.start_date)
                                                + new_ts + "\n")
                        # if self.is_append:
                        #     logger.info(f'{log_text}')

                print("thread id " + str(self.thread_id) + " create file done! " + path)
                self.LPG_idx = 0

            except BaseException as e:
                print("{0}: error during upload".format(self.thread_id))
                print(e.args)
            self.queue.task_done()


# def setting_log():
#     logger.setLevel(logging.INFO)
#     formatter = logging.Formatter('%(message)s')
#
#     stream_handler = logging.StreamHandler()
#     stream_handler.setFormatter(formatter)
#     logger.addHandler(stream_handler)
#
#     time_stamp = calendar.timegm(time.gmtime())
#     file_handler = logging.FileHandler('./filechange_' + str(time_stamp) + '.log')
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)


def make_dir(current_dir: str, dir_count: int, current_depth: int, queue, append: bool):
    if current_depth == 0:
        return

    for i in range(dir_count):
        if current_depth == depth:
            new_path = current_dir + '/' + 'D_' + str(i + 1)
        else:
            new_path = current_dir + '/' + current_dir.split('/')[-1] + '_' + str(i + 1)

        print("new path :", new_path)
        print("parsed :", new_path.split('/')[-1])
        q.put(new_path)

        if not append:
            os.mkdir(new_path)
        make_dir(new_path, dir_count, current_depth - 1, queue, append)
    return


if __name__ == "__main__":
    args = parse_args()
    # logger = logging.getLogger()
    target_path = args.target
    dir_count = args.dircount
    thread_num = args.thread_num
    depth = args.depth
    tmp_append = args.append
    file_count = args.filecount
    rate = args.rate
    tmp_csv = args.csv

    csv_file = None
    if tmp_csv == 'Y':
        is_csv = True
    else:
        is_csv = False

    if target_path is None:
        current_path = os.getcwd()
    else:
        current_path = target_path

    if tmp_append == 'Y':
        isAppend = True
        # setting_log()
    else:
        isAppend = False

    file_name = ['./data/img_s.jpg', './data/img_m.jpg', './data/img_l.tiff']

    print("file_name list : ", file_name)
    rate = list(map(int, rate))

    q = LifoQueue(maxsize=200000)

    start_date = datetime.date(2021, 1, 19)
    start_time = datetime.datetime.now()
    print("Creating dir ")
    make_dir(current_path, dir_count, depth, q, isAppend)
    print("Completed create dir")
    time_stamp = calendar.timegm(time.gmtime())

    if is_csv:
        csv_file = open("./filechange_" + str(time_stamp) + ".csv", "w")
        csv_file.write("file_name, directory, create_date" + "\n")

    for i in range(0, thread_num):
        t = FileCopyWorker(q, i, file_count, rate, file_name, isAppend, csv_file, start_date, is_csv)
        t.daemon = True
        t.start()
    q.join()

    if is_csv:
        csv_file.close()
    end_time = datetime.datetime.now()
    print("Completed!!!!")
    print("total spent time : ", str((end_time - start_time).seconds) + " seconds")
