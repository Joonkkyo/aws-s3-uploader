import boto3
import os
import argparse
import timeit
import humanize
import threading
import datetime as dt
import time
from boto3.s3.transfer import TransferConfig
from queue import LifoQueue


# Thread 변수 공유 클래스
class ThreadVariable:
    def __init__(self):
        self.lock = threading.Lock()
        self.lockedValue = 0
        self.buffer = []

    def plus(self, value):
        self.lock.acquire()
        try:
            self.lockedValue += value
        finally:
            self.lock.release()

    def append(self, value):
        self.lock.acquire()
        try:
            self.buffer.append(value)
        finally:
            self.lock.release()

    def list(self):
        return self.buffer


class Worker(threading.Thread):
    def __init__(self, queue, thread_id, bucket, title, repeat, max_concurrency, chunk_size, is_multipart):
        threading.Thread.__init__(self)
        self.queue = queue
        self.repeat = repeat
        self.thread_id = thread_id
        self.bucket = bucket
        self.total_speed = 0
        self.total_file_size = 0
        self.title = title
        self.is_multipart = is_multipart
        self.config = TransferConfig(multipart_threshold=1024 * chunk_size,
                                     max_concurrency=max_concurrency,
                                     multipart_chunksize=1024 * chunk_size,
                                     use_threads=True)

    def run(self):
        while True:
            try:
                global total_file_size
                global total_speed
                global buffer
                file_name = self.queue.get()
                if file_name[-1] == '/':
                    s3.put_object(Bucket=self.bucket, Key=file_name)

                else:
                    with open(file_name, "rb") as f:
                        file_size = os.fstat(f.fileno()).st_size
                        total_file_size.plus(file_size)
                        print("file size :", file_size)
                        print("file_name :", file_name)
                        start_time = timeit.default_timer()
                        file_list = file_name.split('/')
                        file_path = ''
                        if self.repeat != 1:
                          file_list[0] = file_list[0] + str(self.repeat)
                        for path in file_list:
                            file_path += path + '/'
                        file_path = file_path[:-1]

                        # 25MB 이상 크기의 파일에 대해서 Multipart upload 적용
                        if file_size >= 25 * 1024 * 1024 and is_multipart:
                            s3_resource.Object(self.bucket, file_path).upload_file(file_name, Config=self.config)
                        else:
                            s3.upload_fileobj(f, Bucket=self.bucket, Key=file_path)

                        end_time = timeit.default_timer()
                        diff = end_time - start_time
                        speed = round(file_size / diff, 2)
                        total_speed.plus(speed)
                        text = file_path + "," + str(file_size) + "," + format(diff, ".2f") + "," + str(speed) + "\n"
                        buffer.append(text)
                        print("Uploaded :" + file_path + "  file size : " + humanize.naturalsize(file_size) +
                              "  Timespent(ms): " + format(diff, ".2f") + " Speed(per ms) : " + humanize.naturalsize(speed))

            except BaseException as e:
                print("{0}: error during upload".format(self.thread_id))
                print(e.args)
            self.queue.task_done()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', type=str, required=True, default=None, help="S3 Bucket name")
    parser.add_argument('--dir', type=str, required=True, default=None, help="directory name to upload ")
    parser.add_argument('--thread-num', type=int, default=5, help="the number of threads (default: 5)")
    parser.add_argument('--chunk-size', type=int, default=25, help="Multipart chunk size (default: 25MB)")
    parser.add_argument('--max-concurrency', type=int, default=10, help="Multipart max concurrency (default: 10)")
    parser.add_argument('--repeat', type=int, default=1, help="repeat (default: 1)")
    parser.add_argument('--report-filename', type=str, default="report", help="report file name repo")
    # parser.add_argument('--access-key', type=str, required=True, default=None, help="Access Key for AWS s3")
    # parser.add_argument('--secret-key', type=str, required=True, default=None, help="Secret Key for AWS s3")
    parser.add_argument('--endpoint-url', type=str, default=None, help="End point url for AWS s3")
    parser.add_argument('--multipart', type=str, default='N', choices=['Y', 'N'], help="End point url for AWS s3")

    return parser.parse_args()


def delete_bucket(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()
    # bucket.delete()


def upload(root_dir, bucket, thread_num, title, repeat, max_concurrency, chunk_size, is_multipart):
    global total_file_size
    global total_speed
    global buffer

    total_file_size = ThreadVariable()
    total_speed = ThreadVariable()
    buffer = ThreadVariable()

    file_count = 0
    dir_count = 1
    max_workers = thread_num
    total_start_time = dt.datetime.now()

    output = open(title, mode='wt', encoding='utf-8')
    output.write("Filename, Filesize(B), Timespent(ms), Speed(B/ms)\n")
    for count in range(1, repeat + 1):
        q = LifoQueue(maxsize=2000)
        for i in range(max_workers):
            t = Worker(q, i, bucket, title, count, max_concurrency, chunk_size, is_multipart)
            t.daemon = True
            t.start()

        for root, dirs, files, in os.walk(root_dir):
            if len(dirs) > 0:
                for dir in dirs:
                    dir_count += 1
                    dir_list = root.split('/')
                    dir_path = ''
                    if count != 1:
                        dir_list[0] = dir_list[0] + str(count)
                    for dir in dir_list:
                        dir_path += dir + '/'
                    q.put(dir_path)
            if len(files) > 0:
                for file in files:
                    q.put(root + '/' + file)
                    file_count += 1
                    while q.qsize() > (q.maxsize - max_workers):
                        time.sleep(10)
        q.join()
    total_end_time = dt.datetime.now()
    output.write(''.join(buffer.list()))

    print("-----------------summary----------")
    print("시작 시간 : ", total_start_time.strftime('%Y-%m-%d %H:%M:%S'))
    output.write("시작 시간 : " + total_start_time.strftime('%Y-%m-%d %H:%M:%S') +'\n')
    print("종료 시간 : ", total_end_time.strftime('%Y-%m-%d %H:%M:%S'))
    output.write("종료 시간 : " + total_end_time.strftime('%Y-%m-%d %H:%M:%S') +'\n')
    print("소요 시간 : ", str((total_end_time - total_start_time).seconds) + " seconds")
    output.write("소요 시간 : " + str((total_end_time - total_start_time).seconds) + " seconds" + '\n')
    print("전체 파일 갯수 : " + str(file_count))
    output.write("전체 파일 갯수 : " + str(file_count) + '\n')
    print("전체 디렉터리 갯수 : ", str(dir_count))
    output.write("전체 디렉터리 갯수 : " + str(dir_count) + '\n')
    print("평균 전송 속도 : ", humanize.naturalsize(total_speed.lockedValue / file_count))
    output.write("평균 전송 속도 : " + humanize.naturalsize(total_speed.lockedValue / file_count) + '\n')
    print("전체 사이즈 :" + humanize.naturalsize(total_file_size.lockedValue))
    output.write("전체 사이즈 :" + humanize.naturalsize(total_file_size.lockedValue) + '\n')
    print("---------------------------------------")
    output.close()


def bucket_object_list(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    print("Bucket: " + bucket_name)
    print("-----------------------")
    for obj in bucket.objects.all():
        print(obj.key, obj.size)
    print("-----------------------")


if __name__ == '__main__':
    args = parse_args()
    # delete_bucket('sdc-mzc')

    s3 = boto3.client('s3',
#                       aws_access_key_id
#                       aws_secret_access_key
                      endpoint_url=args.endpoint_url)

    s3_resource = boto3.resource('s3')
    time = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))

    tmp_multipart = args.multipart
    if tmp_multipart == 'Y':
        is_multipart = True
    else:
        is_multipart = False

    upload(args.dir, args.bucket, args.thread_num, args.report_filename + time + ".txt", args.repeat,
           args.max_concurrency, args.chunk_size, is_multipart)
    bucket = s3_resource.Bucket(args.bucket)

    print("Bucket: " + args.bucket)
    # print("-----------------------")
    # for obj in bucket.objects.all():
    #     print(obj.key, obj.size)
    # print("-----------------------")

    # bucket_object_list('sdc-mzc')
