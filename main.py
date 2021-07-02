import boto3
import os
from sys import argv
from botocore.exceptions import NoCredentialsError
import time
import humanize
import datetime as dt
import math

def local_dir_tree_info(root_dir):
    for (root,dirs,files) in os.walk(root_dir):
        print("# root : " + root)
        if len(dirs) > 0:
            for dir_name in dirs:
                print("dir: " + dir_name)

        if len(files) > 0:
            for file_name in files:
                print("file: " + file_name)


def create_bucket(bucket_name, region):
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def bucket_list():
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    print('Existign buckets')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')

def bucket_object_list(bucket_name):
    s3 = boto3.resource('s3')
    bucket=s3.Bucket(bucket_name)
    print("Bucket :" + bucket_name)
    print("-----------------------")
    for obj in bucket.objects.all():
	    print(obj.key,obj.size)
    print("-----------------------")

def delete_bucket(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()

def upload_to_s3(bucket_name,root_dir,title):
    s3 = boto3.client('s3')
    dir_count, file_count,total_speed,total_file_size= 0,0,0,0
    print("---------------Uploading files--------------------")

    output = open(title,'w')
    output.write("Filename,Filesize(B),Timespent(ms),Speed(B/ms)\n")
    #로컬 디렉토리 구조 확인
    for (root, dirs, files) in os.walk(root_dir):
        total_start_time = dt.datetime.now()
        #dir 생성
        if len(dirs) > 0:
            for dir in dirs:
                dir_count+=1
                s3.put_object(Bucket=bucket_name, Key=root + '/' + dir + '/')
        #파일 생성
        if len(files) > 0:
            for file in files:
                file_count+=1
                with open(root + '/' + file, "rb") as f:
                    start_time = time.time()
                    file_size = os.fstat(f.fileno()).st_size
                    total_file_size += file_size

                    s3.upload_fileobj(f, Bucket=bucket_name, Key=root + '/' + file)
                    end_time = time.time()
                    diff = end_time-start_time

                    speed=round(file_size/diff,2)
                    total_speed+=speed
                    output.write(file + ","+ str(file_size)+","+ format(diff,".2f") +","+str(speed)+"\n")
                    print("Uploaded :" +file + "  file size : "+ humanize.naturalsize(file_size)+
                          "  Timespent(ms): "+format(diff,".2f") +" Speed(per ms) : "+humanize.naturalsize(speed))
        total_end_time = dt.datetime.now()
    print("-----------------summary----------")
    print("시작 시간 : ",total_start_time.strftime('%Y-%m-%d %H:%M:%S'))
    print("종료 시간 : ",total_end_time.strftime('%Y-%m-%d %H:%M:%S'))
    print("소요 시간 : ",str((total_end_time-total_start_time).seconds) + " seconds")
    print("전체 파일 갯수 : ",file_count)
    print("전체 디렉터리 갯수 : ",dir_count)
    print("평균 전송 속도 : ",humanize.naturalsize(total_speed/file_count))
    print("전체 사이즈 :"+humanize.naturalsize(total_file_size))



root_dir = "model-store2"
bucket_name="sdc-mzc"
upload_to_s3(bucket_name=bucket_name,root_dir=root_dir,title="title")




