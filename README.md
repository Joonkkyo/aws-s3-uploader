# aws s3 uploader

```
python aws_s3_upload.py --thread-num [the number of threads (default = 5)] 
```
## 기능
* AWS Bucket에 사용자가 설정한 절대 경로의 하위 디렉토리에 포함되는 모든 폴더, 파일을 멀티 쓰레드를 이용하여 업로드
* 업로드가 끝나면 다운로드 시작/종료시간, 파일 크기, 다운로드 속도, 파일/폴더 개수 출력
* 25MB 크기가 넘어가는 파일에 대해서는 Multipart upload 적용

* Multipart가 적용된 파일에 대한 업로드 결과 (config의 max_concurrency를 높이면 더 빠른 속도로 다운로드가 가능)
<img width="983" alt="스크린샷 2021-05-01 오후 4 58 52" src="https://user-images.githubusercontent.com/12121282/116775565-84881e80-aa9e-11eb-8fec-542f0e6daf43.png">

## Quick start

1. AWS S3애 접근 할 수 있는 token을 설정합니다. HOME/.aws/credentials 파일에 다음과 같이 access token,access secret key를 등록합니다.
   ```
   [default]
    aws_access_key_id = ACCESS_KEY
    aws_secret_access_key = ACCESS_TOKEN_KEY
   ```
2. 데이터 생성을 위한 파일을 실행합니다.
   ```
   python data_resize.py
   ```

3. install requirements
    ```
   pip install -r requirments.txt
   ```
4. python aws_s3_upload.py
   실행 시 다음과 같은 화면에 따라 입력을 진행합니다. 업로드 할 디렉터리는 현재 폴더의 상대 경로를 기반으로 적용합니다. 반복 횟수는 업로드 디렉토리를 반복해서 업로드 합니다.
  ![image](https://user-images.githubusercontent.com/37431938/116812816-a0b5b980-ab8b-11eb-8252-cf1d0bea9cb7.png)
   이후 다음과 같은 화면으로 진행 사항을 확인 할 수 있습니다.
   ![image](https://user-images.githubusercontent.com/37431938/116812971-7284a980-ab8c-11eb-998c-08c568a5a37b.png)
