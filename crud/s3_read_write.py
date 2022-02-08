import os.path
import boto3
from pprint import pprint

HTML_FILE = 'index.html'


class S3Operations:
    def __init__(self, bucket):
        self.bucket = bucket
        self.s3_client = boto3.client('s3')
        self.files = []

    def read_s3object(self, file_path):
        obj = self.s3_client.get_object(
            Bucket=self.bucket,
            Key=file_path
        )
        if file_path is None:
            self.list_s3_object()
            file_path = self.files[int(input('Enter the number to read file : '))-1]
            with open(file_path.split('/')[1], 'wb') as file_obj:
                file_obj.write(obj['Body'].read())
        else:
            with open(os.path.join(os.path.dirname(__file__), file_path), 'wb') as file_obj:
                file_obj.write(obj['Body'].read())
        print(f"{file_path} has been downloaded")

    def write_s3object(self, file_path, local_filename):
        with open(os.path.join(os.path.dirname(__file__), local_filename)) as file:
            response = self.s3_client.put_object(
                Body=file.read(),
                Key=file_path,
                Bucket=self.bucket
            )
        print(f'{local_filename} has been uploaded to bucket')
        pprint(response)

    def delete_s3object(self):
        self.list_s3_object()
        file_name = self.files[int(input('Enter the number to delete file : '))-1]
        self.s3_client.delete_object(
            Bucket=self.bucket,
            Key=file_name,
        )
        print(f'{file_name} has been deleted')

    def list_s3_object(self):
        response = self.s3_client.list_objects(
                    Bucket=self.bucket,
                    Prefix='canvas/',
                    Delimiter='/'
        )
        self.files = [content['Key'] for content in response['Contents']]
        print(f'Following files are present in {self.bucket} ')
        [print(f"{index+1}. {filename}") for index, filename in enumerate(self.files)]


if __name__ == "__main__":
    bucket_name = input('Enter bucket name you want to connect to : ')
    s3_obj = S3Operations(bucket_name)
    while True:
        print('Choose the operation you want to perform on your s3 bucket ')
        print('1. Read a file from bucket')
        print('2. Upload a file to bucket')
        print('3. Delete a file from bucket')
        print('4. List items in bucket')
        print('Press any other key To exit')
        choice = int(input('input : '))
        if choice == 1:
            s3_obj.read_s3object()
        elif choice == 2:
            local_file = input('enter file name to upload')
            path = input('enter path to save in bucket')
            s3_obj.write_s3object(path, local_file)
        elif choice == 3:
            s3_obj.delete_s3object()
        elif choice == 4:
            s3_obj.list_s3_object()
            input('Press Enter key to continue : ')
        else:
            break
