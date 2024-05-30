import boto3
import os

s3 = boto3.client("s3", region_name = "us-east-1")
# print((s3.list_buckets()["Buckets"][0]["Name"]))
# access name for each bucket
for el in s3.list_buckets()["Buckets"]:
    print(el["Name"])


# creating  bucket
s3.create_bucket(Bucket = "food-delivery-system")
# upload files

# s3.upload_file()
for uploads in os.listdir("./resources"):
    # print(os.path.abspath(uploads))
    s3.upload_file(os.getcwd()+"\\resources\\" + uploads, "food-delivery-system", uploads.rstrip(".csv"))
    # print(os.getcwd()+"\\resources\\" + uploads)
