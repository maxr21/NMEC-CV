import duckdb
import boto3 # <- AWS API SDK lets you interact with s3 buckets, official AWS tool
from botocore.config import Config # <- used to authenticate
from botocore import UNSIGNED # <- some sort of enum value, we don't need creds to access this bucket
import io
from PIL import Image # Could be replaced by OpenCV or Ultralytics

def connect_to_annotations():

    con = duckdb.connect()
    con.install_extension('httpfs')
    con.load_extension('httpfs')

    # Configure S3 (for public buckets you only need region)
    con.execute("SET s3_region='us-east-1'")
    con.execute("SET s3_access_key_id=''")
    con.execute("SET s3_secret_access_key=''")
    con.execute("SET s3_session_token=''")
    return con

def connect_to_img():
    # Anonymous S3 client (public bucket; no creds needed)
    return boto3.client(
        "s3",
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED)
    ) # communicate to AWS thru this object

def get_annotations(con: duckdb.DuckDBPyConnection = connect_to_annotations()):

    s3_url = "s3://coral-reef-training/mermaid/mermaid_confirmed_annotations.parquet"

    df_annotations = con.execute(f"SELECT * FROM read_parquet('{s3_url}')").df()
    df_images = df_annotations[["image_id","region_id","region_name"]].drop_duplicates("image_id")
    
    return df_annotations, df_images

def get_image_s3(image_id: str, bucket: str = "coral-reef-training", thumbnail: bool = False) -> Image.Image:
    s3 = connect_to_img()
    key = f"mermaid/{image_id}_thumbnail.png" if thumbnail else f"mermaid/{image_id}.png" # image to get
    resp = s3.get_object(Bucket=bucket, Key=key) # what is returned from s3
    return Image.open(io.BytesIO(resp["Body"].read()))