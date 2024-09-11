from minio import Minio
from Configuration import Configuration

config = Configuration()

def list_objects(minio_client: Minio, bucket_name: str):
    # 列出bucket中的所有对象
    objects = minio_client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        print(obj.object_name)

if __name__ == "__main__":
    # 创建一个MinIO客户端实例
    minioClient: Minio = Minio(
        config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        secure=False  # 如果MinIO服务器是HTTPS，则设置为True
    )

    # 指定需要列出对象的bucket名称
    bucket_name = config.minio_bucket_name_videos

    # 调用函数列出bucket中的对象
    list_objects(minioClient, bucket_name)