import yaml
import threading

class Configuration:
    _lock = threading.Lock()
    _local = threading.local()

    def __init__(self):
        with open("application.yaml", "r") as stream:
            config = yaml.safe_load(stream)

        # Redis.
        self.redis_host = config['redis']['host']
        self.redis_port = config['redis']['port']
        self.redis_db = config['redis']['db']

        # MinIO.
        self.minio_endpoint = config['minio']['endpoint']
        self.minio_access_key = config['minio']['access-key']
        self.minio_secret_key = config['minio']['secret-key']
        self.minio_bucket_name_videos = config['minio']['bucket-name-videos']

    @classmethod
    def get_instance(cls):
        with cls._lock:
            if not hasattr(cls._local, 'instance'):
                cls._local.instance = cls()
        return cls._local.instance