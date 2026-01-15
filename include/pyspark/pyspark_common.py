import os
import sys
import requests
import clickhouse_connect
from pyspark.sql import SparkSession

class ClickHouseSparkManager:
    def __init__(self, db_host="host.docker.internal", db_user="default", db_password="123456", db_port="8123", db_name="news"):
        # 1. 基础配置 (统一使用小写 news)
        self.db_name = db_name
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        
        # 2. 驱动配置 (保留 JDBC 驱动以备不时之需，但不再依赖它写入)
        self.jar_name = "clickhouse-jdbc-0.6.4-all.jar"
        self.jar_url = f"https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.4/{self.jar_name}"
        
        # 3. 初始化原生连接参数 (用于 clickhouse-connect)
        self.conn_params = {
            "host": self.db_host,
            "port": int(self.db_port),
            "username": self.db_user,
            "password": self.db_password,
            "database": self.db_name,
            "connect_timeout": 30
        }
        
        self._client = None

    @property
    def client(self):
        """懒加载原生客户端，确保连接在需要时才建立"""
        if self._client is None:
            try:
                self._client = clickhouse_connect.get_client(**self.conn_params)
            except Exception as e:
                print(f"⚠️ ClickHouse 原生连接失败: {e}")
        return self._client

    def _prepare_jdbc_driver(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        jar_path = os.path.join(current_dir, self.jar_name)
        if not os.path.exists(jar_path):
            print(f"🚚 正在下载驱动...")
            response = requests.get(self.jar_url, stream=True)
            with open(jar_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        return jar_path

    def create_session(self, app_name="NewsAnalysisProject"):
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        jar_path = self._prepare_jdbc_driver()
        
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars", jar_path) \
            .config("spark.sql.caseSensitive", "true") \
            .getOrCreate()

    def execute_ddl(self, sql_command):
        """使用原生客户端执行 DDL (比 JDBC 稳得多)"""
        try:
            self.client.command(sql_command)
            return True
        except Exception as e:
            print(f"❌ DDL 执行失败: {e}")
            return False

    def fast_insert(self, df, table_name):
        params = self.conn_params
        # 获取 DataFrame 的所有列名，确保只插入这些列
        columns = df.columns 
        
        def batch_insert(partition):
            import clickhouse_connect
            local_client = clickhouse_connect.get_client(**params)
            # 将 Row 转为 List 而不是 Dict，这样配合 column 写入最稳
            batch = [list(row) for row in partition]
            if batch:
                try:
                    # 明确指定列名进行插入，跳过有默认值的 load_time
                    local_client.insert(table_name, batch, column_names=columns)
                except Exception as e:
                    print(f"❌ 分区写入失败: {e}")
                finally:
                    local_client.close()

        print(f"🚀 正在通过 HTTP 协议分布式写入到 {table_name} (列: {columns})...")
        df.rdd.foreachPartition(batch_insert)
        print(f"✅ 写入完成")