import sys
import os

# 动态获取当前 Notebook 所在的目录
# 如果是在 papermill/airflow 中运行，getcwd() 通常能拿到正确路径
current_dir = os.getcwd() 

if current_dir not in sys.path:
    sys.path.append(current_dir)
from pyspark.sql import SparkSession
from pyspark_common import ClickHouseSparkManager 
import time
import os
import shutil
from pathlib import Path

class DBSetupManager():
    def __init__(self, spark_session, db_host="host.docker.internal", db_user="default", db_password="123456", db_name="news"):
        self.spark = spark_session
        self.db_name = db_name  # 建议使用小写 news
        
        # 保存配置，用于初始化 Manager
        self.db_config = {
            "db_host": db_host,
            "db_user": db_user,
            "db_password": db_password,
            "db_name": self.db_name
        }
        # 初始化管理工具
        self.manager = ClickHouseSparkManager(**self.db_config)
        root = Path(__file__).resolve().parent.parent
        self.checkpoint_path = str(root / "checkpoints")

    def create_straitstimes_news_bz(self):
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.db_name}.straitstimes_news_bz (
            title Nullable(String),
            publish_date Nullable(String),
            update_date Nullable(String),
            img_url Nullable(String),
            caption_text Nullable(String),
            tags_list Nullable(String),
            full_article Nullable(String),
            url String,
            load_time DateTime
        ) ENGINE = MergeTree() 
        ORDER BY (url)
        """
        if self.manager.execute_ddl(create_sql):
            print(f"✅ DDL 执行完成。{self.db_name}.straitstimes_news_bz")

    def create_straitstimes_news_sl(self):
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.db_name}.straitstimes_news_sl (
            title Nullable(String),
            publish_date Nullable(DateTime),
            update_date Nullable(DateTime),
            img_url Nullable(String),
            caption_text Nullable(String),
            tags_list Nullable(String),
            full_article Nullable(String),
            url String,
            load_time DateTime,
            update_time DateTime
        ) ENGINE = ReplacingMergeTree(load_time)
        ORDER BY (url)
        """
        if self.manager.execute_ddl(create_sql):
            print(f"✅ DDL 执行完成。{self.db_name}.straitstimes_news_sl")
    
    def create_fact_news_gl(self):
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.db_name}.fact_news_gl (
            url String,
            title Nullable(String),
            publish_date Nullable(Date),
            update_date Nullable(Date),
            img_url Nullable(String),
            caption_text Nullable(String),
            full_article Nullable(String),
            publish_timekey Nullable(String),
            update_timekey Nullable(String),
            update_time DateTime
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (url)
        """
        if self.manager.execute_ddl(create_sql):
            print(f"✅ DDL 执行完成。{self.db_name}.fact_news_gl")

    def create_dim_tags_gl(self):
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.db_name}.dim_tags_gl (
            url String,
            tag String,
            update_time DateTime
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (url, tag)
        """
        if self.manager.execute_ddl(create_sql):
            print(f"✅ DDL 执行完成。{self.db_name}.dim_tags_gl")       
            
    def create_data_quality_quarantine(self):
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.db_name}.data_quality_quarantine (
            table_name Nullable(String),
            gx_batch_id Nullable(String),
            violated_rules Nullable(String),
            raw_data Nullable(String),
            ingestion_time DateTime DEFAULT now()
        ) 
        ENGINE = MergeTree()
        ORDER BY (ingestion_time)
        """

        if self.manager.execute_ddl(create_sql):
            print("✅ 隔离表 DDL 执行完成。{self.db_name}.data_quality_quarantine")

    def assert_table_structure(self, table_name):
        """
        使用 clickhouse-connect 原生客户端验证表结构。
        """
        print(f"🔍 正在执行物理结构验证: {self.db_name}.{table_name}")
        
        try:
            # 直接查询 ClickHouse 系统元数据
            res = self.manager.client.query(f"DESCRIBE TABLE {self.db_name}.{table_name}")
            
            if len(res.result_rows) > 0:
                print(f"✅ 物理验证成功！表 {table_name} 存在，包含 {len(res.result_rows)} 个字段:")
                for row in res.result_rows:
                    print(f"   - {row[0]}: {row[1]}")
                return True
            else:
                print(f"❌ 警告: 表 {table_name} 似乎是空的或不存在。")
                return False
        except Exception as e:
            print(f"❌ 物理验证失败: {e}")
            return False

    def setup(self):
        start = int(time.time())
        print(f"正在准备 ClickHouse 环境 (库: {self.db_name})...")
        self.manager.execute_ddl(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        self.create_straitstimes_news_bz()
        self.create_straitstimes_news_sl()
        self.create_data_quality_quarantine()
        self.create_fact_news_gl()
        self.create_dim_tags_gl()
        print(f"🚀 初始化完成，耗时 {int(time.time()) - start} 秒")

    def validate(self):
        self.assert_table_structure("straitstimes_news_bz")
        self.assert_table_structure("straitstimes_news_sl")
        self.assert_table_structure("fact_news_gl")
        self.assert_table_structure("dim_tags_gl")
        self.assert_table_structure("data_quality_quarantine")        

    def cleanup(self):
        self.manager.execute_ddl(f"DROP TABLE IF EXISTS {self.db_name}.straitstimes_news_bz")
        self.manager.execute_ddl(f"DROP TABLE IF EXISTS {self.db_name}.straitstimes_news_sl")
        self.manager.execute_ddl(f"DROP TABLE IF EXISTS {self.db_name}.fact_news_gl")
        self.manager.execute_ddl(f"DROP TABLE IF EXISTS {self.db_name}.dim_tags_gl")
        self.manager.execute_ddl(f"DROP TABLE IF EXISTS {self.db_name}.data_quality_quarantine")
        if os.path.exists(self.checkpoint_path):
            shutil.rmtree(self.checkpoint_path)
            print("✅ Checkpoint 已重置")
        print("✅ 清理完成。")

    def check_duplicates(self, df):    
        print('Checking for duplicates ',end='')
        cleaned_df = df.dropDuplicates()
        print('OK')
        return df
        
    def check_all_null(self, df):
        print('Checking for all-null rows ', end='')
        cleaned_df = df.dropna(how='all')
        print('OK')
        return cleaned_df
    
    def check_null(self, df, columns):
        print('Checking for nulls on string columns ',end = '')
        processed_df1 = df.fillna('Unknown', subset = columns)
        print('OK')
        print('Checking for nulls on numeric columns ',end = '')
        processed_df2  = processed_df1.fillna(0, subset = columns)
        print('OK')
        return processed_df2
    
    def preprocessing(self, df):
        df1 = self.check_duplicates(df)
        df2 = self.check_all_null(df1)
        df3 = self.check_null(df2, df2.schema.names)
        return df3
