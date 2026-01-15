import os
import time
from pathlib import Path
import pyspark.sql.functions as F

class Silver():
    def __init__(self, spark_session, db_setup_manager):
        self.spark = spark_session
        self.manager = db_setup_manager # 这是 DBSetupManager 实例
        self.db_name = "news"
        self.bz_table = f"{self.db_name}.straitstimes_news_bz"
        self.sl_table = f"{self.db_name}.straitstimes_news_sl"

    def _get_ch_client(self):
        """
        辅助函数：安全地获取 ClickHouse 客户端
        根据你的 DBSetupManager 结构，客户端通常在 manager 属性下
        """
        if hasattr(self.manager, 'client'):
            return self.manager.client
        elif hasattr(self.manager, 'manager') and hasattr(self.manager.manager, 'client'):
            return self.manager.manager.client
        else:
            raise AttributeError("❌ 无法在 DBSetupManager 中找到 ClickHouse 客户端，请检查 manager 结构")

    def get_last_load_time(self):
        """从 Silver 表获取最大水位线"""
        client = self._get_ch_client()
        query = f"SELECT max(load_time) FROM {self.sl_table}"
        try:
            result = client.query(query).result_rows
            if result and result[0][0]:
                # 确保返回的是字符串格式，方便 SQL 拼接
                return str(result[0][0])
        except Exception as e:
            print(f"⚠️ 无法获取水位线 (可能是新表): {e}")
        return "1970-01-01 00:00:00"

    def upsert_straitstimes_news_sl(self):
        """执行增量批处理"""
        client = self._get_ch_client()
        
        # 1. 获取水位线
        last_time = self.get_last_load_time()
        print(f"🔍 当前 Silver 表水位线: {last_time}")

        # 2. 从 Bronze 读取增量
        # 显式列出字段，确保 Spark DataFrame 构建时顺序一致
        columns = [
            'title', 'publish_date', 'update_date', 'img_url', 'caption_text', 
            'tags_list', 'full_article', 'url', 'load_time'
        ]
        cols_str = ", ".join(columns)
        
        incremental_query = f"""
            SELECT {cols_str} FROM {self.bz_table} 
            WHERE load_time > '{last_time}'
        """
        
        new_data_rows = client.query(incremental_query).result_rows
        
        if not new_data_rows:
            print("✨ 暂无新数据需要同步。")
            return

        # 3. 转换为 Spark DataFrame
        df_incremental = self.spark.createDataFrame(new_data_rows, schema=columns)\
                        .withColumn("publish_date", F.to_timestamp(F.col("publish_date"), "MMM dd, yyyy, hh:mm a"))\
                        .withColumn("update_date", F.to_timestamp(F.col("update_date"), "MMM dd, yyyy, hh:mm a"))\
                        .withColumn("update_time", F.current_timestamp()) # 记录本次处理时间

        # 3. 清洗转换
        processed_df = self.manager.preprocessing(df_incremental)
        # 5. 写入 Silver (ReplacingMergeTree 自动去重)
        row_count = processed_df.count()
        if row_count > 0:
            try:
                # 这里的 manager.manager.fast_insert 取决于你的实例层级
                # 如果 DBSetupManager 封装了 fast_insert，直接调用即可
                insert_manager = self.manager if hasattr(self.manager, 'fast_insert') else self.manager.manager
                insert_manager.fast_insert(processed_df, self.sl_table)
                print(f"✅ 成功同步 {row_count} 条数据至 Silver 层。")
            except Exception as e:
                print(f"❌ 写入 Silver 失败: {e}")

    def upsert(self, loop=False, interval=30):
        while True:
            start = time.time()
            print(f"\n{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 开始执行 Silver 层增量同步...")
            
            try:
                self.upsert_straitstimes_news_sl()
            except Exception as e:
                print(f"❌ 执行过程中发生错误: {e}")
            
            duration = int(time.time() - start)
            print(f"✨ 本次批处理耗时: {duration} 秒")
            
            if not loop:
                break
            time.sleep(interval)



            
