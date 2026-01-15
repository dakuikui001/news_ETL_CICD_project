import os
import time
from pathlib import Path
import pyspark.sql.functions as F

class Gold():
    def __init__(self, spark_session, db_setup_manager):
        self.spark = spark_session
        self.manager = db_setup_manager
        self.db_name = "news"
        self.sl_table = f"{self.db_name}.straitstimes_news_sl"
        self.fact_table = f"{self.db_name}.fact_news_gl"
        self.dim_tags_table = f"{self.db_name}.dim_tags_gl"

    def _get_ch_client(self):
        if hasattr(self.manager, 'client'):
            return self.manager.client
        return self.manager.manager.client

    def get_last_update_time(self, table_name):
        client = self._get_ch_client()
        query = f"SELECT max(update_time) FROM {table_name}"
        try:
            # 强制不使用驱动的时区转换
            result = client.query(query).result_rows
            if result and result[0][0]:
                return str(result[0][0])
        except:
            pass
        return "1970-01-01 00:00:00"
    
    def process_gold_layer(self):
        client = self._get_ch_client()
        last_time = self.get_last_update_time(self.fact_table)
        
        # 1. 从 Silver 读取数据，将时间强转为字符串以防止驱动干扰
        query = f"""
            SELECT 
                title, 
                toString(publish_date) as pub_str, 
                toString(update_date) as upd_str, 
                img_url, caption_text, 
                tags_list, full_article, url, load_time, update_time
            FROM {self.sl_table} 
            WHERE update_time > '{last_time}'
        """
        rows = client.query(query).result_rows
        if not rows:
            print("✨ Silver 层无新数据。")
            return

        # 定义 Schema 为全 String（不给 Spark 计算时区的机会）
        sl_columns = ['title', 'pub_str', 'upd_str', 'img_url', 'caption_text', 
                      'tags_list', 'full_article', 'url', 'load_time', 'update_time']
        df_sl = self.spark.createDataFrame(rows, schema=sl_columns)

        # 2. 物理截取与格式转换
        fact_df = (df_sl
            # --- 处理 publish_date：截取前 10 位 (yyyy-MM-dd) 并转为 Date ---
            .withColumn("publish_date", F.to_date(F.substring(F.col("pub_str"), 1, 10)))
            .withColumn("update_date", F.to_date(F.substring(F.col("upd_str"), 1, 10)))
            
            # --- 处理 timekey：截取 12-16 位 (HH:mm) ---
            .withColumn("publish_timekey", F.substring(F.col("pub_str"), 12, 5))
            .withColumn("update_timekey", F.substring(F.col("upd_str"), 12, 5))
            
            .withColumn("update_time", F.current_timestamp())
            .select(
                "url", "title", "publish_date", "update_date", 
                "img_url", "caption_text", "full_article", 
                "publish_timekey", "update_timekey", "update_time"
            )
        )

        dim_tags_df = (df_sl
            .select("url", "tags_list")
            .withColumn("cleaned_tags", F.regexp_replace(F.col("tags_list"), r"[\[\]']", ""))
            .withColumn("tag_array", F.split(F.col("cleaned_tags"), r",\s*"))
            .withColumn("tag", F.explode(F.col("tag_array")))
            .withColumn("tag", F.trim(F.col("tag")))
            .filter((F.col("tag") != "") & (F.col("tag").isNotNull()))
            .withColumn("update_time", F.current_timestamp())
            .select("url", "tag", "update_time")
        )

        # 写入
        insert_manager = self.manager if hasattr(self.manager, 'fast_insert') else self.manager.manager
        try:
            if fact_df.limit(1).count() > 0:
                insert_manager.fast_insert(fact_df, self.fact_table)
                print(f"✅ Fact 表成功写入 {fact_df.count()} 条")
            
            if dim_tags_df.limit(1).count() > 0:
                insert_manager.fast_insert(dim_tags_df, self.dim_tags_table)
                print(f"✅ Dim Tags 表成功写入 {dim_tags_df.count()} 条")
        except Exception as e:
            print(f"❌ Gold 层写入失败: {e}")

    def upsert(self):
        print(f"\n🚀 开始加工 Gold 层数据...")
        self.process_gold_layer()