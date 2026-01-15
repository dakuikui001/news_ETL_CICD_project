import great_expectations_common as gec
import os
from pathlib import Path

class Bronze():
    def __init__(self, spark_session, db_setup_manager):
        self.spark = spark_session
        self.manager = db_setup_manager
        
        # 1. 适配 Airflow 容器路径
        # 在 Astro 环境中，绝对路径通常是 /usr/local/airflow/include/
        if os.environ.get('AIRFLOW_HOME'):
            root = Path("/usr/local/airflow/include")
        else:
            # 兼容本地开发环境
            root = Path(__file__).resolve().parent.parent

        # 2. 确保指向正确的子文件夹
        # 根据你之前的截图，pyspark 脚本在 include/pyspark，data 在 include/data
        # 如果 root 是 include，那么 data 就在 root / "data"
        self.data_path = str(root / "data")
        self.checkpoint_path = str(root / "checkpoints")

        print(f"📂 数据加载路径: {self.data_path}")
        print(f"💾 Checkpoint 路径: {self.checkpoint_path}")

    def consume_straitstimes_news_bz(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F

        schema = '''
            title String,
            publish_date String,
            update_date String,
            img_url String,
            caption_text String,
            tags_list String,
            full_article String,
            url String
        '''
        
        df_stream = (self.spark.readStream
                        .format("csv")
                        .schema(schema)
                        .option("header", "true")
                        .option("recursiveFileLookup", "true") 
                        .option("pathGlobFilter", "*.csv")
                        .option("maxFilesPerTrigger", 10) 
                        .load(self.data_path)
                        .withColumn("load_time", F.current_timestamp())
                    )
        #processed_stream = preprocessing(df_stream)
        return self._write_stream_append(df_stream, "straitstimes_news_bz", "straitstimes_news_bz_ingestion_stream", "bronze_p1", once, processing_time)
    

    def _write_stream_append(self, df, path, query_name, pool, once, processing_time):
        table_name = path 
        checkpoint_path = self.checkpoint_path + f'/{path}'
        # 确保目录存在，否则 Spark Streaming 会报错
        os.makedirs(checkpoint_path, exist_ok=True)
        stream_writer = (df.writeStream
            .foreachBatch(lambda micro_df, batch_id: gec.validate_and_insert_process_batch(micro_df, batch_id, table_name, self.manager))
            .option("checkpointLocation", checkpoint_path)
            .queryName(query_name)
        ) 

        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
        
        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()


    def consume(self, once=True, processing_time="5 seconds"):
        import time
        start = int(time.time())
        print(f"\nStarting bronze layer consumption ...")
        
        self.consume_straitstimes_news_bz(once, processing_time)
        
        if once:
            for stream in self.spark.streams.active:
                stream.awaitTermination()
                
        print(f"Completed bronze layer consumtion {int(time.time()) - start} seconds")

