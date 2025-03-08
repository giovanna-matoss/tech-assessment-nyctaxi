from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
import os, argparse, logging

# Carrega o arquivo .env
load_dotenv()

# Inicializando o logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurar o parser de argumentos
parser = argparse.ArgumentParser()
parser.add_argument('--start_date', required=True, help='Data inicial')
parser.add_argument('--end_date', required=True, help='Data final')

args = parser.parse_args()

# Acesso dos parametros
start_date = args.start_date
end_date = args.end_date

# Variáveis
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPIC = 'taxi-fares'
MINIO_URL = os.getenv('MINIO_URL')
ACCESS_KEY = os.getenv('MINIO_USER')
SECRET_KEY = os.getenv('MINIO_PASSWORD')
BUCKET_NAME = os.getenv('BUCKET_NAME')
SILVER_PATH = os.getenv('SILVER_PATH')
MINIO_PATH = f's3a://{BUCKET_NAME}/{SILVER_PATH}'

schema = T.StructType([
    T.StructField('key', T.StringType(), True),
    T.StructField('fare_amount', T.DoubleType(), True),
    T.StructField('pickup_datetime', T.StringType(), True),
    T.StructField('pickup_longitude', T.DoubleType(), True),
    T.StructField('pickup_latitude', T.DoubleType(), True),
    T.StructField('dropoff_longitude', T.DoubleType(), True),
    T.StructField('dropoff_latitude', T.DoubleType(), True),
    T.StructField('passenger_count', T.DoubleType(), True)
])

# Funções
# Escrita do dataframe na S3 (MinIO)
# def write_df(df, path):
#     df.write.partitionBy('PROCESS_YEAR', 'PROCESS_MONTH', 'PROCESS_DAY') \
#         .mode('append') \
#         .parquet(path)

# Criando sessão Spark e atribuindo credenciais de configuração da S3 (MinIO)
spark = SparkSession.builder \
    .appName('KafkaConsumer') \
    .config('spark.hadoop.fs.s3a.endpoint', MINIO_URL) \
    .config('spark.hadoop.fs.s3a.access.key', ACCESS_KEY) \
    .config('spark.hadoop.fs.s3a.secret.key', SECRET_KEY) \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .getOrCreate()

# Utilizando streaming do spark para consumir os eventos que foram gerados
logger.info(f'Realizando consumo do tópico {TOPIC}...')
df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BROKER) \
    .option('subscribe', TOPIC) \
    .option('startingOffsets', 'earliest') \
    .load()

# Tratamento para converter JSON novamente em dataframe
logger.info(f'Convertendo JSON em Dataframe...')
df_parsed = df.selectExpr('CAST(value AS STRING)') \
    .select(
        F.from_json(
            F.col('value'), schema
        ).alias('data')
    ).select('data.*')

# Aplicando filtro no campo de data
df_filtered = df_parsed \
    .filter((F.col('pickup_datetime') >= start_date) & (F.col('pickup_datetime') <= end_date))

# Criação de campos com data de processamento para particionamento dos dados na S3
# Visando melhorar desempenho de consumo posterior dos dados
df_partioned = df_filtered \
    .withColumn('PROCESS_YEAR', F.year(F.current_date())) \
    .withColumn('PROCESS_MONTH', F.month(F.current_date())) \
    .withColumn('PROCESS_DAY', F.dayofmonth(F.current_date()))

# Escrita em modo de streaming dos dados na S3 (MinIO)
logger.info(f'Realizando disponibilização dos dados em parquet no caminho: {MINIO_PATH}')
query = df_partioned.writeStream \
    .format('parquet') \
    .option('path', MINIO_PATH) \
    .option('checkpointLocation', 'tmp/checkpoints/taxi-fares') \
    .partitionBy('PROCESS_YEAR', 'PROCESS_MONTH', 'PROCESS_DAY') \
    .outputMode('append') \
    .start()

    # Caso seja necessário utilização de função para delegar a escrita na S3
    # .foreachBatch(lambda batch_df, batch_id: write_df(batch_df, MINIO_PATH)) \
    # .outputMode('append') \
    # .start()

query.awaitTermination()
logger.info('Disponibilização no Datalake realizada!')