from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os, argparse

# Carrega o arquivo .env
load_dotenv()

# Configurar o parser de argumentos
parser = argparse.ArgumentParser()
parser.add_argument('--year', required=True, help='Ano do processamento')
parser.add_argument('--month', required=True, help='Mês do processamento')
parser.add_argument('--day', required=True, help='Dia do processamento')

args = parser.parse_args()

# Acesso dos parametros
PROCESSED_YEAR = args.year
PROCESSED_MONTH = args.month
PROCESSED_DAY = args.day

# Variáveis
MINIO_URL = os.getenv('MINIO_URL')
ACCESS_KEY = os.getenv('MINIO_USER')
SECRET_KEY = os.getenv('MINIO_PASSWORD')
BUCKET_NAME = os.getenv('BUCKET_NAME')
SILVER_PATH = os.getenv('SILVER_PATH')
GOLD_PATH = os.getenv('GOLD_PATH')

# Criando a sessão Spark
spark = SparkSession.builder \
    .appName('analyzeRevenue') \
    .config('spark.hadoop.fs.s3a.endpoint', MINIO_URL) \
    .config('spark.hadoop.fs.s3a.access.key', ACCESS_KEY) \
    .config('spark.hadoop.fs.s3a.secret.key', SECRET_KEY) \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .getOrCreate()

# Lendo os dados convertidos em parquet
processed = spark.read.parquet(f's3a://{BUCKET_NAME}/{SILVER_PATH}/PROCESSED_YEAR={PROCESSED_YEAR}/PROCESSED_MONTH={PROCESSED_MONTH}/PROCESSED_DAY={PROCESSED_DAY}')

# Criação de coluna com semana e ano (YYYY-WW)
df_transformed = processed.withColumn(
    'week_of_year', F.concat_ws(
        '-',
        F.year(F.col('pickup_datetime')), 
        F.weekofyear(F.col('pickup_datetime'))
    )
)

# Agregação dos dados por semana
df_weekly = (
    df_transformed.groupBy('week_of_year')
    .agg(
        F.count('*').alias('total_rides'),  # Total de corridas na semana
        F.sum('fare_amount').alias('total_revenue'),  # Receita total da semana
        F.avg('fare_amount').alias('avg_ticket'),  # Ticket médio
        F.avg('passenger_count').alias('avg_passenger')  # Média de passageiros por corrida
    )
)

# Definição de ranking baseado em receita total e número de passageiros
df_ranked = df_weekly.orderBy(F.col('total_revenue').desc(), F.col('avg_passenger').desc())

# Escrita dos dados na S3 (MinIO)
df_ranked.write.mode('overwrite').parquet(f's3a://{BUCKET_NAME}/{GOLD_PATH}/PROCESSED_YEAR={PROCESSED_YEAR}/PROCESSED_MONTH={PROCESSED_MONTH}/PROCESSED_DAY={PROCESSED_DAY}')
