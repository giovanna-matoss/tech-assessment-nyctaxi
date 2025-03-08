from dotenv import load_dotenv
from pyspark.sql import SparkSession
from confluent_kafka import Producer
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os, logging, time

# Carrega o arquivo .env
load_dotenv()

# Inicializando o logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Variáveis
MINIO_URL = os.getenv('MINIO_URL')
ACCESS_KEY = os.getenv('MINIO_USER')
SECRET_KEY = os.getenv('MINIO_PASSWORD')
BUCKET_NAME = os.getenv('BUCKET_NAME')
BRONZE_PATH = os.getenv('BRONZE_PATH')
FILE_NAME = 'train.csv'
MINIO_PATH = f's3a://{BUCKET_NAME}/{BRONZE_PATH}/{FILE_NAME}'

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

# Criando sessão Spark e atribuindo credenciais de configuração da S3 (MinIO)
spark = SparkSession.builder \
    .appName('KafkaProducer') \
    .config('spark.hadoop.fs.s3a.endpoint', MINIO_URL) \
    .config('spark.hadoop.fs.s3a.access.key', ACCESS_KEY) \
    .config('spark.hadoop.fs.s3a.secret.key', SECRET_KEY) \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .getOrCreate()

# Configuração do Kafka e inicialização do Producer
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'taxi-fares'
producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

# Funções
# Função de callback para rastreabilidade de erros
def delivery_report(err, msg):
    if err:
        logger.error(f'Erro ao enviar mensagem para {msg.topic()} [{msg.partition()}]: {err}')
    else:
        logger.info(f'Mensagem enviada para {msg.topic()} [{msg.partition()}]')

# Leitura do csv na S3 (MinIO)
logger.info(f'Realizando leitura do CSV no caminho da S3: {MINIO_PATH}...')
df = spark.read.csv(MINIO_PATH, header=True, schema=schema)

# Converção dos dados de todo o dataframe em uma coluna JSON, para que o Kafka possa interpretar os dados como eventos
df_json = df.withColumn('value', F.to_json(F.struct([F.col(c) for c in df.columns])))

# Extração da coluna JSON para uma variável
data = df_json.select('value').rdd.map(lambda row: row.value).collect()

# Produção dos eventos no Kafka
logger.info(f'Enviando {len(data)} eventos para o Kafka...')
for event in data:
    try:
        producer.produce(TOPIC, key=None, value=event, callback=delivery_report)
        
        producer.flush()
        logger.info(f'Evento enviado para o Kafka: {event}')
        
        # Delay para simulação de chegada gradual
        time.sleep(2)
    except Exception as err:
        logger.error(f'Erro ao processar o evento {event}: {err}')

# Finaliza o envio
producer.flush()
logger.info('Produção de eventos finalizada com sucesso!')
