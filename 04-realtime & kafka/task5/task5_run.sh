# Как в task4 (все partition)
spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 \
task5.py

# Только конкретная partition
spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 \
task5.py \
--partition 0

# От конкретного offset
spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 \
task5.py \
--partition 0 \
--offset-from 100

# До конкретного offset
spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 \
task5.py \
--partition 0 \
--offset-to 500

# Диапазон offset
spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 \
task5.py \
--partition 0 \
--offset-from 100 \
--offset-to 500
