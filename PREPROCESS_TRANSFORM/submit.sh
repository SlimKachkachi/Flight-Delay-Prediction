spark-submit \
  --master spark://localhost:7077 \
  --deploy-mode client \
  --executor-cores 4 \
  --num-executors 3 \
  --executor-memory 6G \
  --class com.slim.FligthsDelayPrediction \
  target/scala-2.12/flightsprojects_2.12-0.1.jar
