spark-submit \
  --master spark://localhost:7077 \
  --deploy-mode client \
  --executor-cores 3 \
  --num-executors 4 \
  --executor-memory 10G \
  --class com.slim.FlightsDelayPredictionML \
  target/scala-2.12/flightsprojectsml_2.12-0.1.jar
