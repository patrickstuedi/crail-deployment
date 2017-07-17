
# Vanilla terasort

# Generate data
./bin/spark-submit --master yarn --deploy-mode client --class com.github.ehiggs.spark.terasort.TeraGen /home/ubuntu/crail-deployment/spark/apps/jars-ana/spark-terasort-1.0-jar-with-dependencies.jar 1g data/terasort_in_1g


./bin/spark-submit --master yarn --deploy-mode client --class com.github.ehiggs.spark.terasort.TeraSort /home/ubuntu/crail-deployment/spark/apps/jars-ana/spark-terasort-1.0-jar-with-dependencies.jar data/terasort_in_1g data/terasort_out_1g


./bin/spark-submit --master yarn --deploy-mode client --class com.github.ehiggs.spark.terasort.TeraValidate /home/ubuntu/crail-deployment/spark/apps/jars-ana/spark-terasort-1.0-jar-with-dependencies.jar data/terasort_out_1g data/terasort_validate



./bin/spark-submit --master yarn --deploy-mode client --num-executors 8 --executor-cores 8 --executor-memory 48G --driver-memory 48G --class com.github.ehiggs.spark.terasort.TeraGen /home/ubuntu/crail-deployment/spark/apps/jars-ana/spark-terasort-1.0-jar-with-dependencies.jar 6000g data/terasort_in_6000g
