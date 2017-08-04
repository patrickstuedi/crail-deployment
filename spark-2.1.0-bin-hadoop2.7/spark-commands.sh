
# Vanilla terasort

# Generate data
./bin/spark-submit --master yarn --deploy-mode client --class com.github.ehiggs.spark.terasort.TeraGen /home/ubuntu/crail-deployment/spark/apps/jars-ana/spark-terasort-1.0-jar-with-dependencies.jar 1g data/terasort_in_1g


./bin/spark-submit --master yarn --deploy-mode client --class com.github.ehiggs.spark.terasort.TeraSort /home/ubuntu/crail-deployment/spark/apps/jars-ana/spark-terasort-1.0-jar-with-dependencies.jar data/terasort_in_1g data/terasort_out_1g


./bin/spark-submit --master yarn --deploy-mode client --class com.github.ehiggs.spark.terasort.TeraValidate /home/ubuntu/crail-deployment/spark/apps/jars-ana/spark-terasort-1.0-jar-with-dependencies.jar data/terasort_out_1g data/terasort_validate



./bin/spark-submit --master yarn --deploy-mode client --num-executors 16 --executor-cores 16 --executor-memory 48G --driver-memory 48G --class com.github.ehiggs.spark.terasort.TeraGen /home/ubuntu/crail-deployment/spark/apps/jars-ana/spark-terasort-1.0-jar-with-dependencies.jar 4000g data/terasort_in_4000g; ./bin/spark-submit --master yarn --deploy-mode client --num-executors 16 --executor-cores 16 --executor-memory 48G --driver-memory 48G --class com.github.ehiggs.spark.terasort.TeraSort /home/ubuntu/crail-deployment/spark/apps/jars-ana/spark-terasort-1.0-jar-with-dependencies.jar data/terasort_in_4000g data/terasort_out_4000g

./bin/spark-submit --master yarn --deploy-mode client --num-executors 8 --executor-cores 16 --executor-memory 48G --driver-memory 48G --class com.github.ehiggs.spark.terasort.TeraSort /home/ubuntu/crail-deployment/spark/apps/jars-ana/spark-terasort-1.0-jar-with-dependencies.jar data/terasort_in_750g data/terasort_out_750g

./bin/spark-submit --master yarn --deploy-mode client --num-executors 16 --executor-cores 16 --executor-memory 48G --driver-memory 48G --class com.github.ehiggs.spark.terasort.TeraGen /home/ubuntu/crail-deployment/spark/apps/jars-ana/spark-terasort-1.0-jar-with-dependencies.jar 4000g data/terasort_in_4000g; cd ~/PAT/PAT-collecting-data; ./pat run vanilla-spark-i3.8x-8node-2nvme-terasort4TB-16exec-16cores


#ebs-disk
./bin/spark-submit --master yarn --deploy-mode client --num-executors 16 --executor-cores 4 --executor-memory 12G --driver-memory 48G --class com.github.ehiggs.spark.terasort.TeraGen /home/ubuntu/crail-deployment/spark/apps/jars-ana/spark-terasort-1.0-jar-with-dependencies.jar 800g data/terasort_in_800g; cd ~/PAT/PAT-collecting-data; ./pat run vanilla-spark-r4.xl-8node-ebs-hdd-terasort800GB-16exec-4cores
