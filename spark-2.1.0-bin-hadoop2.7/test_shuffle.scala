val textFile = sc.textFile("file:///home/ubuntu/crail-deployment/spark/README.md");
val linesWithSpark = textFile.filter(line => line.contains("Spark"));
val pairs = linesWithSpark.map(s => (s, 1));
val counts = pairs.reduceByKey((a, b) => a + b);
counts.first
