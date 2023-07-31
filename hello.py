from pyspark.sql import SparkSession

def word_count(file_path):
    # Create a SparkSession
    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    # Read the text file into an RDD
    lines = spark.sparkContext.textFile(file_path)

    # Perform word count
    word_counts = lines.flatMap(lambda line: line.split(" ")) \
                      .map(lambda word: (word, 1)) \
                      .reduceByKey(lambda a, b: a + b)

    # Print the result
    for word, count in word_counts.collect():
        print(f"{word}: {count}")

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    # Replace 'path/to/your/textfile.txt' with the actual path of the text file you want to analyze
    file_path = "hello.txt"
    word_count(file_path)