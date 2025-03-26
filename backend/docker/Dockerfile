# Use an official Spark base image with Python support
FROM apache/spark-py:3.5.0

# Set the working directory
WORKDIR /opt/spark/work-dir

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install delta-spark==3.0.0 boto3

# Download Hadoop AWS and AWS SDK JARs for S3 access
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar -o /opt/spark/jars/hadoop-aws-3.3.6.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.261/aws-java-sdk-bundle-1.12.261.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.261.jar

# Copy the script into the container
COPY ConvertParquetToDelta.py .

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Command to run the script (will be overridden in docker run)
CMD ["spark-submit", "ConvertParquetToDelta.py"]