FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
default-jre wget \
&& rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN wget https://repo1.maven.org/maven2/ch/cern/sparkmeasure/spark-measure_2.13/0.27/spark-measure_2.13-0.27.jar
RUN mkdir -p /root/.ivy2.5.2/jars/
RUN mv spark-measure_2.13-0.27.jar /root/.ivy2.5.2/jars/ch.cern.sparkmeasure:spark-measure_2.13:0.27.jar

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN groupadd -g 1001 appuser && useradd -d /app -r -u 1001 -g appuser appuser
RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
