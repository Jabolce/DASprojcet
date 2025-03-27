
FROM python:3.10-slim

ENV TZ=Europe/Skopje
RUN apt-get update && apt-get install -y cron tzdata gcc g++ \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone

COPY templates/ /app/templates/

RUN apt-get update && apt-get install -y default-jdk curl gnupg2 procps \
 && export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::") \
 && echo "export JAVA_HOME=$JAVA_HOME" >> /etc/profile \
 && echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /etc/profile

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-u", "Data_Ingestion_Service.py"]
CMD ["python", "-u", "Data_Analysis_Service.py"]



