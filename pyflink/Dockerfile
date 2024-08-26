FROM flink:1.18.1

# Install Python
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && \
rm -rf /var/lib/apt/lists/*

# Ensure python command points to python3
RUN ln -s /usr/bin/python3 /usr/bin/python

# Optional: Install any additional Python packages you need
# RUN pip3 install <additional-packages>

# In case that packages are downloaded manually
# COPY apache-flink*.tar.gz /
# RUN pip3 install /apache-flink-libraries*.tar.gz && pip3 install /apache-flink*.tar.gz

# install Java & updates
RUN apt-get update && \
    apt-get install -y openjdk-21-jdk && \
    apt-get install -y ant && \
    apt-get clean;

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64

WORKDIR /opt/flink
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY download_libs.sh .
RUN chmod +x ./download_libs.sh && ./download_libs.sh

COPY ./usr_jobs/ ./usr_jobs