FROM flink:1.18.0

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

COPY requirements.txt . 
RUN pip install -r requirements.txt

COPY ./usr_jobs/ ./usr_jobs

# WORKDIR /opt/flink
