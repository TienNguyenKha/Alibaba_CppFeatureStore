FROM python:3.8-slim

# Copy app handler code
COPY ./data_sample.csv data_sample.csv
COPY ./produce_json.py produce.py

# Install dependencies
RUN pip3 install kafka-python==2.0.2
RUN pip3 install pandas==1.5.1
RUN pip3 install pymongo==4.5.0

CMD [ "python", "-u", "produce.py", "--mode", "setup", "--bootstrap_servers", "broker:29092"]
