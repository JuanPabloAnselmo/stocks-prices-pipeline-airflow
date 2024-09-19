# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.10.1

# Copy the requirements.txt file into the root directory of the container
COPY requirements.txt /

# Install the dependencies specified in the requirements.txt file
# along with the specific version of Apache Airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
