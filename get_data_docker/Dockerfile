FROM python:3.10.9-bullseye

# Set the working directory inside the container
WORKDIR /app

# Copy your Python script to the container
COPY get_data.py /app/get_data.py
COPY requirements.txt /app/requirements.txt
COPY config.ini /app/config.ini
RUN pip install -r requirements.txt


# Start your Python script
CMD ["python", "/app/get_data.py"]
