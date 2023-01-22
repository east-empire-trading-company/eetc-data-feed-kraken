#Pull slim Python image
FROM python:3.10-slim-buster

# Set the working directory
WORKDIR /eetc_kraken

# Copy requirements.txt and install dependencies
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# Copy all required files
COPY . .

CMD ["python3", "-u", "main.py"]


