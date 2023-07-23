FROM python:3.9
WORKDIR /app
COPY requirements.txt ./
RUN pip3 install -r ./requirements.txt --no-cache-dir
COPY ./ ./