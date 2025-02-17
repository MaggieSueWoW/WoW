FROM python:3.11.9
WORKDIR /app

RUN apt-get update && apt-get install -y \
    && apt-get -y clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENV TZ=America/Los_Angeles

ENTRYPOINT ["python3"]
