FROM python:3.12.3

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ./src/producers /app
COPY ../src/data /app/data
RUN ls -al /app/data

CMD ["python", "/app/app.py"]
