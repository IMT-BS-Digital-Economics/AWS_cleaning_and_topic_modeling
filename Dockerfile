FROM python:3.9
LABEL authors="bricetoffolon"

WORKDIR /app

COPY . /app

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python", "main.py"]