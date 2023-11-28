FROM python:3.9
LABEL authors="bricetoffolon"

WORKDIR /app

COPY . /app

RUN /app/clone.sh

RUN cp /app/aws_topic_modeling_env/.env .

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python", "main.py"]