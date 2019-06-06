FROM python:2.7.15-alpine
COPY . /app
WORKDIR /app
RUN apk update && apk add postgresql-dev gcc python2-dev musl-dev
RUN pip install -r requirements.txt
CMD python ./main.py