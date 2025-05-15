FROM python:3.12-alpine
ENV TZ "Europe/Moscow"
WORKDIR /home/scheduler

RUN apk update
RUN apk add make git automake gcc g++ subversion python3-dev zbar zbar-dev

RUN python -m pip install --upgrade pip
COPY requirements.txt .
RUN pip install -U -r requirements.txt

COPY . .

CMD ["python", "-m", "main"]
