FROM alpine:latest
RUN apk update
RUN apk add py-pip
RUN apk add --no-cache python3-dev 
RUN pip install --upgrade pip
WORKDIR /app
COPY ./app /app
RUN pip install -r requirements.txt
CMD ["python3", "app.py"]