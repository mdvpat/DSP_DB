FROM ubuntu:20.04

RUN mkdir /root/api
COPY main.py fonctions.py /root/api/
RUN apt update && apt install python3-pip -y 
RUN apt install libmysqlclient-dev -y
RUN apt install uvicorn -y
RUN pip3 install fastapi pandas pydantic mysql mysql-connector-python python-multipart httptools==0.1.* uvloop
WORKDIR /root/api
CMD ["/entrypoint.sh"]
