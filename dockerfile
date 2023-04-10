FROM debian:latest
LABEL maintainer = 'Juyoung Jo <kitkat7333@gmail.com>'
RUN apt update -y && apt upgrade -y
# install python3
RUN apt install python3 -y
RUN apt install python3-pip -y
# install openmpi and non-root user setting
RUN apt install openmpi-bin libopenmpi-dev glibc-source -y
COPY requirements.txt /
RUN pip3 install -r requirements.txt
# change uid/gid and username @ each environment
RUN adduser -uid 1001 juyoung
RUN mkdir -p /home/juyoung/swmm521
WORKDIR /home/juyoung/swmm521

