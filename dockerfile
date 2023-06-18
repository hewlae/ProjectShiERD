FROM debian:latest
LABEL maintainer = 'Juyoung Jo <kitkat7333@gmail.com>'
RUN apt-get update -y && apt-get upgrade -y
RUN DEBIAN_FRONTED=noninteractive apt-get install -y locales
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    dpkg-reconfigure --frontend=noninteractive locales && \
    update-locale LANG=en_US.UTF-8
ENV LANG en_US.UTF-8 
# install python3
RUN apt-get install python3 -y
RUN apt-get install python3-pip -y
# install openmpi and non-root user setting
RUN apt-get install openmpi-bin libopenmpi-dev glibc-source -y
COPY requirements.txt /
RUN pip3 install -r requirements.txt --break-system-packages
# change uid/gid and username @ each environment
RUN adduser -uid 1010 juyoung
RUN mkdir -p /home/juyoung/swmm521
WORKDIR /home/juyoung/swmm521

