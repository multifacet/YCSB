FROM ubuntu:focal

ARG UNAME=tester
ARG UID=1000
ARG GID=1000

RUN apt-get -y update

# system dependencies
RUN apt-get -y install locales

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN apt-get -y install git
RUN apt-get -y install vim
RUN apt-get -y install build-essential
RUN apt-get -y install gcc
RUN apt-get -y install python3
RUN apt-get -y install maven
RUN apt-get -y install libz-dev
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata
RUN apt-get -y install openjdk-14-jdk

#ENV PATH="$venv/bin:$PATH"

RUN groupadd -g $GID $UNAME
RUN useradd -m -u $UID -g $GID -s /bin/bash $UNAME
RUN mkdir -p /home/$UNAME
USER $UNAME

WORKDIR /home/$UNAME

CMD /bin/bash
