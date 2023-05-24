FROM erlang:24

RUN apt update
RUN apt install -y cmake