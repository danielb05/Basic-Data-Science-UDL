FROM openjdk:8-slim

WORKDIR /

RUN apt-get update 
RUN apt-get install -y git wget
RUN git clone https://github.com/OpenRefine/OpenRefine.git

WORKDIR /OpenRefine
RUN chmod +x refine
RUN ./refine build
EXPOSE 3333

VOLUME [ "/data" ]
CMD ["./refine", "-i", "0.0.0.0", "-d", "/data", "run"]