FROM foundationdb/build:rockylinux9_arm64-latest

WORKDIR /tmp
COPY . /tmp
RUN ./build.sh
EXPOSE 5050:5050

CMD ["/bin/bash", "./start.sh"]
