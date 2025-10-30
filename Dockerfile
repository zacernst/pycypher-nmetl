# Use the official Ubuntu image as the base
FROM ubuntu:22.04

# Update the package lists and install Python 3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget bash && \
    rm -rf /var/lib/apt/lists/*


# Set the working directory inside the container

COPY . /app

WORKDIR /app

RUN wget https://github.com/apple/foundationdb/releases/download/7.4.4/foundationdb-clients_7.4.4-1_aarch64.deb
RUN dpkg -i foundationdb-clients_7.4.4-1_aarch64.deb

RUN pip install uv
# RUN fdbcli --no-status -C "/app/fdb.cluster" --exec "configure new single ssd"
# RUN uv venv -p 3.14t
# RUN uv sync
# RUN uv build

CMD ["fdbcli --no-status --exec \"configure new single ssd\";writemode on;clearrange \"\" \"\\xFF\""]
CMD ["uv", "run", "packages/fastopendata/src/fastopendata/ingest.py"]
