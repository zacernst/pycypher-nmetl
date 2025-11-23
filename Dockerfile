# Use the official Ubuntu image as the base
FROM ubuntu:22.04

# Update the package lists and install Python 3 and pip
RUN apt-get update && \
    apt-get install -y neovim python3 python3-pip wget bash libgdal-dev unzip dos2unix git && \
    rm -rf /var/lib/apt/lists/*

RUN apt update
RUN apt install -y -V ca-certificates lsb-release wget
RUN wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt update
RUN apt install -y -V libarrow-dev libarrow-glib-dev libarrow-dataset-dev libarrow-dataset-glib-dev libarrow-acero-dev libarrow-flight-dev libarrow-flight-glib-dev libarrow-flight-sql-dev libarrow-flight-sql-glib-dev libgandiva-dev libgandiva-glib-dev libparquet-dev libparquet-glib-dev 

# Set the working directory inside the container

# COPY . /app

RUN git clone https://github.com/zacernst/pycypher-nmetl.git
WORKDIR /pycypher-nmetl

RUN wget https://github.com/apple/foundationdb/releases/download/7.3.69/foundationdb-clients_7.3.69-1_aarch64.deb
RUN dpkg -i foundationdb-clients_7.3.69-1_aarch64.deb
RUN pip install uv
RUN uv venv -p 3.14t
RUN uv pip install packages/pycypher
RUN uv pip install packages/nmetl
RUN uv pip install packages/fastopendata

# RUN fdbcli --no-status -C "/app/fdb.cluster" --exec "configure new single ssd"
# RUN uv venv -p 3.14t
# RUN uv sync
# RUN uv build

# CMD ["fdbcli --no-status --exec \"configure new single ssd\";writemode on;clearrange \"\" \"\\xFF\""]
# CMD ["uv", "run", "python", "packages/fastopendata/src/fastopendata/ingest.py"]
# CMD ["sleep 1000000"]
CMD ["tail", "-f", "/dev/null"]
