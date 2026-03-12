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

RUN git clone https://github.com/zacernst/pycypher.git
WORKDIR /pycypher

RUN make

## RUN pip install uv
## RUN uv venv -p 3.14t
## RUN uv pip install packages/pycypher
## RUN uv pip install packages/shared

# RUN fdbcli --no-status -C "/app/fdb.cluster" --exec "configure new single ssd"
# RUN uv venv -p 3.14t
# RUN uv sync
# RUN uv build

CMD ["tail", "-f", "/dev/null"]
