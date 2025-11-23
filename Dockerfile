# Use the official Ubuntu image as the base
FROM ubuntu:22.04

# Update the package lists and install Python 3 and pip
RUN apt-get update && \
    apt-get install -y neovim python3 python3-pip wget bash libgdal-dev unzip git && \
    rm -rf /var/lib/apt/lists/*

RUN apt update
RUN apt install -y -V ca-certificates lsb-release wget
RUN wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt update
RUN apt install -y -V libarrow-dev # For C++
RUN apt install -y -V libarrow-glib-dev # For GLib (C)
RUN apt install -y -V libarrow-dataset-dev # For Apache Arrow Dataset C++
RUN apt install -y -V libarrow-dataset-glib-dev # For Apache Arrow Dataset GLib (C)
RUN apt install -y -V libarrow-acero-dev # For Apache Arrow Acero
RUN apt install -y -V libarrow-flight-dev # For Apache Arrow Flight C++
RUN apt install -y -V libarrow-flight-glib-dev # For Apache Arrow Flight GLib (C)
RUN apt install -y -V libarrow-flight-sql-dev # For Apache Arrow Flight SQL C++
RUN apt install -y -V libarrow-flight-sql-glib-dev # For Apache Arrow Flight SQL GLib (C)
RUN apt install -y -V libgandiva-dev # For Gandiva C++
RUN apt install -y -V libgandiva-glib-dev # For Gandiva GLib (C)
RUN apt install -y -V libparquet-dev # For Apache Parquet C++
RUN apt install -y -V libparquet-glib-dev # For Apache Parquet GLib (C)


# Set the working directory inside the container

# COPY . /app

WORKDIR /pycypher-nmetl
RUN git clone https://github.com/zacernst/pycypher-nmetl.git

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
CMD ["make", "-j8", "ingest"]
