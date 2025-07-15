"""Configuration for Pyroscope -> Grafana"""

import pyroscope


pyroscope.configure(
    application_name="nmetl",  # replace this with some name for your application
    server_address="http://localhost:4040",  # replace this with the address of your Pyroscope server
    sample_rate=100,  # default is 100
    detect_subprocesses=True,  # detect subprocesses started by the main process; default is False
    oncpu=True,  # report cpu time only; default is True
    gil_only=False,  # only include traces for threads that are holding on to the Global Interpreter Lock; default is True
    enable_logging=False,  # does enable logging facility; default is False
)
