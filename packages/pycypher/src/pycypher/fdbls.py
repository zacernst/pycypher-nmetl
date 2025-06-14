import click

from pycypher.fact_collection.foundationdb import FoundationDBFactCollection

@click.command()
def go():
    f = FoundationDBFactCollection()
    counter = 0
    for i in f.parallel_read(increment=1024):
        print(counter, i)
        counter += 1


if __name__ == '__main__':
    go()
