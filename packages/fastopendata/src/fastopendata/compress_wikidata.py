import bz2
import json
import rich
import sys
from rich.progress import Progress, MofNCompleteColumn, SpinnerColumn


total_counter = 0
def lines():
    global total_counter
    for line in sys.stdin:
        total_counter += 1
        if len(line) < 3 or "latitude" not in line:
            continue
        d = json.loads(line[:-2])
        if 'descriptions' in d.keys() and 'en' in d['descriptions']:
            d['descriptions'] = d['descriptions']['en']
        if 'labels' in d.keys() and 'en' in d['labels']:
            d['labels'] = d['labels']['en']
        if 'aliases' in d.keys() and 'en' in d['aliases']:
            d['aliases'] = d['aliases']['en']
        if 'sitelinks' in d.keys():
            del d['sitelinks']
        yield d, total_counter


with Progress(SpinnerColumn(), *Progress.get_default_columns(), MofNCompleteColumn()) as pbar:
    task = pbar.add_task("Shrinking data...", total=98631069547)
    inner_counter = 0
    for line, total_counter in lines():
        inner_counter += 1
        print(json.dumps(line))
        # f.write(bytes(json.dumps(line), encoding='utf8') + b'\n')
        # pbar.update(task, completed=offset)
