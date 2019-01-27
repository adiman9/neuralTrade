""" Parse and clean Bitmex Market Depth data """
import os
import json
from dotenv import load_dotenv
from collections import deque

load_dotenv()

DATA_DIR = os.getenv('DATA_DIRECTORY')
timestamp_map = {}

def get_section_timestamps():

    timestamps = []
    for f in os.listdir(DATA_DIR):
        if f.startswith('all_10sec'):
            s = f.split('_')
            ts = s[-1]
            try:
                ts = int(ts)
            except ValueError:
                continue

            timestamps.append(ts)
            timestamp_map[ts] = f

    timestamps.sort()
    data_sections = [[]]

    for ts in timestamps:
        current_section = data_sections[-1]

        if current_section:
            if ts <= current_section[-1] + 15:
                current_section.append(ts)
            else:
                data_sections.append([ts])
        else:
            current_section.append(ts)

    return data_sections

# All Markets available:
# ['XBTUSD', 'XBT7D_U105', 'XBT7D_D95', 'ETHUSD', 'XBTH19', 'XBTM19', 'ETHH19', 'LTCH19', 'XRPH19', 'BCHH19', 'ADAH19', 'EOSH19', 'TRXH19']

def get_raw_data(market, seq_len):
    data_sections = get_section_timestamps()
    for sect in data_sections:
        data = deque([], seq_len)

        for ts in sorted(sect):
            with open(f'{DATA_DIR}/{timestamp_map[ts]}') as f:
                market_data = json.loads(f.read())
            data.append(market_data[market])

            if len(data) == seq_len:
                yield list(data)


for section in get_raw_data('XBTUSD', 100):
    print(len(section))

