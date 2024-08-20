# Output is under vwap_output.txt, the columns are [ timestamp, company symbol , vwap ]
# The output company name may have issues because there were non-printable characters in the .gz file. I used a function to c
# lean the bytes first, which might have removed some characters from the name.

import gzip
import struct
from datetime import datetime
from collections import defaultdict

class VWAPCalculator:
    def __init__(self):
        # This will store trade data
        self.trades = defaultdict(lambda: defaultdict(lambda: {'amount': 0, 'volume': 0}))

    def add_trade(self, timestamp, symbol, price, volume):
        hour = timestamp.strftime('%H')
        self.trades[hour][symbol]['amount'] += price * volume
        self.trades[hour][symbol]['volume'] += volume

    def calculate_vwap(self):
        results = []
        for hour, symbols in self.trades.items():
            for symbol, values in symbols.items():
                amount = values['amount']
                volume = values['volume']
                vwap = amount / volume if volume != 0 else 0
                vwap = round(vwap, 2)
                results.append({'timestamp': f"{hour}:00:00", 'symbol': symbol, 'vwap': vwap})
        return results

def process_messages(file_path, output_file):
    with gzip.open(file_path, 'rb') as file:
        vwap_calculator = VWAPCalculator()
        
        while True:
            msg_header = file.read(1)
            if not msg_header:
                break

            # Checking for 'P' symbol trades as this only is going to impact heavily.
            if msg_header == b'P':  # Trade message
              
                message = file.read(43)  # Length of 'P' message
                timestamp, symbol, price, volume = parse_trade_message(message)
                vwap_calculator.add_trade(timestamp, symbol, price, volume)
              
        # Calculate VWAP
        vwap_results = vwap_calculator.calculate_vwap()

        # Sort results by timestamp
        vwap_results_sorted = sorted(vwap_results, key=lambda x: x['timestamp'])
        
        # Save VWAP to file
        with open(output_file, 'w') as f:
            for result in vwap_results_sorted:
                f.write(f"{result['timestamp']}{' '} {result['symbol']}{'       '}{result['vwap']:.2f}\n")

        print(f"VWAP data saved to {output_file}")

# company symbols were not coming clean, so added this function, this will remove unprintable char from byte
def clean_and_decode_symbol(byte_string):
    # Filter out non-printable ASCII characters (0x20 to 0x7E)
    printable_bytes = bytes([b for b in byte_string if 0x20 <= b <= 0x7E])
    
    # Remove trailing null bytes and spaces
    cleaned_bytes = printable_bytes.rstrip(b'\x00').rstrip(b' ')
    
    # Remove all spaces from the symbol
    cleaned_bytes = cleaned_bytes.replace(b' ', b'')
    
    # Decode to Latin-1
    symbol = cleaned_bytes.decode('latin-1')
    
    return symbol

def parse_trade_message(message):
    # Update unpacking format according to the provided information
    unpacked = struct.unpack('>2s2s6s8s1s4s8s4s8s', message)
    
    # Parse fields
    timestamp = convert_timestamp(int.from_bytes(unpacked[2], byteorder='big'))
    shares = int.from_bytes(unpacked[5], byteorder='big')
    
    # Decode and clean symbol field
    
    symbol = clean_and_decode_symbol(unpacked[6])
    
    # Convert price and match number
    price = int.from_bytes(unpacked[7], byteorder='big') / 10000
    
    return timestamp, symbol, price, shares

def convert_timestamp(raw_timestamp):
    return datetime.fromtimestamp(raw_timestamp / 1e9)

input_file = '01302019.NASDAQ_ITCH50.gz'
output_file = 'vwap_output.txt'
process_messages(input_file, output_file)
