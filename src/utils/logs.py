from os import path

from datetime import datetime


def write_thread_logs(process_name, message):
    if not path.isdir('./logs'):
        return

    with open(f'./logs/{process_name}.txt', 'a') as f:
        f.write(f"[{datetime.now().strftime('%H:%M:%S %d/%m/%Y')}] - {process_name}: {message}\n")