#!/usr/bin/env python3

import os
from datetime import date


def main():
    # Updating last update txt file
    with open(f'{os.getcwd()}/test.txt', 'w') as f:
        f.write(date.today().strftime(format='%b %d, %Y'))
    print('SCRIPT IS WORKING!!')
    print(date.today().strftime(format='%b %d, %Y'))


if __name__ == '__main__':
    main()