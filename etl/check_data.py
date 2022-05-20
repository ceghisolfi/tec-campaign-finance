import os 
import requests
from datetime import date, datetime, timezone, timedelta
import numpy as np
import pandas as pd
from datetime import date
from io import BytesIO
import re
import time
from zipfile import ZipFile


def main():
    filers = pd.read_csv(f'{os.getcwd()}/data/processed/filers.csv')
    print(filers.head(2))

if __name__ == '__main__':
    main()