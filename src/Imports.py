import os
import sys
sys.path.append(os.path.join('venv','lib'))
import pandas as pd
import numpy as np
import re
import os 
import time
import glob 
import datetime
from datetime import datetime as dt, timedelta
from io import StringIO
import time as tm
from urllib.parse import quote
from sqlalchemy import Table, MetaData, create_engine, Column, VARCHAR,text
from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine 
from sqlalchemy import create_engine as ce
from sqlalchemy.dialects.mysql import insert
import sqlalchemy as sqa
import string
import csv
import dask.dataframe as dd
import dask
import pymysql
import logging.config
import logging
import yaml
import zipfile
import json
import asyncio
import telegram
import platform
import fileinput
import tempfile
import shutil
from xlsx2csv import Xlsx2csv
# from fastparquet import to_csv_optimized
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.dialects.mysql import insert