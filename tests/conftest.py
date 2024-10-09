import sys
from os.path import abspath, dirname, join
from dotenv import load_dotenv

load_dotenv()

project_root = abspath(join(dirname(__file__), "..", "mentre/dags"))
sys.path.insert(0, project_root)
