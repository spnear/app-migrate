from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

class Connection:
    def __init__(self) -> None:
        pass

    def engine(self):
        DB_URL = os.environ["DB_URL"]
        conn = create_engine(DB_URL)
        
        return conn