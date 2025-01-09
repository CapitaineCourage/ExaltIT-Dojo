# main.py
import schedule
import time
from etl_pipeline import extract_data, transform_data, load_data
from gestion_erreurs import job


schedule.every(2).hours.do(job)

if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(1)
