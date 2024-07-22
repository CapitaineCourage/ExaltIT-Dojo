def job():
    try:
        logging.info("ETL job started")
        data = extract_data()
        transformed_data = transform_data(data)
        load_data(transformed_data)
        logging.info("ETL job finished")
    except Exception as e:
        logging.error(f"ETL job failed: {str(e)}")
