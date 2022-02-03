import logging
import os
from string import Template
from datetime import datetime, timedelta
import glob
import pandas as pd

import google.cloud.bigquery.dbapi as bq
from appstoreconnect import Api
from google.cloud import storage, bigquery
from google.cloud.bigquery import WriteDisposition

import config


yesterday = datetime.today() - timedelta(days=2)
date_format = "%Y-%m-%d"
to_date = yesterday.date().strftime(date_format)


def run(report_from_date=to_date, report_to_date=to_date, report_date_format=date_format):
    """Pushes the AppStore report into BigQuery"""

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(config.config_vars['bucket_name'])
    blob = bucket.get_blob(config.config_vars['appstore_key_path'])
    key_path = blob.download_as_string()
    api = Api(
        config.config_vars['appstore_key_id'],
        key_path,
        config.config_vars['appstore_issuer_id'])
    freq = config.config_vars['report_download_freq']

    # downloading reports
    date_i = report_from_date
    while date_i <= report_to_date:
        logging.info(f'Downloading .csv daily sales report from AppStore Connect for date: {date_i}')
        filters_dict = {
            'vendorNumber': config.config_vars['vendor_number'],
            'frequency': freq,
            'reportType': config.config_vars['report_type'],
            'reportSubType': config.config_vars['report_subtype'],
            'reportDate': date_i}
        report_csv = f'/tmp/report_{freq}_{date_i}.csv'
        api.download_sales_and_trends_reports(filters=filters_dict, save_to=report_csv)
        logging.info(f'Download completed. Uploading data...')
        date_i = (datetime.strptime(date_i, date_format) + timedelta(days=1)).strftime(date_format)

    # merge csvs into one
    os.chdir("/tmp")
    all_filenames = [i for i in glob.glob('*.{}'.format('csv'))]
    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
    save_to_path = '/tmp/reports.csv'
    combined_csv.to_csv(save_to_path, index=False, encoding='utf-8-sig')

    # delete and insert into BQ
    client = bigquery.Client()
    table_id = f"{config.config_vars['project_id']}.{config.config_vars['output_dataset_id']}.{config.config_vars['output_table_name']}"

    con = bq.connect()
    cursor = con.cursor()
    query = f"""
    DELETE FROM {table_id}
    WHERE Begin_Date >= PARSE_DATE('{report_date_format}', '{report_from_date}')
    AND Begin_Date <= PARSE_DATE('{report_date_format}', '{report_to_date}') """
    cursor.execute(query)
    con.commit()
    con.close()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        field_delimiter='\t',
        write_disposition=WriteDisposition.WRITE_APPEND
    )

    with open(save_to_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    logging.info(f'Upload completed')
    job.result()


def main(data, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        current_time = datetime.utcnow()
        log_message = Template('Cloud Function was triggered on $time')
        logging.info(log_message.safe_substitute(time=current_time))
        logging.info(f"Data received from PubSub: {data}")

        try:
            if data.get('attributes') is None or data.get('attributes').get('report_from_date') is None:
                logging.warning('No report start date was provided. Using default one (t-2)')
                run()
            else:
                report_from_date = data.get('attributes').get('report_from_date')
                report_to_date = data.get('attributes').get('report_to_date')
                run(report_from_date, report_to_date)

        except Exception as error:
            log_message = Template('Query failed due to '
                                   '$message.')
            logging.error(log_message.safe_substitute(message=error))

    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)


if __name__ == '__main__':
    main(dict(), 'context')
