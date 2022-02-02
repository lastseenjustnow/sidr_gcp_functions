from datetime import datetime, timedelta

from google.cloud import pubsub_v1

import config


project_id = config.config_vars['project_id']
topic_id = config.config_vars['topic_id']

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
date_format = "%Y-%m-%d"


def fetch_reports(date_from: str, date_to: str):
    """
    Send messages to PubSub topic to fetch & upload all the report between dates
    :param date_from: Start date
    :param date_to: End date
    :return:
    """
    date_i = date_from
    while date_i <= date_to:
        data_str = f"Generate report for {date_i}"
        data = data_str.encode("utf-8")
        future = publisher.publish(
            topic_path, data, report_date=date_i
        )
        print(future.result())
        date_i = (datetime.strptime(date_i, date_format) + timedelta(days=1)).strftime(date_format)

    return "Ok."