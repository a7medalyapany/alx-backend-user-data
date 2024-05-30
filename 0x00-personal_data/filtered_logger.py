#!/usr/bin/env python3
"""
This module contains functions for logging and filtering data.
"""

import logging
import os
import mysql.connector
import re

PII_FIELDS = ("name", "email", "phone", "ssn", "password")


class RedactingFormatter(logging.Formatter):
    """ Redacting Formatter class """

    REDACTION = "***"
    FORMAT = "[HOLBERTON] %(name)s %(levelname)s %(asctime)-15s: %(message)s"
    SEPARATOR = ";"

    def __init__(self, fields=PII_FIELDS):
        super(RedactingFormatter, self).__init__(self.FORMAT)
        self.fields = fields

    def format(self, record: logging.LogRecord) -> str:
        log_message = super().format(record)
        return self.filter_datum(self.fields, self.REDACTION, log_message, self.SEPARATOR)

    @staticmethod
    def filter_datum(fields, redaction, message, separator):
        pattern = re.compile(
            r'(\b{}\b=[^{}{}]+)'.format('|'.join(fields), separator, separator))
        return re.sub(pattern, lambda x: x.group().split(separator)[0] + "=" + redaction + separator, message)


def get_logger() -> logging.Logger:
    """Creates a logger object."""
    logger = logging.getLogger("user_data")
    logger.setLevel(logging.INFO)
    formatter = RedactingFormatter()
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def get_db() -> mysql.connector.connection.MySQLConnection:
    """Connects to the database and returns the connection."""
    db_config = {
        'user': os.getenv('PERSONAL_DATA_DB_USERNAME', 'root'),
        'password': os.getenv('PERSONAL_DATA_DB_PASSWORD', ''),
        'host': os.getenv('PERSONAL_DATA_DB_HOST', 'localhost'),
        'database': os.getenv('PERSONAL_DATA_DB_NAME')
    }
    return mysql.connector.connect(**db_config)


def main() -> None:
    """Main function to retrieve data from the database and log it."""
    logger = get_logger()
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM users;")
    for row in cursor.fetchall():
        log_message = ""
        for i, field in enumerate(row):
            log_message += f"{PII_FIELDS[i]}={field}; "
        logger.info(log_message)
    cursor.close()
    db.close()


if __name__ == "__main__":
    main()
