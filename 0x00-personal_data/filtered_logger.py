#!/usr/bin/env python3
"""
This module contains functions and classes for filtering sensitive information
from log messages, setting up a secure logger, and connecting to a MySQL database.
"""

import re
import os
import logging
import mysql.connector
from typing import List
from mysql.connector.connection import MySQLConnection

PII_FIELDS = ("name", "email", "phone", "ssn", "password")


def filter_datum(fields: List[str], redaction: str, message: str, separator: str) -> str:
    """
    Obfuscates specified fields in a log message.

    Args:
        fields: List of field names to obfuscate.
        redaction: String to replace field values with.
        message: The log message containing fields.
        separator: Character separating the fields in the message.

    Returns:
        The obfuscated log message.
    """
    pattern = f"({'|'.join(fields)})=.+?{separator}"
    return re.sub(pattern, lambda m: m.group(0).split('=')[0] + f'={redaction}{separator}', message)


class RedactingFormatter(logging.Formatter):
    """ Redacting Formatter class for filtering sensitive information. """

    REDACTION = "***"
    FORMAT = "[HOLBERTON] %(name)s %(levelname)s %(asctime)-15s: %(message)s"
    SEPARATOR = ";"

    def __init__(self, fields: List[str]):
        super(RedactingFormatter, self).__init__(self.FORMAT)
        self.fields = fields

    def format(self, record: logging.LogRecord) -> str:
        original_message = super(RedactingFormatter, self).format(record)
        return filter_datum(self.fields, self.REDACTION, original_message, self.SEPARATOR)


def get_logger() -> logging.Logger:
    """
    Creates and configures a logger with a redacting formatter.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger("user_data")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = logging.StreamHandler()
    handler.setFormatter(RedactingFormatter(fields=PII_FIELDS))
    logger.addHandler(handler)
    return logger


def get_db() -> MySQLConnection:
    """
    Connects to a MySQL database using environment variables for credentials.

    Returns:
        MySQLConnection object.
    """
    return mysql.connector.connect(
        host=os.getenv("PERSONAL_DATA_DB_HOST", "localhost"),
        user=os.getenv("PERSONAL_DATA_DB_USERNAME", "root"),
        password=os.getenv("PERSONAL_DATA_DB_PASSWORD", ""),
        database=os.getenv("PERSONAL_DATA_DB_NAME")
    )


def main():
    """
    Retrieves and logs user data from the database with sensitive information obfuscated.
    """
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT name, email, phone, ssn, password, ip, last_login, user_agent FROM users;")
    logger = get_logger()

    for row in cursor:
        message = f"name={row[0]}; email={row[1]}; phone={row[2]}; ssn={row[3]}; password={row[4]}; ip={row[5]}; last_login={row[6]}; user_agent={row[7]};"
        logger.info(message)

    cursor.close()
    db.close()


if __name__ == "__main__":
    main()
