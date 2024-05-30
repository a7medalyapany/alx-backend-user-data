#!/usr/bin/env python3
"""
This module contains functions for encrypting passwords.
"""

import bcrypt


def hash_password(password: str) -> bytes:
    """
    Hashes a password using bcrypt.

    Args:
        password: The plaintext password to hash.

    Returns:
        The salted, hashed password as a byte string.
    """
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())


def is_valid(hashed_password: bytes, password: str) -> bool:
    """
    Validates a password against its hashed version.

    Args:
        hashed_password: The hashed password to check against.
        password: The plaintext password to validate.

    Returns:
        True if the password matches the hash, False otherwise.
    """
    return bcrypt.checkpw(password.encode('utf-8'), hashed_password)


if __name__ == "__main__":
    password = "MyAmazingPassw0rd"
    encrypted_password = hash_password(password)
    print(encrypted_password)
    print(is_valid(encrypted_password, password))
