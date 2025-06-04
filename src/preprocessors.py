from abc import ABC, abstractmethod


class Encryptor(ABC):
    """
    Base class to hide implementation details.
    """

    @abstractmethod
    def encrypt(self, data: bytes) -> bytes | None:
        pass

    @abstractmethod
    def decrypt(self, data: bytes) -> bytes | None:
        pass


def default_preprocessor(value: str | None) -> str | None:
    """
    Default preprocessor function to handle None values.

    :param value: The value to preprocess.
    :return: The preprocessed value.
    """

    if value is None:
        return None

    value = value.strip()
    if value == "":
        return None

    return value


def decrypt_preprocessor(encryptor: Encryptor) -> callable:

    def preprocessor(value: str | None) -> str | None:

        if value is None:
            return None

        value_bytes = encryptor.decrypt(value.encode())

        if value_bytes is None:
            raise ValueError(f"Failed to decrypt value '{value}'.")

        return value_bytes.decode()

    return preprocessor


def encrypt_postprocessor(encryptor: Encryptor) -> callable:

    def postprocessor(value: str | None) -> str | None:

        if value is None:
            return None

        value_bytes = encryptor.encrypt(value.encode())

        if value_bytes is None:
            raise ValueError(f"Failed to encrypt value '{value}'.")

        return value_bytes.decode()

    return postprocessor
