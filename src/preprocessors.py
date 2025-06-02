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

        if value is not None:
            value = encryptor.decrypt(value.encode()).decode()

        return value

    return preprocessor


def encrypt_postprocessor(encryptor: Encryptor) -> callable:

    def postprocessor(value: str | None) -> str | None:

        if value is not None:
            value = encryptor.encrypt(value.encode()).decode()

        return value

    return postprocessor
