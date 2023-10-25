import pkg_resources
from kafka import KafkaProducer, KafkaConsumer

CA_FILE = pkg_resources.resource_filename("kafka_python_yc", "YandexInternalRootCA.crt")


def make_yc_kafka_producer(username: str, password: str, *args, **kwargs) -> KafkaProducer:
    kwargs = {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_plain_username": username,
        "sasl_plain_password": password,
        "ssl_cafile": CA_FILE,
        **kwargs,
    }

    return KafkaProducer(*args, **kwargs)


def make_yc_kafka_consumer(username: str, password: str, *args, **kwargs) -> KafkaConsumer:
    kwargs = {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_plain_username": username,
        "sasl_plain_password": password,
        "ssl_cafile": CA_FILE,
        **kwargs,
    }

    return KafkaConsumer(*args, **kwargs)
