# Flight Kafka wrapper module - re-exports from kafka-python
# This allows local imports while using the installed kafka-python package

try:
    from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
    from kafka import errors
    from kafka.admin import NewTopic
    
    __all__ = [
        'KafkaProducer',
        'KafkaConsumer',
        'KafkaAdminClient',
        'NewTopic',
        'errors',
    ]
except ImportError as e:
    raise ImportError(
        f"Failed to import kafka-python. "
        f"Make sure it's installed: pip install kafka-python\n{e}"
    )
