from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(
    bootstrap_servers="boot-public-byg.mpcs53014kafka.2siu49.c2.kafka.us-east-1.amazonaws.com:9196",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username="mpcs53014-2025",
    sasl_plain_password="A3v4rd4@ujjw"
)

topic = NewTopic(
    name="wuh_fifa_events",
    num_partitions=3,
    replication_factor=3
)

admin.create_topics(new_topics=[topic])
print("Created topic wuh_fifa_events")
