<?php
return [
    'kafka_topics_script' => env('KAFKA_TOPICS_SCRIPT', 'bin/kafka-topics.sh'),
    'kafka_consumer_group_script' => env('KAFKA_CONSUMER_GROUP_SCRIPT', 'bin/kafka-consumer-groups.sh'),
    'zookeeper_addr' => env('ZOOKEEPER_ADDR', 'localhost:2181')
];