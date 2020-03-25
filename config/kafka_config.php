<?php
return [
    'topic_name' => env('TOPIC_NAME', 'test'),
    'kafka_addr' => env('KAFKA_ADDR', 'localhost:9092'),
    'topic_rule' => '%s_%s_%s',
    'rdkafka_producer_config' => [
        'metadata.broker.list' => ['val' => '', 'func' => 'getBrokerList'], 
        'socket.keepalive.enable' => ['val' => 'true'], 
        'log.connection.close' => ['val' => 'false']
    ]
];