<?php
return [
    'topic_name' => env('TOPIC_NAME', 'test'),
    'kafka_addr' => env('KAFKA_ADDR', 'localhost:9092'),
    'topic_rule' => '%s_%s_%s'
];