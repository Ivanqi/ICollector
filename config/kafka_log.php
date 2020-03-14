<?php
return [
    'queue_name' => 'kafka_log_job',
    'faile_queue_name' => 'kafka_fail_job',
    'queue_max_timeout' => 5,
    'multi_max_times' => env('MULTI_MAX_TIMES', 5)
];