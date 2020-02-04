<?php declare(strict_types=1);
/**
 * This file is part of Swoft.
 *
 * @link     https://swoft.org
 * @document https://swoft.org/docs
 * @contact  group@swoft.org
 * @license  https://github.com/swoft-cloud/swoft/blob/master/LICENSE
 */

namespace App\Process;

use Swoft\Log\Helper\CLog;
use Swoft\Process\Annotation\Mapping\Process;
use Swoft\Process\Contract\ProcessInterface;
use Swoole\Coroutine;
use Swoole\Process\Pool;
use Swoft\Redis\Redis;

/**
 * Class LogProcess
 *
 * @since 2.0
 *
 * @Process(workerId={0,1,2,3,4,5,6,7})
 */
class LogProcess implements ProcessInterface
{
    private static $queueName;
    private static $faileQueueName;
    private static $maxTimeout;
    private static $kafkaAddr;
    private static $kafkaTopic;

    public function __construct()
    {
        self::$queueName = config('kafka_log.queue_name');
        self::$faileQueueName = config('kafka_log.faile_queue_name');
        self::$maxTimeout = config('kafka_log.queue_max_timeout');
        self::$kafkaAddr = config('kafka_config.kafka_addr');
        self::$kafkaTopic = config('kafka_config.topic_name');
    }
    /**
     * @param Pool $pool
     * @param int  $workerId
     */
    public function run(Pool $pool, int $workerId): void
    { 
        while (true) {

            $this->logHandle();

            usleep(100);
        }
    }

    private function logHandle(): void
    {
        $logData = Redis::BRPOPLPUSH(self::$queueName, self::$faileQueueName, self::$maxTimeout);
        if ($logData) {
            CLog::info('logData- '. $logData);
            if ($this->kafkaProducer($logData)) {
                Redis::lrem(self::$faileQueueName, $logData);
            }
        }
    }

    private function kafkaProducer(string $data): bool
    {
        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', self::$kafkaAddr);
        $producer = new \RdKafka\Producer($conf);
    
        $topic = $producer->newTopic(self::$kafkaTopic);

        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $data);


        for ($flushRetries = 0; $flushRetries < 10; $flushRetries++) {
            $result = $producer->flush(10);
            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                break;
            }
        }

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            return false;
        }

        return true; 
    }
}