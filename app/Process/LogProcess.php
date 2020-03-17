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
use Swoft\Stdlib\Helper\ArrayHelper;
/**
 * Class LogProcess
 *
 * @since 2.0
 *
 * @Process(workerId={0,1,2,3,4,5,6,7,8,9,10,11})
 */
class LogProcess implements ProcessInterface
{
    const LOSS_PROJECT  = 0;
    private static $queueName;
    private static $faileQueueName;
    private static $maxTimeout;
    private static $kafkaAddr;
    private static $kafkaTopic;
    private static $topicRule;
    private static $producer;
    private static $conf;
    private static $topics = [];
    private static $multiMaxTimes = 1;

    public function __construct()
    {
        self::$queueName = config('kafka_log.queue_name');
        self::$faileQueueName = config('kafka_log.faile_queue_name');
        self::$maxTimeout = config('kafka_log.queue_max_timeout');
        self::$kafkaAddr = config('kafka_config.kafka_addr');
        self::$kafkaTopic = config('kafka_config.topic_name');
        self::$topicRule = config('kafka_config.topic_rule');
        self::$multiMaxTimes = config('kafka_log.multi_max_times');

        self::$conf = new \RdKafka\Conf();
        self::$conf->set('metadata.broker.list', self::$kafkaAddr);
        self::$conf->set('socket.keepalive.enable', 'true');
        self::$conf->set('log.connection.close', 'false');

        /**
         * 设置错误回调
         */
        self::$conf->setErrorCb(__CLASS__ . "::setErrorCb");

        /**
         * 设置投放回调
         */
        // self::$conf->setDrMsgCb(__CLASS__ . "::setDrMsgCb");

    }
    /**
     * @param Pool $pool
     * @param int  $workerId
     */
    public function run(Pool $pool, int $workerId): void
    { 
        while (true) {
            for ($i = 0; $i < self::$multiMaxTimes; $i++) {
                $this->logHandle();
            }
            Coroutine::sleep(0.1);
        }
    }

    public static function setErrorCb($producer, $err, $reason)
    {
        CLog::error(rd_kafka_err2str($err) . ':' . $reason);
    }

    public static function setDrMsgCb($producer, $msg)
    {
        if ($msg->err) {
            CLog::error("Message delivery failed: ". $msg->errstr());
        }
    }

    private function logHandle(): void
    {
        $logData = Redis::BRPOPLPUSH(self::$queueName, self::$faileQueueName, self::$maxTimeout);
        if ($logData) {
            $data = unserialize($logData);
            $kafkaStatus = $this->kafkaProducer($data);
            if ($kafkaStatus) {
                Redis::lrem(self::$faileQueueName, $logData);
            } else {
                CLog::error("kafka无法发送数据");
            }
        }
    }

    private function kafkaProducer(array $data): bool
    {
        if (self::$producer == NULL) {
            self::$producer = new \RdKafka\Producer(self::$conf);
        }

        foreach($data as $projectName => $records) {
            if (!is_array($records)) continue;
            foreach ($records as $recordsName => $recordsData) {
                $topicName = sprintf(self::$topicRule, self::$kafkaTopic, $projectName, $recordsName);
                if (!isset(self::$topics[$topicName])) {
                    self::$topics[$topicName] = self::$producer->newTopic($topicName);
                } else {
                    if (self::$topics[$topicName] == NULL) {
                        self::$topics[$topicName] = self::$producer->newTopic($topicName);
                    }
                }
                if (!self::$producer->getMetadata(false, self::$topics[$topicName], 2 * 1000)) {
                    CLog::error('Failed to get metadata, is broker down?');
                }
                self::$topics[$topicName]->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($recordsData));
                self::$producer->poll(0);
            }
        }

        while ((self::$producer->getOutQLen())) {
            self::$producer->poll(20);
        }

       return self::$producer->getOutQLen() > 0 ? false : true;
    }
}