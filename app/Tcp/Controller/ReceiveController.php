<?php declare(strict_types=1);
/**
 * This file is part of Swoft.
 *
 * @link     https://swoft.org
 * @document https://swoft.org/docs
 * @contact  group@swoft.org
 * @license  https://github.com/swoft-cloud/swoft/blob/master/LICENSE
 */

namespace App\Tcp\Controller;

use Swoft\Tcp\Server\Annotation\Mapping\TcpController;
use Swoft\Tcp\Server\Annotation\Mapping\TcpMapping;
// use App\Validatorg\DataValidator;
use Swoft\Tcp\Server\Request;
use Swoft\Tcp\Server\Response;
use function strrev;
use Swoft\Log\Helper\Log;
use Swoft\Redis\Redis;
use Swoft\Log\Helper\CLog;

/**
 * Class ReceiveController
 *
 * @TcpController()
 */
class ReceiveController
{
    private static $queueName;
    private static $kafkaAddr;
    private static $kafkaTopic;

    public function __construct()
    {
        self::$queueName = config('error_collect.queue_name');
        self::$kafkaAddr = config('kafka_config.kafka_addr');
        self::$kafkaTopic = config('kafka_config.topic_name');
    }
    /**
     * @TcpMapping("receive", root=true)
     * @param Request  $request
     * @param Response $response
     */
    public function receive(Request $request, Response $response): void
    {
        $data = $request->getPackage()->getData();
        $result = validate($data, \DataValidator::class, [], ['DataValidator']);
        if (!$result['ret']) {
            \return_failed($response, '校验失败，非法数据');
        } else {
            $this->kafkaProducer(json_encode($data));
            \return_success($response);
        }
    }

    private function kafkaProducer(string $data): void
    {
        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', self::$kafkaAddr);
        try {
            $producer = new \RdKafka\Producer($conf);
        
            $topic = $producer->newTopic(self::$kafkaTopic);

            $topic->produce(RD_KAFKA_PARTITION_UA, 0, $data);


            for ($flushRetries = 0; $flushRetries < 10; $flushRetries++) {
                $result = $producer->flush(100);
                if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                    break;
                }
            }

            if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
                Redis::lPush(self::$queueName, $data); 
            }
        }catch(\Exception $e) {
            Log::error($e->getMessage());
            Redis::lPush(self::$queueName, $data); 
        }
        
    }
}
