<?php declare(strict_types=1);

namespace App\Console\Command;

use Swoft\Console\Annotation\Mapping\Command;
use Swoft\Console\Annotation\Mapping\CommandMapping;
use Swoft\Console\Exception\ConsoleErrorException;
use Swoft\Console\Helper\Show;
use Swoft\Http\Server\Router\Route;
use Swoft\Console\Input\Input;
use Swoft\Console\Output\Output;
use KafkaScriptCommand\TopicCommand;
use KafkaScriptCommand\ConsumerGroupCommand;
use function input;
use function output;
use function sprintf;

/**
 * Class KafkaCommand
 * @Command(name="kafka",coroutine=false)
 */
class KafkaCommand 
{
    public $kafkaTopicsScript;
    public $kafkaConsumerGroupScript;
    public $zookeeperAddr;
    public $kafkaAddr;
    private $errorFlag = 'error';

    public function __construct()
    {
        $this->kafkaTopicsScript = config('kafka_script_config.kafka_topics_script');
        $this->kafkaConsumerGroupScript = config('kafka_script_config.kafka_consumer_group_script');
        $this->zookeeperAddr = config('kafka_script_config.zookeeper_addr');
        $this->kafkaAddr = config('kafka_config.kafka_addr');
    }

    private function checkError(string $msg)
    {
        $pos = strpos(strtolower($msg), $this->errorFlag);
        if ($pos === false) {
            return false;
        } else {
            return true;
        }
    }
    /**
     * @CommandMapping(name="create_topic", desc="创建topic")
     */
    public function createTopic(Input $input, Output $output): void
    {
        $args = $input->getArgs();
        if (count($args) < 3) {
            Show::aList(TopicCommand::$createTopicDesc);
            return;
        }
        $topicName = $args[0];
        $partitons = $args[1];
        $relication = $args[2];

        $ret = TopicCommand::createTopic($topicName, (int) $partitons, (int) $relication, $this->kafkaTopicsScript, $this->zookeeperAddr);
        if ($this->checkError($ret)) {
            $output->error($ret);
        } else {
            $output->info($ret);
        }
    }

    /**
     * @CommandMapping(name="alter_topic", desc="修改topic")
     */
    public function alterTopic(Input $input, Output $output)
    {
        $args = $input->getArgs();
        if (count($args) < 2) {
            Show::aList(TopicCommand::$alterTopicDesc);
            return;
        }
        $topicName = $args[0];
        $partitons = $args[1];

        $ret = TopicCommand::alterTopic($topicName, (int) $partitons, $this->kafkaTopicsScript, $this->zookeeperAddr);
        if ($this->checkError($ret)) {
            $output->error($ret);
        } else {
            $output->info($ret);
        }
    }

    /**
     * @CommandMapping(name="show_topic", desc="查看topic信息")
     */
    public function showTopic(Input $input, Output $output)
    {
        $args = $input->getArgs();
        if (count($args) < 1) {
            Show::aList(TopicCommand::$showTopicDesc);
            return;
        }
        $topicName = $args[0];

        $ret = TopicCommand::showTopic($topicName, $this->kafkaTopicsScript, $this->zookeeperAddr);
        if ($this->checkError($ret)) {
            $output->error($ret);
        } else {
            $output->info($ret);
        }
    }

    /**
     * @CommandMapping(name="show_consumer_group", desc="查看消费组")
     */
    public function showConsumerGroup(Input $input, Output $output)
    {
        $ret = ConsumerGroupCommand::showConsumerGroup($this->kafkaConsumerGroupScript, $this->kafkaAddr);
        if ($this->checkError($ret)) {
            $output->error($ret);
        } else {
            $output->info($ret);
        }
    }

    /**
     * @CommandMapping(name="show_consumeption", desc="查看消费组")
     */
    public function showConsumeption(Input $input, Output $output)
    {
        $args = $input->getArgs();
        if (count($args) < 1) {
            Show::aList(ConsumerGroupCommand::$showConsumeptionpDesc);
            return;
        }
        $groupName = $args[0];
        
        $ret = ConsumerGroupCommand::showConsumeption($groupName, $this->kafkaConsumerGroupScript, $this->kafkaAddr);
        if ($this->checkError($ret)) {
            $output->error($ret);
        } else {
            $output->info($ret);
        }
    }
}
