package kafka.consumer

/**
 * date 2015/8/3.
 * @author daikui
 */
class ZkKafkaConsumerConnector(override val config: ConsumerConfig, override val enableFetcher: Boolean) extends ZookeeperConsumerConnector(config, enableFetcher)