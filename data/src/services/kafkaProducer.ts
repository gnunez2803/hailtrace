import { Kafka, logLevel, Producer } from 'kafkajs';
import logger from '../utils/logger';

class KafkaProducer {
    private client: Producer;
    private topic: string;

    constructor(broker: string, topic: string, clientId: string) {
        var kafkaClient = new Kafka({
            logLevel: logLevel.INFO,
            brokers: [broker],
            clientId: clientId,
          }
        )
        this.topic = topic;
        this.client = kafkaClient.producer();
    }

    async sendMessages(messages: any[]): Promise<void> {
        await this.client.connect();
        try {
            await this.client.send({
                topic: this.topic,
                messages: messages,
            });
            logger.info('Messages sent successfully');
        }
        catch (error) {
            logger.error('Error sending messages:', error);
        }
    }
}

export default KafkaProducer;