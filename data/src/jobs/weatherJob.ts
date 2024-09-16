import TornadoCollector from "../services/tornadoCollector";
import config from "../config/config";
import cron from 'node-cron';

import KafkaProducer from "../services/kafkaProducer";
import HailCollector from "../services/hailCollector";
import WindCollector from "../services/windCollector";

import logger from "../utils/logger";

// Schedule a task to run every minute
const weatherCronJob = () => {
    var kafkaProducerClient = new KafkaProducer(
        config.kafkaConfig.broker,
        config.kafkaConfig.topic,
        config.kafkaConfig.clientId,
    )

    var tornadoClient = new TornadoCollector(
        config.tornadoCsvPath,
        config.apiEndpoint,
        config.eventDate,
    );

    var hailClient = new HailCollector(
        config.hailCsvPath,
        config.apiEndpoint,
        config.eventDate,
    );

    var windClient = new WindCollector(
        config.windCsvPath,
        config.apiEndpoint,
        config.eventDate,
    );

    const weatherClients: any[] = [tornadoClient, hailClient, windClient];

    cron.schedule(config.cronInterval, async () => {
        for (const x in weatherClients)  {
            try {
                logger.info(weatherClients[x].getWeatherType() + " data job started")
                logger.info("Collect data from NOAA");
                await weatherClients[x].collectData();
                logger.info("Parse CSV file")
                await weatherClients[x].parseData();
                logger.info("send data to Kafka")
                await weatherClients[x].sendData(kafkaProducerClient); 
                logger.info(weatherClients[x].getWeatherType() + " data job completed")
            } catch (error) {
                logger.error("Error syncing tornado data. ", error);
            }
        }
        logger.info("cron job completed")
    });
}

export default weatherCronJob;