import dotenv from 'dotenv';
import path from 'path';

dotenv.config({ path: path.resolve(__dirname, '../../.env') });


interface KafkaConfig {
    topic: string;
    broker: string;
    clientId: string;
}

interface AppConfig {
    apiEndpoint: string;
    cronInterval: string;
    kafkaConfig: KafkaConfig;
    tornadoCsvPath: string;
    windCsvPath: string;
    hailCsvPath: string;
    logPath: string;
    eventDate: string;
}

// Helper function to throw error if env var is missing
const getEnvVariable = (key: string, defaultValue?: string): string => {
    const value = process.env[key];
    if (!value) {
      if (defaultValue) {
        return defaultValue;
      }
      throw new Error(`Missing environment variable: ${key}`);
    }
    return value;
  };

// If non critical value is missing, the program should still be able to run. Otherwise
// this will fail. 
const config: AppConfig = {
    apiEndpoint: getEnvVariable('API_ENDPOINT'), // Throw error if missing
    cronInterval: getEnvVariable('CRON_INTERVAL', '* * * * *'),
    kafkaConfig: {
      broker: getEnvVariable('KAFKA_BROKER', 'localhost:9092'),
      topic: getEnvVariable('KAFKA_TOPIC', 'raw-weather-reports'),
      clientId: getEnvVariable('KAFKA_CLIENT_ID', 'node-collector')
    },
    logPath: getEnvVariable('LOG_FILE_PATH'),
    tornadoCsvPath: getEnvVariable('TORNADO_CSV_PATH', 'tornado.csv'),
    windCsvPath: getEnvVariable('WIND_CSV_PATH', 'wind.csv'),          
    hailCsvPath: getEnvVariable('HAIL_CSV_PATH', 'hail.csv'), 
    eventDate: getEnvVariable('EVENT_DATE') // Throw error if missing         
};

export default config;