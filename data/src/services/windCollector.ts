import fs from 'fs';
import { parse } from 'csv-parse';

import StormCollector from './stormCollector';
import KafkaProducer from './kafkaProducer';
import logger from '../utils/logger';

interface WindData {
    Time: number,
    EmitTs: number,
    EventTs: number,
    Speed: string,
    Location: string,
    County: string,
    State: string,
    Lat: number,
    Lon: number,
    Comments: string,
  }
  
class WindCollector extends StormCollector {
    private reportData: WindData[] = [];

    constructor(csvFilePath: string, apiEndpoint: string, eventDate: string) {
        super(csvFilePath, apiEndpoint, eventDate);
        this.weatherType = "wind";
        this.apiEndpoint = apiEndpoint + "/" + this.eventDate + "_rpts_" + this.weatherType + ".csv"
        this.fullWeatherType = this.weatherType;
        this.csvColumns = ["Time", "Speed", "Location", "County", "State", "Lat", "Lon", "Comments"];
    }

    async parseData(): Promise<void> {
        const fileContent = fs.readFileSync(this.csvFilePath);
        const headers = this.csvColumns;
        parse(
            fileContent, {
                delimiter: ',',
                columns: headers,
                from_line: 2,
            }, (error, results: WindData[]) => {
                if (error) {
                    console.error("Unable to parse file: ", error)
                }
                this.reportData = results
            }
        );
    }

    async sendData(client: KafkaProducer): Promise<void> {
        logger.info("wind")
        logger.info(this.reportData)
        if (this.reportData.length > 0) {
            const messages = this.reportData.map((data) => ({
                value: JSON.stringify({
                    Time: data.Time,
                    EmitTs: Date.now(),
                    EventTs: this.convertEventDateToDate(),
                    Speed: data.Speed,
                    Location: data.Location,
                    County: data.County,
                    State: data.State,
                    Lat: data.Lat,
                    Lon: data.Lon,
                    Comments: data.Comments,
                }),
            }));
            await client.sendMessages(messages);
        } else {
            logger.info("No wind data to sent");
        }
    }
}

  export default WindCollector;