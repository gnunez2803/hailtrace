import { parse } from 'csv-parse';
import fs from 'fs';

import StormCollector from './stormCollector';
import KafkaProducer from './kafkaProducer';
import logger from '../utils/logger';

interface HailData {
    Time: number,
    Size: number
    Location: string,
    County: string,
    State: string,
    Lat: number,
    Lon: number,
    Comments: string,
    EventTs: number,
    EmitTs: number,
}
  
export class HailCollector extends StormCollector {
    private reportData: HailData[] = [];

    constructor(csvFilePath: string, apiEndpoint: string, eventDate: string) {
        super(csvFilePath, apiEndpoint, eventDate);
        this.weatherType = "hail";
        this.fullWeatherType = this.weatherType;
        this.apiEndpoint = apiEndpoint + "/" + eventDate + "_rpts_" + this.weatherType + ".csv"
        this.csvColumns = ["Time", "Size", "Location", "County", "State", "Lat", "Lon", "Comments"];
    }

    async parseData(): Promise<void> {
        const fileContent = fs.readFileSync(this.csvFilePath);
        const headers = this.csvColumns;
        parse(
            fileContent, {
                delimiter: ',',
                columns: headers,
                from_line: 2,
            }, (error, results: HailData[]) => {
                if (error) {
                    logger.info("Unable to parse file: ", error)
                }
                this.reportData = results;
            }
        );
    }

    async sendData(client: KafkaProducer): Promise<void> {
        logger.info("hail")
        logger.info(this.reportData)
        if (this.reportData.length > 0) {
            const messages = this.reportData.map((data) => ({
                value: JSON.stringify({
                    EventTs: this.convertEventDateToDate(),
                    EmitTs: Date.now(),
                    Time: data.Time,
                    Size: data.Size,
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
            logger.info("No hail data to sent");
        }
    }
}

export default HailCollector;