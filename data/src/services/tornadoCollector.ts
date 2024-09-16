import fs from 'fs';
import { parse } from 'csv-parse';

import KafkaProducer from './kafkaProducer';
import StormCollector from './stormCollector';
import logger from '../utils/logger';

interface TornadoData {
    Time: number,
    F_Scale: string,
    Location: string,
    County: string,
    State: string,
    Lat: number,
    Lon: number,
    Comments: string,
    EventTs: number,
    EmitTs: number,
}
  
class TornadoCollector extends StormCollector {
    private reportData: TornadoData[] = [];

    constructor(csvFilePath: string, apiEndpoint: string, eventDate: string) {
        super(csvFilePath, apiEndpoint, eventDate);
        this.weatherType = "torn";
        this.apiEndpoint = apiEndpoint + "/" + this.eventDate + "_rpts_" + this.weatherType + ".csv"
        this.csvColumns = ["Time", "F_Scale", "Location", "County", "State", "Lat", "Lon", "Comments"];
        this.fullWeatherType = "tornado";
    }

    async parseData(): Promise<void> {
        const fileContent = fs.readFileSync(this.csvFilePath);
        const headers = this.csvColumns;
        parse(
            fileContent, {
                delimiter: ',',
                columns: headers,
                from_line: 2,
            }, (error, results: TornadoData[]) => {
                if (error) {
                    console.error("Unable to parse file: ", error)
                }
            this.reportData = results
            }
        );
    }

    async sendData(client: KafkaProducer): Promise<void> {
        logger.info("tornado")
        logger.info(this.reportData)
        if (this.reportData.length > 0) {
            const messages = this.reportData.map((data) => ({
                value: JSON.stringify({
                    Time: data.Time,
                    EventTs: this.convertEventDateToDate(),
                    EmitTs: Date.now(),
                    FScale: data.F_Scale,
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
            logger.info("No tornado data to sent");
        }
    }
}

export default TornadoCollector;