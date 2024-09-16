import axios, { HttpStatusCode } from 'axios';
import fs from 'fs';

import logger from '../utils/logger';

class StormCollector {
    protected csvFilePath: string;
    protected apiEndpoint: string;
    protected eventDate: string = '';
    protected fullWeatherType: string = '';
    public weatherType: string = '';
    protected csvColumns: string[] = [];
  
    constructor(csvFilePath: string, apiEndpoint: string, eventDate: string) {
      this.csvFilePath = csvFilePath;
      this.apiEndpoint = apiEndpoint;
      this.eventDate = eventDate;
    }

    public getWeatherType(): string {
        return this.fullWeatherType;
    }

    protected convertEventDateToDate(): number {
      // Ensure the string is exactly 6 characters long
      if (this.eventDate.length !== 6) {
        throw new Error('Invalid date string format. It should be YYMMDD.');
      }
    
      // Extract time parts from string
      const year = parseInt(this.eventDate.substring(0, 2), 10);
      const month = parseInt(this.eventDate.substring(2, 4), 10);
      const day = parseInt(this.eventDate.substring(4, 6), 10);
    
      // Convert two-digit year into a full year
      const fullYear = year > 24 ? 1900 + year : 2000 + year;
      
      logger.info(year + " " + month + " " + day)

      // Create and return the Date object
      return Date.UTC(fullYear, month - 1, day); 
    }

    protected async collectData(): Promise<void> {
      try {
        const response = await axios.get(
          this.apiEndpoint, {
            responseType: 'stream',
          }
        );
    
        // Save the response stream to a file
        const writer = fs.createWriteStream(this.csvFilePath);
        response.data.pipe(writer);
    
        return new Promise<void>((resolve, reject) => {
            writer.on('finish', () => {
              logger.info('CSV file downloaded successfully');
              resolve(); // Resolve the promise when download is successful
            });
      
            writer.on('error', (err) => {
              logger.error('Error writing file:', err);
              reject(new Error('Error writing file: ' + err.message)); // Reject if an error occurs while writing
            });
          });
      } catch (error) {
        if (axios.isAxiosError(error)) {
            logger.error(`Axios error: ${error.response?.status} - ${error.response?.statusText}`);
            throw new Error(`Failed to download CSV: ${error.response?.status} - ${error.response?.statusText}`);
        } else {
            logger.error('Error downloading CSV:', error);
            throw new Error(`Failed to download CSV: ` + error);
        }
      }
    }
  }

  export default StormCollector;
