import { createLogger, format, transports } from 'winston';

const { combine, timestamp, printf, colorize } = format;

// Define the custom format for log messages
const logFormat = printf(({ level, message, timestamp }) => {
  return `${timestamp} [${level}]: ${message}`;
});

// Create a logger instance (singleton)
const logger = createLogger({
    level: process.env.NODE_ENV === 'production' ? 'error' : 'debug',
    format: combine(
      timestamp(),
      colorize(),
      logFormat
    ),
    transports: [
      new transports.Console(),
      new transports.File({ filename: 'combined.log' })
    ]
  });
  
  // Export the singleton instance of the logger
  export default logger;