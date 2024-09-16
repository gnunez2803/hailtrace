declare module 'node-cron' {
    export interface ScheduledTask {
      start: () => void;
      stop: () => void;
      destroy: () => void;
    }
  
    export function schedule(
      cronExpression: string,
      func: () => void,
      options?: { scheduled?: boolean; timezone?: string }
    ): ScheduledTask;
  }
  