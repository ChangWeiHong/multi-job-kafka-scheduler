export function secondsFromNow(seconds: number): Date {
  return new Date(Date.now() + seconds * 1000);
}

export function retryDelaySeconds(attempt: number): number {
  return 10 * 2 ** Math.max(0, attempt - 1);
}

export function lockUntil(): Date {
  return secondsFromNow(120);
}
