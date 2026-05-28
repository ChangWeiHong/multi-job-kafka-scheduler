import { retryDelaySeconds } from '../src/common/time';

describe('retryDelaySeconds', () => {
  it('uses a small exponential backoff', () => {
    expect(retryDelaySeconds(1)).toBe(10);
    expect(retryDelaySeconds(2)).toBe(20);
    expect(retryDelaySeconds(3)).toBe(40);
  });
});
