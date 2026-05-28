import { Prisma } from '@prisma/client';

export function asPayloadObject(payload: Prisma.JsonValue): Record<string, unknown> {
  if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
    return payload as Record<string, unknown>;
  }
  return {};
}

export function failIfRequested(payload: Record<string, unknown>) {
  if (payload.forceFail === true) {
    throw new Error('Forced failure requested by payload.forceFail');
  }
}

export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
