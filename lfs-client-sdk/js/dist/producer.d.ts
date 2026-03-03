import { LfsEnvelope } from './envelope.js';
export interface ProduceOptions {
    topic: string;
    key?: Uint8Array;
    headers?: Record<string, string>;
}
export declare function produceLfs(endpoint: string, payload: Uint8Array, options: ProduceOptions): Promise<LfsEnvelope>;
