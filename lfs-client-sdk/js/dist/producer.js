import { request } from 'undici';
export async function produceLfs(endpoint, payload, options) {
    const headers = {
        'X-Kafka-Topic': options.topic,
    };
    if (options.key) {
        headers['X-Kafka-Key'] = Buffer.from(options.key).toString('utf8');
    }
    if (options.headers) {
        Object.assign(headers, options.headers);
    }
    const res = await request(endpoint, {
        method: 'POST',
        headers,
        body: payload,
    });
    const body = await res.body.text();
    if (res.statusCode < 200 || res.statusCode >= 300) {
        throw new Error(`produce failed: ${res.statusCode} ${body}`);
    }
    return JSON.parse(body);
}
