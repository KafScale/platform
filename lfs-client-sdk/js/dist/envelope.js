export function isLfsEnvelope(value) {
    if (!value || value.length < 15)
        return false;
    if (value[0] !== 123)
        return false;
    const prefix = new TextDecoder().decode(value.slice(0, Math.min(50, value.length)));
    return prefix.includes('"kfs_lfs"');
}
export function decodeEnvelope(value) {
    const env = JSON.parse(new TextDecoder().decode(value));
    if (!env.kfs_lfs || !env.bucket || !env.key || !env.sha256) {
        throw new Error('invalid envelope: missing required fields');
    }
    return env;
}
