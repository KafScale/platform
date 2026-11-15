import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { DeleteObjectsCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';

test('S3 XML parsing and serialization work with the security overrides', async (t) => {
  const key = 'blob & café';
  let deleteBody = '';
  const server = createServer(async (request, response) => {
    response.setHeader('Content-Type', 'application/xml');
    if (request.method === 'GET') {
      response.end('<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        + '<Name>test</Name><Contents><Key>blob &amp; café</Key><Size>7</Size></Contents></ListBucketResult>');
    } else {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(Buffer.from(chunk));
      deleteBody = Buffer.concat(chunks).toString('utf8');
      response.end('<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        + '<Deleted><Key>blob &amp; café</Key></Deleted></DeleteResult>');
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const client = new S3Client({
    region: 'us-east-1', endpoint: `http://127.0.0.1:${address.port}`, forcePathStyle: true,
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
  t.after(async () => {
    client.destroy();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  });
  const listed = await client.send(new ListObjectsV2Command({ Bucket: 'test' }));
  assert.equal(listed.Contents?.[0].Key, key);
  assert.equal(listed.Contents?.[0].Size, 7);
  const deleted = await client.send(new DeleteObjectsCommand({ Bucket: 'test', Delete: { Objects: [{ Key: key }] } }));
  assert.equal(deleted.Deleted?.[0].Key, key);
  assert.match(deleteBody, /<Key>blob &amp; café<\/Key>/);
});
