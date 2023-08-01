from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
from urllib.parse import urlparse
import io
from uuid import uuid4
from datetime import datetime
import re

class WarcFilePipeline:

    def open_spider(self, spider):
        self.current_url = None

    def close_spider(self, spider):
        if self.current_url is not None:
            self.warc_file.close()

    def process_item(self, item, spider):
        if self.current_url != item['url']:
            if self.current_url is not None:
                self.warc_file.close()

            parsed_url = urlparse(item['url'])
            safe_url = re.sub('[^a-zA-Z0-9-_*.]', '_', parsed_url.geturl())
            self.warc_file = open(f'{safe_url}.warc.gz', 'wb')
            self.writer = WARCWriter(self.warc_file, gzip=True)

            self.current_url = item['url']

        http_headers = StatusAndHeaders('200 OK', item['headers'], protocol='HTTP/1.1')
        warc_headers = {
            'WARC-Type': 'response',
            'WARC-Target-URI': item['url'],
            'Content-Type': 'application/http; msgtype=response',
            'WARC-Date': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'WARC-Record-ID': f'<urn:uuid:{uuid4()}>',
        }
        warc_headers = StatusAndHeaders('response', warc_headers, protocol='WARC/1.1')
        payload = io.BytesIO(item['content'].encode())
        record = self.writer.create_warc_record(item['url'], 'response',
                                                 payload=payload,
                                                 http_headers=http_headers,
                                                 warc_headers=warc_headers)
        self.writer.write_record(record)
        return item
