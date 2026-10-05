import json
import os
import time

from .common import FileDownloader
from ..networking import Request
from ..networking.exceptions import RequestError
from ..postprocessor.ffmpeg import FFmpegPostProcessor
from ..utils import ExtractorError, PostProcessingError, prepend_extension, sanitize_open

PING_INTERVAL = 30


class ShowupStormSession:
    """Minimal client for the Storm v2 WebSocket protocol used by showup.tv"""

    def __init__(self, ydl, ws_url):
        self._ws = ydl.urlopen(Request(ws_url))
        try:
            self._handshake(ydl)
        except BaseException:
            self.close()
            raise

    def _handshake(self, ydl):
        self._send('clientHandshake', {
            'player': {'type': 'js', 'version': 'yt-dlp', 'branch': '', 'protocolVer': 1},
            'environment': {
                'domain': 'showup.tv',
                'userAgent': ydl.params.get('http_headers', {}).get('User-Agent') or 'yt-dlp',
                'locale': 'en',
                'timezone': 'UTC',
                'timezoneOffset': 0,
                'screenWidth': 1920,
                'screenHeight': 1080,
            },
            'capabilities': {
                'mse': True,
                'webcodecs': False,
                'hls': False,
                'webtransport': False,
                'sharedArrayBuffer': False,
                'videoCodecs': ['avc1'],
                'audioCodecs': ['mp4a'],
            },
            'userId': None,
        })
        self.wait_for('appInfo')
        self._send('appAuthRequest', {'secret': None})
        self._expect_success(self.wait_for('appAuthResult'), 'result')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._ws.close()

    def _send(self, packet_id, data=None):
        self._ws.send(json.dumps({'packetId': packet_id, 'data': data or {}}))

    @staticmethod
    def _expect_success(data, key='status'):
        if data.get(key) != 'success':
            raise ExtractorError(
                f'Server refused the request: {data.get("reason") or data.get(key)}', expected=True)

    def recv(self):
        """Returns binary media frames as bytes and control packets as (packet_id, data)"""
        message = self._ws.recv()
        if isinstance(message, bytes):
            return message
        packet = json.loads(message)
        return packet.get('packetId'), packet.get('data') or {}

    def wait_for(self, packet_id):
        while True:
            message = self.recv()
            if isinstance(message, tuple) and message[0] == packet_id:
                return message[1]

    def subscribe(self, subscription_key):
        self._send('subscribeRequest', {'streamKey': subscription_key})
        data = self.wait_for('subscribeResult')
        self._expect_success(data)
        return data

    def play(self, stream_key, subscription_key):
        self._send('playRequest', {
            'streamKey': stream_key,
            'subscriptionKey': subscription_key,
            'packetizer': 'mse',
            'accessPoint': 0,
        })
        data = self.wait_for('playResult')
        self._expect_success(data)
        if data.get('streamState') != 'PUBLISHED':
            raise ExtractorError(f'Stream is not live (state: {data.get("streamState")})', expected=True)

    def ping(self):
        self._send('ping')


class ShowupLiveFD(FileDownloader):
    """
    Downloads a live stream from showup.tv.
    The server sends fragmented MP4 as binary WebSocket frames; they are written to the file as they arrive.
    """

    def real_download(self, filename, info_dict):
        opts = info_dict['downloader_options']
        tmpfilename = self.temp_name(filename)
        self.report_destination(filename)

        # Connect before creating the file, so that nothing is left behind if no server is reachable
        session = self._open_session(info_dict, opts)
        start_time = last_ping = last_progress = time.time()
        byte_count = 0
        stream, tmpfilename = sanitize_open(tmpfilename, 'wb')

        def hook_progress(status):
            elapsed = time.time() - start_time
            self._hook_progress({
                'status': status,
                'downloaded_bytes': byte_count,
                'filename': filename,
                'tmpfilename': tmpfilename,
                'elapsed': elapsed,
                'speed': byte_count / elapsed if elapsed else None,
                **({'total_bytes': byte_count} if status == 'finished' else {}),
            }, info_dict)

        try:
            with session:
                while True:
                    message = session.recv()
                    if isinstance(message, bytes):
                        stream.write(message)
                        byte_count += len(message)
                    elif message[0] == 'playbackStop':
                        break
                    elif message[0] == 'subscribeUpdate' and message[1].get('streamState') != 'PUBLISHED':
                        break

                    now = time.time()
                    if now - last_ping >= PING_INTERVAL:
                        session.ping()
                        last_ping = now
                    if now - last_progress >= 0.5:
                        hook_progress('downloading')
                        last_progress = now
        except KeyboardInterrupt:
            if not info_dict.get('is_live'):
                raise
            self.to_screen('Interrupted by user')
        except RequestError as e:
            if not byte_count:
                raise
            # The server closes the connection when the broadcast ends
            self.write_debug(f'Connection closed: {e}')
        finally:
            stream.close()

        if not byte_count:
            self.try_remove(tmpfilename)
            self.report_error('The downloaded file is empty')
            return False

        self.try_rename(tmpfilename, filename)
        if filename != '-':
            self._make_seekable(filename)
            byte_count = self.filesize_or_none(filename) or byte_count
        hook_progress('finished')
        return True

    def _open_session(self, info_dict, opts):
        # Try the other edge servers if one is unreachable. This is only possible before
        # any media is received; a new connection starts with a new init segment and timestamps
        ws_urls = list(dict.fromkeys([info_dict['url'], *opts.get('fallback_urls', [])]))
        for i, ws_url in enumerate(ws_urls):
            try:
                session = ShowupStormSession(self.ydl, ws_url)
                try:
                    session.subscribe(opts['subscription_key'])
                    session.play(opts['stream_key'], opts['subscription_key'])
                except BaseException:
                    session.close()
                    raise
                return session
            except RequestError as e:
                if i == len(ws_urls) - 1:
                    raise
                self.report_warning(f'Unable to connect to {ws_url}: {e.msg}. Trying the next server')

    def _make_seekable(self, filename):
        # The raw fragmented MP4 has no global index or duration, so players cannot seek in it
        temp_filename = prepend_extension(filename, 'temp')
        self.to_screen(f'[showup.tv] Remuxing "{filename}" to make it seekable')
        try:
            pp = FFmpegPostProcessor(self.ydl)
            pp.run_ffmpeg(filename, temp_filename, [*pp.stream_copy_opts(ext='mp4'), '-movflags', '+faststart'])
            os.replace(temp_filename, filename)
        except PostProcessingError as e:
            self.try_remove(temp_filename)
            self.report_warning(f'Unable to remux the file, it will not be seekable: {e}')
