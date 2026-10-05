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


class _SegmentWriter:
    """
    Writes media frames to a file. A new init segment (ftyp box) starts a new file,
    because a restarted stream has its own timestamps and must not be appended to the previous one
    """

    def __init__(self, filename):
        self._filename = filename
        self._stream = None
        self._written = 0
        self.paths = []

    def write(self, frame):
        if self._stream is None or (self._written and self._filename != '-' and frame[4:8] == b'ftyp'):
            self._next_file()
        self._stream.write(frame)
        self._written += len(frame)

    def _next_file(self):
        self.close()
        self._stream, path = sanitize_open(
            f'{self._filename}.{len(self.paths)}' if self.paths else self._filename, 'wb')
        self.paths.append(path)
        self._written = 0

    def close(self):
        if self._stream:
            self._stream.close()
            self._stream = None


class ShowupLiveFD(FileDownloader):
    """
    Downloads a live stream from showup.tv.
    The server sends fragmented MP4 as binary WebSocket frames; they are written to the file as they arrive.

    When the stream is interrupted (e.g. while a private show is requested), the connection is kept
    and playback is requested again once the stream is back, for up to `resume_timeout` seconds.
    Every resumed part goes to its own file, and the parts are joined once the download has finished.
    The timeout is checked whenever a message arrives, so it can be overrun by up to the socket timeout
    if the server goes completely silent.
    """

    def real_download(self, filename, info_dict):
        opts = info_dict['downloader_options']
        # The parts of an interrupted stream can't be joined when writing to stdout
        resume_timeout = 0 if filename == '-' else opts.get('resume_timeout') or 0
        subscription_key = opts['subscription_key']
        tmpfilename = self.temp_name(filename)
        self.report_destination(filename)

        # Connect before creating any file, so that nothing is left behind if no server is reachable
        session = self._open_session(info_dict, opts)
        start_time = last_ping = last_progress = time.time()
        writer = _SegmentWriter(tmpfilename)
        byte_count = 0
        interrupted_at = None

        def hook_progress(status):
            elapsed = time.time() - start_time
            self._hook_progress({
                'status': status,
                'downloaded_bytes': byte_count,
                'filename': filename,
                'tmpfilename': writer.paths[-1] if writer.paths else tmpfilename,
                'elapsed': elapsed,
                'speed': byte_count / elapsed if elapsed else None,
                **({'total_bytes': byte_count} if status == 'finished' else {}),
            }, info_dict)

        try:
            while True:
                try:
                    message = session.recv()
                except RequestError as e:
                    if not byte_count:
                        raise
                    # The server closes the connection when the broadcast ends
                    self.write_debug(f'Connection lost: {e}')
                    session.close()
                    interrupted_at = interrupted_at or time.time()
                    session = self._reconnect(info_dict, opts, interrupted_at + resume_timeout)
                    if not session:
                        break
                    interrupted_at = time.time()
                    continue

                now = time.time()
                if isinstance(message, bytes):
                    writer.write(message)
                    byte_count += len(message)
                    interrupted_at = None
                else:
                    packet_id, data = message
                    if packet_id == 'subscribeUpdate' and data.get('streamKey') in (None, subscription_key):
                        state = data.get('streamState')
                        self.write_debug(f'Stream state: {state}')
                        if state in ('CLOSING', 'CLOSED'):
                            break
                        elif state != 'PUBLISHED':
                            interrupted_at = interrupted_at or now
                        elif interrupted_at and resume_timeout:
                            self._play(session, info_dict, opts, data)
                    elif packet_id == 'playbackStop':
                        self.write_debug('Playback stopped by the server')
                        interrupted_at = interrupted_at or now
                        if resume_timeout:
                            self._play(session, info_dict, opts)

                if interrupted_at and now - interrupted_at >= resume_timeout:
                    break
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
        finally:
            writer.close()
            if session:
                session.close()

        if not byte_count:
            for path in writer.paths:
                self.try_remove(path)
            self.report_error('The downloaded file is empty')
            return False

        if len(writer.paths) > 1:
            self._join_parts(writer.paths, filename)
        else:
            self.try_rename(writer.paths[0], filename)
            if filename != '-':
                self._make_seekable(filename)
        if filename != '-':
            byte_count = self.filesize_or_none(filename) or byte_count
        hook_progress('finished')
        return True

    @staticmethod
    def _select_stream_key(subscription, info_dict, opts):
        # The keys of the streams may change when the broadcast restarts, so fall back to the quality label
        streams = (subscription or {}).get('streamList') or []
        if not streams or any(stream.get('streamKey') == opts['stream_key'] for stream in streams):
            return opts['stream_key']
        for stream in streams:
            if stream.get('streamInfo', {}).get('label') == info_dict.get('format_id'):
                return stream['streamKey']
        return opts['stream_key']

    def _play(self, session, info_dict, opts, subscription=None):
        """Request playback; returns False if the server refuses, e.g. because the stream is not published yet"""
        try:
            session.play(self._select_stream_key(subscription, info_dict, opts), opts['subscription_key'])
        except (ExtractorError, RequestError) as e:
            self.write_debug(f'Unable to resume playback: {e}')
            return False
        return True

    def _open_session(self, info_dict, opts, quiet=False):
        # Try the other edge servers if one is unreachable
        ws_urls = list(dict.fromkeys([info_dict['url'], *opts.get('fallback_urls', [])]))
        for i, ws_url in enumerate(ws_urls):
            try:
                session = ShowupStormSession(self.ydl, ws_url)
                try:
                    subscription = session.subscribe(opts['subscription_key'])
                    session.play(self._select_stream_key(subscription, info_dict, opts), opts['subscription_key'])
                except BaseException:
                    session.close()
                    raise
                return session
            except RequestError as e:
                if i == len(ws_urls) - 1:
                    raise
                (self.write_debug if quiet else self.report_warning)(
                    f'Unable to connect to {ws_url}: {e.msg}. Trying the next server')

    def _reconnect(self, info_dict, opts, deadline):
        """Connect again after the connection was lost; returns None if that wasn't possible before the deadline"""
        while time.time() < deadline:
            try:
                session = self._open_session(info_dict, opts, quiet=True)
                self.to_screen('[showup.tv] Reconnected, the stream is back')
                return session
            except (RequestError, ExtractorError) as e:
                self.write_debug(f'Unable to reconnect: {e}')
            time.sleep(max(0, min(5, deadline - time.time())))
        return None

    def _join_parts(self, paths, filename):
        self.to_screen(f'[showup.tv] The stream was interrupted; joining {len(paths)} parts')
        temp_filename = prepend_extension(filename, 'temp')
        try:
            FFmpegPostProcessor(self.ydl).concat_files(paths, temp_filename)
            os.replace(temp_filename, filename)
        except PostProcessingError as e:
            self.try_remove(temp_filename)
            self.try_rename(paths[0], filename)
            kept = [filename]
            for i, path in enumerate(paths[1:], 2):
                kept.append(prepend_extension(filename, f'part{i}'))
                self.try_rename(path, kept[-1])
            self.report_warning(f'Unable to join the parts, they were kept as separate files: {", ".join(kept)}: {e}')
            return
        for path in paths:
            if path != filename:  # With --no-part the first part is the output file itself
                self.try_remove(path)

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
