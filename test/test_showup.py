#!/usr/bin/env python3

# Allow direct execution
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import contextlib
import json
import socket
import threading
import time

import pytest

from test.helper import FakeYDL
from yt_dlp import YoutubeDL
from yt_dlp.dependencies import websockets
from yt_dlp.downloader import showup
from yt_dlp.downloader.showup import ShowupLiveFD, ShowupStormSession
from yt_dlp.extractor import showuptv
from yt_dlp.extractor.showuptv import ShowupTvIE
from yt_dlp.networking.exceptions import RequestError
from yt_dlp.utils import DownloadError, ExtractorError, UserNotLive

try:
    from websockets.sync.server import serve
except ImportError:
    serve = None

pytestmark = pytest.mark.skipif(not (websockets and serve), reason='websockets is not installed')

STREAM_KEY = 'streamkey'
ALIAS_KEY = 'aliaskey'
FRAMES = [b'\x00\x00\x00\x08ftyp', b'moov-bytes', b'moof-mdat-1', b'moof-mdat-2']


def storm_server(play_status='success', stream_state='PUBLISHED', frames=FRAMES, after_play=None):
    """A fake Storm v2 server. Returns (port, list of received packets)

    after_play(ws, send) replaces the default stream of frames followed by playbackStop
    """
    received = []

    def send(ws, packet_id, data):
        ws.send(json.dumps({'packetId': packet_id, 'data': data}))

    def handler(ws):
        for raw in ws:
            packet = json.loads(raw)
            packet_id = packet['packetId']
            received.append(packet)
            if packet_id == 'clientHandshake':
                send(ws, 'serverHandshake', {'serverName': 'fake'})
                send(ws, 'appInfo', {'name': 'live', 'playbackSettings': {'tokenRequired': False}})
            elif packet_id == 'appAuthRequest':
                send(ws, 'appAuthResult', {'result': 'success'})
            elif packet_id == 'subscribeRequest':
                send(ws, 'subscribeResult', {
                    'streamKey': packet['data']['streamKey'],
                    'streamState': 'PUBLISHED',
                    'status': 'success',
                    'streamList': [
                        {'streamKey': STREAM_KEY, 'streamInfo': {'label': '720p', 'width': 1280, 'height': 720, 'bitrate': 0}},
                        {'streamKey': f'{STREAM_KEY}_480p', 'streamInfo': {'label': '480p', 'width': 854, 'height': 480, 'bitrate': 1464}},
                    ],
                })
            elif packet_id == 'playRequest':
                send(ws, 'playResult', {'streamKey': packet['data']['streamKey'], 'streamState': stream_state, 'status': play_status})
                if play_status != 'success' or stream_state != 'PUBLISHED':
                    return
                if after_play:
                    return after_play(ws, send)
                for frame in frames:
                    send(ws, 'playbackProgress', {})  # text frames must not end up in the file
                    ws.send(frame)
                send(ws, 'playbackStop', {})
                return

    server = serve(handler, '127.0.0.1', 0, open_timeout=2)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.socket.getsockname()[1], received


def unused_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def download(tmp_path, url, ydl_params=None, **options):
    ydl = YoutubeDL({'quiet': True, 'noprogress': True, 'nopart': True, **(ydl_params or {})})
    filename = str(tmp_path / 'out.mp4')
    info_dict = {
        'id': 'test',
        'url': url,
        'ext': 'mp4',
        'is_live': True,
        'protocol': 'showup_live',
        'downloader_options': {'stream_key': STREAM_KEY, 'subscription_key': ALIAS_KEY, **options},
    }
    fd = ShowupLiveFD(ydl, ydl.params)
    # Remuxing needs ffmpeg and a real MP4
    fd._make_seekable = lambda filename: None
    return fd.download(filename, info_dict), filename


class TestShowupDownloader:
    def test_session_subscribe(self):
        port, received = storm_server()
        with ShowupStormSession(FakeYDL(), f'ws://127.0.0.1:{port}') as session:
            data = session.subscribe(ALIAS_KEY)
        assert [s['streamInfo']['label'] for s in data['streamList']] == ['720p', '480p']
        handshake = received[0]['data']
        assert handshake['capabilities']['mse'] is True
        assert handshake['userId'] is None
        assert [p['packetId'] for p in received[1:]] == ['appAuthRequest', 'subscribeRequest']
        assert received[2]['data'] == {'streamKey': ALIAS_KEY}

    def test_writes_binary_frames_only(self, tmp_path):
        port, received = storm_server()
        result, filename = download(tmp_path, f'ws://127.0.0.1:{port}')
        assert result
        with open(filename, 'rb') as f:
            assert f.read() == b''.join(FRAMES)
        play = next(p for p in received if p['packetId'] == 'playRequest')['data']
        assert play == {
            'streamKey': STREAM_KEY, 'subscriptionKey': ALIAS_KEY, 'packetizer': 'mse', 'accessPoint': 0}

    def test_connection_closed_without_stop_packet(self, tmp_path):
        def after_play(ws, send):
            for frame in FRAMES:
                ws.send(frame)
            ws.close()

        port, _ = storm_server(after_play=after_play)
        result, filename = download(tmp_path, f'ws://127.0.0.1:{port}')
        assert result
        with open(filename, 'rb') as f:
            assert f.read() == b''.join(FRAMES)

    def test_stream_unpublished_midway(self, tmp_path):
        def after_play(ws, send):
            ws.send(FRAMES[0])
            send(ws, 'subscribeUpdate', {'streamState': 'UNPUBLISHED'})
            ws.send(b'must-not-be-written')
            time.sleep(0.5)

        port, _ = storm_server(after_play=after_play)
        result, filename = download(tmp_path, f'ws://127.0.0.1:{port}')
        assert result
        with open(filename, 'rb') as f:
            assert f.read() == FRAMES[0]

    def test_stalled_connection_keeps_what_was_received(self, tmp_path):
        def after_play(ws, send):
            ws.send(FRAMES[0])
            time.sleep(3)

        port, _ = storm_server(after_play=after_play)
        result, filename = download(tmp_path, f'ws://127.0.0.1:{port}', ydl_params={'socket_timeout': 1})
        assert result
        with open(filename, 'rb') as f:
            assert f.read() == FRAMES[0]

    def test_sends_ping_while_streaming(self, tmp_path, monkeypatch):
        monkeypatch.setattr(showup, 'PING_INTERVAL', 0.1)

        def after_play(ws, send):
            for frame in FRAMES:
                ws.send(frame)
                time.sleep(0.2)
            # Pick up what the client sent while we were busy
            with contextlib.suppress(TimeoutError):
                while True:
                    received.append(json.loads(ws.recv(timeout=0.3)))

        port, received = storm_server(after_play=after_play)
        result, _ = download(tmp_path, f'ws://127.0.0.1:{port}')
        assert result
        assert 'ping' in [p['packetId'] for p in received]

    def test_falls_back_to_next_server(self, tmp_path):
        port, _ = storm_server()
        dead_url = f'ws://127.0.0.1:{unused_port()}'
        result, filename = download(
            tmp_path, dead_url, fallback_urls=[dead_url, f'ws://127.0.0.1:{port}'])
        assert result
        with open(filename, 'rb') as f:
            assert f.read() == b''.join(FRAMES)

    def test_unreachable_servers_leave_no_file(self, tmp_path):
        dead_url = f'ws://127.0.0.1:{unused_port()}'
        with pytest.raises(RequestError):
            download(tmp_path, dead_url, fallback_urls=[f'ws://127.0.0.1:{unused_port()}'])
        assert not list(tmp_path.iterdir())

    def test_play_denied(self, tmp_path):
        port, _ = storm_server(play_status='denied')
        with pytest.raises(ExtractorError, match='Server refused'):
            download(tmp_path, f'ws://127.0.0.1:{port}')
        assert not list(tmp_path.iterdir())

    def test_stream_not_published(self, tmp_path):
        port, _ = storm_server(stream_state='UNPUBLISHED')
        with pytest.raises(ExtractorError, match='not live'):
            download(tmp_path, f'ws://127.0.0.1:{port}')

    def test_empty_download_is_an_error(self, tmp_path):
        port, _ = storm_server(frames=[])
        with pytest.raises(DownloadError, match='empty'):
            download(tmp_path, f'ws://127.0.0.1:{port}')
        assert not list(tmp_path.iterdir())


def next_data_page(page_props, status=200):
    data = json.dumps({'props': {'pageProps': page_props}, 'page': '/[username]'})
    return f'<html><script id="__NEXT_DATA__" type="application/json">{data}</script></html>'


def profile_page(load_state='ONLINE', address_type='PUB', alias=ALIAS_KEY, edges=('edge1', 'edge2')):
    return next_data_page({
        'serverData': {'streamServers': {'EDGE': [{'address': f'{e}.example.com', 'appName': 'live'} for e in edges]}},
        'broadcastData': {
            'broadcast': {
                'host': {'id': 1234, 'username': 'some_user'},
                'broadcastAddress': {'type': address_type},
                'aliasStreamKey': alias,
                'imageUrl': 'https://cdn.example.com/thumb.webp',
                'loadBehaviour': load_state,
            },
            'profile': {'about': {'name': ' Some Name ', 'description': 'About me'}},
        },
    })


class TestShowupExtractor:
    @pytest.fixture
    def extract(self, monkeypatch):
        def extract(webpage, url='https://showup.tv/some_user'):
            ie = ShowupTvIE(FakeYDL())
            monkeypatch.setattr(ie, '_download_webpage', lambda *a, **kw: webpage)
            return ie._real_extract(url)
        return extract

    @pytest.fixture
    def fake_server(self, monkeypatch):
        port, _ = storm_server()
        original = showuptv.ShowupStormSession
        monkeypatch.setattr(
            showuptv, 'ShowupStormSession',
            lambda ydl, ws_url: original(ydl, f'ws://127.0.0.1:{port}'))

    def test_online(self, extract, fake_server):
        info = extract(profile_page())
        assert {k: info[k] for k in (
            'id', 'title', 'uploader', 'uploader_id', 'uploader_url', 'channel_id',
            'thumbnail', 'description', 'age_limit', 'is_live')
        } == {
            'id': 'some_user',
            'title': 'Some Name',
            'uploader': 'Some Name',
            'uploader_id': 'some_user',
            'uploader_url': 'https://showup.tv/some_user',
            'channel_id': '1234',
            'thumbnail': 'https://cdn.example.com/thumb.webp',
            'description': 'About me',
            'age_limit': 18,
            'is_live': True,
        }
        formats = {f['format_id']: f for f in info['formats']}
        assert set(formats) == {'720p', '480p'}
        assert (formats['720p']['width'], formats['720p']['height'], formats['720p'].get('tbr')) == (1280, 720, None)
        assert formats['480p']['tbr'] == 1464
        assert formats['480p']['protocol'] == 'showup_live'
        assert (formats['480p']['vcodec'], formats['480p']['acodec']) == ('avc1', 'mp4a.40.2')
        assert formats['480p']['downloader_options'] == {
            'stream_key': f'{STREAM_KEY}_480p',
            'subscription_key': ALIAS_KEY,
            'fallback_urls': [
                'wss://edge1.example.com/storm/v2/live',
                'wss://edge2.example.com/storm/v2/live',
            ],
        }

    def test_username_case_is_canonicalised(self, extract, fake_server):
        # The URL in `extract` is lowercase, the page has the canonical casing
        page = profile_page().replace('some_user', 'Some_User')
        info = extract(page, url='https://showup.tv/some_user')
        assert (info['id'], info['uploader_id'], info['uploader_url']) == (
            'Some_User', 'Some_User', 'https://showup.tv/Some_User')

    def test_offline(self, extract):
        with pytest.raises(UserNotLive):
            extract(profile_page(load_state='OFFLINE', alias=None))

    def test_nonexistent(self, extract):
        page = next_data_page({'broadcastData': {'error': {'message': 'not found', 'status': 404}}})
        with pytest.raises(ExtractorError, match='was not found') as exc_info:
            extract(page)
        assert exc_info.value.expected

    def test_private_show(self, extract):
        with pytest.raises(ExtractorError, match='private or group show') as exc_info:
            extract(profile_page(address_type='PRV'))
        assert exc_info.value.expected

    def test_unknown_state(self, extract):
        with pytest.raises(ExtractorError, match='not available') as exc_info:
            extract(profile_page(load_state='SOMETHING_NEW'))
        assert exc_info.value.expected

    def test_all_servers_unreachable(self, extract, monkeypatch):
        dead = f'ws://127.0.0.1:{unused_port()}'
        original = showuptv.ShowupStormSession
        monkeypatch.setattr(showuptv, 'ShowupStormSession', lambda ydl, ws_url: original(ydl, dead))
        with pytest.raises(ExtractorError, match='any stream server') as exc_info:
            extract(profile_page())
        assert exc_info.value.expected
