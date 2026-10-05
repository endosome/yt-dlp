from .common import InfoExtractor
from ..dependencies import websockets
from ..downloader.showup import ShowupStormSession
from ..networking.exceptions import HTTPError, RequestError
from ..utils import ExtractorError, UserNotLive, int_or_none, str_or_none, url_or_none
from ..utils.traversal import traverse_obj


class ShowupTvIE(InfoExtractor):
    IE_NAME = 'showup.tv'
    _RESERVED_PATHS = (
        '404', '500', '502', '504', 'activate', 'administrators-partners', 'broadcast', 'buy-tokens', 'community',
        'contact', 'forgot-password', 'group', 'history', 'inbox', 'maintenance-static', 'mod',
        'model-commission-table', 'my-fans', 'my-stars', 'notifications', 'privacy-policy', 'profile', 'prv',
        'regulations', 'reset-password', 'settings', 'sign-in', 'sign-out', 'sign-up', 'site', 'user-fee-table',
        'verification', 'withdraw', 'en', 'pl',
    )
    _VALID_URL = rf'https?://(?:www\.)?showup\.tv/(?:(?:en|pl)/)?(?!(?:{"|".join(_RESERVED_PATHS)})(?:[/?#]|$))(?P<id>[\w-]+)/?(?:[?#]|$)'
    _TESTS = [{
        'url': 'https://showup.tv/example_user',
        'info_dict': {
            'id': 'example_user',
            'ext': 'mp4',
            'title': str,
            'uploader': str,
            'uploader_id': 'example_user',
            'uploader_url': 'https://showup.tv/example_user',
            'channel_id': str,
            'thumbnail': r're:https?://.+',
            'age_limit': 18,
            'live_status': 'is_live',
        },
        'params': {'skip_download': 'livestream'},
        'skip': 'Livestream; replace the username with a performer who is currently online',
    }, {
        'url': 'https://showup.tv/en/example_user',
        'only_matching': True,
    }, {
        'url': 'https://showup.tv/example_user?foo=bar#baz',
        'only_matching': True,
    }, {
        'url': 'https://www.showup.tv/example-user/',
        'only_matching': True,
    }]

    def _real_extract(self, url):
        username = self._match_id(url)
        if not websockets:
            raise ExtractorError('The websockets library is required to download showup.tv streams', expected=True)

        # Cloudflare rejects non-browser clients with a 403
        try:
            webpage = self._download_webpage(
                f'https://showup.tv/{username}', username, impersonate=True, require_impersonation=True,
                expected_status=404)
        except ExtractorError as e:
            if isinstance(e.cause, HTTPError) and e.cause.status == 403:
                raise ExtractorError(
                    'Blocked by Cloudflare (HTTP 403). Do not use a custom --user-agent, and if you use cookies, '
                    'try removing the "cf_clearance" cookie from them', expected=True)
            raise
        page_props = traverse_obj(self._search_nextjs_data(webpage, username), ('props', 'pageProps', {dict})) or {}
        broadcast_data = page_props.get('broadcastData') or {}

        if traverse_obj(broadcast_data, ('error', 'status')) == 404:
            raise ExtractorError(f'Broadcast for user "{username}" was not found', expected=True)

        broadcast = broadcast_data.get('broadcast') or {}
        load_state = broadcast.get('loadBehaviour')
        if load_state == 'OFFLINE':
            raise UserNotLive(video_id=username)
        alias_stream_key = broadcast.get('aliasStreamKey')
        if load_state != 'ONLINE' or not alias_stream_key:
            raise ExtractorError(f'Broadcast is not available (state: {load_state})', expected=True)
        if traverse_obj(broadcast, ('broadcastAddress', 'type')) != 'PUB':
            raise ExtractorError('This broadcast is a private or group show, which is not supported', expected=True)

        # Subscribing only lists the available qualities; the downloader opens its own connection
        subscription = ws_url = None
        ws_urls = [
            f'wss://{edge["address"]}/storm/v2/{edge.get("appName") or "live"}'
            for edge in traverse_obj(page_props, ('serverData', 'streamServers', 'EDGE', lambda _, v: v['address']))]
        for ws_url in ws_urls:
            try:
                with ShowupStormSession(self._downloader, ws_url) as session:
                    subscription = session.subscribe(alias_stream_key)
                break
            except RequestError as e:
                self.report_warning(f'Unable to connect to {ws_url}: {e.msg}')
        if not subscription:
            raise ExtractorError(
                'Unable to connect to any stream server. A custom --user-agent that does not look '
                'like a browser can cause this', expected=True)
        if subscription.get('streamState') != 'PUBLISHED':
            raise UserNotLive(video_id=username)

        # How long to wait for the stream to come back when it is interrupted, e.g. by a private show request
        resume_timeout = max(int_or_none(self._configuration_arg('resume_timeout', ['60'])[0], default=60), 0)

        formats = [{
            'url': ws_url,
            'ext': 'mp4',
            'protocol': 'showup_live',
            # The handshake only advertises H.264 and AAC, which is all that has been observed
            'vcodec': 'avc1',
            'acodec': 'mp4a.40.2',
            'downloader_options': {
                'stream_key': stream['streamKey'],
                'subscription_key': alias_stream_key,
                'fallback_urls': ws_urls,
                'resume_timeout': resume_timeout,
            },
            **traverse_obj(stream, {
                'format_id': ('streamInfo', 'label', {str}),
                'width': ('streamInfo', 'width', {int_or_none}),
                'height': ('streamInfo', 'height', {int_or_none}),
                'tbr': ('streamInfo', 'bitrate', {int_or_none}, {lambda x: x or None}),
            }),
        } for stream in subscription.get('streamList') or [] if stream.get('streamKey')]

        # Usernames are case-insensitive in URLs; use the canonical form from the page for the IDs
        username = traverse_obj(broadcast, ('host', 'username', {str})) or username
        uploader = traverse_obj(broadcast_data, ('profile', 'about', 'name', {str.strip})) or username
        return {
            'id': username,
            'title': uploader,
            'uploader': uploader,
            'uploader_id': username,
            'uploader_url': f'https://showup.tv/{username}',
            'channel_id': traverse_obj(broadcast, ('host', 'id', {str_or_none})),
            'thumbnail': traverse_obj(broadcast, ('imageUrl', {url_or_none})),
            'description': traverse_obj(broadcast_data, ('profile', 'about', 'description', {str.strip})) or None,
            'age_limit': 18,
            'is_live': True,
            'formats': formats,
        }
