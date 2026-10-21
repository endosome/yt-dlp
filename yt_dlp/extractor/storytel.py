import itertools
import uuid

from .common import InfoExtractor
from ..aes import aes_cbc_encrypt_bytes
from ..networking.exceptions import HTTPError
from ..utils import (
    ExtractorError,
    float_or_none,
    int_or_none,
    parse_iso8601,
    url_or_none,
    urlencode_postdata,
)
from ..utils.traversal import traverse_obj


class StorytelBaseIE(InfoExtractor):
    _NETRC_MACHINE = 'storytel'
    _API_BASE = 'https://api.storytel.net'
    _APP_VERSION = '24.22'
    _USER_AGENT = f'Storytel/{_APP_VERSION} (Android 14; Google Pixel 8 Pro) Release/2288629'
    # Static key/IV used by the Android app to "encrypt" the login password
    _PASSWORD_KEY = b'VQZBJ6TD8M9WBUWT'
    _PASSWORD_IV = b'joiwef08u23j341a'
    # Storytel invalidates all sessions (and emails a security warning) when too many
    # audio files are fetched with one session; logging in again regularly avoids that
    _RELOGIN_EVERY = 10

    _jwt = None
    _credentials = None
    _asset_requests = 0

    def _perform_login(self, username, password):
        StorytelBaseIE._credentials = (username, password)
        self._login(username, password)

    def _login(self, username, password):
        encrypted_password = aes_cbc_encrypt_bytes(
            password.encode(), self._PASSWORD_KEY, self._PASSWORD_IV).hex()
        try:
            response = self._download_json(
                'https://www.storytel.com/api/login.action', None, 'Logging in',
                query={
                    'm': '1',
                    'token': 'guestsv',
                    'userid': '-1',
                    'version': self._APP_VERSION,
                    'terminal': 'android',
                    'locale': 'sv',
                    'deviceId': str(uuid.uuid4()),
                    'kidsMode': 'false',
                },
                data=urlencode_postdata({'uid': username, 'pwd': encrypted_password}),
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'User-Agent': self._USER_AGENT,
                })
        except ExtractorError as e:
            if isinstance(e.cause, HTTPError) and e.cause.status in (401, 403):
                if e.cause.status == 403 and b'Cloudflare' in e.cause.response.read():
                    raise ExtractorError('Login request was blocked by Cloudflare', expected=True)
                raise ExtractorError('Invalid username or password', expected=True)
            raise

        jwt = traverse_obj(response, ('accountInfo', 'jwt', {str}))
        if not jwt:
            raise ExtractorError('Login failed: no token in response', expected=True)
        StorytelBaseIE._jwt = jwt
        StorytelBaseIE._asset_requests = 0

    def _auth_headers(self):
        headers = {'User-Agent': self._USER_AGENT}
        if self._jwt:
            headers['Authorization'] = f'Bearer {self._jwt}'
        return headers

    def _call_api(self, path, item_id, country, note='Downloading JSON metadata', query=None, fatal=True):
        # Logged in, the store comes from the token (the account's own store);
        # anonymous requests need it explicitly
        query = {
            **({} if self._jwt else {'store': f'STHP-{country.upper()}'}),
            **(query or {}),
        }
        return self._download_json(
            f'{self._API_BASE}/{path}', item_id, note,
            query=query, headers=self._auth_headers(), fatal=fatal)


class StorytelIE(StorytelBaseIE):
    IE_NAME = 'storytel'
    _VALID_URL = r'https?://(?:www\.)?(?:storytel|mofibo)\.com/(?P<country>[a-z]{2})(?:/[a-z]{2})?/books/(?:[^/?#]+-)?(?P<id>\d+)'
    _TESTS = [{
        'url': 'https://www.storytel.com/pl/books/chrzest-ognia-700144',
        'info_dict': {
            'id': '700144',
            'ext': 'mp3',
            'title': 'Chrzest ognia',
            'description': str,
            'artists': ['Andrzej Sapkowski'],
            'creators': ['Andrzej Sapkowski'],
            'cast': ['Zespół lektorów'],
            'genres': ['Fantasy'],
            'language': 'pl',
            'duration': 50497,
            'series': 'Wiedźmin',
            'series_number': 3,
            'chapters': 'count:29',
            'thumbnail': r're:https://covers\.storytel\.com/.+',
        },
        'params': {'skip_download': True},
        'skip': 'Requires subscription',
    }, {
        'url': 'https://www.storytel.com/se/sv/books/shantaram-1404854?appRedirect=true',
        'only_matching': True,
    }, {
        'url': 'https://www.mofibo.com/dk/da/books/shantaram-1404854',
        'only_matching': True,
    }]

    def _get_audio_url(self, book_id):
        if self._credentials and self._asset_requests and self._asset_requests % self._RELOGIN_EVERY == 0:
            self.write_debug('Refreshing login session')
            self._login(*self._credentials)
        StorytelBaseIE._asset_requests += 1
        try:
            urlh = self._request_webpage(
                f'{self._API_BASE}/assets/v2/consumables/{book_id}/abook', book_id,
                'Resolving audio URL', headers=self._auth_headers())
        except ExtractorError as e:
            if isinstance(e.cause, HTTPError) and e.cause.status in (401, 403):
                raise ExtractorError(
                    'Storytel refused the audio request; the login has expired or '
                    'this book is not included in your subscription', expected=True)
            raise
        # The endpoint redirects to the actual MP3; don't read the body here
        urlh.close()
        return urlh.url

    def _extract_chapters(self, book_id, country, book_title, duration):
        playback = self._call_api(
            f'playback-metadata/consumable/{book_id}', book_id, country,
            'Downloading chapter metadata', fatal=False)
        abook = traverse_obj(playback, ('formats', lambda _, v: v['type'] == 'abook', any))
        # Surface everything but the chapter list, in case alternate streams/bitrates show up
        self.write_debug(f'Playback metadata: {traverse_obj(abook, {lambda x: {k: v for k, v in x.items() if k != "chapters"}})}')
        chapters = []
        start_ms = 0
        for chapter in traverse_obj(abook, ('chapters', lambda _, v: v['durationInMilliseconds'])):
            title = chapter.get('title')
            if title and len(title) > len(book_title) and title.startswith(book_title):
                title = title[len(book_title):].strip(' -')
            end_ms = start_ms + chapter['durationInMilliseconds']
            chapters.append({
                'title': title or f'Chapter {chapter.get("number") or len(chapters) + 1}',
                'start_time': start_ms / 1000,
                'end_time': end_ms / 1000,
            })
            start_ms = end_ms
        if chapters and duration:
            chapters[-1]['end_time'] = max(chapters[-1]['end_time'], duration)
        return chapters

    def _real_extract(self, url):
        country, book_id = self._match_valid_url(url).group('country', 'id')
        try:
            details = self._call_api(
                f'book-details/consumables/{book_id}', book_id, country,
                query={'kidsMode': 'false', 'configVariant': 'default'})
        except ExtractorError as e:
            if self._jwt and isinstance(e.cause, HTTPError) and e.cause.status == 404:
                raise ExtractorError(
                    'Book not found; it may not be available in your account\'s Storytel store', expected=True)
            raise

        abook = traverse_obj(details, ('formats', lambda _, v: v['type'] == 'abook', any))
        if not abook:
            raise ExtractorError('This book has no audiobook version', expected=True)
        if abook.get('isReleased') is False:
            raise ExtractorError('This audiobook has not been released yet', expected=True)
        if not self._jwt:
            self.raise_login_required('A Storytel subscription is required', method='password')

        title = details['title']
        authors = traverse_obj(details, ('authors', ..., 'name', {str}))
        duration = traverse_obj(details, ('duration', {
            lambda x: x['hours'] * 3600 + x['minutes'] * 60 + x['seconds']}))

        audio_url = self._get_audio_url(book_id)

        return {
            'id': book_id,
            'title': title,
            'alt_title': traverse_obj(details, ('originalTitle', {str}, filter)),
            'description': details.get('description'),
            'thumbnail': traverse_obj(details, ('cover', 'url', {url_or_none})),
            'artists': authors,
            'creators': authors,
            'cast': traverse_obj(details, ('narrators', ..., 'name', {str})),
            'genres': traverse_obj(details, ('category', 'name', {str}, all, filter)),
            'language': details.get('language'),
            'duration': duration,
            'average_rating': traverse_obj(details, ('ratings', 'averageRating', {float_or_none})),
            'series': traverse_obj(details, ('seriesInfo', 'name', {str})),
            'series_number': traverse_obj(details, ('seriesInfo', 'orderInSeries', {int_or_none})),
            'release_timestamp': parse_iso8601(abook.get('releaseDate')),
            'webpage_url': f'https://www.storytel.com/{country}/books/{book_id}',
            'chapters': self._extract_chapters(book_id, country, title, duration) or None,
            'formats': [{
                'format_id': 'abook',
                'url': audio_url,
                'ext': 'mp3',
                'vcodec': 'none',
                'acodec': 'mp3',
                # The CDN path names the encoder bitrate, e.g. /mp3encoder-64/
                'abr': int_or_none(self._search_regex(
                    r'/mp3encoder-(\d+)/', audio_url, 'bitrate', default=None)),
                # The signed CDN URL needs no Authorization; don't leak the token to it
                'http_headers': {'User-Agent': self._USER_AGENT},
            }],
        }


class StorytelListIE(StorytelBaseIE):
    IE_NAME = 'storytel:list'
    _VALID_URL = r'https?://(?:www\.)?(?:storytel|mofibo)\.com/(?P<country>[a-z]{2})(?:/[a-z]{2})?/(?P<type>series|authors|narrators)/(?:[^/?#]+-)?(?P<id>\d+)'
    _TESTS = [{
        'url': 'https://www.storytel.com/se/authors/gregory-david-roberts-10580',
        'info_dict': {
            'id': 'authors-10580',
            'title': 'Gregory David Roberts',
        },
        'playlist_mincount': 20,
    }, {
        'url': 'https://www.storytel.com/se/sv/narrators/stefano-fresi-511120',
        'only_matching': True,
    }]

    def _entries(self, list_type, list_id, country, first_page):
        page = first_page
        for page_num in itertools.count(2):
            for item in traverse_obj(page, ('items', lambda _, v: v['id'])):
                if not traverse_obj(item, ('formats', lambda _, v: v['type'] == 'abook' and v['isReleased'], any)):
                    continue
                yield self.url_result(
                    f'https://www.storytel.com/{country}/books/{item["id"]}', StorytelIE,
                    item['id'], item.get('title'))
            next_token = page.get('nextPageToken')
            if not next_token:
                break
            page = self._fetch_page(list_type, list_id, country, page_num, next_token)

    def _fetch_page(self, list_type, list_id, country, page_num, next_token=None):
        return self._call_api(
            f'explore/lists/{list_type}/{list_id}', list_id, country,
            f'Downloading page {page_num}', query={
                'includeListDetails': 'true',
                'includeFormats': 'abook',
                'kidsMode': 'false',
                **({'nextPageToken': next_token} if next_token else {}),
            })

    def _real_extract(self, url):
        country, list_type, list_id = self._match_valid_url(url).group('country', 'type', 'id')
        first_page = self._fetch_page(list_type, list_id, country, 1)
        return self.playlist_result(
            self._entries(list_type, list_id, country, first_page),
            f'{list_type}-{list_id}', first_page.get('title'))
