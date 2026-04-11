# -*- coding: utf-8 -*-

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.

"""Extractors for https://www.facebook.com/"""

from .common import Extractor, Message, Dispatch
from .. import text, util
import binascii
import json

BASE_PATTERN = r"(?:https?://)?(?:[\w-]+\.)?facebook\.com"
USER_PATTERN = (BASE_PATTERN +
                r"/(?!media/|photo/|photo.php|watch/|permalink.php)"
                r"(?:profile\.php\?id=|people/[^/?#]+/)?([^/?&#]+)")


class FacebookExtractor(Extractor):
    """Base class for Facebook extractors"""
    category = "facebook"
    root = "https://www.facebook.com"
    directory_fmt = ("{category}", "{username}", "{title}{set_id:? (/)/}")
    filename_fmt = "{id}.{extension}"
    archive_fmt = "{id}.{extension}"

    def _init(self):
        headers = self.session.headers
        headers["Accept"] = (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8"
        )
        headers["Sec-Fetch-Dest"] = "empty"
        headers["Sec-Fetch-Mode"] = "navigate"
        headers["Sec-Fetch-Site"] = "same-origin"

        self.fallback_retries = self.config("fallback-retries", 2)
        self.videos = self.config("videos", True)
        self.author_followups = self.config("author-followups", False)
        self._detect_jump = True

    def _safe_decode(self, txt):
        """Decode FB's unicode-escaped text with error tolerance."""
        try:
            decoded = txt.encode().decode("unicode_escape") \
                .encode("utf_16", "surrogatepass").decode("utf_16")
        except (UnicodeDecodeError, UnicodeEncodeError):
            decoded = txt
        return text.unescape(decoded).replace("\\/", "/")

    # backward-compat alias
    decode_all = _safe_decode

    def _decode_metadata(self, data):
        """Decode unicode-escaped strings in a metadata dict.

        Called once before yielding so only values that actually
        reach output get decoded, avoiding redundant work during
        the parse / search phase.
        """
        for key in ("username", "title", "caption", "body",
                     "user_pfbid", "biography"):
            val = data.get(key)
            if isinstance(val, str) and val:
                data[key] = self._safe_decode(val)

        for reply in data.get("replies", ()):
            for rkey in ("text", "author"):
                val = reply.get(rkey)
                if isinstance(val, str) and val:
                    reply[rkey] = self._safe_decode(val)

        return data

    def _decode_url(self, url_text):
        """Minimal decode for URL fields — unescape slashes and entities."""
        return url_text.replace("\\/", "/").replace("&amp;", "&")

    def _decode_creation_story_id(self, encoded):
        """Decode the base64 'creation_story.id' value.

        The encoded string decodes to 'S:_I<post_user_id>:<post_id>:<post_id>'
        where both ids are stable numeric identifiers (independent of any
        per-session 'pfbid' token). Returns (post_user_id, post_id) as
        strings, or (None, None) on any failure.
        """
        try:
            decoded = binascii.a2b_base64(encoded + "==").decode()
        except Exception:
            return None, None
        parts = decoded.split(":")
        if len(parts) >= 3 and parts[1].startswith("_I"):
            return parts[1][2:], parts[2]
        return None, None

    def _extract_post_identity(self, page):
        """Extract the canonical (post_user_id, post_id) of a post page.

        A Facebook post page may embed multiple posts (sidebar, recommended,
        cross-post sources). The first 'creation_story' block in the HTML
        belongs to the page's own post. Decoding its 'id' field yields
        clean numeric identifiers regardless of the URL form (vanity vs
        numeric, pfbid vs numeric).

        Returns (post_user_id, post_id) on success or (None, None) if no
        creation_story block can be parsed.
        """
        block = text.extr(page, '"creation_story":{', '}')
        if not block:
            return None, None
        token = text.extr(block, '"id":"', '"')
        if not token:
            return None, None
        return self._decode_creation_story_id(token)

    def _find_own_mediaset_token(self, page, post_id):
        """Return 'pcb.<post_id>' if the page contains a mediaset_token
        whose numeric part matches 'post_id', otherwise None.

        For multi-photo posts, FB embeds the post's own set as
        '"mediaset_token":"pcb.<numeric_post_id>"'. Other embedded posts
        in the same HTML use their own (different) numeric ids, so an
        exact match against 'post_id' unambiguously identifies the
        post's own set without any byte-distance heuristic.
        """
        if not post_id:
            return None
        if page.find('"mediaset_token":"pcb.' + post_id + '"') >= 0:
            return "pcb." + post_id
        return None

    def _extract_owner_username(self, page, post_user_id):
        """Find the display name of the post owner (raw, not decoded)."""
        if not post_user_id:
            return ""
        needle = '"id":"' + post_user_id + '"'
        for actor in text.extract_iter(page, '"actors":[{', '}'):
            if needle in actor:
                return text.extr(actor, '"name":"', '"') or ""
        return ""

    def _extract_owner_hint(self, url):
        """Extract an owner identifier from a URL.

        Returns a string (numeric user_id or vanity) that can be
        searched for in the page HTML to locate the owner's post
        block. Returns '' if no hint can be derived.
        """
        # /<vanity_or_numeric>/posts/<token>
        if "/posts/" in url:
            return url.split("/posts/")[0].rsplit("/", 1)[-1]
        # ?id=<numeric> or &id=<numeric> (may be last query param)
        query = url.partition("?")[2]
        if query:
            params = text.parse_query(query)
            val = params.get("id", "")
            if val.isdigit():
                return val
        return ""

    def _find_owner_context(self, page, owner_hint):
        """Search the page HTML near owner_hint occurrences for post data.

        Scans all positions where owner_hint appears and checks a
        ±4 KB window around each for:
          mediaset_token, post_id, body (message text),
          owner_uid (numeric), owner_name (display name).

        Returns a dict of found fields (first find wins per field)
        plus '_owner_pos' — the byte offset where post_id was found,
        used to scope reply extraction.
        """
        if not owner_hint:
            return {}

        RADIUS = 4096
        result = {}
        pos = -1

        while True:
            pos = page.find(owner_hint, pos + 1)
            if pos < 0:
                break

            start = max(0, pos - RADIUS)
            chunk = page[start:min(len(page), pos + RADIUS)]

            # mediaset_token
            if "mediaset_token" not in result:
                needle = '"mediaset_token":"'
                idx = chunk.find(needle)
                if idx >= 0:
                    vs = idx + len(needle)
                    ve = chunk.find('"', vs)
                    if ve > vs:
                        result["mediaset_token"] = chunk[vs:ve]

            # post_id
            if "post_id" not in result:
                needle = '"post_id":"'
                idx = chunk.find(needle)
                if idx >= 0:
                    vs = idx + len(needle)
                    ve = chunk.find('"', vs)
                    if ve > vs and chunk[vs:ve].isdigit():
                        result["post_id"] = chunk[vs:ve]
                        result["_owner_pos"] = pos

            # body (message text — NOT "body":{"text"} which is comments)
            if "body" not in result:
                mi = chunk.find('"message":{')
                if mi >= 0:
                    ti = chunk.find('"text":"', mi)
                    if 0 <= ti - mi < 2000:
                        te = chunk.find('"', ti + 8)
                        if te > ti + 8:
                            result["body"] = chunk[ti + 8:te]

            # owner_uid: resolve vanity → numeric user_id
            if "owner_uid" not in result:
                if owner_hint.isdigit():
                    # Numeric hint IS the user_id
                    result["owner_uid"] = owner_hint
                else:
                    # Vanity resolution via '/<vanity>","id":"<uid>"'
                    vn = 'facebook.com\\/' + owner_hint + '","id":"'
                    vi = chunk.find(vn)
                    if vi >= 0:
                        vs = vi + len(vn)
                        ve = chunk.find('"', vs)
                        if ve > vs and chunk[vs:ve].isdigit():
                            result["owner_uid"] = chunk[vs:ve]

            # owner_name: find "name":"<val>" near resolved uid
            if "owner_uid" in result and "owner_name" not in result:
                uid_needle = '"id":"' + result["owner_uid"] + '"'
                ui = chunk.find(uid_needle)
                if ui >= 0:
                    nearby = chunk[max(0, ui - 200):ui + 200]
                    ni = nearby.find('"name":"')
                    if ni >= 0:
                        ns = ni + 8
                        ne = nearby.find('"', ns)
                        if ne > ns:
                            result["owner_name"] = nearby[ns:ne]

            if all(k in result for k in (
                "mediaset_token", "post_id", "body",
                "owner_uid", "owner_name",
            )):
                break

        return result

    def _extract_replies(self, page, owner_pos):
        """Extract pre-loaded replies from already-fetched HTML.

        Searches for comment body nodes ('"body":{"text":"..."}')
        within ~500 KB after the owner's post block position.
        This range covers the own post's pre-rendered comments
        but excludes sidebar / recommended content that appears
        much later in the HTML.

        Does NOT make additional HTTP requests.
        """
        if owner_pos < 0:
            return []

        section = page[owner_pos:owner_pos + 524288]
        replies = []
        needle = '"body":{"text":"'
        pos = -1

        while True:
            pos = section.find(needle, pos + 1)
            if pos < 0:
                break

            ts = pos + len(needle)
            te = section.find('"', ts)
            if te <= ts:
                continue
            body_text = section[ts:te]

            # author — search FORWARD for the "author" block that
            # FB places AFTER the body in its relay-style HTML
            fwd = section[te:te + 5000]
            author = ""
            author_id = ""
            author_needle = '"author":{"__typename":"User","id":"'
            ai = fwd.find(author_needle)
            if ai >= 0:
                id_start = ai + len(author_needle)
                id_end = fwd.find('"', id_start)
                if id_end > id_start:
                    author_id = fwd[id_start:id_end]
                ni = fwd.find('"name":"', id_end)
                if ni >= 0 and ni - ai < 200:
                    ns = ni + 8
                    ne = fwd.find('"', ns)
                    if ne > ns:
                        author = fwd[ns:ne]

            replies.append({
                "author": author,
                "author_id": author_id,
                "text": body_text,
            })

        return replies

    def _extract_url_pfbid(self):
        """Extract a pfbid token from the request URL, if present."""
        pos = self.url.find("pfbid")
        if pos < 0:
            return ""
        end = pos + 5
        url = self.url
        while end < len(url) and url[end].isalnum():
            end += 1
        return url[pos:end]

    def parse_set_page(self, set_page):
        directory = {
            "set_id": text.extr(
                set_page, '"mediaSetToken":"', '"'
            ) or text.extr(
                set_page, '"mediasetToken":"', '"'
            ),
            "username": (
                text.extr(
                    set_page, '"user":{"__isProfile":"User","name":"', '","'
                ) or text.extr(
                    set_page, '"actors":[{"__typename":"User","name":"', '","'
                ) or ""
            ),
            "user_id": text.extr(
                set_page, '"owner":{"__typename":"User","id":"', '"'
            ),
            "user_pfbid": "",
            "title": text.extr(
                set_page, '"title":{"text":"', '"'
            ) or "",
            "first_photo_id": text.extr(
                set_page,
                '{"__typename":"Photo","__isMedia":"Photo","',
                '","creation_story"'
            ).rsplit('"id":"', 1)[-1] or
            text.extr(
                set_page, '{"__typename":"Photo","id":"', '"'
            )
        }

        if directory["user_id"].startswith("pfbid"):
            directory["user_pfbid"] = directory["user_id"]
            directory["user_id"] = (
                text.extr(
                    set_page, '"actors":[{"__typename":"User","id":"', '"') or
                text.extr(
                    set_page, '"userID":"', '"') or
                directory["set_id"].split(".")[1])

        return directory

    def parse_photo_page(self, photo_page):
        photo = {
            "id": text.extr(
                photo_page, '"__isNode":"Photo","id":"', '"'
            ),
            "set_id": text.extr(
                photo_page,
                '"url":"https:\\/\\/www.facebook.com\\/photo\\/?fbid=',
                '"'
            ).rsplit("&set=", 1)[-1],
            "username": text.extr(
                photo_page, '"owner":{"__typename":"User","name":"', '"'
            ) or "",
            "user_id": text.extr(
                photo_page, '"owner":{"__typename":"User","id":"', '"'
            ),
            "user_pfbid": "",
            "caption": text.extr(
                photo_page,
                '"message":{"delight_ranges"',
                '"},"message_preferred_body"'
            ).rsplit('],"text":"', 1)[-1],
            "date": self.parse_timestamp(
                text.extr(photo_page, '\\"publish_time\\":', ',') or
                text.extr(photo_page, '"created_time":', ',')
            ),
            "url": self._decode_url(text.extr(
                photo_page, ',"image":{"uri":"', '","'
            )),
            "next_photo_id": text.extr(
                photo_page,
                '"nextMediaAfterNodeId":{"__typename":"Photo","id":"',
                '"'
            )
        }

        if photo["user_id"].startswith("pfbid"):
            photo["user_pfbid"] = photo["user_id"]
            photo["user_id"] = text.extr(
                photo_page, r'\"content_owner_id_new\":\"', r'\"')

        text.nameext_from_url(photo["url"], photo)

        photo["followups_ids"] = []
        for comment_raw in text.extract_iter(
            photo_page, '{"node":{"id"', '"cursor":null}'
        ):
            if ('"is_author_original_poster":true' in comment_raw and
                    '{"__typename":"Photo","id":"' in comment_raw):
                photo["followups_ids"].append(text.extr(
                    comment_raw,
                    '{"__typename":"Photo","id":"',
                    '"'
                ))

        return photo

    def parse_post_page(self, post_page):
        first_photo_url = text.extr(
            text.extr(
                post_page, '"__isMedia":"Photo"', '"target_group"'
            ), '"url":"', ','
        )

        if post_page.count('"__isMedia":"Photo"') > 2:
            post = {
                "set_id": text.extr(post_page, '{"mediaset_token":"', '"') or
                text.extr(first_photo_url, 'set=', '"').rsplit("&", 1)[0]
            }
        else:
            post = {"set_id": None}

        post["post_photo"] = first_photo_url
        return post

    def parse_video_page(self, video_page):
        video = {
            "id": text.extr(
                video_page, '\\"video_id\\":\\"', '\\"'
            ),
            "username": text.extr(
                video_page, '"actors":[{"__typename":"User","name":"', '","'
            ) or "",
            "user_id": text.extr(
                video_page, '"owner":{"__typename":"User","id":"', '"'
            ),
            "date": self.parse_timestamp(text.extr(
                video_page, '\\"publish_time\\":', ','
            )),
            "type": "video"
        }

        if not video["username"]:
            video["username"] = text.extr(
                video_page,
                '"__typename":"User","id":"' + video["user_id"] + '","name":"',
                '","'
            ) or ""

        first_video_raw = text.extr(
            video_page, '"permalink_url"', '\\/Period>\\u003C\\/MPD>'
        )

        audio = {
            **video,
            "url": self._decode_url(text.extr(
                text.extr(
                    first_video_raw,
                    "AudioChannelConfiguration",
                    "BaseURL>\\u003C"
                ),
                "BaseURL>", "\\u003C\\/"
            )),
            "type": "audio"
        }

        video["urls"] = {}

        for raw_url in text.extract_iter(
            first_video_raw, 'FBQualityLabel=\\"', '\\u003C\\/BaseURL>'
        ):
            resolution = raw_url.split('\\"', 1)[0]
            video["urls"][resolution] = self._decode_url(
                raw_url.split('BaseURL>', 1)[1]
            )

        if not video["urls"]:
            return video, audio

        video["url"] = max(
            video["urls"].items(),
            key=lambda x: text.parse_int(x[0][:-1])
        )[1]

        text.nameext_from_url(video["url"], video)
        audio["filename"] = video["filename"]
        audio["extension"] = "m4a"

        return video, audio

    def photo_page_request_wrapper(self, url, **kwargs):
        LEFT_OFF_TXT = "" if url.endswith("&set=") else (
            "\nYou can use this URL to continue from "
            "where you left off (added \"&setextract\"): "
            "\n" + url + "&setextract"
        )

        res = self.request(url, **kwargs)

        if res.url.startswith(self.root + "/login"):
            raise self.exc.AuthRequired(
                message=("You must be logged in to continue viewing images." +
                         LEFT_OFF_TXT))

        if b'{"__dr":"CometErrorRoot.react"}' in res.content:
            raise self.exc.AbortExtraction(
                "You've been temporarily blocked from viewing images.\n"
                "Please try using a different account, "
                "using a VPN or waiting before you retry." + LEFT_OFF_TXT)

        return res

    def extract_set(self, set_data):
        set_id = set_data["set_id"]
        all_photo_ids = [set_data["first_photo_id"]]

        retries = 0
        i = 0

        while i < len(all_photo_ids):
            photo_id = all_photo_ids[i]
            photo_url = f"{self.root}/photo/?fbid={photo_id}&set={set_id}"
            photo_page = self.photo_page_request_wrapper(photo_url).text

            photo = self.parse_photo_page(photo_page)
            photo["num"] = i + 1

            if self.author_followups:
                for followup_id in photo["followups_ids"]:
                    if followup_id not in all_photo_ids:
                        self.log.debug(
                            "Found a followup in comments: %s", followup_id
                        )
                        all_photo_ids.append(followup_id)

            if not photo["url"]:
                if retries < self.fallback_retries and self._interval_429:
                    seconds = self._interval_429()
                    self.log.warning(
                        "Failed to find photo download URL for %s. "
                        "Retrying in %s seconds.", photo_url, seconds,
                    )
                    self.wait(seconds=seconds, reason="429 Too Many Requests")
                    retries += 1
                    continue
                else:
                    self.log.error(
                        "Failed to find photo download URL for " + photo_url +
                        ". Skipping."
                    )
                    retries = 0
            else:
                retries = 0
                photo.update(set_data)
                self._decode_metadata(photo)
                yield Message.Directory, "", photo
                yield Message.Url, photo["url"], photo

            if not photo["next_photo_id"]:
                self.log.debug(
                    "Can't find next image in the set. "
                    "Extraction is over."
                )
            elif photo["next_photo_id"] in all_photo_ids:
                if photo["next_photo_id"] != photo["id"]:
                    self.log.debug(
                        "Detected a loop in the set, it's likely finished. "
                        "Extraction is over."
                    )
            elif self._detect_jump and \
                    int(photo["next_photo_id"]) > int(photo["id"]) + i*120:
                self.log.info(
                    "Detected jump to the beginning of the set. (%s -> %s)",
                    photo["id"], photo["next_photo_id"])
                if self.config("loop", False):
                    all_photo_ids.append(photo["next_photo_id"])
            else:
                all_photo_ids.append(photo["next_photo_id"])

            i += 1

    def _extract_profile(self, profile, set_id=False):
        if set_id:
            url = f"{self.root}/{profile}/photos_by"
        else:
            url = f"{self.root}/{profile}"
        return self._extract_profile_page(url)

    def _extract_profile_page(self, url):
        for _ in range(self.fallback_retries + 1):
            page = self.request(url).text

            if page.find('>Page Not Found</title>', 0, 3000) > 0:
                break
            if ('"props":{"title":"This content isn\'t available right now"' in
                    page):
                raise self.exc.AuthRequired(
                    "authenticated cookies", "profile",
                    "This content isn't available right now")

            set_id = self._extract_profile_set_id(page)
            user = self._extract_profile_user(page)
            if set_id or user:
                user["set_id"] = set_id
                return user

            self.log.debug("Got empty profile photos page, retrying...")
        return {}

    def _extract_profile_set_id(self, profile_photos_page):
        set_ids_raw = text.extr(
            profile_photos_page, '"pageItems"', '"page_info"'
        )

        set_id = text.extr(
            set_ids_raw, 'set=', '"'
        ).rsplit("&", 1)[0] or text.extr(
            set_ids_raw, '\\/photos\\/', '\\/'
        )

        return set_id

    def _extract_profile_user(self, page):
        data = text.extr(page, '","user":{"', '},"viewer":{')

        user = None
        try:
            user = util.json_loads(f'{{"{data}}}')
            if user["id"].startswith("pfbid"):
                user["user_pfbid"] = user["id"]
                user["id"] = text.extr(page, '"userID":"', '"')
            user["username"] = (text.extr(page, '"userVanity":"', '"') or
                                text.extr(page, '"vanity":"', '"'))
            user["profile_tabs"] = [
                edge["node"]
                for edge in (user["profile_tabs"]["profile_user"]
                             ["timeline_nav_app_sections"]["edges"])
            ]

            if bio := text.extr(page, '"best_description":{"text":"', '"'):
                user["biography"] = bio
            elif (pos := page.find(
                    '"__module_operation_ProfileCometTileView_profileT')) >= 0:
                user["biography"] = text.rextr(
                    page, '"text":"', '"', pos) or ""
            else:
                user["biography"] = text.unescape(text.remove_html(text.extr(
                    page, "</span></span></h2>", "<ul>")))
        except Exception:
            if user is None:
                self.log.debug("Failed to extract user data: %s", data)
                user = {}
        return user


class FacebookPostExtractor(FacebookExtractor):
    """Extractor for Facebook Post pages"""
    subcategory = "post"
    directory_fmt = ("{category}", "{username} ({user_id})", "{post_id}")
    filename_fmt = "{id}.{extension}"
    archive_fmt = "{post_id}_{id}.{extension}"
    pattern = BASE_PATTERN + r"/[^/?#]+/posts/([^/?#]+)"
    example = "https://www.facebook.com/USERNAME/posts/POST_ID"

    def items(self):
        token = self.groups[0]
        post_page = self.request(self.url).text

        # Owner-anchored extraction (primary)
        owner_hint = self._extract_owner_hint(self.url)
        ctx = self._find_owner_context(post_page, owner_hint) \
            if owner_hint else {}

        # Creation story decode (supplementary / fallback)
        cs_uid, cs_pid = self._extract_post_identity(post_page)

        # Merge: owner context preferred, creation_story as fallback
        owner_id = ctx.get("owner_uid") or cs_uid or \
            (owner_hint if owner_hint.isdigit() else "")
        post_id = ctx.get("post_id") or cs_pid
        username = ctx.get("owner_name") or \
            self._extract_owner_username(post_page, owner_id)
        body = ctx.get("body", "")
        own_token = ctx.get("mediaset_token")
        post_pfbid = self._extract_url_pfbid()

        # Fallback: if URL token is a pure numeric post_id
        if not post_id and token.isdigit():
            post_id = token
        # Derive post_id from mediaset_token (pcb.<post_id>)
        if not post_id and own_token and own_token.startswith("pcb."):
            post_id = own_token[4:]

        if not post_id:
            self.log.warning(
                "Could not extract post identity from '%s'", self.url)
            return

        # Extract replies from already-fetched HTML
        replies = self._extract_replies(
            post_page, ctx.get("_owner_pos", -1))

        # Directory metadata
        directory = {
            "post_id"    : post_id,
            "user_id"    : owner_id,
            "username"   : username,
            "body"       : body,
            "post_pfbid" : post_pfbid,
        }

        # For single-photo posts, pre-fetch the photo and its parent
        # album so that set_id / title are available in directory
        # metadata BEFORE the first yield (keeps JSON + photo in
        # the same directory when users customise directory_fmt).
        photo_data = None
        if not own_token:
            photo_data = self._resolve_single_photo(post_page)
            if photo_data:
                directory["set_id"] = photo_data.get("set_id", "")
                directory["title"] = photo_data.get("title", "")

        self._decode_metadata(directory)

        # Decode replies for JSON output
        for reply in replies:
            for rkey in ("text", "author"):
                val = reply.get(rkey)
                if isinstance(val, str) and val:
                    reply[rkey] = self._safe_decode(val)

        yield Message.Directory, "", directory

        # Dump post content as JSON file
        content = json.dumps({
            "post_id"    : post_id,
            "user_id"    : owner_id,
            "username"   : directory["username"],
            "post_pfbid" : post_pfbid,
            "body"       : directory["body"],
            "replies"    : replies,
        }, ensure_ascii=False, indent=2)
        yield Message.Url, "text:" + content, {
            **directory,
            "filename" : post_id,
            "extension": "json",
            "id"       : post_id,
        }

        # Yield photo(s)
        if own_token:
            yield from self._post_photos_multi(
                post_page, directory, own_token)
        elif photo_data:
            photo_data.update(directory)
            self._decode_metadata(photo_data)
            yield Message.Url, photo_data["url"], photo_data

    def _post_photos_multi(self, post_page, directory, set_token):
        """Walk a multi-photo post's set with clean post metadata."""
        set_url = f"{self.root}/media/set/?set={set_token}"
        set_page = self.request(set_url).text
        set_data = self.parse_set_page(set_page)

        # Override owner identity with clean values from post page
        set_data["user_id"] = directory["user_id"]
        set_data["username"] = directory["username"]
        set_data["post_id"] = directory["post_id"]
        set_data["post_pfbid"] = directory.get("post_pfbid", "")
        set_data["body"] = directory.get("body", "")

        self._detect_jump = False
        yield from self.extract_set(set_data)

    def _resolve_single_photo(self, post_page):
        """Fetch the single photo and its parent album metadata.

        Returns a photo dict with set_id, title, url, id, etc.
        populated, or None if extraction fails.  Does NOT yield —
        the caller handles Directory/Url emission so that JSON
        content and the photo share the same directory context.
        """
        media_block = text.extr(
            post_page, '"__isMedia":"Photo"', '"target_group"')
        first_photo_url = (
            text.extr(media_block, '"url":"', ',')
            if media_block else "")
        if not first_photo_url:
            return None

        params = text.parse_query(first_photo_url.partition("?")[2])
        photo_fbid = params.get("fbid")
        if not photo_fbid:
            return None

        photo_url = f"{self.root}/photo/?fbid={photo_fbid}&set="
        photo_page = self.photo_page_request_wrapper(photo_url).text
        photo = self.parse_photo_page(photo_page)
        photo["num"] = 1
        photo["id"] = photo.get("id") or photo_fbid

        # Populate album metadata (set_id + title) from the photo's
        # parent set — same as PhotoExtractor does.  This makes
        # {title} and {set_id} available for users who customise
        # directory_fmt to use SetExtractor-style paths.
        if photo.get("set_id"):
            set_url = f"{self.root}/media/set/?set={photo['set_id']}"
            set_page = self.request(set_url).text
            album = self.parse_set_page(set_page)
            photo["title"] = album.get("title", "")

        return photo


class FacebookPermalinkExtractor(FacebookExtractor):
    """Resolver for Facebook permalink and event post URLs.

    Fetches the URL, identifies the content type from the server
    response, and dispatches to the appropriate extractor
    (PostExtractor for posts, SetExtractor as fallback).
    """
    subcategory = "permalink"
    pattern = (
        BASE_PATTERN +
        r"/(?:(?:groups/)?(?:[^/?#]+/)?permalink(?:\.php)?"
        r"(?:/(\d+)|\?\w+=([^/?#]+))"
        r"|events/[^/?#]+/\??post_id=(\d+))"
    )
    example = ("https://www.facebook.com/"
               "permalink.php?story_fbid=STORY_ID&id=USER_ID")

    def items(self):
        page = self.request(self.url).text

        # Try to resolve to a post via owner-anchored extraction
        owner_hint = self._extract_owner_hint(self.url)
        ctx = self._find_owner_context(page, owner_hint) \
            if owner_hint else {}
        cs_uid, cs_pid = self._extract_post_identity(page)

        owner_id = ctx.get("owner_uid") or cs_uid or \
            (owner_hint if owner_hint.isdigit() else "")
        post_id = ctx.get("post_id") or cs_pid

        if post_id and post_id.isdigit() and owner_id and owner_id.isdigit():
            # Resolved as a post → dispatch to PostExtractor
            canonical = f"{self.root}/{owner_id}/posts/{post_id}"
            yield Message.Queue, canonical, {
                "_extractor": FacebookPostExtractor,
            }
            return

        # Fallback: construct a set URL from the captured token
        pcb1, pcb2, pcb3 = self.groups
        raw = pcb1 or pcb2 or pcb3
        if raw:
            token = raw.partition("&")[0]  # strip &id=... suffix
            set_url = f"{self.root}/media/set/?set=pcb.{token}"
            yield Message.Queue, set_url, {
                "_extractor": FacebookSetExtractor,
            }


class FacebookPhotoExtractor(FacebookExtractor):
    """Base class for Facebook Photo extractors"""
    subcategory = "photo"
    pattern = (BASE_PATTERN +
               r"/(?:[^/?#]+/photos/[^/?#]+/|photo(?:.php)?/?\?"
               r"(?:[^&#]+&)*fbid=)([^/?&#]+)[^/?#]*(?<!&setextract)$")
    example = "https://www.facebook.com/photo/?fbid=PHOTO_ID"

    def items(self):
        photo_id = self.groups[0]
        photo_url = f"{self.root}/photo/?fbid={photo_id}&set="
        photo_page = self.photo_page_request_wrapper(photo_url).text

        i = 1
        photo = self.parse_photo_page(photo_page)
        photo["num"] = i

        set_url = f"{self.root}/media/set/?set={photo['set_id']}"
        set_page = self.request(set_url).text

        directory = self.parse_set_page(set_page)

        for key in ("set_id", "title", "user_id", "user_pfbid", "username"):
            if not directory.get(key):
                directory[key] = photo.get(key)
            elif not photo.get(key):
                photo[key] = directory.get(key)

        self._decode_metadata(directory)
        self._decode_metadata(photo)
        yield Message.Directory, "", directory
        yield Message.Url, photo["url"], photo

        if self.author_followups:
            for comment_photo_id in photo["followups_ids"]:
                comment_photo = self.parse_photo_page(
                    self.photo_page_request_wrapper(
                        f"{self.root}/photo/?fbid={comment_photo_id}&set="
                    ).text
                )
                i += 1
                comment_photo["num"] = i
                yield Message.Url, comment_photo["url"], comment_photo


class FacebookSetExtractor(FacebookExtractor):
    """Extractor for direct Facebook media set URLs (albums, profile sets)"""
    subcategory = "set"
    pattern = (
        BASE_PATTERN +
        r"/(?:(?:media/set|photo)/?\?(?:[^&#]+&)*set=([^&#]+)"
        r"[^/?#]*(?<!&setextract)$"
        r"|photo/\?(?:[^&#]+&)*fbid=([^/?&#]+)&set=([^/?&#]+)&setextract)"
    )
    example = "https://www.facebook.com/media/set/?set=SET_ID"

    def items(self):
        set_id, first_pid, set_id2 = self.groups
        if not set_id:
            set_id = set_id2

        if set_id.startswith("pcb."):
            self._detect_jump = False

        set_url = f"{self.root}/media/set/?set={set_id}"
        set_page = self.request(set_url).text
        set_data = self.parse_set_page(set_page)
        if first_pid:
            set_data["first_photo_id"] = first_pid

        if set_id.startswith("pcb.") and set_id[4:].isdigit():
            set_data["post_id"] = set_id[4:]

        return self.extract_set(set_data)


class FacebookVideoExtractor(FacebookExtractor):
    """Base class for Facebook Video extractors"""
    subcategory = "video"
    directory_fmt = ("{category}", "{username}", "{subcategory}")
    pattern = BASE_PATTERN + r"/(?:[^/?#]+/videos/|watch/?\?v=)([^/?&#]+)"
    example = "https://www.facebook.com/watch/?v=VIDEO_ID"

    def items(self):
        video_id = self.groups[0]
        video_url = self.root + "/watch/?v=" + video_id
        video_page = self.request(video_url).text

        video, audio = self.parse_video_page(video_page)

        if "url" not in video:
            return

        self._decode_metadata(video)
        self._decode_metadata(audio)
        yield Message.Directory, "", video

        if self.videos == "ytdl":
            yield Message.Url, "ytdl:" + video_url, video
        elif self.videos:
            yield Message.Url, video["url"], video
            if audio["url"]:
                yield Message.Url, audio["url"], audio


class FacebookInfoExtractor(FacebookExtractor):
    """Extractor for Facebook Profile data"""
    subcategory = "info"
    directory_fmt = ("{category}", "{username}")
    pattern = USER_PATTERN + r"/info"
    example = "https://www.facebook.com/USERNAME/info"

    def items(self):
        user = self.cache(self._extract_profile, self.groups[0])
        self._decode_metadata(user)
        return iter(((Message.Directory, "", user),))


class FacebookAlbumsExtractor(FacebookExtractor):
    """Extractor for Facebook Profile albums"""
    subcategory = "albums"
    pattern = USER_PATTERN + r"/photos_albums(?:/([^/?#]+))?"
    example = "https://www.facebook.com/USERNAME/photos_albums"

    def items(self):
        profile, name = self.groups
        url = f"{self.root}/{profile}/photos_albums"
        page = self.request(url).text

        pos = page.find(
            '"TimelineAppCollectionAlbumsRenderer","collection":{"id":"')
        if pos < 0:
            return
        if name is not None:
            name = name.lower()

        items = text.extract(page, '},"pageItems":', '}}},', pos)[0]
        edges = util.json_loads(items + "}}")["edges"]

        # TODO: use /graphql API endpoint
        for edge in edges:
            node = edge["node"]
            album = node["node"]
            album["title"] = title = node["title"]["text"]
            if name is not None and name != title.lower():
                continue
            album["_extractor"] = FacebookSetExtractor
            album["thumbnail"] = (img := node["image"]) and img["uri"]
            yield Message.Queue, album["url"], album


class FacebookPhotosExtractor(FacebookExtractor):
    """Extractor for Facebook Profile Photos"""
    subcategory = "photos"
    pattern = USER_PATTERN + r"/photos(?:_by)?"
    example = "https://www.facebook.com/USERNAME/photos"

    def items(self):
        set_id = self.cache(
            self._extract_profile, self.groups[0], True)["set_id"]
        if not set_id:
            return iter(())

        set_url = f"{self.root}/media/set/?set={set_id}"
        set_page = self.request(set_url).text
        set_data = self.parse_set_page(set_page)
        return self.extract_set(set_data)


class FacebookAvatarExtractor(FacebookExtractor):
    """Extractor for Facebook Profile Avatars"""
    subcategory = "avatar"
    pattern = USER_PATTERN + r"/avatar"
    example = "https://www.facebook.com/USERNAME/avatar"

    def items(self):
        user = self.cache(self._extract_profile, self.groups[0])

        if avatar_page := user.get("profilePhoto"):
            avatar_page_url = avatar_page["url"]
            avatar_page = self.photo_page_request_wrapper(avatar_page_url).text

            avatar = self.parse_photo_page(avatar_page)
            avatar["count"] = avatar["num"] = 1
            avatar["type"] = "avatar"

            set_url = f"{self.root}/media/set/?set={avatar['set_id']}"
            set_page = self.request(set_url).text
            directory = self.parse_set_page(set_page)
        else:
            for key in ("profilePicLarge",
                        "profilePicMedium",
                        "profilePicSmall"):
                if url := user.get(key):
                    url = url["uri"]
                    break
            else:
                return

            directory = {
                "set_id"    : "",
                "username"  : user.get("username"),
                "user_id"   : user.get("id"),
                "user_pfbid": user.get("user_pfbid"),
                "title"     : "Profile pictures",
            }
            avatar = text.nameext_from_url(url, {
                **directory,
                "id"   : (a := user.get("user_avatar")) and a.get("id"),
                "url"  : url,
                "count": 1,
                "type" : "avatar",
            })

        self._decode_metadata(directory)
        yield Message.Directory, "", directory
        yield Message.Url, avatar["url"], avatar


class FacebookUserExtractor(Dispatch, FacebookExtractor):
    """Extractor for Facebook Profiles"""
    pattern = USER_PATTERN + r"/?(?:$|\?|#)"
    example = "https://www.facebook.com/USERNAME"

    def items(self):
        base = f"{self.root}/{self.groups[0]}/"
        return self._dispatch_extractors((
            (FacebookInfoExtractor  , base + "info"),
            (FacebookAvatarExtractor, base + "avatar"),
            (FacebookPhotosExtractor, base + "photos"),
            (FacebookAlbumsExtractor, base + "photos_albums"),
        ), ("photos",))
