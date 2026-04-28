# -*- coding: utf-8 -*-

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.

"""Extractor for Bunny Stream video embeds (bunny.net video service)

Bunny Stream is a video-hosting service by bunny.net.  Sites that embed
videos from it use one of two URL shapes:

* ``https://iframe.mediadelivery.net/embed/<library>/<video-uuid>`` —
  the iframe ``src`` users see in the embedding page.  The HTML body of
  this URL exposes the actual video file in ``<meta property="og:video">``
  tags (an MP4 URL on bunny's CDN).
* ``https://iframe.mediadelivery.net/play/<library>/<video-uuid>`` —
  same content under a different path.

This extractor handles both forms and resolves them to the underlying
MP4 hosted on ``vz-<hash>.b-cdn.net``.  Other extractors can chain to it
via ``Message.Queue`` — gallery-dl auto-dispatches by URL pattern, so
the parent doesn't need to know who's downloading.

Each video also gets a ``<filename>.json`` sidecar (default on) recording
``embed_url``, ``mp4_url``, ``thumbnail`` and the resolution timestamp.
Other extractors that yield a Bunny Stream URL via ``Message.Queue``
can correlate their own metadata to the downloaded video by matching on
``embed_url``.  Toggle with ``extractor.bunnystream.metadata``.
"""

import json
import datetime

from .common import Extractor, Message
from .. import text


class BunnystreamVideoExtractor(Extractor):
    """Extractor for a single Bunny Stream embed"""
    category = "bunnystream"
    subcategory = "video"
    root = "https://iframe.mediadelivery.net"
    directory_fmt = ("{category}", "{library}")
    filename_fmt = "{filename}.{extension}"
    archive_fmt = "{library}_{video_id}"
    pattern = (r"(?:https?://)?iframe\.mediadelivery\.net"
               r"/(?:embed|play)/(\d+)/([0-9a-f-]+)")
    example = ("https://iframe.mediadelivery.net/embed/"
               "123456/00000000-0000-0000-0000-000000000000")

    def _init(self):
        self._write_metadata = self.config("metadata", True)

    def items(self):
        library, video_id = self.groups
        url = f"{self.root}/embed/{library}/{video_id}"
        page = self.request(url, notfound="video").text

        og = self._og(page)
        mp4_url = og.get("video:secure_url") or og.get("video:url")
        if not mp4_url:
            raise self.exc.AbortExtraction(
                f"no og:video URL on {url}")

        # og:title typically holds the original filename ("Foo - Bar (3).mp4")
        og_title = og.get("title") or ""
        og_filename = (og_title.rsplit(".", 1)[0]
                       if "." in og_title else og_title)
        stem = og_filename or video_id
        extension = mp4_url.rpartition(".")[2].lower() or "mp4"

        data = {
            "library"   : text.parse_int(library),
            "video_id"  : video_id,
            "embed_url" : url,
            "mp4_url"   : mp4_url,
            "thumbnail" : og.get("image:secure_url") or og.get("image") or "",
            "og_title"  : og_title,
            "fetched_at": datetime.datetime.now(
                datetime.timezone.utc).isoformat(),
        }

        # Snapshot the user-facing fields before yielding Directory,
        # since gallery-dl injects internal keys (_path, _extr, …) into
        # the kwdict it carries forward.
        snapshot = dict(data)

        yield Message.Directory, "", data

        if self._write_metadata:
            payload = json.dumps(
                snapshot, ensure_ascii=False, indent=2, default=str)
            json_kwdict = dict(data)
            json_kwdict["filename"] = stem
            json_kwdict["extension"] = "json"
            yield Message.Url, "text:" + payload, json_kwdict

        info = dict(data)
        info["filename"] = stem
        info["extension"] = extension
        yield Message.Url, mp4_url, info

    @staticmethod
    def _og(html):
        """Collect every <meta property="og:..."> from a page"""
        out = {}
        for tag in text.extract_iter(html, "<meta", ">"):
            prop = text.extr(tag, 'property="og:', '"')
            if not prop:
                continue
            content = text.extr(tag, 'content="', '"')
            if content:
                out[prop] = text.unescape(content)
        return out
