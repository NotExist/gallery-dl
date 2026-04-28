# -*- coding: utf-8 -*-

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.

"""Generic extractor for WordPress sites

This module is site-agnostic and ships with **no** instances
pre-registered.  Reach a site via either:

* the generic ``wordpress:<site-url>/<path>`` URL prefix, or
* a per-site entry under ``extractor.wordpress.<category>`` in your
  gallery-dl config (see the snippet at the bottom of this file).

Two backends, tried in order:

1. **REST API** at ``/wp-json/wp/v2/`` — the primary path.  Used for
   post lookup, taxonomy resolution, and listing pagination.
2. **Syndication feed + post-page HTML** — the fallback when REST is
   disabled.  The feed (``/feed/``, ``/category/<slug>/feed/``, …)
   handles enumeration and basic metadata; image URLs are extracted
   from each post's HTML page (``data-lazy`` / ``srcset`` / ``src``
   filtered to ``/wp-content/uploads/``).  XML-RPC and the oEmbed route
   sit under the same ``rest_authentication_errors`` filter on
   locked-down sites and provide no extra path.

Fallback behavior — ``extractor.wordpress.fallback``:

* ``"auto"`` (default) — try REST, fall back on failure
* ``"rest"``           — REST only; error if disabled
* ``"feed"``           — skip REST, go straight to feed+HTML

GraphQL: WPGraphQL (``/graphql``) is a drop-in alternative backend.
Subclass :class:`WordpressExtractor` and override the ``api_*`` helpers
to use it; not enabled by default.

Exposed post fields (kwdict ``post[…]``)::

    id, slug, title, link, date, modified, status, type,
    featured_media, categories, tags, author,
    content       (raw HTML of the post body),
    content_text  (HTML stripped, newline-friendly plaintext),
    excerpt       (rendered excerpt / meta description),
    source        ("rest" | "fallback")

JSON sidecar — emitted as a routine part of extraction (default on),
in the same way ``facebook:post`` writes its JSON dump and
``pixiv:novel`` writes its ``.txt`` body.  Each post produces a
``<post-id>.json`` file alongside its images.  Image filenames are
preserved from the source URL (``filename_fmt = "{filename}.{extension}"``);
override either via the standard gallery-dl config keys.

Toggle::

    {"extractor": {"wordpress": {"metadata": false}}}   # disable
    gallery-dl -o metadata=false <url>                  # one-off skip
"""

import json

from .common import BaseExtractor, Message
from .. import text


class _RestUnavailable(Exception):
    """Internal signal: REST API is disabled or returns no_route."""


class WordpressExtractor(BaseExtractor):
    """Base class for WordPress extractors"""
    basecategory = "wordpress"
    directory_fmt = ("{category}", "{post[id]} {post[title]}")
    filename_fmt = "{filename}.{extension}"
    archive_fmt = "{post[id]}_{filename}.{extension}"

    PER_PAGE = 100

    def _init(self):
        self._embed = self.config("embed", True)
        self._videos = self.config("videos", True)
        self._fallback_policy = self.config("fallback", "auto")
        self._write_metadata = self.config("metadata", True)
        self._yield_iframes = self.config("iframes", True)

    # ------------------------------------------------------------------ #
    # Top-level emission
    # ------------------------------------------------------------------ #
    def items(self):
        for post in self._dispatch_posts():
            yield from self._emit_post(post)

    def _dispatch_posts(self):
        if self._fallback_policy == "feed":
            yield from self.posts_fallback()
            return
        try:
            yield from self.posts_rest()
            return
        except _RestUnavailable as exc:
            if self._fallback_policy == "rest":
                raise self.exc.AbortExtraction(str(exc))
            self.log.warning("%s", exc)
            self.log.info("Falling back to feed + HTML scraping")
        yield from self.posts_fallback()

    def posts_rest(self):
        """Yield normalized post dicts via the REST API (override)"""
        return ()

    def posts_fallback(self):
        """Yield normalized post dicts via feed/HTML (override)"""
        return ()

    # ------------------------------------------------------------------ #
    # Message emission
    # ------------------------------------------------------------------ #
    def _emit_post(self, post):
        urls = self._collect_media(post)
        iframes = self._collect_iframes(post)
        meta = self._post_metadata(post)
        data = {"post": meta, "count": len(urls)}
        yield Message.Directory, "", data

        # Emit a JSON sidecar as part of the normal output flow,
        # mirroring how facebook:post and pixiv:novel record post bodies.
        # gallery-dl's built-in TextDownloader writes the literal payload
        # of any "text:..." URL straight to disk -- no post-processor
        # configuration required.
        if self._write_metadata:
            sidecar = dict(meta)
            sidecar["images"] = list(urls)
            sidecar["iframes"] = list(iframes)
            payload = json.dumps(
                sidecar, ensure_ascii=False, indent=2, default=str)
            json_kwdict = dict(data)
            json_kwdict["num"] = 0
            json_kwdict["filename"] = str(
                meta.get("id") or meta.get("slug") or "post")
            json_kwdict["extension"] = "json"
            yield Message.Url, "text:" + payload, json_kwdict

        for num, url in enumerate(urls, 1):
            info = dict(data)
            info["num"] = num
            text.nameext_from_url(url, info)
            yield Message.Url, url, info

        # Hand iframe URLs over to whichever extractor matches; the host
        # may be Bunny Stream, YouTube, Vimeo, etc.  Unmatched iframes
        # produce a "no extractor" warning but never abort.
        if self._yield_iframes:
            for url in iframes:
                yield Message.Queue, url, {"post": meta}

    # ------------------------------------------------------------------ #
    # REST helpers
    # ------------------------------------------------------------------ #
    def api_url(self, path):
        return f"{self.root}/wp-json/wp/v2{path}"

    def api_get(self, path, params=None, embed=None):
        if params is None:
            params = {}
        if embed or (embed is None and self._embed):
            params["_embed"] = "1"
        response = self.request(
            self.api_url(path), params=params, fatal=False)
        if response.status_code >= 400:
            self._api_error(response)
        return response.json(), response

    def api_paginated(self, path, params=None):
        if params is None:
            params = {}
        params.setdefault("per_page", self.PER_PAGE)
        page = 1
        while True:
            params["page"] = page
            data, response = self.api_get(path, params=params)
            if not isinstance(data, list) or not data:
                return
            yield from data
            total_pages = text.parse_int(
                response.headers.get("X-WP-TotalPages"), 1)
            if page >= total_pages or len(data) < params["per_page"]:
                return
            page += 1

    def _api_error(self, response):
        try:
            data = response.json()
            code = data.get("code") or ""
            msg = data.get("message") or ""
        except Exception:
            code = ""
            msg = response.text[:200]
        if code in ("rest_disabled", "rest_no_route", "rest_not_logged_in"):
            raise _RestUnavailable(
                f"WordPress REST API unavailable at {self.root} "
                f"({code}: {msg})")
        if response.status_code == 404:
            raise self.exc.AbortExtraction(f"Not found: {response.url}")
        raise self.exc.AbortExtraction(
            f"WordPress REST API error "
            f"(HTTP {response.status_code}, {code or 'unknown'}): {msg}")

    def _resolve_term(self, endpoint, slug):
        data, _ = self.api_get(
            endpoint, params={"slug": text.unquote(slug)}, embed=False)
        if not data:
            raise self.exc.AbortExtraction(
                f"{endpoint[1:]}: no term with slug '{slug}'")
        return data[0]

    # ------------------------------------------------------------------ #
    # Feed helpers
    # ------------------------------------------------------------------ #
    FEED_PAGE_LIMIT = 1000  # hard safety ceiling

    def feed_iter(self, path):
        """Iterate RSS <item> blocks from /<path>feed/ across paged results"""
        base = f"{self.root}{path}feed/"
        for page in range(1, self.FEED_PAGE_LIMIT + 1):
            url = base if page == 1 else f"{base}?paged={page}"
            response = self.request(url, fatal=False)
            if response.status_code == 404:
                return
            if response.status_code >= 400:
                raise self.exc.AbortExtraction(
                    f"Feed request failed ({response.status_code}): {url}")
            body = response.text
            items = list(text.extract_iter(body, "<item>", "</item>"))
            if not items:
                return
            yield from items

    def _feed_item_to_post(self, item):
        """Convert a minimal RSS <item> block to a post-like dict.

        The image list is left empty; :meth:`_scrape_post_html` will fill
        it in when the post URL is eventually visited.
        """
        link = text.unescape(text.extr(item, "<link>", "</link>")).strip()
        title = text.unescape(
            text.extr(item, "<title>", "</title>")
            .replace("<![CDATA[", "").replace("]]>", "").strip())
        guid = text.extr(item, "<guid", "</guid>")
        guid = text.extr(guid, ">", "") if ">" in guid else guid
        post_id = 0
        if "?p=" in guid:
            post_id = text.parse_int(guid.rpartition("?p=")[2])

        pub = text.extr(item, "<pubDate>", "</pubDate>").strip()
        categories = []
        for cat in text.extract_iter(item, "<category>", "</category>"):
            name = cat.replace("<![CDATA[", "").replace("]]>", "").strip()
            if name:
                categories.append({"id": 0, "slug": "", "name": name})
        creator = text.extr(item, "<dc:creator>", "</dc:creator>")
        creator = creator.replace("<![CDATA[", "").replace("]]>", "").strip()

        return {
            "__link"    : link,
            "id"        : post_id,
            "slug"      : link.rstrip("/").rsplit("/", 1)[-1],
            "title"     : title,
            "link"      : link,
            "date"      : pub,
            "categories": categories,
            "author"    : {"id": 0, "slug": "", "name": creator} if creator
                          else None,
        }

    def _scrape_post_html(self, url, seed=None):
        """Fetch a post page and return a post-like dict with images."""
        response = self.request(url, fatal=False)
        if response.status_code >= 400:
            self.log.warning(
                "post page returned HTTP %s: %s", response.status_code, url)
            return None
        html = response.text
        post = dict(seed) if seed else {}
        post.setdefault("link", url)
        if not post.get("slug"):
            post["slug"] = url.rstrip("/").rsplit("/", 1)[-1]
        # Narrow to the post body.  Terminators are tried in order of
        # specificity; we stop at the first article close-tag to keep
        # sidebar / related-post widgets out.
        body = ""
        # Try multiple opening markers.  Each is the CLOSE of the wrapping
        # tag so the captured body starts at the first real child element.
        for begin in ('itemprop="articleBody">',
                      'class="entry-content clearfix">',
                      'class="entry-content">'):
            for end in ("</article>", "<!-- /.entry-content -->",
                        "<!-- .entry-content -->",
                        '<footer class="entry-footer'):
                body = text.extr(html, begin, end)
                if body:
                    break
            if body:
                break
        if not body:
            # Last-resort: locate the opening, advance past the tag.
            marker = 'class="entry-content'
            p = html.find(marker)
            if p >= 0:
                q = html.find(">", p)
                if q >= 0:
                    body = html[q + 1:html.find("</article>", q)]
        if not body:
            body = html
        images = self._images_from_html(body)
        # Featured image from og:image meta (often the cover not in content)
        og = text.extr(html, '<meta property="og:image" content="', '"')
        if og and "/wp-content/uploads/" in og and og not in images:
            images.insert(0, og)
        # Fill in missing metadata from HTML tags / meta
        if not post.get("title"):
            t = (text.extr(html, 'class="entry-title">', "<")
                 or text.extr(html, '<meta property="og:title" content="', '"'))
            t = text.unescape(t).strip() if t else ""
            # Strip trailing " - Site" / " | Site" suffix (og:title)
            for sep in (" - ", " | ", " – "):
                site = self.root.rsplit("/", 1)[-1]
                if t.endswith(sep + site) or t.endswith(
                        sep + site.capitalize()):
                    t = t.rsplit(sep, 1)[0]
                    break
            post["title"] = t
        if not post.get("date"):
            post["date"] = (
                text.extr(html,
                          '<time class="entry-date published" datetime="', '"')
                or text.extr(html, '<time datetime="', '"')
                or text.extr(
                    html,
                    '<meta property="article:published_time" content="', '"'))
        if not post.get("id"):
            sl = text.extr(html, '<link rel="shortlink" href="', '"')
            if "?p=" in sl:
                post["id"] = text.parse_int(sl.rpartition("?p=")[2])
            else:
                # body class fallback: "postid-<n>"
                m = text.extr(html, "postid-", " ")
                post["id"] = text.parse_int(m) if m else 0
        post["__fallback_images"] = images
        post["__fallback_body"] = body
        # Site-wide description meta often serves as a decent excerpt
        post.setdefault("__fallback_excerpt", text.extr(
            html, '<meta name="description" content="', '"') or
            text.extr(html, '<meta property="og:description" content="', '"'))
        return post

    # ------------------------------------------------------------------ #
    # Media extraction
    # ------------------------------------------------------------------ #
    LAZY_ATTRS = ("data-lazy", "data-src", "data-original",
                  "data-lazy-src", "data-cfsrc")

    def _collect_media(self, post):
        # Fallback path short-circuits: scraper already found images.
        if "__fallback_images" in post:
            return list(post["__fallback_images"])

        html = (post.get("content") or {}).get("rendered") or ""
        urls = self._images_from_html(html)
        if self._videos:
            urls.extend(self._videos_from_html(html))

        # Prepend featured media if not already present
        emb = post.get("_embedded") or {}
        for m in emb.get("wp:featuredmedia") or ():
            if src := (m or {}).get("source_url"):
                if src not in urls:
                    urls.insert(0, src)
        return urls

    def _collect_iframes(self, post):
        if "__fallback_body" in post:
            html = post["__fallback_body"] or ""
        else:
            html = (post.get("content") or {}).get("rendered") or ""
        seen = set()
        urls = []
        for tag in text.extract_iter(html, "<iframe", ">"):
            src = text.extr(tag, 'src="', '"')
            if not src or not src.startswith(("http://", "https://")):
                continue
            src = text.unescape(src)
            if src in seen:
                continue
            seen.add(src)
            urls.append(src)
        return urls

    def _images_from_html(self, html):
        seen = set()
        urls = []
        for img in text.extract_iter(html, "<img", ">"):
            src = self._pick_image_src(img)
            if not src:
                continue
            src = text.unescape(src)
            if src in seen:
                continue
            seen.add(src)
            urls.append(src)
        return urls

    def _pick_image_src(self, tag):
        # Lazy-load attributes take precedence over the placeholder src
        for attr in self.LAZY_ATTRS:
            if val := text.extr(tag, f'{attr}="', '"'):
                if "/wp-content/uploads/" in val:
                    return val
        # srcset: pick the widest candidate
        if srcset := text.extr(tag, 'srcset="', '"'):
            best, best_w = "", 0
            for candidate in srcset.split(","):
                candidate = candidate.strip()
                if not candidate:
                    continue
                url, _, size = candidate.rpartition(" ")
                if not url:
                    url = candidate
                    size = ""
                width = text.parse_int(size.rstrip("w"))
                if width >= best_w and "/wp-content/uploads/" in url:
                    best, best_w = url, width
            if best:
                return best
        if src := text.extr(tag, 'src="', '"'):
            if "/wp-content/uploads/" in src:
                return src
        return ""

    def _videos_from_html(self, html):
        urls = []
        for tag in text.extract_iter(html, "<video", "</video>"):
            for src in self._src_attrs(tag):
                if "/wp-content/uploads/" in src and src not in urls:
                    urls.append(src)
        return urls

    @staticmethod
    def _src_attrs(tag):
        if direct := text.extr(tag, 'src="', '"'):
            yield direct
        for source in text.extract_iter(tag, "<source", ">"):
            if src := text.extr(source, 'src="', '"'):
                yield src

    # ------------------------------------------------------------------ #
    # Metadata normalization
    # ------------------------------------------------------------------ #
    def _post_metadata(self, post):
        # Fallback-sourced posts are already flattened
        if "__fallback_images" in post:
            date = post.get("date") or ""
            body = post.get("__fallback_body") or ""
            return {
                "id"            : post.get("id") or 0,
                "slug"          : post.get("slug") or "",
                "title"         : post.get("title") or "",
                "link"          : post.get("link") or "",
                "date"          : self.parse_datetime_iso(date)
                                  if "T" in date else self.parse_datetime(
                                      date, "%a, %d %b %Y %H:%M:%S %z")
                                  if date else None,
                "modified"      : None,
                "status"        : "publish",
                "type"          : "post",
                "featured_media": 0,
                "categories"    : post.get("categories") or [],
                "tags"          : post.get("tags") or [],
                "author"        : post.get("author"),
                "content"       : body,
                "content_text"  : text.unescape(text.remove_html(body))
                                  if body else "",
                "excerpt"       : text.unescape(
                    post.get("__fallback_excerpt") or ""),
                "source"        : "fallback",
            }

        emb = post.get("_embedded") or {}
        terms = {}
        for group in emb.get("wp:term") or ():
            for t in group or ():
                tax = t.get("taxonomy") or "other"
                terms.setdefault(tax, []).append({
                    "id"  : t.get("id"),
                    "slug": t.get("slug"),
                    "name": text.unescape(t.get("name") or ""),
                })

        author = None
        if authors := emb.get("author"):
            a = authors[0] or {}
            author = {
                "id"  : a.get("id"),
                "slug": a.get("slug"),
                "name": text.unescape(a.get("name") or ""),
            }

        title_html = (post.get("title") or {}).get("rendered") or ""
        content_html = (post.get("content") or {}).get("rendered") or ""
        excerpt_html = (post.get("excerpt") or {}).get("rendered") or ""
        return {
            "id"            : post.get("id"),
            "slug"          : post.get("slug") or "",
            "title"         : text.unescape(text.remove_html(title_html)),
            "link"          : post.get("link") or "",
            "date"          : self.parse_datetime_iso(
                post.get("date_gmt") or post.get("date") or ""),
            "modified"      : self.parse_datetime_iso(
                post.get("modified_gmt") or post.get("modified") or ""),
            "status"        : post.get("status"),
            "type"          : post.get("type"),
            "featured_media": post.get("featured_media") or 0,
            "categories"    : terms.get("category") or [],
            "tags"          : terms.get("post_tag") or [],
            "author"        : author,
            "content"       : content_html,
            "content_text"  : text.unescape(text.remove_html(content_html)),
            "excerpt"       : text.unescape(text.remove_html(excerpt_html)),
            "source"        : "rest",
        }


# No instances are pre-registered: the WordPress universe is too broad
# to enumerate.  Use either the ``wordpress:<site-url>...`` URL prefix or
# add per-site entries under ``extractor.wordpress.<category>`` in your
# gallery-dl config.  Example::
#
#     {"extractor": {"wordpress": {"mysite": {
#         "root":    "https://mysite.example",
#         "pattern": "(?:www\\.)?mysite\\.example"
#     }}}}
BASE_PATTERN = WordpressExtractor.update({})


class WordpressPostExtractor(WordpressExtractor):
    """Extractor for a single WordPress post"""
    subcategory = "post"
    pattern = BASE_PATTERN + (
        r"/(?!wp-|category/|tag/|author/|feed(?:/|$))"
        r"([\w%-]+(?:/[\w%-]+)*?)/?(?:[?#]|$)"
    )
    example = "wordpress:https://example.com/post-slug/"

    def posts_rest(self):
        last = self.groups[-1].rsplit("/", 1)[-1]
        if last.isdigit():
            post, _ = self.api_get(f"/posts/{last}")
            if post:
                yield post
            return
        slug = text.unquote(last)
        found, _ = self.api_get("/posts", params={"slug": slug})
        if isinstance(found, list):
            yield from found

    def posts_fallback(self):
        post = self._scrape_post_html(self.url)
        if post:
            yield post


class _ListingExtractor(WordpressExtractor):
    """Mixin for taxonomy-based listing extractors (category/tag/author)"""
    rest_endpoint = None      # /categories | /tags | /users
    rest_filter_key = None    # categories | tags | author
    feed_path_prefix = None   # /category/ | /tag/ | /author/

    def posts_rest(self):
        slug = self.groups[-1].rsplit("/", 1)[-1]
        term = self._resolve_term(self.rest_endpoint, slug)
        params = {self.rest_filter_key: term["id"]}
        yield from self.api_paginated("/posts", params=params)

    def posts_fallback(self):
        slug = self.groups[-1].rsplit("/", 1)[-1]
        path = f"{self.feed_path_prefix}{slug}/"
        for item in self.feed_iter(path):
            seed = self._feed_item_to_post(item)
            if not seed.get("__link"):
                continue
            post = self._scrape_post_html(seed["__link"], seed=seed)
            if post:
                yield post


class WordpressCategoryExtractor(_ListingExtractor):
    """Extractor for a WordPress category archive"""
    subcategory = "category"
    pattern = BASE_PATTERN + r"/category/([\w%/-]+?)/?(?:[?#]|$)"
    example = "wordpress:https://example.com/category/SLUG/"
    rest_endpoint = "/categories"
    rest_filter_key = "categories"
    feed_path_prefix = "/category/"


class WordpressTagExtractor(_ListingExtractor):
    """Extractor for a WordPress tag archive"""
    subcategory = "tag"
    pattern = BASE_PATTERN + r"/tag/([\w%/-]+?)/?(?:[?#]|$)"
    example = "wordpress:https://example.com/tag/SLUG/"
    rest_endpoint = "/tags"
    rest_filter_key = "tags"
    feed_path_prefix = "/tag/"


class WordpressAuthorExtractor(_ListingExtractor):
    """Extractor for a WordPress author archive"""
    subcategory = "author"
    pattern = BASE_PATTERN + r"/author/([\w%-]+?)/?(?:[?#]|$)"
    example = "wordpress:https://example.com/author/SLUG/"
    rest_endpoint = "/users"
    rest_filter_key = "author"
    feed_path_prefix = "/author/"
