# -*- coding: utf-8 -*-

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.

"""Generic extractor for shops running on the 91app SaaS platform

91app powers a large family of Taiwan-based e-commerce sites
(``www.qmomo.com.tw``, ``www.peachjohn.com.tw``, ``www.miniqueen.tw`` …).
Two URL flavors are supported:

* ``<host>/SalePage/Index/<id>`` — product detail pages.  The
  rendered HTML embeds the full view-model inline as
  ``window.ServerRenderData["SalePageIndexViewModel"] = {…}`` — that
  single JSON blob feeds the product extractor.
* ``<host>/page/<slug>`` — CMS landing / campaign pages.  These are
  React SPAs whose widget content is fetched client-side from a
  non-public 91app CMS API; pure HTTP can only see what's in the SSR
  shell (shop info, breadcrumb, og-meta, occasional cms-static
  assets).  The page extractor records that and writes a timestamped
  JSON snapshot, but full widget media requires a headless browser.

This module is site-agnostic and ships with **no** instances
pre-registered.  Reach a 91app shop via either:

* the ``91app:<host-url>/<path>`` URL prefix, or
* a per-site entry under ``extractor.91app.<category>`` in your
  gallery-dl config (see the snippet at the bottom of this file).

Product extractor (``subcategory=product``) yields:

* all ``ImageList[].PicUrl`` images (img.91app.com CDN)
* a UTC-timestamped JSON sidecar per run, mirroring the amiami
  pattern, so re-running the same product accumulates a
  price/stock/promotion history without overwriting earlier snapshots
* any external media from description bodies or
  ``MainImageVideo.VideoUrl`` (typically YouTube product videos or
  91app CMS-static images), recorded under ``external_media`` in the
  JSON and yielded as ``Message.Queue`` for sibling extractors

Page extractor (``subcategory=page``) yields:

* whatever images are visible in the SSR HTML (typically the shop logo
  and any cms-static assets baked in by the theme — *not* React-rendered
  widget content)
* a UTC-timestamped JSON sidecar with ``shop_id`` / ``page_slug`` /
  ``page_type`` / ``breadcrumb`` / og-meta + a ``ssr_only`` flag set
  ``true`` so callers know the snapshot is partial

Settings::

    "metadata"     : true    # write the timestamped JSON sidecar
    "externals"    : true    # forward external-media URLs
    "follow-group" : false   # queue SalePageGroup sibling variants

``metadata`` and ``externals`` are read via ``self.config()`` — place
them at the root ``extractor.<key>`` (covers every extractor) or under
a per-shop ``extractor.<category>.<key>`` to override.

``follow-group`` is per-shop policy and lives directly in the instance
dict (same dict as ``root`` / ``pattern``); it is read via
``self.config_instance()``, mirroring mastodon's ``access-token``.

User config to register a shop and enable per-instance settings::

    {"extractor": {"91app": {"qmomo": {
        "root":         "https://www.qmomo.com.tw",
        "pattern":      r"(?:www\\.)?qmomo\\.com\\.tw",
        "follow-group": true
    }}}}

Out of scope:

* SHOPLINE-based shops (e.g. ``www.uwlingerie.com``) use a different
  platform; need their own extractor
"""

import re
import json
import datetime

from .common import BaseExtractor, Message
from .. import text


_VIEWMODEL_RE = re.compile(
    r'window\.ServerRenderData\["SalePageIndexViewModel"\]\s*=\s*'
    r'({.*?});\s*(?:window\.|\(function|\b)',
    re.DOTALL,
)

# Tracking pixels / analytics referenced from CMS bodies — drop these
# rather than feed them to the dispatcher.
_TRACKER_DOMAINS = (
    "tr.line.me",
    "googletagmanager.com",
    "google-analytics.com",
    "facebook.com/tr",
    "/tag.gif",
    "scorecardresearch.com",
    "doubleclick.net",
)


class _91appExtractor(BaseExtractor):
    """Base class for 91app shop extractors"""
    basecategory = "91app"
    directory_fmt = ("{category}", "{salepage_id} {title}")
    filename_fmt = "{filename}.{extension}"
    archive_fmt = "{salepage_id}_{filename}.{extension}"

    def _init(self):
        self._write_metadata = self.config("metadata", True)
        self._yield_externals = self.config("externals", True)
        # config_instance (cf. mastodon's access-token) so the flag can
        # ride in the instance dict; self.config() only walks
        # extractor.<category>.<subcategory> and misses it there.
        self._follow_group = self.config_instance("follow-group", False)


# No instances pre-registered: see module docstring for usage.
BASE_PATTERN = _91appExtractor.update({})


class _91appProductExtractor(_91appExtractor):
    """Extractor for a single 91app SalePage (product)"""
    subcategory = "product"
    pattern = BASE_PATTERN + r"/SalePage/Index/(\d+)"
    example = "91app:https://example.com/SalePage/Index/123"

    def items(self):
        sp_id = self.groups[-1]
        url = f"{self.root}/SalePage/Index/{sp_id}"
        page = self.request(url, notfound="product").text

        vm = self._parse_viewmodel(page)
        addl = self._fetch_additional_info(vm.get("ShopId"), vm.get("Id"))

        body_html = ""
        if isinstance(addl, dict):
            mi = addl.get("moreInfo") or {}
            if isinstance(mi, dict):
                body_html = mi.get("saleProductDesc_Content") or ""

        meta = self._normalize(vm, addl=addl)
        primary = self._collect_primary(vm)
        externals = self._collect_externals(
            vm, body_html=body_html,
            primary_urls=(u for u, _ in primary))

        snapshot = dict(meta)
        snapshot["images"] = [u for u, _ in primary]
        snapshot["external_media"] = [
            {"url": u, "kind": k} for u, k in externals]
        # Full raw API responses kept for forensic completeness — both
        # ViewModel (~50KB) and additional-info (~3KB) live alongside the
        # curated convenience fields, never replacing them.
        snapshot["raw_viewmodel"] = vm
        snapshot["raw_additional_info"] = addl

        yield Message.Directory, "", meta

        if self._write_metadata:
            ts = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y%m%dT%H%M%SZ")
            payload = json.dumps(
                snapshot, ensure_ascii=False, indent=2, default=str)
            yield Message.Url, "text:" + payload, {
                **meta,
                "num"      : 0,
                "filename" : ts,
                "extension": "json",
            }

        for num, (img_url, label) in enumerate(primary, 1):
            kw = dict(meta)
            kw["num"] = num
            kw["filename"] = label
            kw["extension"] = self._ext(img_url)
            yield Message.Url, img_url, kw

        # External media split:
        # * image-kind URLs (kind starts with "img:") download in-place to
        #   the product directory so merchant-CDN body images land alongside
        #   the img.91app.com images.
        # * iframe / main_video URLs go through Message.Queue so a sibling
        #   extractor (yt-dlp via ytdl: prefix, etc.) can take them.
        if self._yield_externals:
            extra_num = len(primary)
            for url_, kind in externals:
                if kind.startswith("img:"):
                    extra_num += 1
                    info = dict(meta)
                    info["num"] = extra_num
                    info["filename"] = self._body_filename(url_, kind, extra_num)
                    info["extension"] = self._ext(url_)
                    info["kind"] = kind
                    yield Message.Url, url_, info
                else:
                    yield Message.Queue, url_, {
                        "product": meta, "kind": kind}

        # follow-group: queue the SalePageGroup siblings (other color /
        # style variants of the same product). Default off; user enables
        # via extractor.91app.follow-group=true.
        if self._follow_group:
            sg = meta.get("salepage_group") or {}
            my_id = meta.get("salepage_id")
            for it in (sg.get("items") or ()):
                sid = it.get("salepage_id")
                if not sid or sid == my_id:
                    continue
                target = f"{self.root}/SalePage/Index/{sid}"
                yield Message.Queue, target, {
                    "product": meta,
                    "kind"   : f"group:{it.get('title') or ''}",
                }

    # ------------------------------------------------------------------ #
    # additional-info API
    # ------------------------------------------------------------------ #
    def _fetch_additional_info(self, shop_id, sp_id):
        """Fetch /salepage-listing/api/salepage/additional-info/<shop>/<sp>.

        Returns the response ``data`` object (dict) on success, ``None`` on
        any failure (network, non-Success code, parse).  Caller treats
        ``None`` as "no extra data" — the SalePage extractor still works,
        just without specs / body images.
        """
        if not shop_id or not sp_id:
            return None
        url = (f"{self.root}/salepage-listing/api/salepage/"
               f"additional-info/{shop_id}/{sp_id}")
        response = self.request(url, fatal=False)
        if response.status_code != 200:
            return None
        try:
            envelope = response.json()
        except Exception:
            return None
        if envelope.get("code") != "Success":
            return None
        data = envelope.get("data")
        return data if isinstance(data, dict) else None

    # ------------------------------------------------------------------ #
    # ViewModel parsing
    # ------------------------------------------------------------------ #
    def _parse_viewmodel(self, html):
        m = _VIEWMODEL_RE.search(html)
        if not m:
            raise self.exc.AbortExtraction(
                "SalePageIndexViewModel not found in page")
        raw = m.group(1)
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # Some shops emit JS-only escapes inside string fields
            # (mydoll's English policy text has ``don\'t`` / ``store\'s``).
            # ``\'`` is not a valid JSON escape; strip the backslash but
            # preserve a real ``\\'`` (escaped backslash then apostrophe)
            # by only rewriting ``\'`` after an even number of backslashes.
            raw = re.sub(r"(?<!\\)((?:\\\\)*)\\'", r"\1'", raw)
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise self.exc.AbortExtraction(
                f"failed to parse SalePageIndexViewModel: {exc}")

    # ------------------------------------------------------------------ #
    # Media collection
    # ------------------------------------------------------------------ #
    def _collect_primary(self, vm):
        """Yield (url, label) for every img.91app.com image in ImageList."""
        urls = []
        for img in vm.get("ImageList") or ():
            if not isinstance(img, dict):
                continue
            src = img.get("PicUrl") or ""
            if not src:
                continue
            if src.startswith("//"):
                src = "https:" + src
            elif src.startswith("/"):
                src = self.root + src
            seq = img.get("Index")
            if seq is None:
                seq = img.get("Seq", len(urls))
            urls.append((src, f"{int(seq)+1:02d}"))
        return urls

    def _collect_externals(self, vm, body_html="", primary_urls=()):
        """Pull (url, kind) tuples from MainImageVideo + description bodies.

        ``kind`` is one of ``main_video`` / ``img:<field>`` / ``iframe:<field>``
        — primarily for the JSON sidecar; gallery-dl's dispatcher only sees
        the URL and decides routing by URL pattern.

        ``body_html`` is the rich body markup from
        ``additional-info.data.moreInfo.saleProductDesc_Content``; its
        embedded ``<img>`` (typically merchant CDN like
        ``photo.<shop>.com.tw``) and ``<iframe>`` are added under the
        ``img:body`` / ``iframe:body`` kinds.

        ``primary_urls`` pre-populates the dedup set so any body img
        whose URL string-matches a primary ImageList URL is skipped.
        Body images on the same CDN host but a different endpoint
        (e.g. ``img.91app.com/webapi/images/r/SalePageDesc/...``) are
        kept — string equality is the only signal we trust.
        """
        out = []
        seen = set(primary_urls)

        miv = vm.get("MainImageVideo") or {}
        if isinstance(miv, dict):
            v = miv.get("VideoUrl")
            if v:
                v = text.unescape(v)
                seen.add(v)
                out.append((v, "main_video"))

        for fld in ("Description", "SubDescript", "ShortDescription"):
            html = vm.get(fld) or ""
            if not html:
                continue
            for tag in text.extract_iter(html, "<img", ">"):
                if src := self._extract_src(tag):
                    if src in seen:
                        continue
                    seen.add(src)
                    out.append((src, f"img:{fld}"))
            for tag in text.extract_iter(html, "<iframe", ">"):
                if src := self._extract_src(tag):
                    if src in seen:
                        continue
                    seen.add(src)
                    out.append((src, f"iframe:{fld}"))

        if body_html:
            for tag in text.extract_iter(body_html, "<img", ">"):
                if src := self._extract_src(tag):
                    if src in seen:
                        continue
                    seen.add(src)
                    out.append((src, "img:body"))
            for tag in text.extract_iter(body_html, "<iframe", ">"):
                if src := self._extract_src(tag):
                    if src in seen:
                        continue
                    seen.add(src)
                    out.append((src, "iframe:body"))

        return out

    @staticmethod
    def _extract_src(tag):
        src = (text.extr(tag, 'src="', '"') or
               text.extr(tag, "src='", "'") or
               text.extr(tag, 'data-src="', '"'))
        if not src or src.startswith("data:"):
            return ""
        src = text.unescape(src)
        if src.startswith("//"):
            src = "https:" + src
        if any(d in src for d in _TRACKER_DOMAINS):
            return ""
        return src

    @staticmethod
    def _ext(url):
        path = url.partition("?")[0]
        candidate = path.rpartition(".")[2].lower()
        if candidate in ("jpg", "jpeg", "png", "gif", "webp", "mp4", "webm"):
            return candidate
        # img.91app.com URLs have no extension; default to jpg
        return "jpg"

    @staticmethod
    def _body_filename(url, kind, num):
        """Derive a filename for a body / description image.

        Prefer the source URL's basename (sans extension) so merchant-CDN
        files keep recognisable names (e.g. ``B52202-01``).  Fall back to a
        ``body_NN`` / ``desc_NN`` numeric tag based on the kind suffix when
        the URL has no usable basename.
        """
        path = url.partition("?")[0].rstrip("/")
        base = path.rpartition("/")[2]
        stem = base.rsplit(".", 1)[0]
        if stem and stem != base:
            return stem
        if base:
            return base
        prefix = kind.partition(":")[2] or "body"
        return f"{prefix}_{num:02d}"

    # ------------------------------------------------------------------ #
    # Metadata normalization
    # ------------------------------------------------------------------ #
    def _normalize(self, vm, addl=None):
        miv = vm.get("MainImageVideo") or {}
        miv = miv if isinstance(miv, dict) and miv else None

        # SalePageGroup — sibling SalePageIds for color / style variants
        sg = vm.get("SalePageGroup") or {}
        salepage_group = None
        if isinstance(sg, dict) and sg.get("SalePageItems"):
            items = []
            for it in sg.get("SalePageItems") or ():
                if not isinstance(it, dict):
                    continue
                thumb = it.get("ItemUrl") or ""
                if thumb.startswith("//"):
                    thumb = "https:" + thumb
                items.append({
                    "salepage_id": it.get("SalePageId"),
                    "title"      : it.get("GroupItemTitle") or "",
                    "sort"       : it.get("ItemSort"),
                    "thumb_url"  : thumb,
                    "image_group": it.get("ImageGroup") or "",
                })
            salepage_group = {
                "group_code" : sg.get("GroupCode") or "",
                "group_title": sg.get("GroupTitle") or "",
                "icon_style" : sg.get("GroupIconStyle") or "",
                "items"      : items,
            }

        # additional-info supplementary fields
        ai = addl if isinstance(addl, dict) else {}
        more_info = ai.get("moreInfo") if isinstance(
            ai.get("moreInfo"), dict) else {}

        return {
            "category"            : self.category,
            "subcategory"         : self.subcategory,
            # Identifiers
            "salepage_id"         : vm.get("Id"),
            "shop_id"             : vm.get("ShopId"),
            "shop_name"           : vm.get("ShopName") or "",
            "url"                 : (
                f"{self.root}/SalePage/Index/{vm.get('Id')}"),
            # Names / descriptions
            "title"               : vm.get("Title") or "",
            "subtitle"            : vm.get("SubTitle") or "",
            "description"         : vm.get("Description") or "",
            "sub_description"     : vm.get("SubDescript") or "",
            "short_description"   : vm.get("ShortDescription") or "",
            # Categorization
            "shop_category_id"    : vm.get("ShopCategoryId"),
            "category_id"         : vm.get("CategoryId"),
            "category_name"       : vm.get("CategoryName") or "",
            "category_levels"     : vm.get("CategoryLevelName") or {},
            "tag_ids"             : vm.get("TagIds") or [],
            # Pricing
            "price"               : vm.get("Price"),
            "max_price"           : vm.get("MaxPrice"),
            "min_price"           : vm.get("MinPrice"),
            "suggest_price"       : vm.get("SuggestPrice"),
            "max_suggest_price"   : vm.get("MaxSuggestPrice"),
            "min_suggest_price"   : vm.get("MinSuggestPrice"),
            "price_with_currency" : vm.get("PriceWithCurrencySymbol") or "",
            # Stock / status
            "stock_qty"           : vm.get("StockQty"),
            "sold_qty"            : vm.get("SoldQty"),
            "status"              : vm.get("StatusDef") or "",
            "is_app_only"         : bool(vm.get("IsAPPOnly")),
            "is_app_only_promo"   : bool(vm.get("IsAPPOnlyPromotion")),
            "is_coming_soon"      : bool(vm.get("IsComingSoon")),
            "is_limit"            : bool(vm.get("IsLimit")),
            "is_restricted"       : bool(vm.get("IsRestricted")),
            "is_oversea_shipping" : bool(vm.get("CanOverseaShipping")),
            "product_type"        : vm.get("ProductTypeDef") or "",
            "salepage_kind"       : vm.get("SalePageKindDef") or "",
            # SKU / promotions
            "sku_property_set_list": vm.get("SKUPropertySetList") or [],
            "major_list"          : vm.get("MajorList") or [],
            "promotions"          : vm.get("Promotions") or [],
            "ecoupons"            : vm.get("ECoupons") or [],
            "shipping_type_list"  : vm.get("ShippingTypeList") or [],
            "pay_profile_type_list": vm.get("PayProfileTypeList") or [],
            # Media metadata
            "image_count"         : vm.get("ImageCount"),
            "main_image_video"    : miv,
            # Timestamps from the platform
            "selling_start"       : vm.get("SellingStartDateTime") or "",
            "listing_start"       : vm.get("ListingStartDateTime") or "",
            "create_time"         : vm.get("CreateDateTime") or "",
            "update_time"         : vm.get("UpdatedDateTime") or "",
            # SEO
            "seo_tag"             : vm.get("SEOTag") or {},
            # Style / color variants (from ViewModel.SalePageGroup)
            "salepage_group"      : salepage_group,
            # Supplementary data from /salepage-listing/api/.../additional-info
            "specifications"      : ai.get("notKeyPropertyList") or [],
            "additional_promotions": ai.get("promotionInfoList") or [],
            "loyalty_point_excluded": bool(ai.get("isLoyaltyPointExcluded")),
            "more_info"           : more_info,
            # Fetch timestamp
            "fetched_at"          : datetime.datetime.now(
                datetime.timezone.utc).isoformat(),
        }


# Pulls the inline ``nineyi.<key> = <value>;`` assignments out of the
# 91app SSR shell.  Used by the page extractor below — these blocks
# carry shop info, locale, page type and the API config but NOT the
# widget data (that is fetched client-side from a non-public CMS API).
def _extract_nineyi_value(js, key):
    """Return the raw text of ``nineyi.<key> = <value>;`` or ``""``."""
    m = re.search(
        rf'nineyi\.{re.escape(key)}\s*=\s*', js)
    if not m:
        return ""
    i = m.end()
    while i < len(js) and js[i] in ' \t\n\r':
        i += 1
    if i >= len(js):
        return ""
    c = js[i]
    if c not in '{["\'':
        j = js.find(';', i)
        return js[i:j].strip() if j > 0 else ""
    if c in '{[':
        depth = 0
        in_str = False
        esc = False
        quote = None
        for j in range(i, len(js)):
            cc = js[j]
            if esc:
                esc = False
                continue
            if in_str:
                if cc == '\\':
                    esc = True
                elif cc == quote:
                    in_str = False
                continue
            if cc in '"\'':
                in_str = True
                quote = cc
                continue
            if cc in '{[':
                depth += 1
            elif cc in '}]':
                depth -= 1
                if depth == 0:
                    return js[i:j + 1]
        return ""
    # Quoted scalar
    quote = c
    esc = False
    for j in range(i + 1, len(js)):
        cc = js[j]
        if esc:
            esc = False
            continue
        if cc == '\\':
            esc = True
            continue
        if cc == quote:
            return js[i:j + 1]
    return ""


_CMS_IMAGE_BASE = "https://cms-static.cdn.91app.com/images/original"

# Keys (case-insensitive) under which a 91app widget attribute tree
# stores a media filename.  Walked recursively in ``_walk_state_media``.
_IMG_KEY_RE = re.compile(r"image[Uu]rl|imageURL", re.ASCII)


class _91appPageExtractor(_91appExtractor):
    """Extractor for a 91app CMS landing page (``/page/<slug>``)

    Primary path: parse the inline ``nineyi['__PRELOADED_STATE__']``
    widget tree (``construct.{header,center,footer}``) and yield every
    image referenced by a widget attribute.  Each material lives at
    ``cms-static.cdn.91app.com/images/original/<shop_id>/<filename>``.

    Fallback (no preloaded state): scrape ``<img>`` from the SSR shell
    and tag the snapshot ``ssr_only=true``.
    """
    subcategory = "page"
    directory_fmt = ("{category}", "page", "{page_slug}")
    archive_fmt = "{page_slug}_{filename}.{extension}"
    pattern = BASE_PATTERN + r"/page/([\w.-]+)"
    example = "91app:https://example.com/page/some-page"

    def _init(self):
        super()._init()
        self._follow_links = self.config("follow-links", False)
        self._include_state = self.config("state", True)

    def items(self):
        slug = self.groups[-1]
        url = f"{self.root}/page/{slug}"
        page = self.request(url, notfound="page").text

        meta = self._extract_page_meta(page, slug)
        state = self._extract_preloaded_state(page)

        if state is not None:
            media_items = self._walk_state_media(state, meta["shop_id"])
            linked = self._collect_linked_salepages(state)
            meta["ssr_only"] = False
            meta["widget_count"] = self._widget_count(state)
            meta["linked_salepage_ids"] = sorted(linked)
        else:
            media_items = self._media_from_ssr(page)
            meta["ssr_only"] = True
            meta["note"] = (
                "nineyi['__PRELOADED_STATE__'] not found; falling back "
                "to SSR <img> scrape. Some widget content may be missing.")

        snapshot = dict(meta)
        snapshot["media"] = media_items
        if state is not None and self._include_state:
            snapshot["state"] = state

        yield Message.Directory, "", meta

        if self._write_metadata:
            ts = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y%m%dT%H%M%SZ")
            payload = json.dumps(
                snapshot, ensure_ascii=False, indent=2, default=str)
            yield Message.Url, "text:" + payload, {
                **meta,
                "num"      : 0,
                "filename" : ts,
                "extension": "json",
            }

        for num, item in enumerate(media_items, 1):
            kw = dict(meta)
            kw["num"] = num
            stem = item.get("item_key") or item.get("material_key") or ""
            kw["filename"] = (f"{num:03d}_{stem}"
                              if stem else f"{num:03d}")
            kw["extension"] = self._ext_for_media(
                item.get("filename") or item.get("url") or "")
            yield Message.Url, item["url"], kw

        if self._follow_links and state is not None:
            for sp_id in sorted(self._collect_linked_salepages(state)):
                target = f"{self.root}/SalePage/Index/{sp_id}"
                yield Message.Queue, f"91app:{target}", {
                    "page": dict(meta), "salepage_id": sp_id}

    # ------------------------------------------------------------------ #
    # SSR extraction
    # ------------------------------------------------------------------ #
    def _extract_page_meta(self, html, slug):
        # Concatenate inline scripts so _extract_nineyi_value can search
        # without us preserving exact <script> boundaries.
        scripts = "\n".join(text.extract_iter(html, "<script", "</script>"))

        shop_id = text.parse_int(
            _extract_nineyi_value(scripts, "shopId"))
        page_type = _extract_nineyi_value(
            scripts, "pageType").strip("'\" ")
        view_id = _extract_nineyi_value(
            scripts, "viewId").strip("'\" ")
        silo = _extract_nineyi_value(scripts, "silo").strip("'\" ")

        # nineyi.dependencies carries shop name + locale + apiConfig
        deps_raw = _extract_nineyi_value(scripts, "dependencies")
        shop_name = ""
        shop_domain = ""
        locale = ""
        market = ""
        page_name = ""
        router_path = ""
        if deps_raw:
            try:
                deps = json.loads(deps_raw)
            except Exception:
                deps = {}
            shop_domain = deps.get("shopDomainName") or ""
            locale = deps.get("locale") or ""
            market = deps.get("market") or ""
            page_name = deps.get("pageName") or ""
            router_path = deps.get("routerPath") or ""
            sp = deps.get("shopProfile") or {}
            sbi = (sp.get("ShopBasicInfo") or {}) if isinstance(
                sp, dict) else {}
            shop_name = sbi.get("ShopName") or ""

        og = self._og(html)
        breadcrumb = self._breadcrumb(html)

        return {
            "category"      : self.category,
            "subcategory"   : self.subcategory,
            # Identifiers
            "page_slug"     : router_path or slug,
            "page_name"     : page_name or "",
            "page_type"     : page_type,
            "view_id"       : view_id,
            "silo"          : silo,
            "shop_id"       : shop_id,
            "shop_name"     : shop_name,
            "shop_domain"   : shop_domain,
            "locale"        : locale,
            "market"        : market,
            "url"           : f"{self.root}/page/{slug}",
            # SSR-visible content
            "og_title"      : og.get("title", ""),
            "og_description": og.get("description", ""),
            "og_image"      : og.get("image", ""),
            "og_url"        : og.get("url", ""),
            "og_type"       : og.get("type", ""),
            "page_title"    : self._title(html),
            "breadcrumb"    : breadcrumb,
            # Fetch timestamp
            "fetched_at"    : datetime.datetime.now(
                datetime.timezone.utc).isoformat(),
        }

    @staticmethod
    def _og(html):
        out = {}
        for tag in text.extract_iter(html, "<meta", ">"):
            prop = text.extr(tag, 'property="og:', '"')
            if not prop:
                continue
            content = text.extr(tag, 'content="', '"')
            if content:
                out[prop] = text.unescape(content)
        return out

    @staticmethod
    def _title(html):
        t = text.extr(html, "<title>", "</title>")
        return text.unescape(t).strip() if t else ""

    @staticmethod
    def _breadcrumb(html):
        for block in text.extract_iter(
                html, '<script type="application/ld+json">', '</script>'):
            try:
                data = json.loads(block.strip())
            except Exception:
                continue
            if isinstance(data, dict) and \
                    data.get("@type") == "BreadcrumbList":
                return [
                    {
                        "position": text.parse_int(it.get("position")),
                        "name"    : it.get("name") or "",
                        "item"    : it.get("item") or "",
                    }
                    for it in (data.get("itemListElement") or [])
                ]
        return []

    # ------------------------------------------------------------------ #
    # PRELOADED_STATE extraction (primary path)
    # ------------------------------------------------------------------ #
    def _extract_preloaded_state(self, html):
        """Locate ``nineyi['__PRELOADED_STATE__'] = {…};`` and parse it."""
        m = re.search(
            r"""nineyi\[\s*['"]__PRELOADED_STATE__['"]\s*\]\s*=\s*""",
            html)
        if not m:
            return None
        i = m.end()
        # Whitespace
        while i < len(html) and html[i] in ' \t\n\r':
            i += 1
        if i >= len(html) or html[i] != '{':
            return None
        # Balanced-brace parse with string awareness
        depth = 0
        in_str = False
        esc = False
        quote = None
        for j in range(i, len(html)):
            c = html[j]
            if esc:
                esc = False
                continue
            if in_str:
                if c == '\\':
                    esc = True
                elif c == quote:
                    in_str = False
                continue
            if c in '"\'':
                in_str = True
                quote = c
                continue
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    raw = html[i:j + 1]
                    try:
                        return json.loads(raw)
                    except json.JSONDecodeError:
                        return None
        return None

    @staticmethod
    def _widget_count(state):
        construct = state.get("construct") or {}
        return {
            region: len(construct.get(region) or ())
            for region in ("header", "center", "footer")
        }

    def _walk_state_media(self, state, shop_id):
        """Yield one entry per imageUrl* leaf in the widget tree.

        Each entry::

            {
                "filename"     : str,
                "url"          : str,                 # full CDN URL
                "field"        : str (e.g. "imageUrl")
                "path"         : str (.construct.center[3]…)
                "section"      : "header"|"center"|"footer",
                "module_index" : int,
                "widget_id"    : str,
                "item_key"     : str,
                "material_key" : str,
                "link_url"     : str,
                "link_info"    : dict | None,
            }
        """
        items = []
        seen = set()
        construct = state.get("construct") or {}
        for section in ("header", "center", "footer"):
            for w in construct.get(section) or ():
                if not isinstance(w, dict):
                    continue
                idx = w.get("moduleIndex", 0)
                wid = w.get("id", "")
                attrs = w.get("attributes") or {}
                self._collect_widget(
                    attrs, items, seen, shop_id,
                    section=section, module_index=idx, widget_id=wid,
                    path=f".construct.{section}[{idx}]")
        return items

    def _collect_widget(self, node, out, seen, shop_id, *,
                        section, module_index, widget_id, path,
                        material_key="", item_key="",
                        link_url="", link_info=None):
        if isinstance(node, dict):
            mk = node.get("materialKey") or material_key
            ik = node.get("itemKey") or item_key
            lu = node.get("linkUrl") or link_url
            li = node.get("linkInfo") if isinstance(
                node.get("linkInfo"), dict) else link_info
            for k, v in node.items():
                if isinstance(v, str) and _IMG_KEY_RE.search(k):
                    if not v or v in seen:
                        continue
                    seen.add(v)
                    url = f"{_CMS_IMAGE_BASE}/{shop_id}/{v}" if shop_id else ""
                    out.append({
                        "filename"     : v,
                        "url"          : url,
                        "field"        : k,
                        "path"         : path + "." + k,
                        "section"      : section,
                        "module_index" : module_index,
                        "widget_id"    : widget_id,
                        "item_key"     : ik or "",
                        "material_key" : mk or "",
                        "link_url"     : lu or "",
                        "link_info"    : li,
                    })
                else:
                    self._collect_widget(
                        v, out, seen, shop_id,
                        section=section, module_index=module_index,
                        widget_id=widget_id,
                        path=path + "." + k,
                        material_key=mk, item_key=ik,
                        link_url=lu, link_info=li)
        elif isinstance(node, list):
            for i, x in enumerate(node):
                self._collect_widget(
                    x, out, seen, shop_id,
                    section=section, module_index=module_index,
                    widget_id=widget_id,
                    path=f"{path}[{i}]",
                    material_key=material_key, item_key=item_key,
                    link_url=link_url, link_info=link_info)

    @staticmethod
    def _collect_linked_salepages(state):
        ids = set()
        raw = json.dumps(state, ensure_ascii=False)
        for m in re.finditer(r'/SalePage/Index/(\d+)', raw):
            ids.add(int(m.group(1)))
        return ids

    # ------------------------------------------------------------------ #
    # SSR fallback (when PRELOADED_STATE is missing)
    # ------------------------------------------------------------------ #
    def _media_from_ssr(self, html):
        """Walk the SSR HTML for visible <img> sources.

        Returns a list shaped like the state-walker output (so JSON
        consumers see a uniform schema), but only ``filename`` / ``url``
        / ``field=`ssr_img``` are populated.
        """
        seen = set()
        out = []
        for tag in text.extract_iter(html, "<img", ">"):
            src = (text.extr(tag, 'src="', '"') or
                   text.extr(tag, "src='", "'") or
                   text.extr(tag, 'data-src="', '"'))
            if not src or src.startswith("data:"):
                continue
            src = text.unescape(src)
            if src.startswith("//"):
                src = "https:" + src
            if not src.startswith(("http://", "https://")):
                continue
            if any(d in src for d in _TRACKER_DOMAINS):
                continue
            if src in seen:
                continue
            seen.add(src)
            out.append({
                "filename"     : src.rsplit("/", 1)[-1],
                "url"          : src,
                "field"        : "ssr_img",
                "path"         : "",
                "section"      : "",
                "module_index" : 0,
                "widget_id"    : "",
                "item_key"     : "",
                "material_key" : "",
                "link_url"     : "",
                "link_info"    : None,
            })
        og_img = text.extr(
            html, '<meta property="og:image" content="', '"')
        if og_img:
            og_img = text.unescape(og_img)
            if og_img.startswith("//"):
                og_img = "https:" + og_img
            if og_img and og_img not in seen and og_img.startswith(
                    ("http://", "https://")):
                seen.add(og_img)
                out.append({
                    "filename": og_img.rsplit("/", 1)[-1],
                    "url": og_img,
                    "field": "og_image", "path": "",
                    "section": "", "module_index": 0,
                    "widget_id": "", "item_key": "",
                    "material_key": "", "link_url": "",
                    "link_info": None,
                })
        return out

    @staticmethod
    def _ext_for_media(url):
        path = url.partition("?")[0]
        candidate = path.rpartition(".")[2].lower()
        if candidate in ("jpg", "jpeg", "png", "gif", "webp", "svg",
                         "mp4", "webm"):
            return candidate
        return "jpg"
