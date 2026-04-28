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

Settings (under ``extractor.91app.*``)::

    "metadata"  : true   # write the timestamped JSON sidecar
    "externals" : true   # forward external-media URLs as Message.Queue

User config to register a shop without using the prefix::

    {"extractor": {"91app": {"qmomo": {
        "root":    "https://www.qmomo.com.tw",
        "pattern": r"(?:www\\.)?qmomo\\.com\\.tw"
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
        meta = self._normalize(vm)
        primary = self._collect_primary(vm)
        externals = self._collect_externals(vm)

        snapshot = dict(meta)
        snapshot["images"] = [u for u, _ in primary]
        snapshot["external_media"] = [
            {"url": u, "kind": k} for u, k in externals]

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

        if self._yield_externals:
            for url_, kind in externals:
                yield Message.Queue, url_, {"product": meta, "kind": kind}

    # ------------------------------------------------------------------ #
    # ViewModel parsing
    # ------------------------------------------------------------------ #
    def _parse_viewmodel(self, html):
        m = _VIEWMODEL_RE.search(html)
        if not m:
            raise self.exc.AbortExtraction(
                "SalePageIndexViewModel not found in page")
        try:
            return json.loads(m.group(1))
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

    def _collect_externals(self, vm):
        """Pull (url, kind) tuples from MainImageVideo + description bodies.

        ``kind`` is one of ``main_video`` / ``img:<field>`` / ``iframe:<field>``
        — primarily for the JSON sidecar; gallery-dl's dispatcher only sees
        the URL and decides routing by URL pattern.
        """
        out = []
        seen = set()

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
                    if "img.91app.com" in src or src in seen:
                        continue
                    seen.add(src)
                    out.append((src, f"img:{fld}"))
            for tag in text.extract_iter(html, "<iframe", ">"):
                if src := self._extract_src(tag):
                    if src in seen:
                        continue
                    seen.add(src)
                    out.append((src, f"iframe:{fld}"))
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

    # ------------------------------------------------------------------ #
    # Metadata normalization
    # ------------------------------------------------------------------ #
    def _normalize(self, vm):
        miv = vm.get("MainImageVideo") or {}
        miv = miv if isinstance(miv, dict) and miv else None

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


class _91appPageExtractor(_91appExtractor):
    """Extractor for a 91app CMS landing page (``/page/<slug>``)

    Best-effort SSR extraction.  React-rendered widget content is not
    accessible to a pure HTTP client; the JSON sidecar carries
    ``ssr_only=true`` to signal that the snapshot is partial.
    """
    subcategory = "page"
    directory_fmt = ("{category}", "page", "{page_slug}")
    archive_fmt = "{page_slug}_{filename}.{extension}"
    pattern = BASE_PATTERN + r"/page/([\w.-]+)"
    example = "91app:https://example.com/page/some-page"

    def items(self):
        slug = self.groups[-1]
        url = f"{self.root}/page/{slug}"
        page = self.request(url, notfound="page").text

        meta = self._extract_page_meta(page, slug)
        media = self._collect_media(page)

        snapshot = dict(meta)
        snapshot["images"] = list(media)
        snapshot["ssr_only"] = True
        snapshot["note"] = (
            "Widget content on 91app CMS pages is rendered client-side "
            "from a non-public CMS API. This snapshot is limited to "
            "what's in the SSR HTML shell (logo, breadcrumb, og-meta, "
            "any cms-static images baked in by the theme). Full widget "
            "media requires a headless browser.")

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

        for num, img_url in enumerate(media, 1):
            kw = dict(meta)
            kw["num"] = num
            kw["filename"] = f"{num:02d}"
            kw["extension"] = self._ext_for_media(img_url)
            yield Message.Url, img_url, kw

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

    def _collect_media(self, html):
        seen = set()
        urls = []
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
            urls.append(src)
        # og:image as fallback / supplement
        og_img = text.extr(
            html, '<meta property="og:image" content="', '"')
        if og_img:
            og_img = text.unescape(og_img)
            if og_img.startswith("//"):
                og_img = "https:" + og_img
            if og_img not in seen and og_img.startswith(
                    ("http://", "https://")):
                seen.add(og_img)
                urls.append(og_img)
        return urls

    @staticmethod
    def _ext_for_media(url):
        path = url.partition("?")[0]
        candidate = path.rpartition(".")[2].lower()
        if candidate in ("jpg", "jpeg", "png", "gif", "webp", "svg",
                         "mp4", "webm"):
            return candidate
        return "jpg"
