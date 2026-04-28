# -*- coding: utf-8 -*-

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.

"""Generic extractor for shops running on the 91app SaaS platform

91app powers a large family of Taiwan-based e-commerce sites
(``www.qmomo.com.tw``, ``www.peachjohn.com.tw``, ``www.miniqueen.tw`` …).
Their product (SalePage) URLs all follow ``<host>/SalePage/Index/<id>``
and the rendered HTML embeds the full product data inline as
``window.ServerRenderData["SalePageIndexViewModel"] = {…}`` — that
single JSON blob feeds this extractor.

This module is site-agnostic and ships with **no** instances
pre-registered.  Reach a 91app shop via either:

* the ``91app:<host-url>/SalePage/Index/<id>`` URL prefix, or
* a per-site entry under ``extractor.91app.<category>`` in your
  gallery-dl config (see the snippet at the bottom of this file).

What gets extracted:

* All ``ImageList[].PicUrl`` images (img.91app.com CDN)
* A UTC-timestamped JSON sidecar per run, mirroring the amiami pattern,
  so re-running the same product accumulates a price/stock/promotion
  history without overwriting earlier snapshots
* Any external media referenced from the description fields or from
  ``MainImageVideo.VideoUrl`` (typically YouTube product videos or
  91app CMS-static images).  These are recorded under
  ``external_media`` in the JSON and yielded as ``Message.Queue`` so
  whichever sibling extractor matches the URL pattern (or yt-dlp via
  ``ytdl:`` configuration) picks them up; they download to that
  extractor's own directory tree, not the 91app one.

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
* 91app CMS landing pages (``<host>/page/<slug>``) — editorial pages,
  not product SalePages
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
