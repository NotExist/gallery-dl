# -*- coding: utf-8 -*-

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.

"""Extractor for amiami.jp / amiami.com product detail pages

Backend: the public ``api-secure.amiami.com/api/v1.0/item`` endpoint
used by amiami's own front-end.  Requests must carry the
``X-User-Key`` header (the ``amiami_dev`` key shipped in their JS
clients); without it the API returns ``Invalid access`` (HTTP 400/403).

Each product yields:

* ``main`` — the hero / cover image
* ``01`` … ``NN`` — the review-image gallery (up to several dozen)
* ``bonus_NN`` — pre-order / first-run bonus illustrations, when any
* ``<UTC-timestamp>.json`` — a timestamped snapshot of the product
  metadata (spec, description, prices, sales status, JAN code, etc.)

The JSON is emitted via gallery-dl's built-in ``text:`` URL scheme — it
is part of the extractor's normal output, not a post-processor.  Image
filenames are stable, so re-running the extractor against the same
product will (a) skip already-downloaded images and (b) drop a fresh
timestamped JSON next to them — useful for tracking price / stock /
status changes over time.

Settings (all under ``extractor.amiami.*``)::

    "lang"     : "eng"        # API language: "eng" (default) or "jpn"
    "metadata" : true         # set false to skip the JSON snapshot
    "bonus"    : true         # include pre-order bonus illustrations
"""

import json
import datetime

from .common import Extractor, Message
from .. import text


class AmiamiProductExtractor(Extractor):
    """Extractor for a single amiami product page"""
    category = "amiami"
    subcategory = "product"
    root = "https://www.amiami.jp"
    directory_fmt = ("{category}", "{gcode}")
    filename_fmt = "{filename}.{extension}"
    archive_fmt = "{gcode}_{filename}.{extension}"
    pattern = (r"(?:https?://)?(?:www\.)?amiami\.(?:jp|com)"
               r"/[^?#]*\?(?:[^#]*&)?gcode=([A-Z0-9_-]+)")
    example = "https://www.amiami.jp/top/detail/detail?gcode=FIGURE-200729"

    API_ROOT = "https://api-secure.amiami.com/api/v1.0"
    API_KEY = "amiami_dev"
    IMG_ROOT = "https://img.amiami.jp"

    def _init(self):
        self._lang = self.config("lang", "eng")
        self._write_metadata = self.config("metadata", True)
        self._include_bonus = self.config("bonus", True)

    def items(self):
        gcode = self.groups[0]
        envelope = self._api_item(gcode)
        if not envelope.get("RSuccess"):
            raise self.exc.AbortExtraction(
                f"amiami API rejected {gcode}: "
                f"{envelope.get('RMessage', 'unknown error')}")
        item = envelope.get("item") or {}
        emb = envelope.get("_embedded") or {}

        meta = self._normalize(item, emb)
        urls = self._collect_urls(item, emb)
        meta["count"] = len(urls)
        meta["images"] = [u for _, u in urls]

        # Snapshot the user-facing fields before yielding Directory,
        # since gallery-dl injects internal keys (_path, _extr, …) into
        # the kwdict it carries forward.
        snapshot = dict(meta)

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

        for num, (label, url) in enumerate(urls, 1):
            kw = dict(meta)
            kw["num"] = num
            ext = url.rpartition(".")[2].lower() or "jpg"
            kw["filename"] = label
            kw["extension"] = ext
            yield Message.Url, url, kw

    # ------------------------------------------------------------------ #
    # API
    # ------------------------------------------------------------------ #
    def _api_item(self, gcode):
        return self.request_json(
            f"{self.API_ROOT}/item",
            params={"gcode": gcode, "lang": self._lang},
            headers={"X-User-Key": self.API_KEY},
            notfound="product",
        )

    # ------------------------------------------------------------------ #
    # Image collection
    # ------------------------------------------------------------------ #
    def _collect_urls(self, item, emb):
        urls = []
        if main := item.get("main_image_url"):
            urls.append(("main", self._abs(main)))
        for i, img in enumerate(emb.get("review_images") or (), 1):
            if u := (img or {}).get("image_url"):
                urls.append((f"{i:02d}", self._abs(u)))
        if self._include_bonus:
            for i, img in enumerate(emb.get("bonus_images") or (), 1):
                if u := (img or {}).get("image_url"):
                    urls.append((f"bonus_{i:02d}", self._abs(u)))
        return urls

    def _abs(self, path):
        if path.startswith(("http://", "https://")):
            return path
        return self.IMG_ROOT + path

    # ------------------------------------------------------------------ #
    # Metadata normalization
    # ------------------------------------------------------------------ #
    def _normalize(self, item, emb):
        cates = []
        for i in range(1, 8):
            v = item.get(f"cate{i}")
            if isinstance(v, list):
                cates.extend(v)
            elif v:
                cates.append(v)

        return {
            "category"          : self.category,
            "gcode"             : item.get("gcode"),
            "scode"             : item.get("scode"),
            # 名稱（英 / 日 / 簡稱）
            "name"              : item.get("gname")
                                  or item.get("sname_simple") or "",
            "name_full"         : item.get("sname") or "",
            "name_jp"           : item.get("sname_simple_j") or "",
            "name_short"        : item.get("sname_simple") or "",
            "name_sub"          : item.get("gname_sub") or "",
            # 式樣 / 解說 / 備註
            "spec"              : item.get("spec") or "",
            "memo"              : item.get("memo") or "",
            "image_comment"     : item.get("image_comment") or "",
            "remarks"           : item.get("remarks") or "",
            "preorder_attention": item.get("preorderattention") or "",
            # 分類
            "categories"        : cates,
            "makers"            : emb.get("makers") or [],
            "series_titles"     : emb.get("series_titles") or [],
            "original_titles"   : emb.get("original_titles") or [],
            "character_names"   : emb.get("character_names") or [],
            # 參考 / 販售價格
            "list_price"        : item.get("list_price"),
            "price"             : item.get("price"),
            "price_taxed"       : item.get("c_price_taxed"),
            "discount_rate"     : item.get("discountrate1"),
            "point"             : item.get("point"),
            # 販售狀態
            "salestatus"        : item.get("salestatus") or "",
            "salestatus_detail" : item.get("salestatus_detail") or "",
            "stock"             : item.get("stock"),
            "soldout"           : bool(item.get("soldout_flg")),
            "preorder"          : bool(item.get("preorderitem")),
            "instock"           : bool(item.get("instock_flg")),
            "onsale"            : bool(item.get("onsale_flg")),
            "saleitem"          : bool(item.get("saleitem")),
            "newitem"           : bool(item.get("newitem")),
            "resale"            : bool(item.get("resale_flg")),
            "end"               : bool(item.get("end_flg")),
            "amiami_limited"    : bool(item.get("amiami_limited")),
            "domesticitem"      : bool(item.get("domesticitem")),
            "releasedate"       : item.get("releasedate") or "",
            "releasechange_text": item.get("releasechange_text") or "",
            # 標籤 / 條碼
            "agelimit"          : item.get("agelimit"),
            "jancode"           : item.get("jancode") or "",
            "maker_name"        : item.get("maker_name") or "",
            "modeler"           : item.get("modeler") or "",
            "modelergroup"      : item.get("modelergroup") or "",
            "copyright"         : item.get("copyright") or "",
            # 連結 / 抓取時間
            "url"               : (
                f"{self.root}/top/detail/detail?"
                f"gcode={item.get('gcode')}"),
            "fetched_at"        : datetime.datetime.now(
                datetime.timezone.utc).isoformat(),
        }
