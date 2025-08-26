import json
import re
from typing import Iterable, Optional

import scrapy
from locations.categories import Categories, apply_category
from locations.dict_parser import DictParser
from locations.items import Feature


class SahamBankMASpider(scrapy.Spider):
    name = "saham_bank_ma"
    allowed_domains = ["sahambank.com", "sgmaroc.com"]
    # Primary store locator (Saham Bank, ex-SG Maroc)
    start_urls = ["https://www.sahambank.com/trouver-une-agence/"]

    # If you discover a direct JSON endpoint while inspecting the page/network tab,
    # set it here and the spider will request it first.
    API_ENDPOINT: Optional[str] = None

    item_attributes = {"brand": "Saham Bank"}  # add brand_wikidata once confirmed

    custom_settings = {
        # The page might be heavy; raise limits a bit and be polite.
        "DOWNLOAD_TIMEOUT": 30,
        "CONCURRENT_REQUESTS": 4,
    }

    def start_requests(self) -> Iterable[scrapy.Request]:
        if self.API_ENDPOINT:
            yield scrapy.Request(self.API_ENDPOINT, callback=self.parse_api)
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_locator)

    # --- Path 1: clean JSON API (if present) ---------------------------------
    def parse_api(self, response, **kwargs):
        data = response.json()
        # Try to find a typical list/array of locations.
        # Adjust keys after inspecting actual API response.
        candidates = []
        if isinstance(data, dict):
            for key in ("locations", "agences", "agencies", "items", "branches", "pois"):
                if key in data and isinstance(data[key], list):
                    candidates = data[key]
                    break
        elif isinstance(data, list):
            candidates = data
        for raw in candidates:
            item = self._dict_to_item(raw)
            if item:
                yield item

    # --- Path 2: scrape embedded JSON from the locator page -------------------
    def parse_locator(self, response, **kwargs):
        # Strategy:
        # 1) Look for <script type="application/json"> or window.__NUXT__/__NEXT_DATA__ payloads
        # 2) Fallback: try to find a JS var containing an array of agencies with coords
        scripts = response.xpath(
            "//script[@type='application/json' or @type='application/ld+json']/text()"
        ).getall()
        scripts += response.xpath("//script/text()").getall()

        locations = []
        for s in scripts:
            # Common SSR payloads
            for pat in (
                r"window\.__NUXT__\s*=\s*({.*?});\s*</",
                r"__NEXT_DATA__\"\s*:\s*({.*?})\s*[,<]",
                r"window\.__APOLLO_STATE__\s*=\s*({.*?});",
                r"window\.__INITIAL_STATE__\s*=\s*({.*?});",
            ):
                for m in re.finditer(pat, s, flags=re.DOTALL):
                    try:
                        blob = json.loads(self._json_sanitize(m.group(1)))
                        locations.extend(self._extract_locations_from_blob(blob))
                    except Exception:
                        pass
            # Heuristic: any JSON array with objects that look like agencies (has name & city)
            for m in re.finditer(r"(\[\s*\{.*?\}\s*\])", s, flags=re.DOTALL):
                try:
                    arr = json.loads(self._json_sanitize(m.group(1)))
                    if isinstance(arr, list) and arr and isinstance(arr[0], dict):
                        sample = arr[0]
                        if any(k in sample for k in ("name", "nom", "title")) and any(
                            k in sample for k in ("city", "ville", "locality")
                        ):
                            locations.extend(arr)
                except Exception:
                    pass

        seen = set()
        for raw in locations:
            item = self._dict_to_item(raw)
            if not item:
                continue
            # de-dupe by (lat,lon,name) if no ref yet
            key = (
                item.get("ref")
                or (
                    str(item.get("lat")),
                    str(item.get("lon")),
                    item.get("name") or item.get("branch"),
                )
            )
            if key in seen:
                continue
            seen.add(key)
            yield item

    # --- Helpers --------------------------------------------------------------
    def _json_sanitize(self, text: str) -> str:
        # Trim unsafe trailing characters
        text = text.strip().rstrip(";")
        # Remove trailing </script if captured
        text = re.sub(r"</script>$", "", text.strip(), flags=re.IGNORECASE)
        return text

    def _extract_locations_from_blob(self, blob) -> list:
        """
        Walk nested dicts/lists and collect arrays that look like location records.
        """
        found = []

        def walk(node):
            if isinstance(node, list) and node and isinstance(node[0], dict):
                sample = node[0]
                # Look for typical keys used by French/Moroccan bank locators
                keys = set(k.lower() for k in sample.keys())
                if (
                    {"name", "lat", "lng"} <= keys
                    or {"nom", "latitude", "longitude"} <= keys
                    or {"title", "coordinates"} <= keys
                    or {"adresse", "ville"} <= keys
                ):
                    found.extend(node)
            if isinstance(node, dict):
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for v in node:
                    walk(v)

        walk(blob)
        return found

    def _dict_to_item(self, raw: dict) -> Optional[Feature]:
        # Normalize lat/lon fields from various key spellings
        lat = raw.get("lat") or raw.get("latitude") or raw.get("Latitude")
        lon = raw.get("lng") or raw.get("lon") or raw.get("longitude") or raw.get("Longitude")
        if isinstance(lat, str):
            lat = lat.replace(",", ".")
        if isinstance(lon, str):
            lon = lon.replace(",", ".")
        item = DictParser.parse(raw)

        # Name/branch cleanup (Saham Bank | Agence XYZ -> XYZ)
        name = item.get("name") or raw.get("name") or raw.get("nom") or raw.get("title")
        if name:
            # common patterns: "Agence <CITY>", "Saham Bank - <BRANCH>"
            branch = re.sub(r"(?i)\b(saham\s*bank)\b\s*[-–:]\s*", "", name).strip()
            branch = re.sub(r"(?i)\bagence\s+", "", branch).strip()
            item["branch"] = branch
            item.pop("name", None)

        # Address fields
        item["street_address"] = (
            item.get("street_address")
            or raw.get("address")
            or raw.get("adresse")
            or raw.get("street")
        )
        item["city"] = item.get("city") or raw.get("city") or raw.get("ville")
        item["state"] = item.get("state") or raw.get("state") or raw.get("region")
        item["postcode"] = item.get("postcode") or raw.get("postcode") or raw.get("cp") or raw.get("zip")

        # Contact
        item["phone"] = item.get("phone") or raw.get("phone") or raw.get("tel") or raw.get("telephone")
        item["email"] = item.get("email") or raw.get("email")

        # Geo
        if lat and lon:
            try:
                item["lat"] = float(lat)
                item["lon"] = float(lon)
            except Exception:
                pass

        # IDs
        item["ref"] = (
            raw.get("id")
            or raw.get("ref")
            or raw.get("code")
            or raw.get("slug")
            or f"{item.get('city','')}-{item.get('branch','')}".strip("-")
        )

        # Category
        apply_category(Categories.BANK, item)

        return item
