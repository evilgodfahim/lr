#!/usr/bin/env python3
"""
RSS Feed Processor — Geopolitics Pipeline

All articles from all feeds go to one Gemini call.
Gemini classifies each headline into signal or noise.
A Gemini call deduplicates near-identical signal titles.

Outputs:
  curated_feed.xml  - signal articles
Stats:
  fetch_stats.json
"""

import feedparser
import json
import os
import time
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET
from google import genai
from mistralai.client import Mistral
from email.utils import parsedate_to_datetime, format_datetime
from urllib.parse import urljoin, urlparse

import requests

try:
    from dateutil import parser as dateutil_parser
except Exception:
    dateutil_parser = None

# -- FEEDS ---------------------------------------------------------------------

FEED_URLS = [
    "https://asiatimes.com/feed/",
    "https://politepaul.com/fd/TefnRxuxFzO0.xml",
    "https://evilgodfahim.github.io/csis/rss.xml",
    "https://evilgodfahim.github.io/intop/filtered.xml",
    "https://politepaul.com/fd/M8QukWfUPWR4.xml",
    "https://politepaul.com/fd/BI4f9BiCvoed.xml",
    "https://www.thecipherbrief.com/feed",
    "https://www.bellingcat.com/feed/",
    "https://rusi.org/rss/latest-publications.xml",
    "https://www.spytalk.co/feed/",
    "https://www.defenseone.com/rss/all/",
    "https://politepaul.com/fd/9RMAFvRRGLst.xml",
    "https://www.globalpolicyjournal.com/blog/author/%2A/feed",
    "https://www.e-ir.info/feed/",
    "https://www.theglobalist.com/feed/",
    "https://responsiblestatecraft.org/feed/",
    "https://politepaul.com/fd/ffERiOdKxWlq.xml",
    "https://politepaul.com/fd/dCWMZKe7BJqi.xml",
    "https://politepaul.com/fd/YJRa9YOT7CyB.xml",
    "https://meduza.io/rss/en/all",
    "https://politepaul.com/fd/JsMAwSx6Pkbr.xml",
    "https://evilgodfahim.github.io/alm/combined.xml",
    "https://evilgodfahim.github.io/start/combined.xml",
    "https://politepaul.com/fd/GbcosKoaAE22.xml",
    "https://www.noemamag.com/article-topic/geopolitics-globalization/feed/",
    "https://zeihan.com/feed/",
    "https://politepaul.com/fd/ELc5hcluIkDO.xml",
    "https://original.antiwar.com/feed/",
    "https://www.atlanticcouncil.org/feed/",
    "https://warontherocks.com/feed/",
    "https://www.thehindu.com/opinion/editorial/?service=rss",
    "https://politepaul.com/fd/aCEp2lWYu3Jn.xml",
    "https://evilgodfahim.github.io/fto/combined.xml",
    "https://evilgodfahim.github.io/nytop/combined.xml",
    "https://theconversation.com/global/home-page.atom",
    "https://politepaul.com/fd/R39To2fYhqqO.xml",
    "https://evilgodfahim.github.io/lemonde/combined.xml",
    "https://eurasiantimes.com/feed/",
    "http://www.irinnews.org/rss/conflict.xml",
    "https://www.bloomberg.com/politics/feeds/site.xml",
    "https://saiia.org.za/thematic-area/foreign-policy/feed/",
    "https://www.vtforeignpolicy.com/feed/",
    "https://medium.com/feed/tag/foreign-policy",
    "https://www.hrw.org/taxonomy/term/9653/feed",
    "https://theconversation.com/us/topics/geopolitics-4230/articles.atom",
    "https://geopoliticaleconomy.substack.com/feed",
    "https://www.newgeopolitics.org/feed/",
    "https://ipdefenseforum.com/feed/",
    "https://www.nytimes.com/svc/collections/v1/publish/",
    "https://www.thenewhumanitarian.org/rss/all.xml",
    "https://feeds.feedburner.com/LongWarJournalSiteWide",
    "https://gulfif.org/feed/",
    "https://ecfr.eu/feed/",
    "https://www.spiegel.de/international/index.rss",
    "https://mondediplo.com/backend",
    "https://eng.globalaffairs.ru/rss",
    "https://www.ft.com/geopolitics",
    "https://ddgeopolitics.substack.com/feed",
    "https://knowledge.skema.edu/tag/geopolitics/feed/",
    "https://lansinginstitute.org/category/geopolitics/feed/",
    "https://geopolitics.co/feed/",
    "https://feeds.feedburner.com/worldpoliticsreview",
    "https://www.worldpoliticsreview.com/feed/",   # added
    "https://www.rand.org/blog.xml",
    "https://thegeopolitics.com/feed/",
    "https://fpif.org/feed/",
    "https://www.fpri.org/feed/",
    "https://www.chathamhouse.org/path/whatsnew.xml",
    "https://www.politico.eu/section/foreign-affairs/feed/",
    "https://www.moonofalabama.org/atom.xml",
    "https://southfront.press/feed/",
    "https://geopoliticaleconomy.com/feed/",
    "https://geopoliticsreport.substack.com/feed",
    "https://www.modadgeopolitics.com/feed",
    "https://geopoliticsagi.substack.com/feed",
    "https://katehon.com/en/rss.xml",
    "https://www.theguardian.com/us/commentisfree/rss",
    "https://evilgodfahim.github.io/intop/filtered.xml",
    "https://blogs.timesofindia.indiatimes.com/feed/defaultrss",
    "https://indianexpress.com/section/explained/feed/",
    "https://indianexpress.com/section/opinion/editorials/feed/",
    "https://indianexpress.com/section/opinion/feed/",
    "https://www.thehindu.com/opinion/?service=rss",
    "https://www.thehindu.com/opinion/editorial/?service=rss",
    "https://www.hindustantimes.com/feeds/rss/opinion/rssfeed.xml",
    "https://feeds.feedburner.com/Consortiumnewscom",
    "https://evilgodfahim.github.io/org/daily_feed.xml",
    "https://www.eiu.com/n/feed/",
    "https://www.lowyinstitute.org/the-interpreter/rss.xml",
    "https://feeds.feedburner.com/AtlanticInternational",
]

EXISTING_API_FEEDS = {
    "https://asiatimes.com/feed/",
    "https://politepaul.com/fd/TefnRxuxFzO0.xml",
    "https://evilgodfahim.github.io/csis/rss.xml",
    "https://evilgodfahim.github.io/intop/filtered.xml",
    "https://politepaul.com/fd/M8QukWfUPWR4.xml",
    "https://politepaul.com/fd/BI4f9BiCvoed.xml",
    "https://www.thecipherbrief.com/feed",
    "https://www.bellingcat.com/feed/",
    "https://rusi.org/rss/latest-publications.xml",
    "https://www.spytalk.co/feed/",
    "https://www.defenseone.com/rss/all/",
    "https://politepaul.com/fd/9RMAFvRRGLst.xml",
    "https://www.globalpolicyjournal.com/blog/author/%2A/feed",
    "https://www.e-ir.info/feed/",
    "https://www.theglobalist.com/feed/",
    "https://responsiblestatecraft.org/feed/",
    "https://politepaul.com/fd/ffERiOdKxWlq.xml",
    "https://politepaul.com/fd/dCWMZKe7BJqi.xml",
    "https://politepaul.com/fd/YJRa9YOT7CyB.xml",
    "https://meduza.io/rss/en/all",
    "https://politepaul.com/fd/JsMAwSx6Pkbr.xml",
    "https://evilgodfahim.github.io/alm/combined.xml",
    "https://evilgodfahim.github.io/start/combined.xml",
    "https://politepaul.com/fd/GbcosKoaAE22.xml",
    "https://www.noemamag.com/article-topic/geopolitics-globalization/feed/",
    "https://zeihan.com/feed/",
    "https://politepaul.com/fd/ELc5hcluIkDO.xml",
    "https://original.antiwar.com/feed/",
    "https://www.atlanticcouncil.org/feed/",
    "https://warontherocks.com/feed/",
    "https://www.thehindu.com/opinion/editorial/?service=rss",
    "https://politepaul.com/fd/aCEp2lWYu3Jn.xml",
    "https://evilgodfahim.github.io/fto/combined.xml",
    "https://evilgodfahim.github.io/nytop/combined.xml",
    "https://theconversation.com/global/home-page.atom",
    "https://politepaul.com/fd/R39To2fYhqqO.xml",
    "https://evilgodfahim.github.io/lemonde/combined.xml",
    "https://eurasiantimes.com/feed/",
    "http://www.irinnews.org/rss/conflict.xml",
    "https://www.bloomberg.com/politics/feeds/site.xml",
    "https://saiia.org.za/thematic-area/foreign-policy/feed/",
    "https://www.vtforeignpolicy.com/feed/",
    "https://medium.com/feed/tag/foreign-policy",
    "https://www.hrw.org/taxonomy/term/9653/feed",
    "https://theconversation.com/us/topics/geopolitics-4230/articles.atom",
    "https://geopoliticaleconomy.substack.com/feed",
    "https://www.newgeopolitics.org/feed/",
    "https://ipdefenseforum.com/feed/",
    "https://www.nytimes.com/svc/collections/v1/publish/",
    "https://www.thenewhumanitarian.org/rss/all.xml",
    "https://feeds.feedburner.com/LongWarJournalSiteWide",
    "https://gulfif.org/feed/",
    "https://ecfr.eu/feed/",
    "https://www.spiegel.de/international/index.rss",
    "https://mondediplo.com/backend",
    "https://eng.globalaffairs.ru/rss",
    "https://www.ft.com/geopolitics",
    "https://ddgeopolitics.substack.com/feed",
    "https://knowledge.skema.edu/tag/geopolitics/feed/",
    "https://lansinginstitute.org/category/geopolitics/feed/",
    "https://geopolitics.co/feed/",
    "https://feeds.feedburner.com/worldpoliticsreview",
    "https://www.worldpoliticsreview.com/feed/",   # added
    "https://www.rand.org/blog.xml",
    "https://thegeopolitics.com/feed/",
    "https://fpif.org/feed/",
    "https://www.fpri.org/feed/",
    "https://www.chathamhouse.org/path/whatsnew.xml",
    "https://www.politico.eu/section/foreign-affairs/feed/",
    "https://www.moonofalabama.org/atom.xml",
    "https://southfront.press/feed/",
    "https://geopoliticaleconomy.com/feed/",
    "https://geopoliticsreport.substack.com/feed",
    "https://www.modadgeopolitics.com/feed",
    "https://geopoliticsagi.substack.com/feed",
    "https://katehon.com/en/rss.xml",
    "https://www.theguardian.com/us/commentisfree/rss",
    "https://evilgodfahim.github.io/intop/filtered.xml",
    "https://blogs.timesofindia.indiatimes.com/feed/defaultrss",
    "https://indianexpress.com/section/explained/feed/",
    "https://indianexpress.com/section/opinion/editorials/feed/",
    "https://indianexpress.com/section/opinion/feed/",
    "https://www.thehindu.com/opinion/?service=rss",
    "https://www.thehindu.com/opinion/editorial/?service=rss",
    "https://www.hindustantimes.com/feeds/rss/opinion/rssfeed.xml",
    "https://feeds.feedburner.com/Consortiumnewscom",
    "https://evilgodfahim.github.io/org/daily_feed.xml",
    "https://www.eiu.com/n/feed/",
    "https://www.lowyinstitute.org/the-interpreter/rss.xml",
    "https://feeds.feedburner.com/AtlanticInternational",
}

KL_API_FEEDS = set()

# -- CONFIG --------------------------------------------------------------------

DEDUP_MODEL           = "gemini-3-flash-preview"
MISTRAL_MODEL         = "gemini-3-flash-preview"
PROCESSED_FILE        = "processed_articles.json"
SELECTED_FILE         = "selected_articles.json"
OUTPUT_XML            = "curated_feed.xml"
STATS_FILE            = "fetch_stats.json"
MAX_ARTICLES_PER_FEED = 100
MAX_AGE_HOURS         = 10
ALLOW_MISSING_DATES   = True
ALLOW_OLDER           = False
MAX_FEED_ITEMS        = 500
# Set this env var to your deployed feed URL to add atom:link self-reference
# e.g. https://user.github.io/repo/curated_feed.xml
FEED_SELF_URL         = os.environ.get("FEED_SELF_URL", "")

# -- PROMPT --------------------------------------------------------------------

PROMPT = """You are a strict geopolitics-only headline filter.

Classify each headline into exactly one bucket:
- SIGNAL: only core geopolitical significance.
- NOISE: everything else.

Use the HIGHEST possible bar. Be conservative. Prefer NOISE unless the headline clearly and directly matters to international power, conflict, diplomacy, security, alliances, sanctions, war, deterrence, borders, major regime change, or major cross-border economic/strategic shifts.

SIGNAL rules:
- Must be core geopolitical significance, not just "important news".
- Must involve major states, alliances, wars, crises, sanctions, diplomacy, intelligence, defense, strategic competition, energy security, trade war with major global impact, or Bangladesh only when it has clear national-scale geopolitical consequence.
- Local incidents, routine statements, domestic politics, routine elections, business, markets, culture, lifestyle, sports, celebrity, crime, and human-interest stories are NOISE.
- Opinion, commentary, explainers, or analysis are SIGNAL only if they are clearly about a major geopolitical issue with broad international relevance.
- Any non-Bangladesh country's internal politics or policy is NOISE unless it directly affects a major geopolitical balance or cross-border crisis.
- Do not mark something as SIGNAL just because it mentions a country, a conflict, or a famous person.

Hard exclusions:
- If the title is only about domestic politics, domestic policy, routine diplomatic remarks, market commentary, business updates, protests, accidents, crime, disasters without cross-border strategic meaning, or general opinion writing, classify as NOISE.
- If uncertain, choose NOISE.
- Omit all NOISE indices entirely.
- Use only the headline text.
- Indices are 0-based.
- Return only valid JSON. No markdown, no backticks, no preamble.

Examples:
Input: ["US and China sign landmark trade agreement", "Premier League club sacks manager", "How the Ottoman Empire collapsed", "Bangladesh central bank raises interest rates amid inflation crisis", "UK Conservative Party elects new leader", "UN warns of imminent famine across the Horn of Africa"]
Output: {{"signal": [0, 3, 5]}}

Input: ["India and Pakistan exchange fire across Line of Control", "Dhaka garment workers strike shuts down hundreds of factories", "Australia holds federal election", "Celebrity couple announces divorce", "IMF approves emergency loan for Bangladesh", "NATO approves new eastern flank forces"]
Output: {{"signal": [0, 1, 4, 5]}}

Input: ["Gaza ceasefire collapses as fighting resumes", "Bangladesh government slashes fuel subsidies nationwide", "A deep dive into the life of a Sundarbans honey collector", "France passes new immigration law", "How microplastics are entering the human bloodstream", "US imposes sanctions on Iranian oil exports"]
Output: {{"signal": [0, 1, 5]}}

Article titles:
{titles}
"""

DEDUP_PROMPT = """You are a news deduplication engine. You will receive a numbered list of article titles.
Your task: identify groups of titles that cover the same story or event (near-duplicates, rephrased versions, or very similar headlines). For each such group, keep only the FIRST occurrence (lowest index) and discard the rest.
Titles that cover clearly distinct topics must all be kept.

Rules:
- Return only the indices (0-based) of titles to KEEP, as a JSON array of integers.
- Always keep at least one title from each duplicate group (the one with the lowest index).
- If all titles are unique, return all indices.
- Return only valid JSON. No markdown, no backticks, no preamble. Example output: [0, 1, 3, 5]

Article titles:
{titles}
"""

# -- CONSTANTS -----------------------------------------------------------------

MEDIA_NS  = "http://search.yahoo.com/mrss/"
MEDIA_TAG = "{%s}" % MEDIA_NS
ET.register_namespace("media", MEDIA_NS)

ATOM_NS  = "http://www.w3.org/2005/Atom"
ATOM_TAG = "{%s}" % ATOM_NS
ET.register_namespace("atom", ATOM_NS)

STATS = {
    "per_feed":             {},
    "per_method":           {"KL": 0, "DIRECT": 0},
    "total_fetched":        0,
    "total_passed_age":     0,
    "total_new":            0,
    "total_signal":         0,
    "total_signal_deduped": 0,
    "timestamp":            None,
}

# -- I/O -----------------------------------------------------------------------

def load_processed_articles():
    if Path(PROCESSED_FILE).exists():
        try:
            with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {
                "article_ids":   data.get("article_ids", []),
                "article_links": data.get("article_links", []),
                "last_updated":  data.get("last_updated"),
            }
        except Exception:
            pass
    return {"article_ids": [], "article_links": [], "last_updated": None}


def save_processed_articles(data):
    data["article_ids"]   = list(dict.fromkeys(data.get("article_ids", [])))
    data["article_links"] = list(dict.fromkeys(data.get("article_links", [])))
    data["last_updated"]  = datetime.utcnow().isoformat()
    with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def save_selected_articles(articles):
    existing = []
    if Path(SELECTED_FILE).exists():
        try:
            with open(SELECTED_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass
    existing_links = {a.get("link") for a in existing}
    merged = existing + [a for a in articles if a.get("link") not in existing_links]
    with open(SELECTED_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)


def save_stats():
    STATS["timestamp"] = datetime.utcnow().isoformat()
    existing = {}
    if Path(STATS_FILE).exists():
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass
    existing.update(STATS)
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

# -- UTILITIES -----------------------------------------------------------------

def normalize_link(link, base=None):
    if not link:
        return ""
    link = link.strip()
    if link.startswith("//"):
        link = "https:" + link
    if base and not urlparse(link).netloc:
        link = urljoin(base, link)
    link = re.sub(r"([?&])utm_[^=]+=[^&]+", r"\1", link)
    link = re.sub(r"([?&])fbclid=[^&]+",    r"\1", link)
    link = re.sub(r"[?&]$", "", link)
    return link.split("#")[0]


def parse_date(entry):
    for key in ("published_parsed", "updated_parsed", "created_parsed", "issued_parsed"):
        st = entry.get(key)
        if st:
            try:
                dt = datetime.fromtimestamp(time.mktime(st), tz=timezone.utc)
                return dt, False
            except Exception:
                pass
    for key in ("published", "updated", "created", "dc_date", "issued"):
        val = entry.get(key)
        if isinstance(val, str) and val.strip():
            try:
                dt = parsedate_to_datetime(val)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc), False
            except Exception:
                pass
            if dateutil_parser:
                try:
                    dt = dateutil_parser.parse(val)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc), False
                except Exception:
                    pass
    if ALLOW_MISSING_DATES:
        return datetime.now(timezone.utc), True
    return None, False


IMG_SRC_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.I)


def find_image_in_html(html, base=None):
    if not html:
        return None
    m = IMG_SRC_RE.search(html)
    if not m:
        return None
    return normalize_link(m.group(1).strip(), base=base)


def get_mime_for_url(url):
    if not url:
        return "image/jpeg"
    path = urlparse(url).path.lower()
    if path.endswith(".png"):  return "image/png"
    if path.endswith(".gif"):  return "image/gif"
    if path.endswith(".webp"): return "image/webp"
    if path.endswith(".svg"):  return "image/svg+xml"
    return "image/jpeg"


def extract_image_url(entry, base_link=None):
    mt = entry.get("media_thumbnail")
    if mt:
        if isinstance(mt, list) and mt[0].get("url"):
            return normalize_link(mt[0]["url"], base=base_link)
        if isinstance(mt, dict) and mt.get("url"):
            return normalize_link(mt["url"], base=base_link)

    mc = entry.get("media_content")
    if mc:
        if isinstance(mc, list) and mc[0].get("url"):
            return normalize_link(mc[0]["url"], base=base_link)
        if isinstance(mc, dict) and mc.get("url"):
            return normalize_link(mc["url"], base=base_link)

    enc = entry.get("enclosures")
    if enc and isinstance(enc, list):
        for e in enc:
            href = e.get("href") or e.get("url") or e.get("link")
            typ  = e.get("type", "")
            if href and (typ.startswith("image/") or re.search(r'\.(jpg|jpeg|png|gif|webp|svg)$', href, re.I)):
                return normalize_link(href, base=base_link)

    links = entry.get("links")
    if links and isinstance(links, list):
        for l in links:
            if l.get("rel") == "enclosure":
                href = l.get("href")
                if href:
                    return normalize_link(href, base=base_link)

    content = entry.get("content")
    if content:
        if isinstance(content, list):
            for c in content:
                if isinstance(c, dict) and c.get("value"):
                    found = find_image_in_html(c.get("value"), base=base_link)
                    if found:
                        return found
        elif isinstance(content, str):
            found = find_image_in_html(content, base=base_link)
            if found:
                return found

    for key in ("summary", "description", "summary_detail", "description_detail"):
        val = entry.get(key)
        if isinstance(val, dict):
            val = val.get("value")
        if isinstance(val, str) and val:
            found = find_image_in_html(val, base=base_link)
            if found:
                return found
    return None

# -- FETCHING ------------------------------------------------------------------

def fetch_via_kl(kl_endpoint, target_feed_url, timeout=20):
    if not kl_endpoint:
        return None
    headers = {"Content-Type": "application/json", "Accept": "application/xml, text/xml, */*"}
    payload = {"url": target_feed_url}
    try:
        resp = requests.post(kl_endpoint, json=payload, headers=headers, timeout=timeout)
        if resp.status_code == 200 and resp.text:
            return feedparser.parse(resp.text)
    except Exception:
        pass
    try:
        resp = requests.get(kl_endpoint, params={"url": target_feed_url}, headers=headers, timeout=timeout)
        if resp.status_code == 200 and resp.text:
            return feedparser.parse(resp.text)
    except Exception:
        pass
    return None


def fetch_feed(url):
    url_norm    = url.strip()
    method_used = "DIRECT"

    if url_norm in EXISTING_API_FEEDS:
        feed        = feedparser.parse(url_norm)
        method_used = "DIRECT"
    elif url_norm in KL_API_FEEDS:
        kl_endpoint = os.environ.get("KL")
        feed        = None
        if kl_endpoint:
            feed = fetch_via_kl(kl_endpoint, url_norm)
            if feed:
                method_used = "KL"
        if not feed:
            feed        = feedparser.parse(url_norm)
            method_used = "DIRECT"
    else:
        feed        = feedparser.parse(url_norm)
        method_used = "DIRECT"

    entries_count = len(getattr(feed, "entries", []))
    STATS["per_feed"].setdefault(url_norm, {"fetched": 0, "passed_age": 0, "capped": 0})
    STATS["per_feed"][url_norm]["fetched"] += entries_count
    STATS["per_method"].setdefault(method_used, 0)
    STATS["per_method"][method_used] += entries_count
    STATS["total_fetched"]            += entries_count

    return feed


def fetch_all_feeds():
    now        = datetime.now(timezone.utc)
    cutoff     = now - timedelta(hours=MAX_AGE_HOURS)
    bd_now     = datetime.now(timezone(timedelta(hours=6)))
    all_articles = []

    for url in FEED_URLS:
        feed       = fetch_feed(url)
        feed_items = []

        for e in feed.entries:
            dt, inferred = parse_date(e)
            if not dt:
                continue
            if (not ALLOW_OLDER) and dt < cutoff:
                continue

            desc = ""
            if e.get("summary"):
                desc = e.get("summary")
            elif e.get("description"):
                desc = e.get("description")
            elif e.get("content") and isinstance(e.get("content"), list):
                desc = "\n".join([c.get("value", "") for c in e.get("content") if isinstance(c, dict)])
            else:
                det = e.get("summary_detail") or e.get("description_detail")
                if isinstance(det, dict):
                    desc = det.get("value", "") or ""

            link       = normalize_link(e.get("link") or "")
            article_id = e.get("id") or link or ""
            image_url  = extract_image_url(e, base_link=link)

            article = {
                "id":          str(article_id),
                "title":       e.get("title", "") or "",
                "link":        link,
                "description": desc or "",
                "published":   format_datetime(dt),
                "source":      url,
            }
            if inferred:
                article["published_inferred"] = True
            if image_url:
                article["thumbnail"]      = image_url
                article["thumbnail_type"] = get_mime_for_url(image_url)

            feed_items.append(article)

        passed = len(feed_items)
        capped = min(passed, MAX_ARTICLES_PER_FEED)
        STATS["per_feed"][url]["passed_age"] = passed
        STATS["per_feed"][url]["capped"]     = capped
        STATS["total_passed_age"]           += passed
        all_articles.extend(feed_items[:MAX_ARTICLES_PER_FEED])

    return all_articles


def get_new_articles(all_articles, processed_data):
    processed_ids   = set(processed_data.get("article_ids", []))
    processed_links = set(processed_data.get("article_links", []))
    new = []
    for a in all_articles:
        aid   = a.get("id")
        alink = a.get("link")
        if (aid and aid not in processed_ids) and (alink and alink not in processed_links):
            new.append(a)
        elif alink and alink not in processed_links and aid not in processed_ids:
            new.append(a)
    return new

# -- CLASSIFICATION ------------------------------------------------------------

def extract_json_object(text):
    text = text.replace("```json", "").replace("```", "").strip()
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        try:
            obj = json.loads(match.group(0))
            if isinstance(obj, dict):
                return {
                    "signal": [i for i in obj.get("signal", []) if isinstance(i, int)],
                }
        except Exception:
            pass
    result = {"signal": []}
    m = re.search(r'"signal"\s*:\s*(\[.*?\])', text, flags=re.DOTALL)
    if m:
        try:
            result["signal"] = [i for i in json.loads(m.group(1)) if isinstance(i, int)]
        except Exception:
            pass
    return result


def send_to_mistral(articles):
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key or not articles:
        return {"signal": []}

    try:
        client      = genai.Client(api_key=api_key)
        titles_text = "\n".join([f"{i}. {a.get('title', '')}" for i, a in enumerate(articles)])

        response = client.models.generate_content(
            model=MISTRAL_MODEL,
            contents=PROMPT.format(titles=titles_text),
            config={"response_mime_type": "application/json"},
        )

        text = response.text if hasattr(response, "text") else ""
        return extract_json_object(text)

    except Exception as e:
        print(f"Gemini classification error: {e}")
        return {"signal": []}


def deduplicate_articles(articles):
    if not articles:
        return articles

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return articles

    try:
        client      = genai.Client(api_key=api_key)
        titles_text = "\n".join([f"{i}. {a.get('title', '')}" for i, a in enumerate(articles)])

        response = client.models.generate_content(
            model=DEDUP_MODEL,
            contents=DEDUP_PROMPT.format(titles=titles_text),
            config={"response_mime_type": "application/json"},
        )

        raw = response.text if hasattr(response, "text") else ""
        raw = raw.replace("```json", "").replace("```", "").strip()

        keep_indices = None
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                keep_indices = [i for i in parsed if isinstance(i, int) and 0 <= i < len(articles)]
        except Exception:
            pass

        if keep_indices is None:
            m = re.search(r"\[[\d,\s]+\]", raw)
            if m:
                try:
                    keep_indices = [
                        i for i in json.loads(m.group(0))
                        if isinstance(i, int) and 0 <= i < len(articles)
                    ]
                except Exception:
                    pass

        if keep_indices is None:
            print("Dedup: could not parse response, keeping all articles.")
            return articles

        keep_indices = sorted(set(keep_indices))
        deduped = [articles[i] for i in keep_indices]
        dropped = len(articles) - len(deduped)
        if dropped:
            print(f"Dedup: removed {dropped} near-duplicate title(s).")
        return deduped

    except Exception as e:
        print(f"Gemini dedup error: {e}")
        return articles

# -- XML -----------------------------------------------------------------------

def _fresh_channel(root, feed_title, feed_description):
    channel = ET.SubElement(root, "channel")
    ET.SubElement(channel, "title").text       = feed_title
    ET.SubElement(channel, "link").text        = "https://yourusername.github.io/yourrepo/"
    ET.SubElement(channel, "description").text = feed_description
    return channel


def _load_or_create(output_file, feed_title, feed_description):
    ET.register_namespace("media", MEDIA_NS)

    if Path(output_file).exists():
        try:
            tree    = ET.parse(output_file)
            root    = tree.getroot()
            channel = root.find("channel")
            if channel is not None:
                return tree, root, channel
            channel = _fresh_channel(root, feed_title, feed_description)
            return tree, root, channel
        except ET.ParseError:
            pass

    root    = ET.Element("rss", {"version": "2.0"})
    tree    = ET.ElementTree(root)
    channel = _fresh_channel(root, feed_title, feed_description)
    return tree, root, channel


def generate_xml_feed(articles, output_file, feed_title=None, feed_description=None):
    feed_title       = feed_title       or "Curated News"
    feed_description = feed_description or "AI-curated news feed"

    tree, root, channel = _load_or_create(output_file, feed_title, feed_description)

    existing_links: set[str] = set()
    for item in channel.findall("item"):
        link_el = item.find("link")
        if link_el is not None and link_el.text:
            existing_links.add(link_el.text.strip())

    added = 0
    for a in articles:
        link = (a.get("link") or "").strip()
        if not link or link in existing_links:
            continue

        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text       = a.get("title", "") or ""
        ET.SubElement(item, "link").text        = link
        guid_val     = a.get("id") or link
        is_permalink = "true" if guid_val.startswith("http") else "false"
        ET.SubElement(item, "guid", {"isPermaLink": is_permalink}).text = guid_val
        ET.SubElement(item, "description").text = a.get("description", "") or ""
        if a.get("published"):
            ET.SubElement(item, "pubDate").text = a["published"]

        thumb = a.get("thumbnail")
        if thumb:
            ET.SubElement(item, MEDIA_TAG + "thumbnail", {"url": thumb})
            mime = a.get("thumbnail_type") or get_mime_for_url(thumb)
            ET.SubElement(item, "enclosure", {"url": thumb, "type": mime, "length": "0"})

        existing_links.add(link)
        added += 1

    all_items = channel.findall("item")
    overflow  = len(all_items) - MAX_FEED_ITEMS
    if overflow > 0:
        for old_item in all_items[:overflow]:
            channel.remove(old_item)

    now_text   = format_datetime(datetime.now(timezone.utc))
    last_build = channel.find("lastBuildDate")
    if last_build is None:
        ET.SubElement(channel, "lastBuildDate").text = now_text
    else:
        last_build.text = now_text

    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass

    tree.write(output_file, encoding="unicode", xml_declaration=False)

    with open(output_file, "r+", encoding="utf-8") as fh:
        body = fh.read()
        fh.seek(0)
        fh.write('<?xml version="1.0" encoding="UTF-8"?>\n' + body)
        fh.truncate()

    return added

# -- STATS ---------------------------------------------------------------------

def print_stats():
    print("\nFetch statistics:")
    print(f"  Timestamp:             {STATS.get('timestamp')}")
    print(f"  Total fetched:         {STATS['total_fetched']}  (raw entries from all feeds)")
    print(f"  Passed age cut:        {STATS['total_passed_age']}  (within {MAX_AGE_HOURS}h window)")
    print(f"  New (unseen):          {STATS['total_new']}")
    print(f"  Signal (classified):   {STATS['total_signal']}")
    print(f"  Signal (after dedup):  {STATS['total_signal_deduped']}  -> {OUTPUT_XML}")
    print("  Per-method (raw fetch):")
    for method, cnt in STATS["per_method"].items():
        print(f"    {method}: {cnt}")
    print("  Per-feed breakdown:")
    for feed, d in STATS["per_feed"].items():
        print(f"    {feed}")
        print(f"      fetched={d.get('fetched',0)}  passed_age={d.get('passed_age',0)}  sent_to_pipeline={d.get('capped',0)}")
    print("")

# -- MAIN ----------------------------------------------------------------------

def main():
    processed_data = load_processed_articles()
    all_articles   = fetch_all_feeds()
    new_articles   = get_new_articles(all_articles, processed_data)

    STATS["total_new"] = len(new_articles)

    result = send_to_mistral(new_articles)

    signal_indices = [
        i for i in result.get("signal", [])
        if isinstance(i, int) and 0 <= i < len(new_articles)
    ]

    signal_articles = [new_articles[i] for i in signal_indices]
    STATS["total_signal"] = len(signal_articles)

    if not signal_articles:
        print("No signal articles this run. Skipping all file writes.")
        print_stats()
        return

    print(f"Deduplicating {len(signal_articles)} signal article(s)...")
    signal_articles = deduplicate_articles(signal_articles)

    STATS["total_signal_deduped"] = len(signal_articles)

    generate_xml_feed(
        signal_articles,
        output_file=OUTPUT_XML,
        feed_title="Curated News",
        feed_description="AI-curated signal: core geopolitical news",
    )

    save_selected_articles(signal_articles)

    processed_data.setdefault("article_ids", []).extend([a["id"] for a in new_articles if a.get("id")])
    processed_data.setdefault("article_links", []).extend([a["link"] for a in new_articles if a.get("link")])
    save_processed_articles(processed_data)

    STATS["timestamp"] = datetime.utcnow().isoformat()
    save_stats()
    print_stats()


if __name__ == "__main__":
    main()