import os
import json
import time
import re
import requests
from datetime import date, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# =========================
# 설정
# =========================
STATE_PATH = Path("pubmed_oph_state.json")
OUT_DIR = Path("pubmed_digests")
OUT_DIR.mkdir(parents=True, exist_ok=True)

NCBI_EMAIL = os.getenv("NCBI_EMAIL", "")
NCBI_TOOL =  "oph-weekly-digest"
NCBI_API_KEY = os.getenv("NCBI_API_KEY", "")

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
DB = "pubmed"

# =========================
# 이메일 발송 설정 (환경변수 사용)
# =========================
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER", "")

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

JOURNALS = {
    "Retina (Philadelphia, Pa.)": "RETINA_ALL",
    "Am J Ophthalmol": "FILTER_KEYWORDS",
    "Ophthalmology": "FILTER_KEYWORDS",
    "Invest Ophthalmol Vis Sci": "FILTER_KEYWORDS",
    "Br J Ophthalmol": "FILTER_KEYWORDS",
}

KEYWORD_REGEX = re.compile(
    r"\b(retina|retinal|macula|macular|choroid|choroidal|vitreoretinal|"
    r"vitreous|uveitis|anti-vegf|vegf|amd|age-related macular|"
    r"diabetic retinopathy|dme|retinal detachment|rrd|pvr|"
    r"epiretinal|macular hole|central serous|csc|pachychoroid)\b",
    re.IGNORECASE
)

def throttle():
    time.sleep(0.35 if not NCBI_API_KEY else 0.12)

def load_state():
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {
        "last_run": (date.today() - timedelta(days=7)).isoformat(),
        "seen_pmids": []
    }

def save_state(state):
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def build_date_clause(start_iso: str, end_iso: str) -> str:
    # dp(Date of Publication) 기준. (원하시면 edat/pdat로 바꿔드릴 수 있습니다.)
    return f'("{start_iso}"[dp] : "{end_iso}"[dp])'

def esearch_count(term: str) -> int:
    params = {
        "db": DB,
        "term": term,
        "retmode": "json",
        "retmax": 0,
        "email": NCBI_EMAIL,
        "tool": NCBI_TOOL,
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY

    r = requests.get(f"{EUTILS_BASE}/esearch.fcgi", params=params, timeout=30)
    r.raise_for_status()
    throttle()
    return int(r.json()["esearchresult"].get("count", 0))

def esearch_all_ids(term: str, batch_size: int = 500) -> list[str]:
    count = esearch_count(term)
    if count == 0:
        return []

    all_ids = []
    retstart = 0
    while retstart < count:
        params = {
            "db": DB,
            "term": term,
            "retmode": "json",
            "retmax": batch_size,
            "retstart": retstart,
            "email": NCBI_EMAIL,
            "tool": NCBI_TOOL,
        }
        if NCBI_API_KEY:
            params["api_key"] = NCBI_API_KEY

        r = requests.get(f"{EUTILS_BASE}/esearch.fcgi", params=params, timeout=30)
        r.raise_for_status()
        throttle()
        data = r.json()
        all_ids.extend(data["esearchresult"].get("idlist", []))
        retstart += batch_size

    return list(dict.fromkeys(all_ids))

def efetch_xml(pmids: list[str]) -> str:
    params = {
        "db": DB,
        "id": ",".join(pmids),
        "retmode": "xml",
        "email": NCBI_EMAIL,
        "tool": NCBI_TOOL,
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY

    r = requests.get(f"{EUTILS_BASE}/efetch.fcgi", params=params, timeout=90)
    r.raise_for_status()
    throttle()
    return r.text

def parse_pubmed_xml(xml_text: str) -> list[dict]:
    root = ET.fromstring(xml_text)
    out = []

    for art in root.findall(".//PubmedArticle"):
        pmid = (art.findtext(".//PMID") or "").strip()
        title = (art.findtext(".//ArticleTitle") or "").strip()
        journal = (art.findtext(".//Journal/Title") or "").strip()

        year = art.findtext(".//PubDate/Year") or ""
        month = art.findtext(".//PubDate/Month") or ""
        day = art.findtext(".//PubDate/Day") or ""
        pub_date = "-".join([p for p in [year, month, day] if p]).strip("-")

        doi = ""
        for aid in art.findall(".//ArticleId"):
            if (aid.get("IdType") or "").lower() == "doi":
                doi = (aid.text or "").strip()
                break

        last = art.findtext(".//AuthorList/Author[1]/LastName") or ""
        fore = art.findtext(".//AuthorList/Author[1]/ForeName") or ""
        first_author = (f"{fore} {last}".strip() or "").strip()
        author_count = len(art.findall(".//AuthorList/Author"))
        authors_short = f"{first_author} et al." if author_count >= 2 and first_author else first_author

        abs_texts = []
        for at in art.findall(".//Abstract/AbstractText"):
            txt = "".join(at.itertext()).strip()
            if txt:
                abs_texts.append(txt)
        abstract = "\n".join(abs_texts).strip()

        out.append({
            "pmid": pmid,
            "title": title,
            "journal": journal,
            "pub_date": pub_date,
            "doi": doi,
            "authors": authors_short,
            "abstract_en": abstract,
        })

    return out

def keep_article(article: dict, rule: str) -> bool:
    if rule == "RETINA_ALL":
        return True

    text = f"{article.get('title','')}\n{article.get('abstract_en','')}"
    return bool(KEYWORD_REGEX.search(text))

def group_by_journal(articles: list[dict]) -> dict[str, list[dict]]:
    grouped = {}
    for a in articles:
        grouped.setdefault(a.get("journal", "Unknown"), []).append(a)
    for j in grouped:
        grouped[j] = sorted(grouped[j], key=lambda x: (x.get("pub_date",""), x.get("title","")))
    return grouped

def pmid_url(pmid: str) -> str:
    return f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

def render_markdown(run_date: str, start: str, end: str, grouped: dict[str, list[dict]]) -> str:
    total = sum(len(v) for v in grouped.values())
    lines = []
    lines.append(f"# PubMed Ophthalmology Weekly Digest ({run_date})\n")
    lines.append(f"- 기간: **{start} ~ {end}**")
    lines.append(f"- 총 논문 수: **{total}**\n")

    for journal in sorted(grouped.keys()):
        items = grouped[journal]
        lines.append(f"---\n## {journal}  \n**({len(items)}편)**\n")

        for idx, a in enumerate(items, start=1):
            lines.append(f"### {idx}. {a['title']}")
            meta = []
            if a["authors"]:
                meta.append(f"Authors: {a['authors']}")
            if a["pub_date"]:
                meta.append(f"Date: {a['pub_date']}")
            if a["pmid"]:
                meta.append(f"PMID: {a['pmid']} ({pmid_url(a['pmid'])})")
            if a["doi"]:
                meta.append(f"DOI: {a['doi']}")
            lines.append("- " + " | ".join(meta))
            lines.append("")

            if a["abstract_en"]:
                lines.append("**Abstract (EN)**\n")
                lines.append(a["abstract_en"])
                lines.append("")

    return "\n".join(lines)

def build_email_body(run_date: str, start: str, end: str, grouped: dict[str, list[dict]], top_n: int = 5) -> str:
    # 이메일 본문은 너무 길어지지 않게 "저널별 상위 N개"만 요약
    total = sum(len(v) for v in grouped.values())
    lines = []
    lines.append(f"PubMed Ophthalmology Weekly Digest ({run_date})")
    lines.append(f"기간: {start} ~ {end}")
    lines.append(f"총 포함 논문 수: {total}")
    lines.append("")
    lines.append("요약(저널별 상위 항목):")
    lines.append("")

    for journal in sorted(grouped.keys()):
        items = grouped[journal]
        lines.append(f"[{journal}] ({len(items)}편)")
        for a in items[:top_n]:
            pmid = a.get("pmid", "")
            url = pmid_url(pmid) if pmid else ""
            title = a.get("title", "").strip()
            authors = a.get("authors", "").strip()
            lines.append(f"- {title}")
            if authors:
                lines.append(f"  - {authors}")
            if pmid:
                lines.append(f"  - PMID {pmid}: {url}")
        if len(items) > top_n:
            lines.append(f"  ... 외 {len(items) - top_n}편")
        lines.append("")

    lines.append("전체 초록/상세 내용은 첨부된 Markdown 파일을 확인해 주세요.")
    return "\n".join(lines)

def send_email(subject: str, body_text: str, attachment_path: Path):
    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = subject

    msg.attach(MIMEText(body_text, "plain", "utf-8"))

    # 첨부파일 추가
    part = MIMEBase("application", "octet-stream")
    part.set_payload(attachment_path.read_bytes())
    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        f'attachment; filename="{attachment_path.name}"'
    )
    msg.attach(part)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, [EMAIL_RECEIVER], msg.as_string())

def main():
    state = load_state()
    last_run = date.fromisoformat(state["last_run"])
    today = date.today()

    start = (last_run + timedelta(days=1)).isoformat()
    end = today.isoformat()
    date_clause = build_date_clause(start, end)

    seen = set(state.get("seen_pmids", []))
    all_new_articles = []

    for journal_name, rule in JOURNALS.items():
        term = f'"{journal_name}"[jour] AND {date_clause}'
        pmids = esearch_all_ids(term)

        pmids = [p for p in pmids if p not in seen]
        if not pmids:
            continue

        CHUNK = 200
        for i in range(0, len(pmids), CHUNK):
            chunk = pmids[i:i+CHUNK]
            xml = efetch_xml(chunk)
            articles = parse_pubmed_xml(xml)
            for a in articles:
                if keep_article(a, rule):
                    all_new_articles.append(a)

    dedup = {a["pmid"]: a for a in all_new_articles if a.get("pmid")}
    all_new_articles = list(dedup.values())

    run_date = today.isoformat()
    out_path = OUT_DIR / f"pubmed_digest_{run_date}.md"

    if not all_new_articles:
        out_path.write_text(
            f"# PubMed Ophthalmology Weekly Digest ({run_date})\n\n- 신규 논문 없음 ({start} ~ {end})\n",
            encoding="utf-8"
        )
        state["last_run"] = today.isoformat()
        save_state(state)

        # 신규 논문 없을 때도 메일 보내고 싶으면 아래 주석 해제
        subject = f"[PubMed Digest] {run_date} (No new papers)"
        body = f"신규 논문 없음 ({start} ~ {end})\n\n첨부 파일 참고: {out_path.name}"
        send_email(subject, body, out_path)

        print(f"No new papers. Saved and emailed: {out_path}")
        return

    grouped = group_by_journal(all_new_articles)
    md = render_markdown(run_date, start, end, grouped)
    out_path.write_text(md, encoding="utf-8")

    state["last_run"] = today.isoformat()
    state["seen_pmids"] = list(seen.union(dedup.keys()))
    save_state(state)

    # 이메일 전송
    subject = f"[PubMed Digest] Ophthalmology Weekly - {run_date} ({sum(len(v) for v in grouped.values())} papers)"
    body = build_email_body(run_date, start, end, grouped, top_n=5)
    send_email(subject, body, out_path)

    print(f"Saved and emailed: {out_path} | New included papers: {sum(len(v) for v in grouped.values())}")

if __name__ == "__main__":
    main()
