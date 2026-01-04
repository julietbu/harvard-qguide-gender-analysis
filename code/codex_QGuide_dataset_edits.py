#!/usr/bin/env python3
"""
Augments 2025springQ.csv with instructor first names.

The script crawls the Harvard Q report link in each row, tries to parse the
instructor's full name from the HTML, and stores the extracted first name in a
new column. It also derives a `course_teacher_sex` value using the
gender-guesser library. Harvard Q requires HarvardKey authentication, so you
must provide a valid cookie header (either via --harvard-q-cookie or the
HARVARD_Q_COOKIE env variable). A DEFAULT_COOKIE constant is also embedded
below; it is used when no cookie argument or environment variable is supplied.

You can copy a fresh cookie from an authenticated browser session and paste it
as a single string, e.g.:

    python add_first_names.py \\
        --harvard-q-cookie \"SMSESSION=...; BIGipServer=...\" \\
        --input 2025springQ.csv

Dependencies: requests, beautifulsoup4, gender-guesser
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, Optional

import requests
from bs4 import BeautifulSoup, NavigableString

try:
    from gender_guesser.detector import Detector
except ImportError:
    Detector = None  # type: ignore[assignment]

DEFAULT_COOKIE = (
    "BlueNextOriginalPath=/rpv-eng.aspx?lang=eng&redi=1&SelectedIDforPrint="
    "3c86a971df4eb6fef9ac8a7cd960ccc1206172746dc4b01efe3d0bea3788252ce8b35e"
    "489855d0602d7c2ba8c4bc78d8&ReportType=2&regl=en-US; "
    "BlueNextRefreshToken=B5BF00B7D9DA396DCB84D9E04682F18C33D26BA309D1F7B92C"
    "1AFE23D03BF764; "
    "BlueNextAccessToken=eyJhbGciOiJSUzI1NiIsImtpZCI6IkYzMUJEQjU5NkU4ODEzRTU4MzA5MEM1"
    "ODdDQUMyMTcwOTk2NTIxODFSUzI1NiIsIng1dCI6Ijh4dmJXVzZJRS1XRENReFlmS3doY0psbEl"
    "ZRSIsInR5cCI6ImF0K2p3dCJ9.eyJuYmYiOjE3NjUzMjExNzksImV4cCI6MTc2NTMyNDc3OSwiaX"
    "NzIjoiaHR0cHM6Ly9teS1oYXJ2YXJkLWF1dGguYmx1ZXJhLmNvbSIsImNsaWVudF9pZCI6IjU3Nj"
    "Y5RkNDLThBRTItNEMzOC1CNUM0LTkzRkNDQTc5QkI4RSIsInN1YiI6IjliMjdkYjMwLThiYzMtNG"
    "FjMy1hZDRmLWM3ZjNlYTI0MmJmOCIsImF1dGhfdGltZSI6MTc2NTMyMTE3OSwiaWRwIjoibG9jYW"
    "wiLCJnc2lkIjoiQTkxNUU4MTdCMTA3MTc0MTQ3OTNDMjdGREU5Rjk4ODAiLCJzc29faWQiOiJiYX"
    "NpYyIsImp0aSI6IjgwMjU4OUVCMjkzRjczNDY3QTczMEEwQjREMDk0QjIxIiwiaWF0IjoxNzY1Mz"
    "IxMTc5LCJzY29wZSI6WyJvcGVuaWQiLCJvZmZsaW5lX2FjY2VzcyJdLCJhbXIiOlsicHdkIl19.I"
    "ySqnnLaS-o4ek_W-hiU8nuZFCdnsU4f-pOX82boJUO0wO2SJBXTdemDrOXFrAvHzPOOWJRw67gFz"
    "fESUkbQji1_9Qa4ag0Yv56g1DS2H8s3VCdFXEHnKPW_loS6Z_jB2zYqiQQ3_cfaEfxSiQW7CLVYP"
    "FO_LRqBh1ldgY_F8zAwGRwTMuz_UoW0n-o-R0ZEaiYhfpnNlw8LFV2QznSHRW9Hi7M1VYsT_tsVu"
    "sydPpkS1hP_NGYe-U-xQj6TrSKyddxL0TLlzKct0mS4KuTE6XwDcJGVBFD4gVO8CapxVG8REmVgF"
    "KB54s54MNQ2xCiwprI1ITBo2Yp0zYqN6BdP2V9iFE2-rGfDIj6-QQfhfYlnEWKaLQDDBQTqNjSky"
    "f1WB3ntV0tWGG3_C0V3lBILt5lMKCvA-d-mCtDM4_Z4YgqViToPMdhpV3vbzyLNgAzgb7afNOfAB"
    "PVrcMxY-8FoUdot9WItQlb5cM3bxH4BH4Q_SLIkn1CLAxjdJ6vfEhYwKO8IT1QMUXllDOOBNc_1o"
    "b9WRNhCjKd3I9NVMvydcu0domsYfrXqn_UMsPeuJAx0xROlUoMsAE3wJ9xe2TC97L955qHW5ziv5"
    "JKofTrQFYeCNYFVG7ylU4BGemGoXOK6f8Y3ogxvgz8v8ChvYcCqWfZKjRZkhtcVbTezlKQokGQ; "
    "BlueNextIdToken=eyJhbGciOiJSUzI1NiIsImtpZCI6IkYzMUJEQjU5NkU4ODEzRTU4MzA5MEM1"
    "ODdDQUMyMTcwOTk2NTIxODFSUzI1NiIsIng1dCI6Ijh4dmJXVzZJRS1XRENReFlmS3doY0psbElZ"
    "RSIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE3NjUzMjExNzksImV4cCI6MTc2NTMyMTQ3OSwiaXNzIjo"
    "iaHR0cHM6Ly9teS1oYXJ2YXJkLWF1dGguYmx1ZXJhLmNvbSIsImF1ZCI6IjU3NjY5RkNDLThBRTI"
    "tNEMzOC1CNUM0LTkzRkNDQTc5QkI4RSIsImlhdCI6MTc2NTMyMTE3OSwiYXRfaGFzaCI6IjVaN2V"
    "XMlhweEVzX181Um51TkQ3enciLCJzX2hhc2giOiJIQ1VMbTZSZDB6RURJcjlMbndUdGNnIiwic3V"
    "iIjoiOWIyN2RiMzAtOGJjMy00YWMzLWFkNGYtYzdmM2VhMjQyYmY4IiwiYXV0aF90aW1lIjoxNzY"
    "1MzIxMTc5LCJpZHAiOiJsb2NhbCIsImFtciI6WyJwd2QiXSwiZ3NpZCI6IkE5MTVFODE3QjEwNzE"
    "3NDE0NzkzQzI3RkRFOUY5ODgwIn0.f1NYiY8QSjZJsJEgE5zGv4g0CagoujinHS7oOILap7trv1t"
    "eLHM7GQ1EAo2cfndpbBzG-_LOy3fAOHQ-z3clw1cwh7nnDMb9avXsq1oQR4WZPReT7F7CSGbZall"
    "OLrtxfksDwujxTUM4Ur2wDNOBRSwGRQ7EvZHuHXVWsMXFarWG7r40HULC9bTLuOOhjbv1ZaR1TXk"
    "5p0bId9M68Otk-IG2Hly29xWoTKsR7LZ3k5UZ02SLWf71ZdfU1cCWCmgiNtQl8bFJRSzdDuPiNar"
    "Plq97g3up62jO45vp4K5Em6hEEQFO4-UMCUk1Sx1O9uwLGrxKgatEM_cYR0AxJ7xM2OZqeVOJlaW"
    "jL1bgdi-7pr1jsRlDNcQ-B7qvgOm023Vcu8_n7BDdG7Sn5ZF66OaNeviX0yR3GmcXL0rggXFZhmZ"
    "ivLVMV-zn4v_Jh-l2HPgTGs6PA8Zd8Hi4VYmFebCHDL4rhv-EYaRVzU3jw8MEnFjakQNfJ_n2bC2"
    "6c1z3Wul1zuT_gAzxfaF-MZqW1I_IlT9w7gR41Q56iqg0oyRk6OLaimrFIrms0AumDCaNPYIUf2r"
    "Ye3NW6-BY6g7xFtYwHU9BSYBHtjKchxOZZQ_exTgR39BUKKZtlL7AF_jxxfcs74ujuXmLlwdInWJ"
    "ghYurJpCe0V_tFbfmlPdcLklQUPQ; "
    "ASP.NET_SessionId=bq1rhp3dtem4qm3a0rnfsvdk; "
    "CookieName=5CAA8B3EF893B39AFB3742D4368536660270B71FFC70E5F6B6C9A4C5F5048FE97D"
    "0C36B99A794C049ADC8230A57E73D99F303AC4304311C9A45B69E3542D1F941748462BDA3AAC"
    "AD4B429392DC23EEBB53A835B0BAC16EE895F617BDBCD8A2F049BCF4AF74CE265C0DB0232FC9"
    "F8E57F5A212849D591D089E5AB955C0BBF450A30E7067ACD9BA406CE33B667478FD99F16197A"
    "45B698E166BD6873914C8A12F2B4B54BB3D646558989A1CFA98904648BC17F8BE68F6D7AC1E1"
    "77BBF1AEF5DD5CB59B0839738430D68E2BCB1D0793E25ADB6947228C7AC631594D22EFB36A82"
    "6E; "
    "session_token=2695601b49754c9eae1872450af0bfe6"
)

def build_gender_detector() -> Optional["Detector"]:
    if Detector is None:
        logging.warning(
            "gender-guesser is not installed; course_teacher_sex will remain empty."
        )
        return None
    try:
        return Detector(case_sensitive=False)
    except Exception as exc:  # pragma: no cover - defensive
        logging.warning("Unable to initialize gender detector: %s", exc)
        return None


def guess_sex(first_name: str, detector: Optional["Detector"]) -> str:
    if not first_name or not detector:
        return ""
    gender = detector.get_gender(first_name)
    if gender in {"male", "mostly_male"}:
        return "male"
    if gender in {"female", "mostly_female"}:
        return "female"
    return "unknown"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add instructor first names to the Harvard Q CSV."
    )
    parser.add_argument(
        "-i",
        "--input",
        default="2025springQ.csv",
        help="Path to the source CSV (default: %(default)s).",
    )
    parser.add_argument(
        "-o",
        "--output",
        help=(
            "Destination CSV. Defaults to <input>_with_first_names.csv "
            "in the same directory."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output file if it already exists.",
    )
    parser.add_argument(
        "--harvard-q-cookie",
        dest="harvard_q_cookie",
        help=(
            "Cookie header to reuse your Harvard Q session. "
            "If omitted, the HARVARD_Q_COOKIE environment variable is used."
        ),
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Seconds to wait between HTTP requests (default: %(default)s).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retries per request before giving up (default: %(default)s).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Only process the first N rows (useful for quick tests).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING...).",
    )
    parser.add_argument(
        "--no-reuse-first-names",
        action="store_true",
        help="Always re-fetch first names even if the input already has values.",
    )
    return parser.parse_args()


class HarvardQResolver:
    """Fetches Harvard Q report pages and extracts instructor first names."""

    LETTER_CLASS = r"[^\W\d_]"
    KEYWORD_PATTERNS = [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"Instructor",
            r"Instructors",
            r"Course\s*Head",
            r"Primary\s+Instructor",
            r"Lecturer",
        )
    ]
    STRUCTURED_HINTS = (
        "feedback",
        "instructor",
        "course head",
        "primary instructor",
        "lecturer",
    )
    INVALID_FIRST_TOKENS = {
        "professor",
        "prof",
        "doctor",
        "dr",
        "mr",
        "mrs",
        "ms",
        "mx",
        "coach",
        "dean",
        "chair",
        "director",
        "instructor",
    }

    def __init__(
        self,
        cookie_header: Optional[str],
        delay: float = 0.5,
        max_retries: int = 3,
    ) -> None:
        self.cookie_header = cookie_header or ""
        self.delay = delay if delay >= 0 else 0.0
        self.max_retries = max(1, max_retries)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "HarvardQFirstNameFetcher/1.0 (+https://github.com/)",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        self._link_cache: Dict[str, Optional[str]] = {}

    def resolve(self, url: str, last_name: str) -> Optional[str]:
        url = (url or "").strip()
        if not url:
            return None
        if url in self._link_cache:
            return self._link_cache[url]
        if not self.cookie_header:
            logging.debug("No Harvard Q cookie provided; skipping %s", url)
            self._link_cache[url] = None
            return None
        html = self._fetch(url)
        if not html:
            self._link_cache[url] = None
            return None
        first = self._extract_first_name(html, last_name)
        if first:
            logging.debug("Resolved %s as %s %s", url, first, last_name)
        else:
            logging.debug("Unable to parse first name for %s", url)
        self._link_cache[url] = first
        return first

    def _fetch(self, url: str) -> Optional[str]:
        headers = {"Cookie": self.cookie_header}
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, headers=headers, timeout=30)
            except requests.RequestException as exc:
                logging.warning("Request failed (%s/%s): %s", attempt, self.max_retries, exc)
                self._sleep_backoff(attempt)
                continue
            if self.delay:
                time.sleep(self.delay)
            if response.status_code != 200:
                logging.warning(
                    "Received HTTP %s for %s (attempt %s/%s)",
                    response.status_code,
                    url,
                    attempt,
                    self.max_retries,
                )
                if response.status_code in {401, 403}:
                    logging.error("Authentication failed for %s. Check your cookie.", url)
                    return None
                self._sleep_backoff(attempt)
                continue
            text = response.text
            if "HarvardKey - Sign In" in text or "login-form" in text:
                logging.error(
                    "Received a HarvardKey login page for %s. Provide a fresh cookie.",
                    url,
                )
                return None
            return text
        return None

    def _sleep_backoff(self, attempt: int) -> None:
        wait = min(self.delay * (2 ** (attempt - 1)), 10)
        if wait > 0:
            time.sleep(wait)

    def _extract_first_name(self, html: str, last_name: str) -> Optional[str]:
        if not last_name:
            return None
        soup = BeautifulSoup(html, "html.parser")
        candidates: Iterable[str] = self._candidate_text_chunks(soup)
        for chunk in candidates:
            first = self._find_name_in_text(chunk, last_name)
            if first:
                return first
        combined = self._first_n_characters(" ".join(soup.stripped_strings), 6000)
        return self._find_name_in_text(combined, last_name)

    def _candidate_text_chunks(self, soup: BeautifulSoup) -> Iterable[str]:
        seen: set[str] = set()
        for selector in ("title", "h1", "h2", "h3"):
            for node in soup.select(selector):
                text = node.get_text(" ", strip=True)
                if text and text not in seen:
                    seen.add(text)
                    yield text
        for pattern in self.KEYWORD_PATTERNS:
            for node in soup.find_all(string=pattern):
                text = self._string_with_parent(node)
                if text and text not in seen:
                    seen.add(text)
                    yield text

    @staticmethod
    def _string_with_parent(node: NavigableString) -> str:
        parent = node.parent
        if parent:
            return parent.get_text(" ", strip=True)
        return str(node).strip()

    @staticmethod
    def _first_n_characters(text: str, limit: int) -> str:
        if len(text) <= limit:
            return text
        return text[:limit]

    def _find_name_in_text(self, text: str, last_name: str) -> Optional[str]:
        if not text or not last_name:
            return None
        text_lower = text.lower()
        normalized_last = self._normalize_last_name(last_name)
        if not normalized_last:
            return None
        if normalized_last.lower() not in text_lower:
            return None
        pattern = self._build_name_pattern(normalized_last)
        first = self._extract_from_pattern(pattern, text, require_capitalized=True)
        if first:
            return first
        if any(hint in text_lower for hint in self.STRUCTURED_HINTS):
            relaxed_pattern = self._build_name_pattern(normalized_last)
            return self._extract_from_pattern(relaxed_pattern, text, require_capitalized=False)
        return None

    @staticmethod
    def _normalize_last_name(last_name: str) -> str:
        cleaned = re.sub(r"\s*\([^)]*\)\s*$", "", last_name or "")
        return cleaned.strip()

    @classmethod
    def _token_pattern(cls) -> str:
        letter = cls.LETTER_CLASS
        # Allow interior apostrophes or hyphens followed by another letter.
        return rf"{letter}(?:{letter}|['’\-]{letter})*"

    @classmethod
    def _build_name_pattern(cls, last_name: str) -> re.Pattern[str]:
        escaped_last = re.escape(last_name.strip())
        token = cls._token_pattern()
        first = rf"(?P<first>{token})(?:\.)?"
        middle = rf"(?:{token}(?:\.)?\s+){{0,3}}"
        regex = rf"\b{first}\s+{middle}(?i:{escaped_last})(?!{cls.LETTER_CLASS})"
        return re.compile(regex)

    def _extract_from_pattern(
        self, pattern: re.Pattern[str], text: str, require_capitalized: bool
    ) -> Optional[str]:
        for match in pattern.finditer(text):
            candidate = match.group("first")
            if not candidate:
                continue
            cleaned = self._clean_candidate(candidate)
            if not cleaned:
                continue
            if require_capitalized and not cleaned[:1].isupper():
                continue
            if cleaned.lower() in self.INVALID_FIRST_TOKENS:
                continue
            return cleaned
        return None

    @staticmethod
    def _clean_candidate(candidate: str) -> Optional[str]:
        candidate = candidate.strip()
        if not candidate:
            return None
        candidate = candidate.replace(".", "")
        if any(char.isdigit() or char == "_" for char in candidate):
            return None
        # Normalize capitalization without breaking Mc/Mac style names.
        if len(candidate) == 1:
            return candidate.upper()
        return candidate[0].upper() + candidate[1:]


def process_rows(
    input_path: Path,
    output_path: Path,
    resolver: HarvardQResolver,
    gender_detector: Optional["Detector"],
    reuse_existing_first_names: bool = True,
    limit: Optional[int] = None,
) -> int:
    with input_path.open(newline="", encoding="utf-8") as src:
        reader = csv.DictReader(src)
        fieldnames = list(reader.fieldnames or [])
        new_column = "course_teacher_first_name"
        if new_column not in fieldnames:
            fieldnames.append(new_column)
        sex_column = "course_teacher_sex"
        if sex_column not in fieldnames:
            fieldnames.append(sex_column)
        rows = list(reader)

    for idx, row in enumerate(rows, start=1):
        if limit is not None and idx > limit:
            row[new_column] = row.get(new_column, "")
            continue
        last_name = (row.get("course_teacher") or "").strip()
        link = (row.get("link") or "").strip()
        if not last_name:
            logging.warning("Row %s lacks a course_teacher value; skipping.", idx)
            continue
        first_name = (row.get(new_column) or "").strip()
        if not first_name or not reuse_existing_first_names:
            first_name = resolver.resolve(link, last_name) or ""
        row[new_column] = first_name
        existing_sex = (row.get("course_teacher_sex") or "").strip()
        if existing_sex:
            row["course_teacher_sex"] = existing_sex
        else:
            row["course_teacher_sex"] = guess_sex(first_name, gender_detector)
        if idx % 25 == 0:
            logging.info("Processed %s rows (last: %s %s)", idx, first_name or "?", last_name)

    with output_path.open("w", newline="", encoding="utf-8") as dst:
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s %(message)s",
    )
    input_path = Path(args.input)
    if not input_path.exists():
        logging.error("Input file %s does not exist.", input_path)
        sys.exit(1)
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.with_name(f"{input_path.stem}_with_first_names{input_path.suffix}")
    if output_path.exists() and not args.force:
        logging.error(
            "Output file %s already exists. Use --force to overwrite.",
            output_path,
        )
        sys.exit(1)
    user_cookie = args.harvard_q_cookie or os.getenv("HARVARD_Q_COOKIE")
    if user_cookie:
        cookie = user_cookie
    else:
        cookie = DEFAULT_COOKIE
        logging.info(
            "No cookie argument or HARVARD_Q_COOKIE detected; falling back to DEFAULT_COOKIE."
        )
    resolver = HarvardQResolver(cookie_header=cookie, delay=args.delay, max_retries=args.max_retries)
    gender_detector = build_gender_detector()
    processed = process_rows(
        input_path=input_path,
        output_path=output_path,
        resolver=resolver,
        gender_detector=gender_detector,
        reuse_existing_first_names=not args.no_reuse_first_names,
        limit=args.limit,
    )
    logging.info("Wrote %s rows to %s", processed, output_path)


if __name__ == "__main__":
    main()
