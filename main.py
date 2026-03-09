#!/usr/bin/env python3
"""
mediawiki_recentchanges_to_git.py

Sync MediaWiki recent changes into a Git repository, one commit per change.

Bootstrapping behavior:
- On first run, fetches recent changes for the chosen window.
- Finds the oldest recent change in that batch.
- Builds a full repository snapshot of the wiki state *before* that change:
  one file per page, using the latest revision at or before that timestamp.
- Creates one bootstrap commit for that snapshot.
- Replays each recent change as its own commit.

Resume behavior:
- Uses git notes to remember the last imported rcid/timestamp.
- No sidecar DB needed.

Requirements:
- Python 3.9+
- git in PATH
- requests
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


NOTES_REF = "refs/notes/mediawiki-sync"
PAGES_DIR = "wiki"
USER_EMAIL_DOMAIN = "user.vrchat.com"
BOOTSTRAP_NOTE_KIND = "bootstrap"
CHANGE_NOTE_KIND = "recentchange"


@dataclass
class RecentChange:
    rcid: int
    type: str
    title: str
    ns: int
    timestamp: str
    comment: str
    user: str
    userid: int
    revid: Optional[int]
    old_revid: Optional[int]
    logtype: Optional[str]
    logaction: Optional[str]
    logparams: Optional[Dict[str, Any]]
    old_title: Optional[str]


class MediaWikiSyncError(Exception):
    pass


def debug(msg: str) -> None:
    print(msg, file=sys.stderr)


def run_git(
    repo: Path,
    args: List[str],
    env_extra: Optional[Dict[str, str]] = None,
    check: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    cmd = ["git", "-C", str(repo)] + args
    return subprocess.run(
        cmd,
        env=env,
        check=check,
        text=True,
        encoding="utf-8",
        capture_output=capture_output,
    )


def ensure_git_repo(repo: Path) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    if not (repo / ".git").exists():
        run_git(repo, ["init", "-b", "main"])
    try:
        run_git(repo, ["config", "user.name"])
    except subprocess.CalledProcessError:
        run_git(repo, ["config", "user.name", "MediaWiki Importer"])
    try:
        run_git(repo, ["config", "user.email"])
    except subprocess.CalledProcessError:
        run_git(repo, ["config", "user.email", "mediawiki-importer@localhost"])


def parse_headers(header_args: List[str]) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for item in header_args:
        if ":" not in item:
            raise ValueError(f"Invalid header format: {item!r}. Use 'Name: value'")
        key, value = item.split(":", 1)
        headers[key.strip()] = value.strip()
    return headers


def api_get(session: requests.Session, api_url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = session.get(api_url, params=params, timeout=120)
    r.raise_for_status()
    text = (r.text or "").strip()
    if not text:
        raise MediaWikiSyncError(f"API returned empty response: {r.url}")
    try:
        data = r.json()
    except requests.exceptions.JSONDecodeError:
        snippet_len = 500
        snippet = "".join(c if c.isprintable() or c in "\n\t" else "?" for c in text[:snippet_len])
        if len(text) > snippet_len:
            snippet += "..."
        raise MediaWikiSyncError(
            f"API response is not valid JSON (URL: {r.url}). Body snippet: {snippet!r}"
        )
    if "error" in data:
        raise MediaWikiSyncError(f"MediaWiki API error: {data['error']}")
    return data


def iso_to_dt(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def dt_to_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_to_git_date(ts: str) -> str:
    dt = iso_to_dt(ts)
    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


def sanitize_title_to_path(title: str) -> Path:
    title = title.replace(" ", "_").strip()
    title = title.replace(":", "/")
    title = re.sub(r'[<>:"\\|?*\x00-\x1f]', "_", title)
    title = title.replace("..", "_")
    return Path(PAGES_DIR) / f"{title}.wikitext"


def git_head_commit(repo: Path) -> Optional[str]:
    try:
        return run_git(repo, ["rev-parse", "HEAD"]).stdout.strip()
    except subprocess.CalledProcessError:
        return None


def read_note(repo: Path, commit: str) -> Optional[str]:
    try:
        return run_git(repo, ["notes", f"--ref={NOTES_REF}", "show", commit]).stdout
    except subprocess.CalledProcessError:
        return None


def write_note(repo: Path, commit: str, text: str) -> None:
    run_git(repo, ["notes", f"--ref={NOTES_REF}", "add", "-f", "-m", text, commit])


def get_head_note_state(repo: Path) -> Optional[Dict[str, Any]]:
    head = git_head_commit(repo)
    if not head:
        return None
    note = read_note(repo, head)
    if not note:
        return None
    try:
        return json.loads(note)
    except json.JSONDecodeError:
        return None


def get_last_imported_rcid(
    repo: Path, max_commits: int = 20000
) -> Tuple[Optional[int], Optional[str]]:
    head = git_head_commit(repo)
    if not head:
        return None, "No HEAD commit."
    result = run_git(repo, ["rev-list", head, f"--max-count={max_commits}"])
    commits = result.stdout.strip().splitlines()
    if not commits:
        return None, "No commits in branch."
    try:
        list_result = run_git(repo, ["notes", f"--ref={NOTES_REF}", "list"])
    except subprocess.CalledProcessError:
        return None, "Notes ref refs/notes/mediawiki-sync missing or not fetched (fetch it in CI)."
    commits_with_notes: set[str] = set()
    for line in list_result.stdout.strip().splitlines():
        parts = line.split()
        if len(parts) >= 2:
            commits_with_notes.add(parts[1])
    if not commits_with_notes:
        return None, "Notes ref is empty."
    for commit in commits:
        if commit not in commits_with_notes:
            continue
        note = read_note(repo, commit)
        if not note:
            continue
        try:
            obj = json.loads(note)
        except json.JSONDecodeError:
            continue
        if obj.get("kind") != CHANGE_NOTE_KIND:
            continue
        rcid = obj.get("rcid")
        if rcid is not None:
            return int(rcid), None
    return (
        None,
        f"No branch commit has a change note (branch has {len(commits)} commits, "
        f"notes ref has {len(commits_with_notes)} entries; push notes and use fetch-depth: 0?).",
    )


def author_email(userid: int) -> str:
    return f"{userid}@{USER_EMAIL_DOMAIN}"


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def clean_empty_dirs(path: Path, stop_at: Path) -> None:
    current = path
    while current != stop_at and current.exists():
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def stage_all(repo: Path) -> None:
    run_git(repo, ["add", "-A"])


def has_staged_changes(repo: Path) -> bool:
    cp = run_git(repo, ["diff", "--cached", "--quiet"], check=False)
    return cp.returncode != 0


def commit_all(
    repo: Path,
    message: str,
    author_name: str,
    author_email_value: str,
    git_date: str,
    allow_empty: bool = False,
) -> str:
    env = {
        "GIT_AUTHOR_NAME": author_name,
        "GIT_AUTHOR_EMAIL": author_email_value,
        "GIT_AUTHOR_DATE": git_date,
        "GIT_COMMITTER_NAME": author_name,
        "GIT_COMMITTER_EMAIL": author_email_value,
        "GIT_COMMITTER_DATE": git_date,
    }
    args = ["commit", "-m", message]
    if allow_empty:
        args.append("--allow-empty")
    run_git(repo, args, env_extra=env)
    head = git_head_commit(repo)
    if not head:
        raise MediaWikiSyncError("Failed to read HEAD after commit")
    return head


def build_commit_message(rc: RecentChange) -> str:
    msg = rc.comment.strip()
    if msg:
        return msg
    if rc.type == "new":
        return f"Create {rc.title}"
    if rc.type == "edit":
        return f"Edit {rc.title}"
    if rc.type == "log":
        details = "/".join(x for x in [rc.logtype, rc.logaction] if x)
        return f"Log {details}: {rc.title}" if details else f"Log: {rc.title}"
    return f"Change: {rc.title}"


def mw_recentchanges(
    session: requests.Session,
    api_url: str,
    start_ts: Optional[str],
    end_ts: Optional[str],
    limit: int = 500,
) -> List[RecentChange]:
    params: Dict[str, Any] = {
        "action": "query",
        "format": "json",
        "list": "recentchanges",
        "rcprop": "title|ids|sizes|flags|user|userid|comment|timestamp|loginfo",
        "rctype": "edit|new|log",
        "rclimit": str(limit),
        "rcdir": "newer",
    }
    if start_ts:
        params["rcstart"] = start_ts
    if end_ts:
        params["rcend"] = end_ts

    out: List[RecentChange] = []
    cont: Optional[Dict[str, Any]] = None

    while True:
        req_params = dict(params)
        if cont:
            req_params.update(cont)
        data = api_get(session, api_url, req_params)

        for item in data.get("query", {}).get("recentchanges", []):
            logparams = item.get("logparams")
            old_title = None
            if isinstance(logparams, dict):
                old_title = (
                    logparams.get("target_title")
                    or logparams.get("oldtitle_title")
                    or logparams.get("4::target")
                )

            out.append(
                RecentChange(
                    rcid=int(item["rcid"]),
                    type=item["type"],
                    title=item["title"],
                    ns=int(item.get("ns", 0)),
                    timestamp=item["timestamp"],
                    comment=item.get("comment", "") or "",
                    user=item.get("user", "Unknown"),
                    userid=int(item.get("userid", 0) or 0),
                    revid=int(item["revid"]) if item.get("revid") else None,
                    old_revid=int(item["old_revid"]) if item.get("old_revid") else None,
                    logtype=item.get("logtype"),
                    logaction=item.get("logaction"),
                    logparams=logparams if isinstance(logparams, dict) else None,
                    old_title=old_title,
                )
            )

        if "continue" not in data:
            break
        cont = data["continue"]

    out.sort(key=lambda x: (x.timestamp, x.rcid))
    return out


def mw_revision_content_by_revids(
    session: requests.Session,
    api_url: str,
    revids: List[int],
) -> Dict[int, Optional[str]]:
    result: Dict[int, Optional[str]] = {}
    if not revids:
        return result

    chunk_size = 50
    for i in range(0, len(revids), chunk_size):
        chunk = revids[i:i + chunk_size]
        params = {
            "action": "query",
            "format": "json",
            "prop": "revisions",
            "revids": "|".join(str(x) for x in chunk),
            "rvprop": "ids|timestamp|content",
            "rvslots": "main",
        }
        data = api_get(session, api_url, params)
        pages = data.get("query", {}).get("pages", {})
        for page in pages.values():
            for rev in page.get("revisions", []):
                rid = int(rev["revid"])
                slots = rev.get("slots", {})
                main = slots.get("main", {})
                content = main.get("*")
                if content is None:
                    content = main.get("content")
                if content is None and "*" in rev:
                    content = rev["*"]
                result[rid] = content
        for rid in chunk:
            result.setdefault(rid, None)

    return result


def mw_namespace_ids(session: requests.Session, api_url: str) -> List[int]:
    """
    Return list of namespace IDs from siteinfo (non-negative only; allpages does not list Special/Media).
    """
    params = {
        "action": "query",
        "format": "json",
        "meta": "siteinfo",
        "siprop": "namespaces",
    }
    data = api_get(session, api_url, params)
    namespaces = data.get("query", {}).get("namespaces", {})
    out: List[int] = []
    for key in namespaces:
        try:
            ns_id = int(key)
        except (TypeError, ValueError):
            continue
        if ns_id >= 0:
            out.append(ns_id)
    return sorted(out)


def mw_all_pages(session: requests.Session, api_url: str, namespace: Optional[int] = None) -> List[Tuple[int, str, int]]:
    """
    Returns [(pageid, title, ns), ...] for all pages.
    """
    params: Dict[str, Any] = {
        "action": "query",
        "format": "json",
        "list": "allpages",
        "aplimit": "500",
    }
    if namespace is not None:
        params["apnamespace"] = str(namespace)

    out: List[Tuple[int, str, int]] = []
    cont: Optional[Dict[str, Any]] = None

    while True:
        req_params = dict(params)
        if cont:
            req_params.update(cont)
        data = api_get(session, api_url, req_params)
        for page in data.get("query", {}).get("allpages", []):
            out.append((int(page["pageid"]), page["title"], int(page["ns"])))
        if "continue" not in data:
            break
        cont = data["continue"]

    return out


def mw_page_revision_before_timestamp(
    session: requests.Session,
    api_url: str,
    title: str,
    cutoff_ts: str,
) -> Optional[Tuple[int, str]]:
    """
    Return (revid, content) for the newest revision at or before cutoff_ts.
    Returns None if the page did not exist yet or no readable revision is available.
    """
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "titles": title,
        "rvlimit": "1",
        "rvdir": "older",
        "rvstart": cutoff_ts,
        "rvprop": "ids|timestamp|content",
        "rvslots": "main",
    }
    data = api_get(session, api_url, params)
    pages = data.get("query", {}).get("pages", {})
    for page in pages.values():
        revs = page.get("revisions", [])
        if not revs:
            return None
        rev = revs[0]
        rid = int(rev["revid"])
        slots = rev.get("slots", {})
        main = slots.get("main", {})
        content = main.get("*")
        if content is None:
            content = main.get("content")
        if content is None and "*" in rev:
            content = rev["*"]
        if content is None:
            return None
        return rid, content
    return None


def clear_pages_tree(repo: Path) -> None:
    pages_root = repo / PAGES_DIR
    if pages_root.exists():
        for path in sorted(pages_root.rglob("*"), reverse=True):
            if path.is_file() or path.is_symlink():
                path.unlink()
            elif path.is_dir():
                try:
                    path.rmdir()
                except OSError:
                    pass
    pages_root.mkdir(parents=True, exist_ok=True)


def bootstrap_snapshot_before_oldest_change(
    session: requests.Session,
    api_url: str,
    repo: Path,
    oldest_change_ts: str,
) -> str:
    """
    Build repository state as it existed immediately before oldest_change_ts.
    """
    cutoff_dt = iso_to_dt(oldest_change_ts) - timedelta(seconds=1)
    cutoff_ts = dt_to_iso_z(cutoff_dt)

    debug(f"Bootstrapping snapshot before oldest change at {oldest_change_ts}")
    debug(f"Snapshot cutoff: {cutoff_ts}")

    clear_pages_tree(repo)

    namespace_ids = mw_namespace_ids(session, api_url)
    all_pages: List[Tuple[int, str, int]] = []
    for ns_id in namespace_ids:
        all_pages.extend(mw_all_pages(session, api_url, namespace=ns_id))
    debug(f"Enumerated {len(all_pages)} current pages across {len(namespace_ids)} namespaces")

    written = 0
    for idx, (_pageid, title, _ns) in enumerate(all_pages, start=1):
        rev = mw_page_revision_before_timestamp(session, api_url, title, cutoff_ts)
        if rev is None:
            continue

        _revid, content = rev
        target = repo / sanitize_title_to_path(title)
        ensure_parent_dir(target)
        target.write_text(content, encoding="utf-8")
        written += 1

        if idx % 100 == 0:
            debug(f"Checked {idx}/{len(all_pages)} pages, wrote {written}")

    stage_all(repo)

    bootstrap_msg = f"Bootstrap snapshot before {oldest_change_ts}"
    bootstrap_author = "MediaWiki Bootstrap"
    bootstrap_email = "bootstrap@user.vrchat.com"
    bootstrap_git_date = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    commit = commit_all(
        repo=repo,
        message=bootstrap_msg,
        author_name=bootstrap_author,
        author_email_value=bootstrap_email,
        git_date=bootstrap_git_date,
        allow_empty=not has_staged_changes(repo),
    )

    note = {
        "kind": BOOTSTRAP_NOTE_KIND,
        "snapshot_before": oldest_change_ts,
        "snapshot_cutoff": cutoff_ts,
        "pages_written": written,
    }
    write_note(repo, commit, json.dumps(note, ensure_ascii=False))
    return commit


def apply_change_to_worktree(
    repo: Path,
    rc: RecentChange,
    content_by_revid: Dict[int, Optional[str]],
) -> Tuple[bool, str]:
    target_rel = sanitize_title_to_path(rc.title)
    target_abs = repo / target_rel

    if rc.type in ("edit", "new"):
        if rc.revid is None:
            return True, "missing revid"
        content = content_by_revid.get(rc.revid)
        if content is None:
            return True, f"missing content for revid {rc.revid}"
        ensure_parent_dir(target_abs)
        target_abs.write_text(content, encoding="utf-8")
        return False, f"write {target_rel}"

    if rc.type == "log":
        if rc.logtype == "delete":
            if target_abs.exists():
                target_abs.unlink()
                clean_empty_dirs(target_abs.parent, repo)
                return False, f"delete {target_rel}"
            return True, f"delete missing {target_rel}"

        if rc.logtype == "move":
            source_title = None
            if rc.logparams:
                source_title = (
                    rc.logparams.get("target_title")
                    or rc.logparams.get("oldtitle_title")
                    or rc.logparams.get("4::target")
                )
            if source_title:
                source_rel = sanitize_title_to_path(source_title)
                source_abs = repo / source_rel
                if source_abs.exists():
                    ensure_parent_dir(target_abs)
                    source_abs.rename(target_abs)
                    clean_empty_dirs(source_abs.parent, repo)
                    return False, f"move {source_rel} -> {target_rel}"
            return True, f"move without known source for {target_rel}"

        return True, f"log event {rc.logtype}/{rc.logaction or ''} on {rc.title}"

    return True, f"unsupported type {rc.type}"


def import_changes(
    session: requests.Session,
    api_url: str,
    repo: Path,
    changes: List[RecentChange],
) -> int:
    revids = [rc.revid for rc in changes if rc.revid is not None]
    content_map = mw_revision_content_by_revids(session, api_url, revids)

    imported = 0
    total = len(changes)
    for idx, rc in enumerate(changes, start=1):
        allow_empty, _desc = apply_change_to_worktree(repo, rc, content_map)
        stage_all(repo)

        commit = commit_all(
            repo=repo,
            message=build_commit_message(rc),
            author_name=rc.user or "Unknown",
            author_email_value=author_email(rc.userid),
            git_date=iso_to_git_date(rc.timestamp),
            allow_empty=allow_empty or not has_staged_changes(repo),
        )

        note = {
            "kind": CHANGE_NOTE_KIND,
            "rcid": rc.rcid,
            "timestamp": rc.timestamp,
            "title": rc.title,
            "revid": rc.revid,
            "type": rc.type,
        }
        write_note(repo, commit, json.dumps(note, ensure_ascii=False))
        imported += 1

        if idx % 100 == 0 or idx == total:
            debug(f"Replayed {idx}/{total} changes")

    return imported


def get_commits_chronological(repo: Path, max_commits: int = 50000) -> List[Tuple[str, str, str]]:
    result = run_git(
        repo,
        ["log", "--reverse", f"--max-count={max_commits}", "--format=%H%x00%aI%x00%s", "HEAD"],
    )
    out: List[Tuple[str, str, str]] = []
    for line in result.stdout.strip().splitlines():
        parts = line.split("\x00", 2)
        if len(parts) >= 3:
            out.append((parts[0], parts[1], parts[2]))
    return out


def regenerate_notes(
    session: requests.Session,
    api_url: str,
    repo: Path,
    limit: int = 500,
) -> None:
    commits = get_commits_chronological(repo)
    if not commits:
        debug("No commits to attach notes to.")
        return

    # Remove existing notes ref so we only have notes for current branch (no orphaned SHAs)
    try:
        run_git(repo, ["update-ref", "-d", NOTES_REF])
    except subprocess.CalledProcessError:
        pass

    bootstrap_re = re.compile(r"^Bootstrap snapshot before (.+)$")
    change_commits: List[Tuple[str, str, str]] = []  # (sha, date_iso, subject)
    bootstrap_commit: Optional[Tuple[str, str, str]] = None

    for sha, date_iso, subject in commits:
        m = bootstrap_re.match(subject.strip())
        if m:
            if bootstrap_commit is not None:
                raise MediaWikiSyncError("Multiple bootstrap commits found; cannot regenerate notes.")
            bootstrap_commit = (sha, date_iso, subject)
            snapshot_before = m.group(1).strip()
            if "Z" not in snapshot_before and "+" not in snapshot_before:
                snapshot_before = snapshot_before + "Z"
            cutoff_dt = iso_to_dt(snapshot_before) - timedelta(seconds=1)
            snapshot_cutoff = dt_to_iso_z(cutoff_dt)
            note = {
                "kind": BOOTSTRAP_NOTE_KIND,
                "snapshot_before": snapshot_before,
                "snapshot_cutoff": snapshot_cutoff,
                "pages_written": 0,
            }
            write_note(repo, sha, json.dumps(note, ensure_ascii=False))
            debug(f"Wrote bootstrap note on {sha[:8]}")
        else:
            change_commits.append((sha, date_iso, subject))

    if not change_commits:
        debug("No change commits to match.")
        return

    first_ts = change_commits[0][1].replace("Z", "+00:00")
    last_ts = change_commits[-1][1].replace("Z", "+00:00")
    start_dt = iso_to_dt(first_ts) - timedelta(minutes=5)
    end_dt = iso_to_dt(last_ts) + timedelta(minutes=5)
    start_ts = dt_to_iso_z(start_dt)
    end_ts = dt_to_iso_z(end_dt)

    debug(f"Fetching recent changes from {start_ts} to {end_ts} to match {len(change_commits)} commits")
    changes = mw_recentchanges(
        session=session,
        api_url=api_url,
        start_ts=start_ts,
        end_ts=end_ts,
        limit=limit,
    )

    if len(changes) != len(change_commits):
        debug(f"Warning: {len(changes)} changes from API vs {len(change_commits)} change commits; matching by order.")

    for i, (sha, _date_iso, _subject) in enumerate(change_commits):
        rc = changes[i] if i < len(changes) else None
        if rc is None:
            debug(f"No matching change for commit {sha[:8]}; skipping note.")
            continue
        note = {
            "kind": CHANGE_NOTE_KIND,
            "rcid": rc.rcid,
            "timestamp": rc.timestamp,
            "title": rc.title,
            "revid": rc.revid,
            "type": rc.type,
        }
        write_note(repo, sha, json.dumps(note, ensure_ascii=False))
        if (i + 1) % 100 == 0 or i + 1 == len(change_commits):
            debug(f"Wrote change notes: {i + 1}/{len(change_commits)}")

    debug("Regenerated notes for current branch.")


def compute_start_timestamp(last_state: Optional[Dict[str, Any]], explicit_start: Optional[str]) -> Optional[str]:
    if explicit_start:
        return explicit_start
    if last_state and last_state.get("kind") == CHANGE_NOTE_KIND and "timestamp" in last_state:
        dt = iso_to_dt(str(last_state["timestamp"])) - timedelta(minutes=5)
        return dt_to_iso_z(dt)
    dt = datetime.now(timezone.utc) - timedelta(days=90)
    return dt_to_iso_z(dt)


def main() -> int:
    parser = argparse.ArgumentParser(description="Import MediaWiki recent changes into Git history.")
    parser.add_argument("--api-url", help="MediaWiki API URL, e.g. https://wiki.vrchat.com/api.php", default="https://wiki.vrchat.com/api.php")
    parser.add_argument("--repo", help="Path to local git repository", default=".")
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        help="Extra HTTP header for API requests, format: 'Name: value'. Can be used multiple times.",
    )
    parser.add_argument(
        "--start",
        help="Explicit start timestamp in ISO 8601 UTC, e.g. 2026-03-01T00:00:00Z. "
             "If omitted, resume from git notes or default to 90 days ago.",
    )
    parser.add_argument("--end", help="Optional end timestamp in ISO 8601 UTC")
    parser.add_argument("--limit", type=int, default=500, help="RecentChanges page size per API request")
    parser.add_argument(
        "--regenerate-notes",
        action="store_true",
        help="Re-attach notes to current branch commits (e.g. after history rewrite). Fetches recent changes from API and matches by order.",
    )
    cli_from_env = os.environ.get("MEDIAWIKI_CLI_ARGS")
    if cli_from_env:
        args_list = [line.strip() for line in cli_from_env.splitlines() if line.strip()]
        args = parser.parse_args(args_list)
    else:
        args = parser.parse_args()


    if not args.api_url.startswith(("http://", "https://")):
        raise ValueError("--api-url must start with http:// or https://")
    repo = Path(args.repo).resolve()
    ensure_git_repo(repo)

    session = requests.Session()
    session.headers.update(parse_headers(args.header))
    session.headers["User-Agent"] = "MediaWiki2git/1.0 hackebein@gmail.com"

    if args.regenerate_notes:
        regenerate_notes(session=session, api_url=args.api_url, repo=repo, limit=args.limit)
        return 0

    last_state = get_head_note_state(repo)
    start_ts = compute_start_timestamp(last_state, args.start)

    debug(f"Using repo: {repo}")
    debug(f"Fetching changes from {start_ts or '(site default)'} to {args.end or '(now)'}")

    changes = mw_recentchanges(
        session=session,
        api_url=args.api_url,
        start_ts=start_ts,
        end_ts=args.end,
        limit=args.limit,
    )

    last_rcid, notes_debug = get_last_imported_rcid(repo)
    if last_rcid is not None:
        before = len(changes)
        changes = [rc for rc in changes if rc.rcid > last_rcid]
        if before > len(changes):
            debug(f"Skipping {before - len(changes)} already-imported changes (last rcid={last_rcid})")
    elif notes_debug:
        debug(notes_debug)

    if not changes:
        debug("No new changes to import.")
        return 0

    if git_head_commit(repo) is None:
        oldest = changes[0]
        bootstrap_snapshot_before_oldest_change(
            session=session,
            api_url=args.api_url,
            repo=repo,
            oldest_change_ts=oldest.timestamp,
        )

    debug(f"Replaying {len(changes)} recent changes")
    imported = import_changes(session, args.api_url, repo, changes)
    debug(f"Imported {imported} commits")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except requests.HTTPError as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        raise SystemExit(2)
    except MediaWikiSyncError as exc:
        print(f"MediaWiki sync error: {exc}", file=sys.stderr)
        raise SystemExit(3)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        raise SystemExit(130)
