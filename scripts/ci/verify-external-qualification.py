#!/usr/bin/env python3
"""Verify externally-produced Windows qualification evidence before promotion."""
from __future__ import annotations
import argparse, hashlib, json, pathlib, sys, zipfile


def sha256(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--manifest', required=True, type=pathlib.Path)
    ap.add_argument('--summary', required=True, type=pathlib.Path)
    ap.add_argument('--archive', required=True, type=pathlib.Path)
    ap.add_argument('--checksums', required=True, type=pathlib.Path)
    ap.add_argument('--expected-tag', required=True)
    ap.add_argument('--expected-host', default='luigi.home.arpa')
    ap.add_argument('--expected-corpus', default='windows-corpus-1')
    args = ap.parse_args()

    data = json.loads(args.manifest.read_text(encoding='utf-8'))
    if data.get('schema_version') != 1: fail('unsupported qualification schema_version')
    if data.get('result') != 'PASS': fail('qualification result is not PASS')
    if data.get('host') != args.expected_host: fail('qualification host mismatch')
    if data.get('corpus_generation') != args.expected_corpus: fail('corpus generation mismatch')
    rel = data.get('release') or {}
    if rel.get('tag') != args.expected_tag: fail('release tag mismatch')

    for key in ('portable_result','installed_result','windows_guards_result','recovery_result'):
        if data.get(key) != 'PASS': fail(f'{key} is not PASS')
    if data.get('benchmark_result') not in ('PASS','BASELINE_ZERO'):
        fail('benchmark_result must be PASS or BASELINE_ZERO')

    expected_summary = data.get('summary_sha256','').lower()
    if expected_summary != sha256(args.summary): fail('qualification summary SHA-256 mismatch')

    archive_hash = sha256(args.archive)
    if rel.get('windows_archive_sha256','').lower() != archive_hash:
        fail('Windows archive SHA-256 mismatch')
    checksum_lines = args.checksums.read_text(encoding='utf-8').splitlines()
    wanted = args.archive.name
    published = None
    for line in checksum_lines:
        parts = line.strip().split(None, 1)
        if len(parts) == 2 and pathlib.Path(parts[1].lstrip('*')).name == wanted:
            published = parts[0].lower(); break
    if published != archive_hash: fail('checksums.txt does not bind the Windows archive hash')

    with zipfile.ZipFile(args.archive) as zf:
        names=[n for n in zf.namelist() if pathlib.PurePosixPath(n).name == 'codebase-memory-cli.exe']
        if len(names) != 1: fail('Windows archive must contain exactly one codebase-memory-cli.exe')
        h=hashlib.sha256(zf.read(names[0])).hexdigest()
    if rel.get('windows_executable_sha256','').lower() != h:
        fail('Windows executable SHA-256 mismatch')

    print(f'External qualification verified: {args.expected_tag} on {args.expected_host} ({args.expected_corpus})')

if __name__ == '__main__':
    main()
