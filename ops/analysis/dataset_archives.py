#!/usr/bin/env python3
"""Metadata-only archive/extraction review. Never unpack, hash, delete or deploy."""
import collections
import concurrent.futures
import csv
import datetime
import json
import os
from pathlib import Path
import re
import stat

HERE = Path(__file__).resolve().parents[2] / "docs/analysis/dataset-overlap-20261102"
P = '/share-mixed/project'
S = '/share-mixed/projset'
OMNI = S+'/eai_pwm/datasets/tactile/OmniVitac_platform_208546131716604003/OmniVitac_20260417'
STAGE = S+'/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021'
JOB = S+'/eai_pwm/operations_logs/2026-10-21/omnivitac-unpack'
CATEGORIES = ('Grasping', 'Assembly', 'Adjustment', 'Cutting', 'Wiping', 'Peeling')
ARCHIVE = re.compile(
    r'(?i)^(?P<stem>.+?)\.(?P<format>tar\.(?:gz|bz2|xz|zst|lz4)|tgz|tbz2|txz|tzst|tar|zip|7z|rar)'
    r'(?P<part>\.(?:part[-_.]?[a-z0-9]+|[0-9]{1,8}|[a-z]{2}))?$')


def archive_name(name):
    match = ARCHIVE.fullmatch(name)
    return match.groupdict() if match else None


def verified_candidate(source, previous, verified, dest):
    return bool(source['archives'] and previous.get('status') == 'PASS'
                and verified.get('status') == 'PASS' and dest['state'] == 'nonempty'
                and source['complete'] and not source['errors']
                and previous.get('compressed_total') == sum(x['bytes'] for x in source['archives']))


def nofollow_directory(path):
    """Pin every directory component; do not follow a symlink into another tree."""
    path = Path(path)
    if not any(str(path) == root or str(path).startswith(root+'/') for root in (P, S)):
        raise ValueError('outside review roots')
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def listing(path, limit=12000):
    record = dict(path=path, entries=0, complete=True, directories=[],
                  archives=[], regular_nonarchives=0, nonarchive_samples=[], errors=[])
    try:
        fd = nofollow_directory(path)
        try:
            with os.scandir(fd) as entries:
                for entry in entries:
                    if record['entries'] >= limit:
                        record['complete'] = False
                        break
                    record['entries'] += 1
                    if entry.is_symlink():
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        record['directories'].append(entry.name)
                        continue
                    match = archive_name(entry.name)
                    if not match:
                        record['regular_nonarchives'] += 1
                        if len(record['nonarchive_samples']) < 8:
                            record['nonarchive_samples'].append(entry.name)
                        continue
                    try:
                        info = entry.stat(follow_symlinks=False)
                        if stat.S_ISREG(info.st_mode):
                            record['archives'].append(dict(path=path+'/'+entry.name,
                                name=entry.name, bytes=info.st_size,
                                allocated_bytes=info.st_blocks*512, nlink=info.st_nlink,
                                dev=info.st_dev, ino=info.st_ino, mtime_ns=info.st_mtime_ns,
                                **match))
                    except OSError as exc:
                        record['errors'].append(dict(name=entry.name, error=str(exc)))
        finally:
            os.close(fd)
    except FileNotFoundError:
        record['absent'] = True
    except OSError as exc:
        record['complete'] = False
        record['errors'].append(dict(error=str(exc)))
    return record


def counterpart(path):
    try:
        fd = nofollow_directory(path)
        try:
            info = os.fstat(fd)
            with os.scandir(fd) as entries:
                first = next(entries, None)
            return dict(path=path, state='nonempty' if first else 'empty',
                        dev=info.st_dev, ino=info.st_ino)
        finally:
            os.close(fd)
    except FileNotFoundError:
        return dict(path=path, state='absent')
    except OSError as exc:
        return dict(path=path, state='error', error=str(exc))


def small_json(path):
    path = Path(path)
    if path.stat().st_size > 1_000_000:
        raise ValueError('status file too large')
    return json.loads(path.read_text())


def main():
    # Whole archive directories, not a recursive traversal of extraction outputs.
    specs = [(OMNI+'/'+category+'_zip_new', 'OmniVitac', category, 12000)
             for category in CATEGORIES]
    primary = [
        (P+'/eai_pwm/datasets/Assembly101', 'Assembly101', '', 12000),
        (P+'/eai_pwm/datasets/Assembly101/recordings', 'Assembly101', '', 12000),
        (P+'/eai_pwm/datasets/EPIC-KITCHENS-100', 'EPIC-KITCHENS-100', '', 12000),
        (P+'/eai_pwm/datasets/train/IKEA-ASM/data', 'IKEA-ASM', '', 12000),
        (P+'/eai_pwm/datasets/Hot3DAssets/_download_cache', 'Hot3DAssets', '', 12000),
        (P+'/eai_pwm/datasets/stanford-vision-lab/gpic/train', 'GPIC', '', 32),
        (P+'/eai_pwm/datasets/FTP-1-Dataset-Unzip', 'FTP-1', '', 4096),
        (P+'/eai_pwm/datasets/download_jt/RoboCOIN', 'RoboCOIN', '', 4096),
        (P+'/eai_pwm/datasets/RoboTwin2.0', 'RoboTwin2.0', '', 4096),
        (P+'/yhzhang/data/OpenAoE-2000h-raw', 'OpenAoE-2000h', '', 4096),
        (S+'/eai_pwm/datasets/OpenAoE-2000h', 'OpenAoE-2000h', '', 4096),
    ]
    specs += primary
    # RoboMIND task archives live below benchmark/h5 subgroup directories.
    extra_listings = []
    for root in (P, S):
        base = root+'/eai_pwm/datasets/RoboMIND'
        for version in ('benchmark1_0_compressed', 'benchmark1_1_compressed',
                        'benchmark1_2_compressed'):
            row = listing(base+'/'+version, 1024)
            extra_listings.append(row)
            for name in row['directories']:
                specs.append((row['path']+'/'+name, 'RoboMIND', version, 12000))
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        rows = list(pool.map(lambda item: listing(item[0], item[3]), specs))
    by_path = {row['path']: row for row in extra_listings+rows}
    results = []
    for spec, row in zip(specs, rows):
        path, dataset, category, _ = spec
        for item in row['archives']:
            item.update(dataset=dataset, category=category,
                        directory_listing_complete=row['complete'])
        if row['archives']:
            results.append(dict(dataset=dataset, category=category, path=path,
                archive_files=len(row['archives']),
                archive_logical_bytes=sum(x['bytes'] for x in row['archives']),
                archive_allocated_bytes_path_sum=sum(x['allocated_bytes'] for x in row['archives']),
                archives_with_nlink_gt1=sum(x['nlink'] > 1 for x in row['archives']),
                listing_complete=row['complete'], errors=row['errors'],
                formats=dict(collections.Counter(x['format'] for x in row['archives']))))
    # Reuse historical completion receipts; never recalculate their content hashes.
    status = small_json(JOB+'/status.json')
    verification = small_json(JOB+'/output_verification_summary.json')
    gate = small_json(JOB+'/source_gate.json')
    omni = []
    for category in CATEGORIES:
        source = by_path[OMNI+'/'+category+'_zip_new']
        previous = status.get('categories', {}).get(category, {})
        verified = verification.get('categories', {}).get(category, {})
        dest = counterpart(STAGE+'/'+category)
        expected = previous.get('compressed_total')
        size = sum(x['bytes'] for x in source['archives'])
        eligible = verified_candidate(source, previous, verified, dest)
        omni.append(dict(category=category, archive_directory=source['path'],
            archive_files=len(source['archives']), archive_bytes=size,
            receipt_compressed_bytes=expected, matches_receipt_total=expected == size,
            historical_extraction_status=previous.get('status'),
            historical_output_verification=verified.get('status'),
            historical_extracted_bytes=verified.get('bytes'),
            extracted_directory=dest, archive_cleanup_review_candidate=eligible,
            usage_confirmed=False,
            note=('Historical PASS + live nonempty output + matching current archive bytes. '
                  'Not proof of current use or current full output closure; no deletion authorized.'
                  if eligible else 'Not eligible for the verified-five-category archive candidate set.')))
    metadata = dict(generated_beijing=datetime.datetime.now(datetime.timezone(
        datetime.timedelta(hours=8))).isoformat(), mode='read_only_metadata',
        listed_directories=len(by_path),
        archive_files=sum(x['archive_files'] for x in results),
        observed_archive_bytes=sum(x['archive_logical_bytes'] for x in results),
        omni_five_category_candidate_bytes=sum(x['archive_bytes'] for x in omni
            if x['archive_cleanup_review_candidate']),
        current_usage_proven=False, datasets_fully_surveyed=False,
        note='Current archive sizes via stat; outputs checked only for nonemptiness. '
             'Historical receipts reused without hashing. Different classification from '
             'same-name duplicates: never add totals blindly. Plain tar may be direct input.')
    extra_destinations = [
        P+'/eai_pwm/datasets/Assembly101/poses@60fps',
        P+'/eai_pwm/datasets/EPIC-KITCHENS-100/data',
        P+'/eai_pwm/pipeline_store/sources/robomind1',
        S+'/eai_pwm/unpacked/robomind1',
    ]
    result = dict(metadata=metadata, omni_categories=omni, archive_directories=results,
        directory_observations=list(by_path.values()),
        additional_counterparts=[counterpart(path) for path in extra_destinations],
        historical_omni_constraints=dict(
            status_ts=status.get('ts'), verification_ts=verification.get('ts'),
            all_six_output_closures_pass=verification.get('all_six_output_closures_pass'),
            canonical_promoted=verification.get('canonical_promoted'),
            cleanup_allowed_only_after_all_six_verified=status.get('cleanup_allowed_only_after_all_six_verified'),
            missing_archive=gate.get('missing_archive'),
            historical_cleanup_permitted=gate.get('archive_or_native_subset_cleanup_permitted')))
    (HERE/'archive-analysis.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    with (HERE/'archive-files.csv').open('w', encoding='utf-8-sig', newline='') as out:
        fields = ['dataset','category','path','bytes','allocated_bytes','nlink','format',
                  'stem','part','directory_listing_complete','dev','ino','mtime_ns']
        writer = csv.DictWriter(out, fields, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerows(row['archives'])
    print(json.dumps(metadata, ensure_ascii=False, indent=2))
    for row in omni:
        print(row['category'], row['archive_files'], round(row['archive_bytes']/1e12, 6),
              row['archive_cleanup_review_candidate'], row['extracted_directory']['state'])
    groups = collections.defaultdict(lambda: [0,0])
    for row in results:
        groups[row['dataset']][0] += row['archive_files']
        groups[row['dataset']][1] += row['archive_logical_bytes']
    print('Observed archive files/bytes by dataset:',dict(groups))


if __name__ == '__main__':
    main()
