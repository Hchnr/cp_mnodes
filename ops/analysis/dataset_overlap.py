#!/usr/bin/env python3
"""Read-only, name-based dataset overlap review; no hashing or deletion."""
import collections
import concurrent.futures
import csv
import datetime
import functools
import gzip
import json
import os
from pathlib import Path
import re
import stat
import unicodedata

REPO = Path(__file__).resolve().parents[2]
HERE = REPO / "docs/analysis/dataset-overlap-20261102"
ROOTS = ('/share-mixed/project', '/share-mixed/projset')
SNAPSHOT = REPO / 'docs/analysis/storage-distribution-20261027/exports/directories-depth4.csv.gz'
LARGE = REPO / 'docs/analysis/storage-distribution-20261027/exports/large-file-directories-depth17.csv.gz'
GENERIC = set('data dataset datasets train test val validation raw processed sources '
              'materialized latents shards videos images frames recordings captures '
              'tactile downloadjt unpacked unzip benchmark static exampledata '
              'actionprocessedr1 actionprocessedr2 actionprocessedr3 logs log '
              'results outputs cache checkpoints models home repos eaipwm '
              'consistent takes handpose annotations assets batches decoded release tools '
              'metadata meta config configs scripts examples samples source target '
              'rgb depth original thumbnails info features statistics'.split())
CONTEXT = {'data', 'dataset', 'datasets', 'prepared_datasets', 'materialized',
           'sources', 'latents', 'unpacked', 'benchmark', 'hand_pose_1023',
           'paxini_cleaning', 'tactile'}
CONTAINERS = (
    'eai_pwm/datasets', 'eai_pwm/datasets/download_jt',
    'eai_pwm/datasets/download_jt/unzip', 'eai_pwm/datasets/train',
    'eai_pwm/datasets/tactile', 'eai_pwm/datasets/action_processed_r1',
    'eai_pwm/datasets/action_processed_r2', 'eai_pwm/datasets/action_processed_r3',
    'eai_pwm/prepared_datasets', 'eai_pwm/pipeline_store/materialized',
    'eai_pwm/pipeline_store/sources', 'eai_pwm/pipeline_store/latents',
    'eai_pwm/unpacked', 'eai_pwm/paxini_cleaning/source',
    'yhzhang/data', 'yhzhang/hand_pose_1023', 'gz/data/tac_data',
    'gz/DexTrain/datasets', 'materialized',
)
FAMILIES = (
    ('RoboMIND 2', r'^robomind(?:2|v2)'),
    ('RoboMIND 1', r'^(?:robomind(?:1|v1|$))'),
    ('Ego-Exo4D', r'^egoexo4d'), ('EgoStandard', r'^egostandard'),
    ('EgoLive', r'^egolive'), ('EgoPro', r'^egopro'),
    ('Egocentric-10K', r'^egocentric10k'), ('Nymeria', r'^nymeria'),
    ('OpenAoE-2000h', r'^openaoe2000h'), ('OpenVidHD', r'^openvidhd'),
    ('OpenNeoData', r'^openneodata'), ('Paxini-Omnisharing', r'^paxiniomnisharing'),
    ('OmniVitac', r'^omnivitac'), ('Lightwheel', r'^lightwheel'),
    ('AgiBotWorld', r'^agibotworld'), ('Galaxea', r'^galaxea'),
    ('RoboTwin 2', r'^(?:robotwin2|robotwin20)'),
    ('RoboCOIN', r'^robocoin'), ('DROID', r'^(?:cosmos3droid|droid)'),
    ('EgoDex', r'^egodex'), ('HoloAssist', r'^holoassist'),
    ('stereo-550', r'^stereo550'), ('KOALA-36M', r'^koala(?:36m|pilot)'),
    ('Action100M', r'^(?:action100m|a100m)'), ('GigaHands', r'^gigahands'),
    ('GPIC', r'^gpic'), ('OakInk 2', r'^oakink(?:2|v2)'),
    ('HO3D', r'^ho3d'), ('DexYCB', r'^dexycb'), ('TACO', r'^taco$'),
    ('RealOmin', r'^realomin'), ('RoboDojo', r'^robodojo'),
    ('MolmoBot', r'^molmobot'), ('InternData A1', r'^interndataa1'),
    ('InternData M1', r'^interndatam1'), ('FastUMI', r'^fastumi'),
    ('T-Rex', r'^(?:trex|trexdataset)'), ('Bench2Dex', r'^bench2dex'),
    ('Sharpa', r'^(?:sharpadex|sharpanorth|ftp1sharpa)'),
    ('OpenLoong', r'^openloong'), ('UltraVideo', r'^ultravideo'),
    ('PH2D', r'^ph2d'), ('PE-Video', r'^pevideo'),
)


def compact(name):
    return ''.join(c for c in unicodedata.normalize('NFKC', name).casefold()
                   if c.isalnum())


@functools.lru_cache(maxsize=65536)
def family(name):
    value = compact(name.lstrip('._'))
    for title, pattern in FAMILIES:
        if re.match(pattern, value):
            return title
    return None


def meaningful(path):
    p = Path(path)
    name = p.name
    key = compact(name)
    known = family(name)
    if any(family(parent) for parent in p.parts[3:-1]) and not known:
        return False
    hidden_extract = name.casefold().startswith('.omnivitac_extract')
    return (key not in GENERIC and len(key) >= 3 and
            (not name.startswith(('.', '_')) or hidden_extract) and
            (not name[:1].isdigit() or known) and
            not re.fullmatch(r'[0-9a-f]{12,}', key) and
            any(x in CONTEXT for x in p.parts[3:-1]))


def stage(path):
    if '/latents/' in path:
        return 'latent/特征'
    if '/materialized/' in path:
        return 'materialized/加工视频'
    if '/hand_pose_' in path or '/preprocess/' in path:
        return '姿态/预处理'
    action = re.search(r'/(action_[^/]+)/', path)
    if action:
        return action.group(1)+'/动作或训练加工'
    if '/sources/' in path:
        return 'pipeline sources/源数据'
    if '/unpacked/' in path or '/unzip/' in path:
        return '解包数据'
    if '/prepared_datasets/' in path:
        return 'prepared/训练打包'
    if '/datasets/' in path or '/data/' in path:
        return '数据集（格式未核验）'
    return '其他'


def main():
    items = {}
    snapshot_rows = 0
    def put(path, size=None, files=None, basis='unknown', allocated=None):
        if path in items and (items[path]['size_basis'] == 'full_20261027'
                              or basis == 'unknown'):
            return
        if not meaningful(path):
            return
        items[path] = dict(path=path, name=Path(path).name,
            root='project' if path.startswith(ROOTS[0]+'/') else 'projset',
            name_key=compact(Path(path).name), family=family(Path(path).name),
            stage=stage(path), logical_bytes=size, regular_file_paths=files,
            allocated_bytes_path_sum=allocated, size_basis=basis)

    with gzip.open(SNAPSHOT, 'rt', encoding='utf-8-sig', newline='') as stream:
        for row in csv.DictReader(stream):
            snapshot_rows += 1
            size = int(row['logical_bytes'])
            if (size >= 1_000_000_000 or
                    (int(row['depth']) <= 3 and family(Path(row['path']).name))):
                put(row['path'], size, int(row['regular_file_paths']),
                    'full_20261027', int(row['allocated_bytes_path_sum']))
    with gzip.open(LARGE, 'rt', encoding='utf-8-sig', newline='') as stream:
        for row in csv.DictReader(stream):
            path = row['directory']
            if (int(row['depth']) <= 7 and family(Path(path).name)
                    and int(row['logical_bytes_path_sum']) >= 1_000_000_000):
                put(path, int(row['logical_bytes_path_sum']),
                    int(row['large_file_paths_ge_1GiB']), 'large_files_only_20261027')

    listings = []
    for root in ROOTS:
        for rel in CONTAINERS:
            path = root+'/'+rel
            try:
                seen = 0
                with os.scandir(path) as stream:
                    for entry in stream:
                        seen += 1
                        if not entry.is_symlink() and entry.is_dir(follow_symlinks=False):
                            put(entry.path)
                        if seen >= 4096:
                            break
                listings.append(dict(path=path, entries=seen, bounded=seen >= 4096))
            except FileNotFoundError:
                listings.append(dict(path=path, absent=True))
            except OSError as exc:
                listings.append(dict(path=path, error=str(exc)))

    # Reduce live metadata requests to meaningful names repeated or known families.
    keys = collections.Counter(i['name_key'] for i in items.values())
    families = collections.Counter(i['family'] for i in items.values() if i['family'])
    selected = [i for i in items.values()
                if keys[i['name_key']] >= 2 or families[i['family']] >= 2]

    def observe(item):
        item = dict(item)
        try:
            info = os.lstat(item['path'])
            if not stat.S_ISDIR(info.st_mode):
                item['live'] = 'symlink' if stat.S_ISLNK(info.st_mode) else 'not_directory'
                return item
            item.update(st_dev=info.st_dev, st_ino=info.st_ino)
            with os.scandir(item['path']) as stream:
                first = next(stream, None)
            item['live'] = 'empty' if first is None else 'nonempty'
        except FileNotFoundError:
            item['live'] = 'absent'
        except OSError as exc:
            item.update(live='error', error=str(exc))
        return item

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        observed = list(pool.map(observe, selected))
    active = [i for i in observed if i['live'] == 'nonempty']
    exact = collections.defaultdict(list)
    related = collections.defaultdict(list)
    for item in active:
        exact[item['name_key']].append(item)
        if item['family']:
            related[item['family']].append(item)

    def prune_nested(rows):
        kept = []
        identities = set()
        for item in sorted(rows, key=lambda x: (len(Path(x['path']).parts), x['path'])):
            identity = item['st_dev'], item['st_ino']
            if identity not in identities and not any(
                    item['path'].startswith(x['path']+'/') for x in kept):
                kept.append(item)
                identities.add(identity)
        return kept

    exact_groups = []
    for key, rows in exact.items():
        rows = prune_nested(rows)
        if len(rows) < 2:
            continue
        sizes = [x['logical_bytes'] for x in rows
                 if x['size_basis'] == 'full_20261027' and x['logical_bytes']]
        same_stage = len({x['stage'] for x in rows}) == 1
        exact_groups.append(dict(name=rows[0]['name'], key=key, copies=rows,
            cross_root=len({x['root'] for x in rows}) > 1,
            same_stage=same_stage,
            smaller_copy_scenario_bytes=(sum(sizes)-max(sizes))
                if len(sizes) == len(rows) else None))
    exact_groups.sort(key=lambda g: (not g['same_stage'],
        -(g['smaller_copy_scenario_bytes'] or 0), g['key']))
    family_groups = []
    for name, rows in related.items():
        rows = prune_nested(rows)
        if len(rows) < 2:
            continue
        family_groups.append(dict(name=name, copies=rows,
            cross_root=len({x['root'] for x in rows}) > 1))
    family_groups.sort(key=lambda g: -sum(x['logical_bytes'] or 0 for x in g['copies']))

    meta = dict(generated_beijing=datetime.datetime.now(datetime.timezone(
        datetime.timedelta(hours=8))).isoformat(), snapshot='2026-10-27 14:28:13 北京时间',
        historical_rows=snapshot_rows, indexed_paths=len(items),
        checked_paths=len(observed), live_nonempty=len(active),
        live_counts=dict(collections.Counter(x['live'] for x in observed)),
        exact_groups=len(exact_groups), family_groups=len(family_groups),
        listing_errors=[x for x in listings if x.get('error') or x.get('bounded')],
        warning='Names only, not identity/subset proof. Historical logical sizes; '
                'not physical savings. Family groups and exact groups overlap. '
                'No dataset-content reads, hashes or deletion; symlink entries excluded.')
    result = dict(metadata=meta, exact_groups=exact_groups, family_groups=family_groups,
                  observations=observed, live_container_listings=listings)
    (HERE/'analysis.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    fields = ['match_type', 'group', 'path', 'root', 'stage', 'live',
              'logical_bytes', 'regular_file_paths', 'size_basis']
    with (HERE/'candidates.csv').open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for kind, groups in [('same_name', exact_groups), ('same_family', family_groups)]:
            for group in groups:
                for item in group['copies']:
                    writer.writerow(dict(item, match_type=kind, group=group['name']))
    print(json.dumps(meta, ensure_ascii=False, indent=2))
    print('SAME NAME (live nonempty directories):')
    for group in exact_groups[:35]:
        print(group['name'], 'cross_root='+str(group['cross_root']),
              'same_stage='+str(group['same_stage']))
        for item in group['copies']:
            print(' ', f"{(item['logical_bytes'] or 0)/1e12:.6f} TB",
                  item['size_basis'], item['path'])
    print('FAMILIES:')
    for group in family_groups:
        print(group['name'], len(group['copies']),
              round(sum(x['logical_bytes'] or 0 for x in group['copies'])/1e12, 3))


if __name__ == '__main__':
    main()
