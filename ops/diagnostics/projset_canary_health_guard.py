#!/usr/bin/env python3
"""Conservative throughput stop rule for synthetic canaries only."""
from collections import deque
import math


class Guard:
    """Stop a canary if all three live audit rates fall far below baseline.

    Counters are completed-directory accounting, so this rule can stop a test
    due to ordinary workload variation. It does not establish causality and
    never sends a signal to a production process.
    """
    def __init__(self, rates, window_seconds=90, ratio=.5):
        names = ('audit_files_links_per_hour', 'audit_logical_TB_per_hour', 'audit_directories_per_hour')
        self.baseline = tuple(rates.get(n) for n in names)
        if (not all(isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x > 0
                    for x in self.baseline) or not math.isfinite(window_seconds) or window_seconds < 60 or
                not math.isfinite(ratio) or not 0 < ratio < 1):
            raise ValueError('invalid synthetic throughput guard')
        self.window_seconds = window_seconds; self.ratio = ratio
        self.samples = deque(); self.latest = None

    def observe(self, audit):
        c = audit['counts']
        sample = (audit['updated'], c['files'] + c['symlinks'], c['file_bytes'], audit['done_dirs'])
        if not all(isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x >= 0 for x in sample):
            raise ValueError('invalid synthetic guard sample')
        if self.samples:
            previous = self.samples[-1]
            if sample[0] < previous[0] or any(a < b for a, b in zip(sample[1:], previous[1:])):
                raise ValueError('production progress counter regressed')
            if sample[0] == previous[0]: return self.latest
        self.samples.append(sample)
        while len(self.samples) > 2 and self.samples[1][0] <= sample[0] - self.window_seconds:
            self.samples.popleft()
        before = self.samples[0]; seconds = sample[0] - before[0]
        if seconds < self.window_seconds: return None
        delta = tuple(a-b for a, b in zip(sample[1:], before[1:]))
        rates = (delta[0]*3600/seconds, delta[1]*3600/seconds/1e12, delta[2]*3600/seconds)
        self.latest = {'seconds': seconds, 'files_links_per_hour': rates[0],
                       'logical_TB_per_hour': rates[1], 'directories_per_hour': rates[2],
                       'baseline_files_links_per_hour': self.baseline[0],
                       'baseline_logical_TB_per_hour': self.baseline[1],
                       'baseline_directories_per_hour': self.baseline[2],
                       'threshold_fraction': self.ratio,
                       'all_below_threshold': all(x < y*self.ratio for x, y in zip(rates, self.baseline))}
        return self.latest
