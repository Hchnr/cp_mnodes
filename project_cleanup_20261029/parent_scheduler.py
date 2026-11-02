"""Bounded, in-memory fair queue; only explicit flat batches share parent quota."""
import collections
import queue
import threading
import time


class LeaseError(ValueError):
    """A result does not identify an outstanding task owned by this node."""


class ParentQueue:
    """One copy of each pending task, bounded by maxsize across all parents.

    A lease records task identity, not another deferred task. Task IDs must be
    unique within a producer run. Only ``flat_batch is True`` consumes the
    (boot,parent,ino) quota; independent directory tasks still receive leases.
    ``empty`` describes pending work, never merely work eligible for one boot.
    """
    def __init__(self, node_boots, *, nodes=None, maxsize=4096, limit=2):
        if type(maxsize) is not int or maxsize < 1:
            raise ValueError('maxsize must be a positive integer')
        self._validate_limit(limit)
        node_boots = dict(node_boots)
        if (not node_boots or any(not isinstance(n, str) or not n or
                not isinstance(b, str) or not b for n, b in node_boots.items())):
            raise ValueError('a complete node-to-boot mapping is required')
        if nodes is not None:
            nodes = list(nodes)
            if len(nodes) != len(set(nodes)) or set(nodes) != set(node_boots):
                raise ValueError('initial node-to-boot mapping is incomplete')
        self.maxsize, self._limit = maxsize, limit
        self._nodes = node_boots
        self._cv = threading.Condition(threading.RLock())
        self._queues = {}  # (parent,ino) -> (flat deque, independent deque)
        self._ready, self._blocked = {}, {}
        for boot in node_boots.values():
            self._ready.setdefault(boot, collections.OrderedDict())
            self._blocked.setdefault(boot, set())
        self._pending, self._sequence, self._stopped = 0, 0, False
        self._ids, self._leases = set(), {}
        self._by_node = {node: set() for node in node_boots}
        self._flat_active = collections.Counter()
        self._parent_active = collections.Counter()
        self._boot_active = collections.Counter()
        self._claims = self._releases = self._blocked_claims = self._parent_switches = 0
        self._last_parent = {}

    @staticmethod
    def _validate_limit(value):
        if value is not None and (type(value) is not int or value < 1):
            raise ValueError('limit must be a positive integer or None')

    def _flat_available(self, boot, key):
        return self._limit is None or self._flat_active[boot, key] < self._limit

    def _refresh(self, key):
        """Update indexes for one parent, O(boot groups), never O(pending)."""
        flat, independent = self._queues.get(key, ((), ()))
        if not flat and not independent:
            self._queues.pop(key, None)
        for boot, ready in self._ready.items():
            blocked = bool(flat) and not self._flat_available(boot, key)
            if blocked:
                self._blocked[boot].add(key)
            else:
                self._blocked[boot].discard(key)
            if independent or (flat and not blocked):
                ready.setdefault(key, None)
            else:
                ready.pop(key, None)

    @staticmethod
    def _deadline(timeout):
        if timeout is not None and timeout < 0:
            raise ValueError('timeout must not be negative')
        return None if timeout is None else time.monotonic()+timeout

    def _wait(self, deadline, exception):
        remaining = None if deadline is None else deadline-time.monotonic()
        if remaining is not None and remaining <= 0:
            raise exception
        self._cv.wait(remaining)

    def put(self, task, timeout=None):
        if (not isinstance(task, dict) or not isinstance(task.get('id'), str) or not task['id']
                or not isinstance(task.get('parent'), str) or type(task.get('ino')) is not int):
            raise ValueError('task requires id, parent and integer ino')
        deadline = self._deadline(timeout)
        with self._cv:
            while self._pending >= self.maxsize and not self._stopped:
                self._wait(deadline, queue.Full)
            if self._stopped:
                raise queue.Full('scheduler stopped')
            if task['id'] in self._ids:
                raise LeaseError('duplicate pending or leased task ID')
            key = task['parent'], task['ino']
            queues = self._queues.setdefault(key, (collections.deque(), collections.deque()))
            self._sequence += 1
            queues[0 if task.get('flat_batch') is True else 1].append((self._sequence, dict(task)))
            self._ids.add(task['id'])
            self._pending += 1
            self._refresh(key)
            self._cv.notify_all()

    def put_nowait(self, task):
        return self.put(task, timeout=0)

    def claim(self, node, timeout=0):
        deadline = self._deadline(timeout)
        with self._cv:
            if node not in self._nodes:
                raise KeyError('unregistered node: '+str(node))
            while True:
                if self._stopped:
                    raise queue.Empty('scheduler stopped')
                boot = self._nodes[node]
                ready = self._ready[boot]
                if ready:
                    key, _ = ready.popitem(last=False)
                    flat, independent = self._queues[key]
                    can_flat = bool(flat) and self._flat_available(boot, key)
                    use_flat = can_flat and (not independent or flat[0][0] < independent[0][0])
                    _, task = (flat if use_flat else independent).popleft()
                    self._pending -= 1
                    task_id = task['id']
                    self._leases[task_id] = node, boot, key, use_flat
                    self._by_node[node].add(task_id)
                    if use_flat:
                        self._flat_active[boot, key] += 1
                    self._parent_active[key] += 1
                    self._boot_active[boot] += 1
                    self._claims += 1
                    if boot in self._last_parent and self._last_parent[boot] != key:
                        self._parent_switches += 1
                    self._last_parent[boot] = key
                    self._refresh(key)  # chosen parent's next turn is at end
                    self._cv.notify_all()  # producers can use the freed slot
                    return task
                if self._pending:
                    self._blocked_claims += 1
                self._wait(deadline, queue.Empty)

    def _release(self, task_id, node):
        lease = self._leases.get(task_id)
        if lease is None or lease[0] != node:
            raise LeaseError('unknown, duplicate, or wrong-node lease release')
        _, boot, key, flat = self._leases.pop(task_id)
        self._by_node[node].remove(task_id)
        self._ids.remove(task_id)
        if flat:
            self._flat_active[boot, key] -= 1
            if not self._flat_active[boot, key]:
                del self._flat_active[boot, key]
        self._parent_active[key] -= 1
        if not self._parent_active[key]:
            del self._parent_active[key]
        self._boot_active[boot] -= 1
        if not self._boot_active[boot]:
            del self._boot_active[boot]
        self._releases += 1
        self._refresh(key)

    def release(self, task_id, node):
        """Call for every result, including rc=1, exactly once per task."""
        with self._cv:
            self._release(task_id, node)
            self._cv.notify_all()

    def release_node(self, node):
        """Only after confirmed worker exit/abort; does not cancel any work."""
        with self._cv:
            if node not in self._nodes:
                raise KeyError('unregistered node: '+str(node))
            task_ids = list(self._by_node[node])
            for task_id in task_ids:
                self._release(task_id, node)
            self._cv.notify_all()
            return len(task_ids)

    def register_node(self, node, boot):
        """Operator/controller must validate the live boot before this call."""
        if not isinstance(node, str) or not node or not isinstance(boot, str) or not boot:
            raise ValueError('node and boot must be nonempty strings')
        with self._cv:
            if self._stopped:
                raise ValueError('scheduler stopped')
            if node in self._nodes:
                if self._nodes[node] == boot:
                    return False
                if self._by_node[node]:
                    raise LeaseError('cannot change boot with outstanding leases')
            self._nodes[node] = boot
            self._by_node.setdefault(node, set())
            if boot not in self._ready:
                self._ready[boot] = collections.OrderedDict()
                self._blocked[boot] = set()
                for key in self._queues:
                    self._refresh(key)
            self._cv.notify_all()
            return True

    def set_limit(self, limit):
        self._validate_limit(limit)
        with self._cv:
            previous, self._limit = self._limit, limit
            for key in self._queues:
                self._refresh(key)
            self._cv.notify_all()
            return previous

    def stop(self):
        with self._cv:
            self._stopped = True
            self._cv.notify_all()

    def qsize(self):
        with self._cv:
            return self._pending

    def empty(self):
        return self.qsize() == 0

    def snapshot(self):
        """O(boot groups) counters; does not enumerate queued tasks/parents."""
        with self._cv:
            return dict(pending=self._pending, maxsize=self.maxsize, limit=self._limit,
                        active_leases=len(self._leases), active_parents=len(self._parent_active),
                        active_flat_groups=len(self._flat_active), pending_parents=len(self._queues),
                        boot_groups=len(self._ready), nodes=len(self._nodes), stopped=self._stopped,
                        claims=self._claims, releases=self._releases,
                        quota_waits=self._blocked_claims, parent_switches=self._parent_switches,
                        eligible_parents_by_boot={boot: len(ready) for boot, ready in self._ready.items()},
                        blocked_flat_parents_by_boot={boot: len(keys) for boot, keys in self._blocked.items()},
                        active_by_boot=dict(self._boot_active))
