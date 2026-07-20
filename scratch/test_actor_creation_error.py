"""Probe: does Actor.options(...).remote() itself raise when __init__ fails?
Or does the error only surface on the first method call?

Also test with get_if_exists=True to mirror our ShuffleManager pattern.
"""
import time
import ray
from ray.exceptions import ActorDiedError, ActorUnavailableError, GetTimeoutError

ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)


@ray.remote(max_restarts=-1)
class Bad:
    def __init__(self, mode):
        print(f"  [actor] __init__(mode={mode}) running")
        if mode == "raise":
            raise RuntimeError("intentional init failure")
        # mode == "ok"
        self._ok = True

    def hello(self):
        return 1


# ============================================================
# 1. Does .remote() itself raise?
# ============================================================
print("\n===== Test 1: does .remote() raise? =====")
start = time.time()
try:
    handle = Bad.options().remote("raise")
    elapsed = time.time() - start
    print(f"  [caller] .remote() returned in {elapsed:.3f}s WITHOUT raising")
    print(f"           handle type = {type(handle).__name__}")
except Exception as e:
    elapsed = time.time() - start
    print(f"  [caller] .remote() itself raised {type(e).__name__} in {elapsed:.3f}s")

# Second: try calling a method
print("  [caller] now try hello() on the handle...")
start = time.time()
try:
    r = ray.get(handle.hello.remote(), timeout=10.0)
    print(f"  [caller] got {r}")
except ActorDiedError as e:
    elapsed = time.time() - start
    print(f"  [caller] hello() → ActorDiedError in {elapsed:.3f}s (as expected)")


# ============================================================
# 2. Named actor + get_if_exists=True with dead prior actor
# ============================================================
print("\n===== Test 2: get_if_exists=True with previously-dead actor =====")

# First, create a named actor that will die
print("  Creating first Bad(name=bad-actor) that will fail init...")
h1 = Bad.options(name="bad-actor", get_if_exists=True, lifetime="detached").remote("raise")
try:
    ray.get(h1.hello.remote(), timeout=10.0)
except ActorDiedError:
    print("  First actor confirmed dead (ActorDiedError on hello)")

# Now try to create/get with same name — will it return dead one or make a new one?
print("\n  Now retrying with same name + get_if_exists=True...")
print("  (this time __init__ will succeed)")
start = time.time()
h2 = Bad.options(name="bad-actor", get_if_exists=True, lifetime="detached").remote("ok")
elapsed = time.time() - start
print(f"  [caller] .remote() returned in {elapsed:.3f}s")

try:
    r = ray.get(h2.hello.remote(), timeout=10.0)
    elapsed = time.time() - start
    print(f"  [caller] hello() → {r} (SUCCESS — new actor was created)")
except ActorDiedError as e:
    print(f"  [caller] hello() → ActorDiedError (bad-actor name kept the dead handle)")
    print(f"           msg: {str(e)[:200]}")


# ============================================================
# 3. Named actor + get_if_exists=True with previously-alive actor
# ============================================================
print("\n===== Test 3: get_if_exists=True returns existing live actor =====")
# By this point 'bad-actor' should be alive (from Test 2)
print("  Try Bad.options(name='bad-actor', get_if_exists=True).remote('raise')...")
print("  (should NOT re-create; should return existing handle)")
start = time.time()
h3 = Bad.options(name="bad-actor", get_if_exists=True, lifetime="detached").remote("raise")
elapsed = time.time() - start
print(f"  [caller] .remote() returned in {elapsed:.3f}s")

try:
    r = ray.get(h3.hello.remote(), timeout=10.0)
    print(f"  [caller] hello() → {r} (existing live actor served)")
except Exception as e:
    print(f"  [caller] hello() → {type(e).__name__}: {str(e)[:200]}")


print("\n===== done =====")
