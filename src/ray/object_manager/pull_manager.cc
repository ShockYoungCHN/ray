// Copyright 2020-2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/object_manager/pull_manager.h"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"
#include "ray/util/time.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

namespace ray {

// Logger storage. The accessor + EmitPullEvent template are declared in
// pull_manager.h so non-PullManager translation units (e.g. object_manager.cc
// for `phase=object_pushed`) can emit directly into the same file.
std::shared_ptr<spdlog::logger> &MutablePullEventLogger() {
  static std::shared_ptr<spdlog::logger> logger;
  return logger;
}

namespace {

// Single place that decides T1 has fired and writes the event line. Safe to
// call from both the warm-construct path in Pull() and OnLocationChange's
// MarkBundleAsPullable site — emits exactly once per bundle since the caller
// only sets `pullable_time` once.
void RecordBundleLocateLatency(uint64_t request_id,
                               size_t bundle_size,
                               absl::Duration latency,
                               bool was_warm_on_construct,
                               const std::string &task_name) {
  EmitPullEvent(
      "phase=bundle_locate req_id={} bundle_size={} latency_ms={} warm={} task={}",
      request_id,
      bundle_size,
      absl::ToDoubleMilliseconds(latency),
      was_warm_on_construct ? 1 : 0,
      task_name.empty() ? "-" : task_name);
}

// T1 -> T2 latency: time the bundle spent waiting in `inactive_requests` for
// plasma quota to free up before being activated. Large values point at
// plasma-quota backpressure (bundle is pullable but we can't afford the
// memory yet).
void RecordBundleActiveLatency(uint64_t request_id,
                               size_t bundle_size,
                               absl::Duration wait,
                               const std::string &task_name) {
  EmitPullEvent(
      "phase=bundle_active req_id={} bundle_size={} wait_ms={} task={}",
      request_id,
      bundle_size,
      absl::ToDoubleMilliseconds(wait),
      task_name.empty() ? "-" : task_name);
}

// T1 -> T3 latency: total transfer time from "all locations resolved" until
// every object is sealed in local plasma. Combined with bundle_locate
// (T0 -> T1) this gives the full raylet-side fetch wall time.
// `transfer_ms` here is from pullable -> complete, i.e. excluding location
// discovery; `total_ms` includes location discovery.
void RecordBundleCompleteLatency(uint64_t request_id,
                                 size_t bundle_size,
                                 absl::Duration transfer,
                                 absl::Duration total,
                                 const std::string &task_name) {
  EmitPullEvent(
      "phase=bundle_complete req_id={} bundle_size={} transfer_ms={} "
      "total_ms={} task={}",
      request_id,
      bundle_size,
      absl::ToDoubleMilliseconds(transfer),
      absl::ToDoubleMilliseconds(total),
      task_name.empty() ? "-" : task_name);
}

// Per-bundle terminal summary, emitted once at CancelPull regardless of
// whether the bundle ever became fully local. This is the single line that
// covers the population of bundles that never reach `bundle_complete` —
// without it those cases are invisible. Also surfaces the deactivate/
// reactivate churn counters that are *not* visible from
// bundle_active.wait_ms (which only measures first activation; see notes
// in ActivateNextBundlePullRequest).
//
// `outcome=complete` iff complete_time was ever stamped. lifetime_ms is
// always T_cancel - T0 so cancelled bundles still get an observable
// duration.
void RecordBundleTerminal(uint64_t request_id,
                          size_t bundle_size,
                          bool completed,
                          int num_deactivations,
                          int num_reactivations,
                          int num_dx_memory,
                          int num_dx_unpullable,
                          absl::Duration dx_total,
                          absl::Duration lifetime,
                          const std::string &task_name) {
  EmitPullEvent(
      "phase=bundle_terminal req_id={} bundle_size={} outcome={} "
      "dx_count={} reax_count={} dx_memory={} dx_unpullable={} "
      "dx_total_ms={} lifetime_ms={} task={}",
      request_id,
      bundle_size,
      completed ? "complete" : "no_complete",
      num_deactivations,
      num_reactivations,
      num_dx_memory,
      num_dx_unpullable,
      absl::ToDoubleMilliseconds(dx_total),
      absl::ToDoubleMilliseconds(lifetime),
      task_name.empty() ? "-" : task_name);
}

}  // namespace

void InitPullEventLogger(const std::string &fallback_log_dir) {
  if (MutablePullEventLogger()) {
    return;
  }

  std::string path;
  const char *env_path = std::getenv("RAY_PULL_EVENTS_LOG_PATH");
  if (env_path != nullptr && *env_path != '\0') {
    path = env_path;
  } else {
    path = "/tmp/raylet_pull_events.out";
  }

  std::error_code mkdir_ec;
  std::filesystem::path fs_path(path);
  if (fs_path.has_parent_path()) {
    std::filesystem::create_directories(fs_path.parent_path(), mkdir_ec);
  }

  RAY_LOG(INFO) << "Initializing pull event logger at " << path;

  auto lg = spdlog::basic_logger_st("raylet_pull_events", path, /*truncate=*/false);
  lg->set_pattern("%E.%f %v");
  lg->set_level(spdlog::level::info);
  lg->flush_on(spdlog::level::info);
  MutablePullEventLogger() = lg;
}

PullManager::PullManager(
    NodeID self_node_id,
    std::function<bool(const ObjectID &)> object_is_local,
    std::function<void(const std::vector<ObjectID> &, const NodeID &)> send_pull_request,
    std::function<void(const ObjectID &)> cancel_pull_request,
    std::function<void(const ObjectID &, rpc::ErrorType)> fail_pull_request,
    RestoreSpilledObjectCallback restore_spilled_object,
    std::function<double()> get_time_seconds,
    int pull_timeout_ms,
    int64_t num_bytes_available,
    std::function<std::unique_ptr<RayObject>(const ObjectID &)> pin_object,
    std::function<std::string(const ObjectID &)> get_locally_spilled_object_url)
    : self_node_id_(std::move(self_node_id)),
      object_is_local_(std::move(object_is_local)),
      send_pull_request_(std::move(send_pull_request)),
      cancel_pull_request_(std::move(cancel_pull_request)),
      restore_spilled_object_(std::move(restore_spilled_object)),
      get_time_seconds_(std::move(get_time_seconds)),
      pull_timeout_ms_(pull_timeout_ms),
      num_bytes_available_(num_bytes_available),
      pin_object_(std::move(pin_object)),
      get_locally_spilled_object_url_(std::move(get_locally_spilled_object_url)),
      fail_pull_request_(std::move(fail_pull_request)),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {}

uint64_t PullManager::Pull(const std::vector<rpc::ObjectReference> &object_ref_bundle,
                           BundlePriority prio,
                           const TaskMetricsKey &task_key,
                           std::vector<rpc::ObjectReference> *objects_to_locate) {
  // To avoid edge cases dealing with duplicated object ids in the bundle,
  // canonicalize the set up-front by dropping all duplicates.
  absl::flat_hash_set<ObjectID> seen;
  std::vector<rpc::ObjectReference> deduplicated;
  for (const auto &ref : object_ref_bundle) {
    const auto id = ObjectRefToId(ref);
    const bool is_new = seen.insert(id).second;
    if (is_new) {
      deduplicated.emplace_back(ref);
    }
  }

  BundlePullRequest bundle_pull_request(ObjectRefsToIds(deduplicated), task_key);
  const uint64_t req_id = next_req_id_++;
  RAY_LOG(DEBUG) << "Start pull request " << req_id
                 << ". Bundle size: " << bundle_pull_request.objects_.size();

  for (const auto &ref : deduplicated) {
    const auto obj_id = ObjectRefToId(ref);
    auto it = object_pull_requests_.find(obj_id);
    if (it == object_pull_requests_.end()) {
      RAY_LOG(DEBUG) << "Pull of object " << obj_id;
      // We don't have a pull for this object yet. Ask the caller to
      // send us notifications about the object's location.
      objects_to_locate->push_back(ref);
      // The first pull request doesn't need to be special case. Instead we can just let
      // the retry timer fire immediately.
      it = object_pull_requests_.emplace(obj_id, ObjectPullRequest(get_time_seconds_()))
               .first;
    } else {
      if (it->second.IsPullable()) {
        bundle_pull_request.MarkObjectAsPullable(obj_id);
      }
      // If the object is already sealed in our local plasma at construction
      // time (warm path), record it on the bundle so the bundle_complete
      // emit below can fire with zero latency instead of waiting for an
      // OnLocationChange that may never come for already-local objects.
      if (object_is_local_(obj_id)) {
        bundle_pull_request.MarkObjectAsLocal(obj_id);
      }
    }
    it->second.bundle_request_ids.insert(req_id);
  }

  // T1 fast path: if the bundle is already pullable at construction time
  // (every object's location/size was already known from prior pulls),
  // `AddBundlePullRequest` short-circuits straight into `inactive_requests`
  // without ever routing through `OnLocationChange -> MarkBundleAsPullable`.
  // We have to record the (zero-latency) event here, otherwise warm bundles
  // are silently dropped from telemetry. Skip empty bundles (no objects =>
  // trivially pullable but meaningless to measure).
  const bool warm_on_construct =
      !bundle_pull_request.objects_.empty() && bundle_pull_request.IsPullable();
  if (warm_on_construct) {
    bundle_pull_request.pullable_time = bundle_pull_request.subscribe_start_time;
    RecordBundleLocateLatency(req_id,
                              bundle_pull_request.objects_.size(),
                              absl::ZeroDuration(),
                              /*was_warm_on_construct=*/true,
                              bundle_pull_request.task_key_.first);
    // If every object was also already local at construction, the bundle is
    // already complete; emit a zero-latency bundle_complete event so warm
    // bundles aren't silently absent from the complete-latency distribution.
    if (bundle_pull_request.IsComplete()) {
      bundle_pull_request.complete_time = bundle_pull_request.subscribe_start_time;
      RecordBundleCompleteLatency(req_id,
                                  bundle_pull_request.objects_.size(),
                                  absl::ZeroDuration(),
                                  absl::ZeroDuration(),
                                  bundle_pull_request.task_key_.first);
    }
  }

  if (prio == BundlePriority::GET_REQUEST) {
    get_request_bundles_.AddBundlePullRequest(req_id, std::move(bundle_pull_request));
  } else if (prio == BundlePriority::WAIT_REQUEST) {
    wait_request_bundles_.AddBundlePullRequest(req_id, std::move(bundle_pull_request));
  } else {
    RAY_CHECK(prio == BundlePriority::TASK_ARGS);
    task_argument_bundles_.AddBundlePullRequest(req_id, std::move(bundle_pull_request));
  }

  // We have a new request. Activate the new request, if the
  // current available memory allows it.
  UpdatePullsBasedOnAvailableMemory(num_bytes_available_);

  return req_id;
}

bool PullManager::ActivateNextBundlePullRequest(BundlePullRequestQueue &bundles,
                                                bool respect_quota,
                                                std::vector<ObjectID> *objects_to_pull) {
  if (bundles.inactive_requests.empty()) {
    // No inactive requests in the queue.
    return false;
  }

  // Get the next pull request in the queue.
  const auto next_request_id = *(bundles.inactive_requests.cbegin());
  const auto &next_request = map_find_or_die(bundles.requests, next_request_id);
  RAY_CHECK(next_request.IsPullable());

  // Activate the pull bundle request if possible.
  {
    absl::MutexLock lock(&active_objects_mu_);

    // First calculate the bytes we need.
    int64_t bytes_to_pull = 0;
    for (const auto &obj_id : next_request.objects_) {
      const bool needs_pull = active_object_pull_requests_.count(obj_id) == 0;
      if (needs_pull) {
        // This is the first bundle request in the queue to require this object.
        // Add the size to the number of bytes being pulled.
        // TODO(ekl) this overestimates bytes needed if it's already available
        // locally.
        bytes_to_pull += map_find_or_die(object_pull_requests_, obj_id).object_size;
      }
    }

    // Quota check.
    if (respect_quota && num_active_bundles_ >= 1 && bytes_to_pull > RemainingQuota()) {
      RAY_LOG(DEBUG) << "Bundle would exceed quota: "
                     << "num_bytes_being_pulled(" << num_bytes_being_pulled_
                     << ") + "
                        "bytes_to_pull("
                     << bytes_to_pull
                     << ") - "
                        "pinned_objects_size("
                     << pinned_objects_size_
                     << ") > "
                        "num_bytes_available("
                     << num_bytes_available_ << ")";
      return false;
    }

    RAY_LOG(DEBUG) << "Activating request " << next_request_id
                   << " num bytes being pulled: " << num_bytes_being_pulled_
                   << " num bytes available: " << num_bytes_available_;
    num_bytes_being_pulled_ += bytes_to_pull;
    for (const auto &obj_id : next_request.objects_) {
      const bool needs_pull = active_object_pull_requests_.count(obj_id) == 0;
      active_object_pull_requests_[obj_id].insert(next_request_id);
      if (needs_pull) {
        RAY_LOG(DEBUG) << "Activating pull for object " << obj_id;
        auto &request = map_find_or_die(object_pull_requests_, obj_id);
        request.activate_time_ms = current_time_ns() / 1e3;

        TryPinObject(obj_id);
        objects_to_pull->push_back(obj_id);
        ResetRetryTimer(obj_id);
      }
    }
  }

  bundles.ActivateBundlePullRequest(next_request_id);

  // T2: bundle just passed the plasma quota check and is now being pulled.
  // Record the activation wait (T1 -> T2) exactly once per bundle. Pullable
  // bundles that get deactivated and re-activated will not be re-counted
  // here — the `!active_time.has_value()` guard locks the value to the
  // first activation.
  //
  // Re-activations (the bundle came back from inactive due to plasma
  // churn) ARE counted, just on a separate axis: bump num_reactivations
  // and accumulate the time the bundle spent in inactive since the last
  // deactivation. That cumulative duration is the real plasma-backpressure
  // signal — first-activation wait is structurally near-zero because
  // admission is opportunistic+preemptive (see notes on `active_time` in
  // pull_manager.h).
  auto &mutable_request = map_find_or_die(bundles.requests, next_request_id);
  const absl::Time now = absl::Now();
  if (!mutable_request.active_time.has_value()) {
    mutable_request.active_time = now;
    const absl::Duration wait =
        mutable_request.pullable_time.has_value()
            ? (now - *mutable_request.pullable_time)
            : absl::ZeroDuration();
    RecordBundleActiveLatency(next_request_id,
                              mutable_request.objects_.size(),
                              wait,
                              mutable_request.task_key_.first);
  } else if (mutable_request.last_deactivated_time.has_value()) {
    mutable_request.num_reactivations++;
    mutable_request.total_deactivated_dur +=
        now - *mutable_request.last_deactivated_time;
    mutable_request.last_deactivated_time.reset();
  }

  num_active_bundles_ += 1;
  return true;
}

void PullManager::DeactivateBundlePullRequest(
    BundlePullRequestQueue &bundles,
    uint64_t request_id,
    std::unordered_set<ObjectID> *objects_to_cancel,
    DeactivationReason reason) {
  // Note: bind to a non-const ref so the churn counters below can be
  // updated. The existing per-object iteration (read-only) used a const
  // ref; switching to non-const is purely additive.
  auto &request = map_find_or_die(bundles.requests, request_id);
  for (const auto &obj_id : request.objects_) {
    absl::MutexLock lock(&active_objects_mu_);
    auto it = active_object_pull_requests_.find(obj_id);
    if (it == active_object_pull_requests_.end() || !it->second.erase(request_id)) {
      // The object is already deactivated, no action is required.
      continue;
    }
    if (it->second.empty()) {
      RAY_LOG(DEBUG) << "Deactivating pull for object " << obj_id;
      num_bytes_being_pulled_ -=
          map_find_or_die(object_pull_requests_, obj_id).object_size;
      active_object_pull_requests_.erase(obj_id);
      UnpinObject(obj_id);
      objects_to_cancel->insert(obj_id);
    }
  }

  bundles.DeactivateBundlePullRequest(request_id);
  num_active_bundles_ -= 1;

  // Churn bookkeeping: kCancelled is final teardown via CancelPull, not
  // plasma backpressure — skip counting it so the dx_count / dx_total_ms
  // fields stay attributable to memory/unpullable pressure.
  if (reason != DeactivationReason::kCancelled) {
    request.num_deactivations++;
    if (reason == DeactivationReason::kMemoryPressure) {
      request.num_dx_memory++;
    } else {
      request.num_dx_unpullable++;
    }
    // Stamp the time so the next re-activation can accumulate inactive
    // duration into total_deactivated_dur.
    request.last_deactivated_time = absl::Now();
  }
}

void PullManager::DeactivateUntilMarginAvailable(
    const std::string &debug_name,
    BundlePullRequestQueue &bundles,
    int retain_min,
    int64_t quota_margin,
    std::unordered_set<ObjectID> *object_ids_to_cancel) {
  while (RemainingQuota() < quota_margin && !bundles.active_requests.empty()) {
    if (num_active_bundles_ <= retain_min) {
      return;
    }
    const uint64_t request_id = *(bundles.active_requests.rbegin());
    RAY_LOG(DEBUG) << "Deactivating " << debug_name << " " << request_id
                   << " num bytes being pulled: " << num_bytes_being_pulled_
                   << " num bytes available: " << num_bytes_available_;
    DeactivateBundlePullRequest(bundles,
                                request_id,
                                object_ids_to_cancel,
                                DeactivationReason::kMemoryPressure);
  }
}

int64_t PullManager::RemainingQuota() {
  // Note that plasma counts pinned bytes as used.
  int64_t bytes_left_to_pull = num_bytes_being_pulled_ - pinned_objects_size_;
  return num_bytes_available_ - bytes_left_to_pull;
}

bool PullManager::OverQuota() { return RemainingQuota() < 0L; }

void PullManager::UpdatePullsBasedOnAvailableMemory(int64_t num_bytes_available) {
  if (num_bytes_available_ != num_bytes_available) {
    RAY_LOG(DEBUG) << "Updating pulls based on available memory: " << num_bytes_available;
  }
  num_bytes_available_ = num_bytes_available;

  std::vector<ObjectID> objects_to_pull;
  std::unordered_set<ObjectID> object_ids_to_cancel;
  // If there are any get requests (highest priority), try to activate them. Since we
  // prioritize get requests over task and wait requests, these requests will be
  // canceled as necessary to make space. We may exit this block over capacity
  // if we run out of requests to cancel, but this will be remedied later
  // by canceling task args and wait requests.
  bool get_requests_remaining = !get_request_bundles_.inactive_requests.empty();
  while (get_requests_remaining) {
    const int64_t margin_required = NextRequestBundleSize(get_request_bundles_);
    DeactivateUntilMarginAvailable("task args request",
                                   task_argument_bundles_,
                                   /*retain_min=*/0,
                                   /*quota_margin=*/margin_required,
                                   &object_ids_to_cancel);
    DeactivateUntilMarginAvailable("wait request",
                                   wait_request_bundles_,
                                   /*retain_min=*/0,
                                   /*quota_margin=*/margin_required,
                                   &object_ids_to_cancel);

    // Activate the next get request unconditionally.
    get_requests_remaining = ActivateNextBundlePullRequest(get_request_bundles_,
                                                           /*respect_quota=*/false,
                                                           &objects_to_pull);
  }

  // Do the same but for wait requests (medium priority).
  bool wait_requests_remaining = !wait_request_bundles_.inactive_requests.empty();
  while (wait_requests_remaining) {
    const int64_t margin_required = NextRequestBundleSize(wait_request_bundles_);
    DeactivateUntilMarginAvailable("task args request",
                                   task_argument_bundles_,
                                   /*retain_min=*/0,
                                   /*quota_margin=*/margin_required,
                                   &object_ids_to_cancel);

    // Activate the next wait request if we have space.
    wait_requests_remaining = ActivateNextBundlePullRequest(wait_request_bundles_,
                                                            /*respect_quota=*/true,
                                                            &objects_to_pull);
  }

  // Do the same but for task arg requests (lowest priority).
  // allowed for task arg requests.
  while (ActivateNextBundlePullRequest(task_argument_bundles_,
                                       /*respect_quota=*/true,
                                       &objects_to_pull)) {
  }

  // While we are over capacity, deactivate requests starting from the back of the queues.
  DeactivateUntilMarginAvailable("task args request",
                                 task_argument_bundles_,
                                 /*retain_min=*/1,
                                 /*quota_margin=*/0L,
                                 &object_ids_to_cancel);
  DeactivateUntilMarginAvailable("wait request",
                                 wait_request_bundles_,
                                 /*retain_min=*/1,
                                 /*quota_margin=*/0L,
                                 &object_ids_to_cancel);

  // Call the cancellation callbacks outside of the lock.
  for (const auto &obj_id : object_ids_to_cancel) {
    RAY_LOG(DEBUG) << "Not enough memory to create requested object " << obj_id
                   << ", aborting.";
    cancel_pull_request_(obj_id);
  }

  {
    absl::MutexLock lock(&active_objects_mu_);
    std::vector<ObjectID> to_pull_filtered;
    to_pull_filtered.reserve(objects_to_pull.size());
    for (const auto &obj_id : objects_to_pull) {
      if (object_ids_to_cancel.count(obj_id) == 0) {
        to_pull_filtered.push_back(obj_id);
      }
    }
    TryToMakeObjectsLocal(to_pull_filtered);
  }
}

std::vector<ObjectID> PullManager::CancelPull(uint64_t request_id) {
  RAY_LOG(DEBUG) << "Cancel pull request " << request_id;

  BundlePullRequestQueue &bundles = GetBundlePullRequestQueue(request_id);
  auto bundle_it = bundles.requests.find(request_id);
  RAY_CHECK(bundle_it != bundles.requests.end());

  // If the pull request was being actively pulled, deactivate it now.
  if (bundles.active_requests.count(request_id) > 0) {
    std::unordered_set<ObjectID> object_ids_to_cancel;
    DeactivateBundlePullRequest(bundles,
                                request_id,
                                &object_ids_to_cancel,
                                DeactivationReason::kCancelled);
    for (const auto &obj_id : object_ids_to_cancel) {
      // Call the cancellation callback outside of the lock.
      RAY_LOG(DEBUG) << "Pull cancellation requested for object " << obj_id
                     << ", aborting creation.";
      cancel_pull_request_(obj_id);
    }
  }

  // Terminal summary: emitted once per bundle at the only universal exit
  // point. Captures the population of bundles that never reached
  // bundle_complete (outcome=no_complete) plus the deactivate/reactivate
  // churn that bundle_active.wait_ms cannot see. Must run before
  // RemoveBundlePullRequest below, which drops the bundle from the map.
  // Empty bundles are skipped (no objects = trivially "complete" and
  // never carried churn — they only exist as a degenerate case).
  if (!bundle_it->second.objects_.empty()) {
    const absl::Duration lifetime =
        absl::Now() - bundle_it->second.subscribe_start_time;
    RecordBundleTerminal(request_id,
                         bundle_it->second.objects_.size(),
                         bundle_it->second.complete_time.has_value(),
                         bundle_it->second.num_deactivations,
                         bundle_it->second.num_reactivations,
                         bundle_it->second.num_dx_memory,
                         bundle_it->second.num_dx_unpullable,
                         bundle_it->second.total_deactivated_dur,
                         lifetime,
                         bundle_it->second.task_key_.first);
  }

  // Erase this pull request.
  std::vector<ObjectID> object_ids_to_cancel_subscription;
  for (const auto &obj_id : bundle_it->second.objects_) {
    auto it = object_pull_requests_.find(obj_id);
    if (it != object_pull_requests_.end()) {
      RAY_LOG(DEBUG) << "Removing an object pull request of id: " << obj_id;
      it->second.bundle_request_ids.erase(bundle_it->first);
      if (it->second.bundle_request_ids.empty()) {
        pull_manager_object_request_time_ms_histogram_.Record(
            current_time_ns() / 1e3 - it->second.request_start_time_ms,
            {{"Type", "StartToCancel"}});
        object_pull_requests_.erase(it);
        object_ids_to_cancel_subscription.push_back(obj_id);
      }
    }
  }
  bundles.RemoveBundlePullRequest(request_id);

  // We need to update the pulls in case there is another request(s) after this
  // request that can now be activated. We do this after erasing the cancelled
  // request to avoid reactivating it again.
  UpdatePullsBasedOnAvailableMemory(num_bytes_available_);

  return object_ids_to_cancel_subscription;
}

void PullManager::OnLocationChange(const ObjectID &object_id,
                                   const std::unordered_set<NodeID> &client_ids,
                                   const std::string &spilled_url,
                                   const NodeID &spilled_node_id,
                                   bool pending_creation,
                                   size_t object_size) {
  // Exit if the Pull request has already been fulfilled or canceled.
  auto it = object_pull_requests_.find(object_id);
  if (it == object_pull_requests_.end()) {
    return;
  }

  // Split bundle_locate into (a) owner responsiveness and (b) pending/size
  // gating: on the FIRST callback per object, log how long the owner took to
  // first respond and what state it reported. If first-cb delay is the tail =>
  // owner slow to serve locations (busy/contended). If first-cb is fast but the
  // object still isn't pullable (pending=1 or size=0) => reconstruction/size
  // gating drives the tail instead.
  if (it->second.first_cb_time_us == 0) {
    const int64_t now_us = current_time_ns() / 1e3;
    it->second.first_cb_time_us = now_us;
    EmitPullEvent(
        "phase=first_loc_cb object_id={} delay_ms={} pending={} size_set={} "
        "nloc={} has_url={}",
        object_id.Hex(),
        (now_us - it->second.request_start_time_ms) / 1e3,
        pending_creation ? 1 : 0,
        object_size > 0 ? 1 : 0,
        client_ids.size(),
        spilled_url.empty() ? 0 : 1);
  }

  const bool was_pullable_before = it->second.IsPullable();
  // Reset the list of clients that are now expected to have the object.
  // NOTE(swang): Since we are overwriting the previous list of clients,
  // we may end up sending a duplicate request to the same client as
  // before.
  it->second.client_locations.clear();
  for (const auto &client_id : client_ids) {
    if (client_id != self_node_id_) {
      // We can't pull from ourselves, so filter these locations out.
      // NOTE(swang): This means that we may try to pull an object even though
      // the directory says that we already have the object local in plasma.
      // This can happen due to a race condition between asynchronous updates
      // or a bug in the object directory.
      it->second.client_locations.push_back(client_id);
    }
  }
  it->second.spilled_url = spilled_url;
  it->second.spilled_node_id = spilled_node_id;
  it->second.pending_object_creation = pending_creation;
  if (!it->second.object_size_set) {
    it->second.object_size = object_size;
    it->second.object_size_set = true;

    if (it->second.object_size == 0) {
      RAY_LOG(WARNING) << "Size of object " << object_id
                       << " stored in object store is zero. This may be a bug since "
                          "objects in the object store should be large, and can result "
                          "in too many objects being fetched to this node";
    }
  }
  const bool is_pullable_after = it->second.IsPullable();

  if (was_pullable_before != is_pullable_after) {
    for (auto &bundle_request_id : it->second.bundle_request_ids) {
      BundlePullRequestQueue &bundles = GetBundlePullRequestQueue(bundle_request_id);
      auto bundle_it = bundles.requests.find(bundle_request_id);
      RAY_CHECK(bundle_it != bundles.requests.end());
      if (is_pullable_after) {
        bundle_it->second.MarkObjectAsPullable(object_id);
        if (bundle_it->second.IsPullable()) {
          bundles.MarkBundleAsPullable(bundle_request_id);
          // T1 slow path: record exactly the first time this bundle reaches
          // fully-pullable. Later spill/restore churn (false->true->false->true)
          // re-enters this branch — guarded by the `!pullable_time` check so
          // we never overwrite the initial transition.
          if (!bundle_it->second.pullable_time.has_value()) {
            const absl::Time now = absl::Now();
            bundle_it->second.pullable_time = now;
            RecordBundleLocateLatency(
                bundle_request_id,
                bundle_it->second.objects_.size(),
                now - bundle_it->second.subscribe_start_time,
                /*was_warm_on_construct=*/false,
                bundle_it->second.task_key_.first);
          }
        }
      } else {
        bundle_it->second.MarkObjectAsUnpullable(object_id);
        RAY_CHECK(!bundle_it->second.IsPullable());
        if (bundles.active_requests.count(bundle_request_id) > 0) {
          // It's active now so we need to deactivate it
          // to free memory for other requests.
          std::unordered_set<ObjectID> objects_to_cancel;
          DeactivateBundlePullRequest(bundles,
                                      bundle_request_id,
                                      &objects_to_cancel,
                                      DeactivationReason::kBecameUnpullable);
          for (const auto &obj_id : objects_to_cancel) {
            cancel_pull_request_(obj_id);
          }
        }
        bundles.MarkBundleAsUnpullable(bundle_request_id);
      }
    }

    UpdatePullsBasedOnAvailableMemory(num_bytes_available_);
    RAY_LOG(DEBUG) << "Updated location of " << object_id
                   << ", num bytes being pulled is now " << num_bytes_being_pulled_;
  }

  // T3 hook: an OnLocationChange may fire because the object just got sealed
  // in our local plasma (object_directory broadcasts our own holdership too).
  // Independently of the pullable-transition branch above, walk every bundle
  // that has this object and mark it locally available. The first time a
  // bundle hits fully-local, emit phase=bundle_complete with the transfer
  // and end-to-end durations relative to T1 (pullable) and T0 (subscribe).
  if (object_is_local_(object_id)) {
    for (auto &bundle_request_id : it->second.bundle_request_ids) {
      BundlePullRequestQueue &bundles = GetBundlePullRequestQueue(bundle_request_id);
      auto bundle_it = bundles.requests.find(bundle_request_id);
      if (bundle_it == bundles.requests.end()) {
        // Bundle was cancelled between the OnLocationChange enqueue and now.
        continue;
      }
      const bool became_complete = bundle_it->second.MarkObjectAsLocal(object_id);
      if (became_complete && !bundle_it->second.complete_time.has_value()) {
        const absl::Time now = absl::Now();
        bundle_it->second.complete_time = now;
        // Transfer phase = pullable -> complete. If the bundle was warm on
        // construct (pullable_time == subscribe_start_time), transfer ≈ total.
        const absl::Duration transfer =
            bundle_it->second.pullable_time.has_value()
                ? (now - *bundle_it->second.pullable_time)
                : absl::ZeroDuration();
        const absl::Duration total =
            now - bundle_it->second.subscribe_start_time;
        RecordBundleCompleteLatency(bundle_request_id,
                                    bundle_it->second.objects_.size(),
                                    transfer,
                                    total,
                                    bundle_it->second.task_key_.first);
      }
    }
  }

  RAY_LOG(DEBUG) << object_id << " OnLocationChange " << spilled_url << " num clients "
                 << client_ids.size();

  {
    absl::MutexLock lock(&active_objects_mu_);
    TryToMakeObjectsLocal({object_id});
  }
}

void PullManager::RestoreFromLocalOrTimeout(const ObjectID &object_id,
                                            ObjectPullRequest &request) {
  // check if we can restore the object directly in the current raylet.
  // first check local spilled objects
  std::string direct_restore_url = get_locally_spilled_object_url_(object_id);
  if (direct_restore_url.empty()) {
    if (!request.spilled_url.empty() && request.spilled_node_id.IsNil()) {
      direct_restore_url = request.spilled_url;
    }
  }
  if (!direct_restore_url.empty()) {
    // Select an url from the object directory update
    UpdateRetryTimer(request, object_id);
    // Avoid restore failure by the object already exists.
    // Details: https://github.com/ray-project/ray/issues/31390
    cancel_pull_request_(object_id);
    restore_spilled_object_(object_id,
                            request.object_size,
                            direct_restore_url,
                            [object_id](const ray::Status &status) {
                              if (!status.ok()) {
                                RAY_LOG(ERROR) << "Object restore for " << object_id
                                               << " failed, will retry later: " << status;
                              }
                            });
    return;
  }

  RAY_CHECK(!request.pending_object_creation);
  // Reached only when PickRandomPullLocation found NO location: client_locations
  // empty AND spilled_node_id nil AND no local spill URL. i.e. the directory has
  // no location for this object at pull time -> the puller is stuck until a
  // location update arrives or the 10s timer re-rolls. This is the "no location"
  // smoking gun for the tail objects.
  EmitPullEvent("phase=no_location object_id={} num_retries={}",
                object_id.Hex(),
                request.num_retries);
  if (request.expiration_time_seconds == 0) {
    RAY_LOG(WARNING) << "Object neither in memory nor external storage "
                     << object_id.Hex();
    request.expiration_time_seconds =
        get_time_seconds_() +
        RayConfig::instance().fetch_fail_timeout_milliseconds() / 1e3;
  } else if (get_time_seconds_() > request.expiration_time_seconds) {
    // Object has no locations and is not being reconstructed by its owner.
    fail_pull_request_(object_id, rpc::ErrorType::OBJECT_FETCH_TIMED_OUT);
    request.expiration_time_seconds = 0;
  }
}

std::optional<NodeID> PullManager::PickRandomPullLocation(const ObjectID &object_id) {
  auto it = object_pull_requests_.find(object_id);
  if (it == object_pull_requests_.end()) {
    return std::nullopt;
  }

  auto &node_vector = it->second.client_locations;
  auto &spilled_node_id = it->second.spilled_node_id;

  if (node_vector.empty()) {
    // Pull from remote node, it will be restored prior to push.
    if (!spilled_node_id.IsNil() && spilled_node_id != self_node_id_) {
      RAY_LOG(DEBUG).WithField(object_id)
          << "Sending pull request from " << self_node_id_ << " to spilled location at "
          << spilled_node_id;
      return spilled_node_id;
    }
    // The timer should never fire if there are no expected client locations.
    return std::nullopt;
  }

  RAY_CHECK(!object_is_local_(object_id));

  // Choose a random client to pull the object from.
  // Generate a random index.
  std::uniform_int_distribution<int> distribution(0, node_vector.size() - 1);
  int node_index = distribution(gen_);
  NodeID node_id = node_vector[node_index];
  RAY_CHECK(node_id != self_node_id_);
  RAY_LOG(DEBUG).WithField(object_id) << "Sending pull request from " << self_node_id_
                                      << " to in-memory location at " << node_id;
  return node_id;
}

void PullManager::TryToMakeObjectsLocal(const std::vector<ObjectID> &object_ids) {
  absl::flat_hash_map<NodeID, std::vector<ObjectID>> batch_by_node;

  const double now = get_time_seconds_();
  for (const auto &object_id : object_ids) {
    if (object_is_local_(object_id)) {
      continue;
    }
    if (active_object_pull_requests_.count(object_id) == 0) {
      continue;
    }
    ObjectPullRequest &request = map_find_or_die(object_pull_requests_, object_id);
    if (request.next_pull_time > now) {
      continue;
    }
    if (std::optional<NodeID> node_id = PickRandomPullLocation(object_id);
        node_id.has_value()) {
      // Real re-pull: no chunk had arrived (being_received=0), so the timer fired
      // and we re-issued the pull. num_locations=1 + has_spilled_loc=1 means the
      // object has a single home (its producer's spill file) — the re-pull can only
      // go back there, so this is slow-first-byte (disk restore), not stale-location.
      EmitPullEvent(
          "phase=repull object_id={} num_retries={} being_received=0 "
          "action=pull num_locations={} has_spilled_loc={}",
          object_id.Hex(),
          request.num_retries,
          request.client_locations.size(),
          request.spilled_node_id.IsNil() ? 0 : 1);
      batch_by_node[*node_id].push_back(object_id);
      UpdateRetryTimer(request, object_id);
    } else {
      RestoreFromLocalOrTimeout(object_id, request);
    }
  }

  for (const auto &[node_id, oids] : batch_by_node) {
    send_pull_request_(oids, node_id);
  }
}

void PullManager::ResetRetryTimer(const ObjectID &object_id) {
  auto it = object_pull_requests_.find(object_id);
  if (it != object_pull_requests_.end()) {
    it->second.next_pull_time = get_time_seconds_();
    it->second.num_retries = 0;
    it->second.expiration_time_seconds = 0;
  }
}

void PullManager::UpdateRetryTimer(ObjectPullRequest &request,
                                   const ObjectID &object_id) {
  const auto time = get_time_seconds_();
  auto retry_timeout_len = (pull_timeout_ms_ / 1000.) * (1UL << request.num_retries);
  request.next_pull_time = time + retry_timeout_len;
  if (retry_timeout_len > max_timeout_) {
    max_timeout_ = retry_timeout_len;
    max_timeout_object_id_ = object_id;
  }

  num_tries_total_++;
  if (request.num_retries > 0) {
    // We've tried this object before.
    num_retries_total_++;
  }

  // Bound the retry time at 10 * 1024 seconds.
  request.num_retries = std::min(request.num_retries + 1, 10);
  // Also reset the fetch expiration timer, since we just tried this pull.
  request.expiration_time_seconds = 0;
}

void PullManager::Tick() {
  absl::MutexLock lock(&active_objects_mu_);
  std::vector<ObjectID> active_objects;
  active_objects.reserve(active_object_pull_requests_.size());
  for (const auto &pair : active_object_pull_requests_) {
    active_objects.push_back(pair.first);
  }
  TryToMakeObjectsLocal(active_objects);
}

void PullManager::PinNewObjectIfNeeded(const ObjectID &object_id) {
  absl::MutexLock lock(&active_objects_mu_);
  bool active = active_object_pull_requests_.count(object_id) > 0;
  if (active) {
    if (TryPinObject(object_id)) {
      RAY_LOG(DEBUG) << "Pinned newly created object " << object_id;
    } else {
      RAY_LOG(DEBUG) << "Failed to pin newly created object " << object_id;
    }
  }
}

bool PullManager::TryPinObject(const ObjectID &object_id) {
  if (pinned_objects_.count(object_id) > 0) {
    return true;
  }
  auto ref = pin_object_(object_id);
  if (ref != nullptr) {
    num_succeeded_pins_total_++;
    pinned_objects_size_ += ref->GetSize();
    pinned_objects_[object_id] = std::move(ref);

    auto it = object_pull_requests_.find(object_id);
    RAY_CHECK(it != object_pull_requests_.end());
    pull_manager_object_request_time_ms_histogram_.Record(
        current_time_ns() / 1e3 - it->second.request_start_time_ms,
        {{"Type", "StartToPin"}});
    if (it->second.activate_time_ms > 0) {
      pull_manager_object_request_time_ms_histogram_.Record(
          current_time_ns() / 1e3 - it->second.activate_time_ms,
          {{"Type", "MemoryAvailableToPin"}});
    }
    return true;
  }

  num_failed_pins_total_++;
  return false;
}

void PullManager::UnpinObject(const ObjectID &object_id) {
  auto it = pinned_objects_.find(object_id);
  if (it != pinned_objects_.end()) {
    pinned_objects_size_ -= it->second->GetSize();
    pinned_objects_.erase(it);
  }
  if (pinned_objects_.empty()) {
    RAY_CHECK(pinned_objects_size_ == 0);
  }
}

int PullManager::NumObjectPullRequests() const { return object_pull_requests_.size(); }

bool PullManager::IsObjectActive(const ObjectID &object_id) const {
  absl::MutexLock lock(&active_objects_mu_);
  return active_object_pull_requests_.count(object_id) == 1;
}

const PullManager::BundlePullRequestQueue &PullManager::GetBundlePullRequestQueue(
    uint64_t request_id) const {
  if (get_request_bundles_.requests.contains(request_id)) {
    return get_request_bundles_;
  } else if (wait_request_bundles_.requests.contains(request_id)) {
    return wait_request_bundles_;
  } else {
    RAY_CHECK(task_argument_bundles_.requests.contains(request_id));
    return task_argument_bundles_;
  }
}

PullManager::BundlePullRequestQueue &PullManager::GetBundlePullRequestQueue(
    uint64_t request_id) {
  return const_cast<BundlePullRequestQueue &>(
      const_cast<const PullManager *>(this)->GetBundlePullRequestQueue(request_id));
}

bool PullManager::PullRequestActiveOrWaitingForMetadata(uint64_t request_id) const {
  const BundlePullRequestQueue &bundles = GetBundlePullRequestQueue(request_id);

  // If a request isn't inactive then it must be
  // either active or unpullable (i.e. waiting for metadata).
  return bundles.inactive_requests.count(request_id) == 0;
}

bool PullManager::HasPullsQueued() const {
  absl::MutexLock lock(&active_objects_mu_);
  return active_object_pull_requests_.size() != object_pull_requests_.size();
}

std::string PullManager::BundleInfo(const BundlePullRequestQueue &bundles) const {
  auto it = bundles.requests.begin();
  if (it == bundles.requests.end()) {
    return "N/A";
  }
  const auto &bundle = it->second;
  std::stringstream result;
  result << bundle.objects_.size() << " objects";
  if (!bundle.IsPullable()) {
    result << " (inactive, waiting for object sizes or locations)";
  } else {
    size_t num_bytes_needed = 0;
    for (const auto &obj_id : bundle.objects_) {
      num_bytes_needed += map_find_or_die(object_pull_requests_, obj_id).object_size;
    }
    result << ", " << num_bytes_needed << " bytes";
    if (bundles.active_requests.count(it->first) > 0) {
      result << " (active)";
    } else {
      result << " (inactive, waiting for capacity)";
    }
  }

  return result.str();
}

int64_t PullManager::NextRequestBundleSize(const BundlePullRequestQueue &bundles) const {
  if (bundles.inactive_requests.empty()) {
    // No inactive requests in the queue.
    return 0L;
  }
  // Get the next pull request in the queue.
  uint64_t next_request_id = *(bundles.inactive_requests.cbegin());
  const auto &next_request = map_find_or_die(bundles.requests, next_request_id);
  RAY_CHECK(next_request.IsPullable());

  absl::MutexLock lock(&active_objects_mu_);

  // Calculate the bytes we need.
  int64_t bytes_needed_calculated = 0;
  for (const auto &obj_id : next_request.objects_) {
    bool needs_pull = active_object_pull_requests_.count(obj_id) == 0;
    if (needs_pull) {
      // This is the first bundle request in the queue to require this object.
      // Add the size to the number of bytes being pulled.
      bytes_needed_calculated +=
          map_find_or_die(object_pull_requests_, obj_id).object_size;
    }
  }

  return bytes_needed_calculated;
}

void PullManager::RecordMetrics() const {
  absl::MutexLock lock(&active_objects_mu_);
  pull_manager_usage_bytes_gauge_.Record(num_bytes_available_, {{"Type", "Available"}});
  pull_manager_usage_bytes_gauge_.Record(num_bytes_being_pulled_,
                                         {{"Type", "BeingPulled"}});
  pull_manager_usage_bytes_gauge_.Record(pinned_objects_size_, {{"Type", "Pinned"}});
  pull_manager_requested_bundles_gauge_.Record(get_request_bundles_.requests.size(),
                                               {{"Type", "Get"}});
  pull_manager_requested_bundles_gauge_.Record(wait_request_bundles_.requests.size(),
                                               {{"Type", "Wait"}});
  pull_manager_requested_bundles_gauge_.Record(task_argument_bundles_.requests.size(),
                                               {{"Type", "TaskArgs"}});
  pull_manager_requested_bundles_gauge_.Record(next_req_id_,
                                               {{"Type", "CumulativeTotal"}});
  pull_manager_requests_gauge_.Record(object_pull_requests_.size(), {{"Type", "Queued"}});
  pull_manager_requests_gauge_.Record(active_object_pull_requests_.size(),
                                      {{"Type", "Active"}});
  pull_manager_requests_gauge_.Record(pinned_objects_.size(), {{"Type", "Pinned"}});
  pull_manager_active_bundles_gauge_.Record(num_active_bundles_);
  pull_manager_retries_total_gauge_.Record(num_retries_total_);
  pull_manager_retries_total_gauge_.Record(num_tries_total_);
  pull_manager_num_object_pins_gauge_.Record(num_succeeded_pins_total_,
                                             {{"Type", "Success"}});
  pull_manager_num_object_pins_gauge_.Record(num_failed_pins_total_,
                                             {{"Type", "Failure"}});
}

std::string PullManager::DebugString() const {
  absl::MutexLock lock(&active_objects_mu_);
  std::stringstream result;
  result << "PullManager:";
  result << "\n- num bytes available for pulled objects: " << num_bytes_available_;
  result << "\n- num bytes being pulled (all): " << num_bytes_being_pulled_;
  result << "\n- num bytes being pulled / pinned: " << pinned_objects_size_;
  result << "\n- get request bundles: " << get_request_bundles_.DebugString();
  result << "\n- wait request bundles: " << wait_request_bundles_.DebugString();
  result << "\n- task request bundles: " << task_argument_bundles_.DebugString();
  result << "\n- first get request bundle: " << BundleInfo(get_request_bundles_);
  result << "\n- first wait request bundle: " << BundleInfo(wait_request_bundles_);
  result << "\n- first task request bundle: " << BundleInfo(task_argument_bundles_);
  result << "\n- num objects queued: " << object_pull_requests_.size();
  result << "\n- num objects actively pulled (all): "
         << active_object_pull_requests_.size();
  result << "\n- num objects actively pulled / pinned: " << pinned_objects_.size();
  result << "\n- num bundles being pulled: " << num_active_bundles_;
  result << "\n- num pull retries: " << num_retries_total_;
  result << "\n- max timeout seconds: " << max_timeout_;
  auto it = object_pull_requests_.find(max_timeout_object_id_);
  if (it != object_pull_requests_.end()) {
    result << "\n- max timeout object id: " << max_timeout_object_id_;
    result << "\n- max timeout object: " << it->second.DebugString();
  } else {
    result << "\n- max timeout request is already processed. No entry.";
  }
  // Guard this more expensive debug message under event stats.
  if (RayConfig::instance().event_stats()) {
    for (const auto &entry : active_object_pull_requests_) {
      auto obj_id = entry.first;
      if (!pinned_objects_.contains(obj_id)) {
        result << "\n- example obj id pending pull: " << obj_id.Hex();
        break;
      }
    }
  }
  return result.str();
}

void PullManager::SetOutOfDisk(const ObjectID &object_id) {
  bool is_actively_pulled = false;
  {
    absl::MutexLock lock(&active_objects_mu_);
    is_actively_pulled = active_object_pull_requests_.count(object_id) > 0;
  }
  if (is_actively_pulled) {
    RAY_LOG(DEBUG) << "Pull of object failed due to out of disk: " << object_id;
    fail_pull_request_(object_id, rpc::ErrorType::OUT_OF_DISK_ERROR);
  }
}

}  // namespace ray
