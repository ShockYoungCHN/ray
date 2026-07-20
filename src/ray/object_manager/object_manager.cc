// Copyright 2017 The Ray Authors.
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

#include "ray/object_manager/object_manager.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "ray/common/filter_local_objects_util.h"
#include "ray/common/protobuf_utils.h"
#include "ray/object_manager/plasma/store_runner.h"
#include "ray/object_manager/spilled_object_reader.h"
#include "ray/util/container_util.h"
#include "ray/util/network_util.h"
#include "ray/util/time.h"

namespace ray {

ObjectStoreRunner::ObjectStoreRunner(const ObjectManagerConfig &config,
                                     SpillObjectsCallback spill_objects_callback,
                                     std::function<void()> object_store_full_callback,
                                     AddObjectCallback add_object_callback,
                                     DeleteObjectCallback delete_object_callback) {
  plasma::plasma_store_runner.reset(
      new plasma::PlasmaStoreRunner(config.store_socket_name,
                                    config.object_store_memory,
                                    config.huge_pages,
                                    config.plasma_directory,
                                    config.fallback_directory));
  // Initialize object store.
  store_thread_ = std::thread(&plasma::PlasmaStoreRunner::Start,
                              plasma::plasma_store_runner.get(),
                              spill_objects_callback,
                              object_store_full_callback,
                              add_object_callback,
                              delete_object_callback);
  // Sleep for sometime until the store is working. This can suppress some
  // connection warnings.
  std::this_thread::sleep_for(std::chrono::microseconds(500));
}

ObjectStoreRunner::~ObjectStoreRunner() {
  plasma::plasma_store_runner->Stop();
  store_thread_.join();
  plasma::plasma_store_runner.reset();
}

ObjectManager::ObjectManager(
    instrumented_io_context &main_service,
    const NodeID &self_node_id,
    const ObjectManagerConfig &config,
    gcs::GcsClient &gcs_client,
    IObjectDirectory *object_directory,
    RestoreSpilledObjectCallback restore_spilled_object,
    std::function<std::string(const ObjectID &)> get_spilled_object_url,
    std::function<std::unique_ptr<RayObject>(const ObjectID &object_id)> pin_object,
    const std::shared_ptr<plasma::PlasmaClientInterface> &buffer_pool_store_client,
    std::unique_ptr<ObjectStoreRunner> object_store_internal,
    std::function<std::shared_ptr<rpc::ObjectManagerClientInterface>(
        const std::string &address,
        const int port,
        rpc::ClientCallManager &client_call_manager)> object_manager_client_factory,
    instrumented_io_context &rpc_service)
    : main_service_(&main_service),
      self_node_id_(self_node_id),
      config_(config),
      gcs_client_(gcs_client),
      object_directory_(object_directory),
      object_store_internal_(std::move(object_store_internal)),
      buffer_pool_store_client_(buffer_pool_store_client),
      buffer_pool_(buffer_pool_store_client_, config_.object_chunk_size),
      rpc_service_(rpc_service),
      object_manager_server_("ObjectManager",
                             config_.object_manager_port,
                             IsLocalhost(config_.object_manager_address),
                             config_.rpc_service_threads_number),
      client_call_manager_(main_service,
                           /*record_stats=*/true,
                           /*local_address=*/"always not local",
                           ClusterID::Nil(),
                           config_.rpc_service_threads_number),
      restore_spilled_object_(std::move(restore_spilled_object)),
      get_spilled_object_url_(std::move(get_spilled_object_url)),
      pull_retry_timer_(*main_service_,
                        boost::posix_time::milliseconds(config.timer_freq_ms)),
      push_manager_(std::make_unique<PushManager>(/* max_chunks_in_flight= */ std::max(
          static_cast<int64_t>(1L),
          static_cast<int64_t>(config_.max_bytes_in_flight /
                               config_.object_chunk_size)))),
      object_manager_client_factory_(std::move(object_manager_client_factory)) {
  RAY_CHECK_GT(config_.rpc_service_threads_number, 0);

  pull_retry_timer_.async_wait([this](const boost::system::error_code &e) { Tick(e); });

  auto object_is_local = [this](const ObjectID &object_id) {
    return local_objects_.count(object_id) != 0;
  };
  auto send_pull_request = [this](const std::vector<ObjectID> &object_ids,
                                  const NodeID &client_id) {
    SendPullRequest(object_ids, client_id);
  };
  auto cancel_pull_request = [this](const ObjectID &object_id) {
    // We must abort this object because it may have only been partially
    // created and will cause a leak if we never receive the rest of the
    // object. This is a no-op if the object is already sealed or evicted.
    buffer_pool_.AbortCreate(object_id);
  };
  auto fail_pull_request = [this](const ObjectID &object_id, rpc::ErrorType error_type) {
    MarkObjectFailed(object_id, error_type);
  };
  auto get_time = []() { return absl::GetCurrentTimeNanos() / 1e9; };
  const int64_t available_memory = std::max<int64_t>(config.object_store_memory, 0);
  pull_manager_ = std::make_unique<PullManager>(self_node_id_,
                                                std::move(object_is_local),
                                                std::move(send_pull_request),
                                                std::move(cancel_pull_request),
                                                std::move(fail_pull_request),
                                                restore_spilled_object_,
                                                std::move(get_time),
                                                config.pull_timeout_ms,
                                                available_memory,
                                                std::move(pin_object),
                                                get_spilled_object_url_);

  RAY_CHECK_OK(
      buffer_pool_store_client_->Connect(config_.store_socket_name.c_str(), "", 300));

  // Start object manager rpc server and send & receive request threads
  StartRpcService();
}

ObjectManager::~ObjectManager() { Stop(); }

void ObjectManager::Stop() {
  // Stop the GRPC server before stopping the object store. This is to make sure when
  // we stop the object server, there will be no ongoing or future GRPC requests.
  StopRpcService();
  if (plasma::plasma_store_runner) {
    plasma::plasma_store_runner->Stop();
  }
  object_store_internal_.reset();
}

bool ObjectManager::IsPlasmaObjectSpillable(const ObjectID &object_id) {
  return plasma::plasma_store_runner->IsPlasmaObjectSpillable(object_id);
}

void ObjectManager::StartRpcService() {
  object_manager_server_.RegisterService(
      std::make_unique<rpc::ObjectManagerGrpcService>(rpc_service_, *this),
      false /* token_auth */);
  object_manager_server_.Run();
}

void ObjectManager::StopRpcService() {
  rpc_service_.stop();
  object_manager_server_.Shutdown();
}

void ObjectManager::HandleObjectAdded(const ObjectInfo &object_info) {
  // Notify the object directory that the object has been added to this node.
  const ObjectID &object_id = object_info.object_id;
  RAY_LOG(DEBUG) << "Object added " << object_id;
  RAY_CHECK(local_objects_.count(object_id) == 0);
  local_objects_[object_id].object_info = object_info;
  used_memory_ += object_info.data_size + object_info.metadata_size;

  // Receiver-side object completion: emitted the moment plasma seal
  // notified us. `last_chunk_to_seal_ms` = time between "last chunk we
  // saw on this node's HandlePush" and "plasma finished seal and told
  // us the object is local". Large values here point at plasma
  // finalize / metadata registration cost that is downstream of the
  // last chunk arriving. Complements chunk_received (per-chunk arrival)
  // and plasma_create_wait (per-chunk buffer alloc). Also clean the
  // last_chunk_recv_ns_ entry so the map stays bounded by in-flight
  // pulls without a completed seal.
  int64_t last_chunk_to_seal_ns = -1;
  {
    absl::MutexLock lock(&pull_sent_time_mu_);
    auto it = last_chunk_recv_ns_.find(object_id);
    if (it != last_chunk_recv_ns_.end()) {
      last_chunk_to_seal_ns = absl::GetCurrentTimeNanos() - it->second;
      last_chunk_recv_ns_.erase(it);
    }
  }
  if (last_chunk_to_seal_ns >= 0) {
    EmitPullEvent(
        "phase=object_sealed object_id={} bytes={} last_chunk_to_seal_ms={}",
        object_id.Hex(),
        object_info.data_size + object_info.metadata_size,
        last_chunk_to_seal_ns / 1e6);
  }

  object_directory_->ReportObjectAdded(object_id, self_node_id_, object_info);

  // Give the pull manager a chance to pin actively pulled objects.
  pull_manager_->PinNewObjectIfNeeded(object_id);

  // Handle the unfulfilled_push_requests_ which contains the push request that is not
  // completed due to unsatisfied local objects.
  auto iter = unfulfilled_push_requests_.find(object_id);
  if (iter != unfulfilled_push_requests_.end()) {
    for (auto &pair : iter->second) {
      auto &node_id = pair.first;
      // Reuse the ORIGINAL pull_recv_ns stamped when HandlePull first put
      // this request into the queue, not "now". That gap — from Pull-recv
      // to the object showing up locally — is exactly what `pull_to_start_ms`
      // should measure. Re-stamping here would collapse the metric back to
      // ~0 and hide the ~3s wait we're investigating.
      const int64_t pull_recv_ns = pair.second.pull_recv_ns;
      main_service_->post(
          [this, object_id, node_id, pull_recv_ns]() {
            Push(object_id, node_id, pull_recv_ns);
          },
          "ObjectManager.ObjectAddedPush");
      // When push timeout is set to -1, there will be an empty timer.
      if (pair.second.timer != nullptr) {
        pair.second.timer->cancel();
      }
    }
    unfulfilled_push_requests_.erase(iter);
  }
}

void ObjectManager::HandleObjectDeleted(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  RAY_CHECK(it != local_objects_.end());
  auto object_info = it->second.object_info;
  local_objects_.erase(it);
  used_memory_ -= object_info.data_size + object_info.metadata_size;
  RAY_CHECK(!local_objects_.empty() || used_memory_ == 0);
  // Object left this node's plasma (e.g. spilled to disk) -> tell the owner
  // directory we no longer hold the in-memory copy. Paired with SPILL_PATH:
  // if the spill also skips ReportObjectSpilled (is_freed), the directory ends
  // up with NO location at all for this object.
  EmitPullEvent("phase=loc_removed object_id={}", object_id.Hex());
  object_directory_->ReportObjectRemoved(object_id, self_node_id_, object_info);

  // Ask the pull manager to fetch this object again as soon as possible, if
  // it was needed by an active pull request.
  pull_manager_->ResetRetryTimer(object_id);
}

void ObjectManager::OnObjectSpilled(const ObjectID &object_id) {
  // The object just became servable from its local spill file (URL now
  // registered). Drain any push requests that were queued while it was
  // mid-spill (neither in plasma nor with a spill URL). Same rescue as
  // HandleObjectAdded, but triggered by spill completion instead of plasma
  // add. Post to main_service_ so the unfulfilled_push_requests_ access is on
  // the same single thread that HandlePull / HandleObjectAdded touch it on,
  // regardless of which thread the spill-completion callback runs on.
  main_service_->post(
      [this, object_id]() {
        // Diagnostic: did the spill-completion callback fire, and for which
        // objects, on the node that queued the push?
        EmitPullEvent("phase=spill_cb object_id={}", object_id.Hex());
        auto iter = unfulfilled_push_requests_.find(object_id);
        if (iter == unfulfilled_push_requests_.end()) {
          return;
        }
        // Diagnostic: a queued push was actually found and rescued. Its
        // timestamp vs the pull tells whether the rescue beat the
        // pull_timeout or fired too late.
        EmitPullEvent("phase=spill_rescue object_id={} n={}",
                      object_id.Hex(),
                      iter->second.size());
        for (auto &pair : iter->second) {
          const auto &node_id = pair.first;
          // Reuse the ORIGINAL pull_recv_ns so pull_to_start_ms reflects the
          // full HandlePull-to-serve gap (matches HandleObjectAdded).
          const int64_t pull_recv_ns = pair.second.pull_recv_ns;
          main_service_->post(
              [this, object_id, node_id, pull_recv_ns]() {
                Push(object_id, node_id, pull_recv_ns);
              },
              "ObjectManager.SpilledPush");
          if (pair.second.timer != nullptr) {
            pair.second.timer->cancel();
          }
        }
        unfulfilled_push_requests_.erase(iter);
      },
      "ObjectManager.OnObjectSpilled");
}

uint64_t ObjectManager::Pull(const std::vector<rpc::ObjectReference> &object_refs,
                             BundlePriority prio,
                             const TaskMetricsKey &task_key) {
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto request_id = pull_manager_->Pull(object_refs, prio, task_key, &objects_to_locate);

  const auto &callback = [this](const ObjectID &object_id,
                                const std::unordered_set<NodeID> &client_ids,
                                const std::string &spilled_url,
                                const NodeID &spilled_node_id,
                                bool pending_creation,
                                size_t object_size) {
    pull_manager_->OnLocationChange(object_id,
                                    client_ids,
                                    spilled_url,
                                    spilled_node_id,
                                    pending_creation,
                                    object_size);
  };

  for (const auto &ref : objects_to_locate) {
    // Subscribe to object notifications. A notification will be received every
    // time the set of node IDs for the object changes. Notifications will also
    // be received if the list of locations is empty. The set of node IDs has
    // no ordering guarantee between notifications.
    auto object_id = ObjectRefToId(ref);
    object_directory_->SubscribeObjectLocations(
        object_directory_pull_callback_id_, object_id, ref.owner_address(), callback);
  }

  return request_id;
}

void ObjectManager::CancelPull(uint64_t request_id) {
  const auto objects_to_cancel = pull_manager_->CancelPull(request_id);
  for (const auto &object_id : objects_to_cancel) {
    object_directory_->UnsubscribeObjectLocations(object_directory_pull_callback_id_,
                                                  object_id);
  }
}

void ObjectManager::MarkObjectFailed(const ObjectID &object_id,
                                     rpc::ErrorType error_type) {
  // TODO(swang): Ideally we should return the error directly to the client
  // that needs this object instead of storing the object in plasma, which is
  // not guaranteed to succeed. This avoids hanging the client if plasma is not
  // reachable.
  RAY_LOG(DEBUG).WithField(object_id)
      << "Mark the object as failed due to " << error_type;
  // Release any in flight pull placeholder first, otherwise the error
  // sentinel write below would silently collide with it.
  buffer_pool_.AbortCreate(object_id);
  const std::string meta = std::to_string(static_cast<int>(error_type));
  std::shared_ptr<Buffer> data;
  Status status = buffer_pool_store_client_->TryCreateImmediately(
      object_id,
      rpc::Address{},
      0,
      reinterpret_cast<const uint8_t *>(meta.c_str()),
      meta.length(),
      &data,
      plasma::flatbuf::ObjectSource::ErrorStoredByRaylet);
  if (status.ok()) {
    status = buffer_pool_store_client_->Seal(object_id);
  }
  if (!status.ok() && !status.IsObjectExists()) {
    std::ostringstream stream;
    stream << "A plasma error (" << status.ToString()
           << ") occurred while saving error code to object " << object_id
           << ". Anyone who's getting this object may hang forever.";
    std::string error_message = stream.str();
    RAY_LOG(ERROR) << error_message;
    auto error_data = gcs::CreateErrorTableData(
        "task", error_message, absl::FromUnixMillis(current_time_ms()));
    gcs_client_.Errors().AsyncReportJobError(std::move(error_data));
  }
}

void ObjectManager::SendPullRequest(const std::vector<ObjectID> &object_ids,
                                    const NodeID &client_id) {
  if (object_ids.empty()) {
    return;
  }
  auto rpc_client = GetRpcClient(client_id);
  // Did the pull actually go out? PullFromRandomLocation returns true (and the
  // retry timer is armed) even when GetRpcClient is null and this no-ops.
  // With batching, one Pull RPC carries N object_ids — emit one pull_sent
  // event per object so downstream aggregation stays per-object.
  {
    const int sent_flag = rpc_client ? 1 : 0;
    for (const auto &oid : object_ids) {
      EmitPullEvent("phase=pull_sent object_id={} dest={} sent={}",
                    oid.Hex(),
                    client_id.Hex(),
                    sent_flag);
    }
  }
  if (rpc_client) {
    // T0 for `phase=object_first_byte`: stamp the wall-clock here so that
    // when the first chunk arrives in HandlePush we can compute send-to-
    // first-byte latency. Overwrite on retry — only the most recent Pull
    // is relevant. Map entry is erased on first chunk receive; if the pull
    // is cancelled before any chunk, the entry leaks (bounded by in-flight
    // count, negligible).
    //
    // Also increment the cumulative pull-attempt counter so the eventual
    // `object_first_byte` line can carry `attempt=N`. `attempt > 1`
    // signals "we sent Pull for this object more than once" — i.e. the
    // object was either retried in-session or was evicted from local
    // plasma after a previous pull cycle and is now being re-fetched.
    // Counts over raylet lifetime, never reset, so the value is comparable
    // across the whole run.
    {
      absl::MutexLock lock(&pull_sent_time_mu_);
      const auto now = absl::Now();
      for (const auto &oid : object_ids) {
        pull_sent_time_[oid] = now;
        ++pull_attempt_count_[oid];
      }
    }
    rpc_service_.post(
        [this, object_ids, client_id, rpc_client]() {
          rpc::PullRequest pull_request;
          pull_request.set_node_id(self_node_id_.Binary());
          for (const auto &oid : object_ids) {
            pull_request.add_object_ids(oid.Binary());
          }
          // The callback needs the full object_ids list to emit a
          // per-object pull_result event, so copy it in. batch_size and
          // first_id are cheap derived views on the same vector.
          const size_t batch_size = object_ids.size();
          const ObjectID first_id = object_ids.front();

          rpc_client->Pull(
              pull_request,
              [object_ids, batch_size, first_id, client_id](
                  const Status &status, const rpc::PullReply &reply) {
                const int ok_flag = status.ok() ? 1 : 0;
                for (const auto &oid : object_ids) {
                  EmitPullEvent("phase=pull_result object_id={} dest={} ok={}",
                                oid.Hex(),
                                client_id.Hex(),
                                ok_flag);
                }
                if (!status.ok()) {
                  RAY_LOG_EVERY_N_OR_DEBUG(INFO, 100)
                      << "Send pull (batch of " << batch_size
                      << ", first=" << first_id << ") request to client "
                      << client_id << " failed due to " << status;
                }
              });
        },
        "ObjectManager.SendPull");
  } else {
    RAY_LOG_EVERY_N_OR_DEBUG(INFO, 100)
        << "Couldn't send pull request from " << self_node_id_ << " to " << client_id
        << " for " << object_ids.size() << " object(s) (first=" << object_ids.front()
        << "), setup rpc connection failed.";
  }
}

void ObjectManager::HandlePushTaskTimeout(const ObjectID &object_id,
                                          const NodeID &node_id) {
  RAY_LOG(WARNING) << "Invalid Push request ObjectID: " << object_id
                   << " after waiting for " << config_.push_timeout_ms << " ms.";
  auto iter = unfulfilled_push_requests_.find(object_id);
  // Under this scenario, `HandlePushTaskTimeout` can be invoked
  // although timer cancels it.
  // 1. wait timer is done and the task is queued.
  // 2. While task is queued, timer->cancel() is invoked.
  // In this case this method can be invoked although it is not timed out.
  // https://www.boost.org/doc/libs/1_66_0/doc/html/boost_asio/reference/basic_deadline_timer/cancel/overload1.html.
  if (iter == unfulfilled_push_requests_.end()) {
    return;
  }
  size_t num_erased = iter->second.erase(node_id);
  RAY_CHECK(num_erased == 1);
  if (iter->second.size() == 0) {
    unfulfilled_push_requests_.erase(iter);
  }
}

void ObjectManager::HandleSendFinished(const ObjectID &object_id,
                                       const NodeID &node_id,
                                       uint64_t chunk_index,
                                       double start_time,
                                       double end_time,
                                       ray::Status status) {
  RAY_LOG(DEBUG).WithField(object_id)
      << "HandleSendFinished on " << self_node_id_ << " to " << node_id
      << " of object, chunk " << chunk_index << ", status: " << status;
  if (!status.ok()) {
    // TODO(rkn): What do we want to do if the send failed?
    RAY_LOG(DEBUG).WithField(object_id).WithField(node_id)
        << "Failed to send a push request for an object to node. Chunk index: "
        << chunk_index;
  }
}

void ObjectManager::Push(const ObjectID &object_id,
                         const NodeID &node_id,
                         int64_t pull_recv_ns) {
  RAY_LOG(DEBUG).WithField(object_id)
      << "Push object on " << self_node_id_ << " to " << node_id << " of object";
  // THE choke point: time from when the pull was received (HandlePull stamped
  // pull_recv_ns and posted this Push onto the single main_service_ thread) to
  // when this Push actually starts executing. A large queue_ms == the raylet
  // main_service_ event loop was backlogged and didn't run the posted Push in
  // time; the receiver's 10s pull_timeout then fires and re-pulls. Fires for
  // every executed Push regardless of branch (unlike push_started, which only
  // fires for servable objects — the stalled ones never reach it on attempt 1).
  if (pull_recv_ns != 0) {
    EmitPullEvent("phase=push_dispatch object_id={} dest={} queue_ms={}",
                  object_id.Hex(),
                  node_id.Hex(),
                  (absl::GetCurrentTimeNanos() - pull_recv_ns) / 1e6);
  }
  const bool in_plasma = (local_objects_.count(object_id) != 0);
  // Producer-local servability at the moment the pull is handled: is the object
  // in this node's plasma, and/or does it have a local spill URL? Both empty ->
  // the object is momentarily not servable here -> unfulfilled branch. This is
  // the decisive check for the tail (pull raced spill/evict on the producer).
  auto object_url = get_spilled_object_url_(object_id);
  EmitPullEvent("phase=serve_check object_id={} dest={} in_plasma={} has_url={}",
                object_id.Hex(),
                node_id.Hex(),
                in_plasma ? 1 : 0,
                object_url.empty() ? 0 : 1);
  if (in_plasma) {
    return PushLocalObject(object_id, node_id, pull_recv_ns);
  }

  // Push from spilled object directly if the object is on local disk.
  if (!object_url.empty() && RayConfig::instance().is_external_storage_type_fs()) {
    return PushFromFilesystem(object_id, node_id, object_url, pull_recv_ns);
  }

  // Avoid setting duplicated timer for the same object and node pair.
  auto &nodes = unfulfilled_push_requests_[object_id];

  if (nodes.count(node_id) == 0) {
    // If config_.push_timeout_ms < 0, we give an empty timer
    // and the task will be kept infinitely.
    std::unique_ptr<boost::asio::deadline_timer> timer;
    if (config_.push_timeout_ms == 0) {
      // The Push request fails directly when config_.push_timeout_ms == 0.
      RAY_LOG(WARNING) << "Invalid Push request ObjectID " << object_id
                       << " due to direct timeout setting. (0 ms timeout)";
    } else if (config_.push_timeout_ms > 0) {
      // Put the task into a queue and wait for the notification of Object added.
      timer.reset(new boost::asio::deadline_timer(*main_service_));
      auto clean_push_period = boost::posix_time::milliseconds(config_.push_timeout_ms);
      timer->expires_from_now(clean_push_period);
      timer->async_wait(
          [this, object_id, node_id](const boost::system::error_code &error) {
            // Timer killing will receive the boost::asio::error::operation_aborted,
            // we only handle the timeout event.
            if (!error) {
              HandlePushTaskTimeout(object_id, node_id);
            }
          });
    }
    if (config_.push_timeout_ms != 0) {
      // Preserve the original pull_recv_ns so when HandleObjectAdded fires
      // and re-runs Push(), we can attribute the gap to the "waited for
      // sender-side object arrival" bucket rather than a fresh pull.
      nodes[node_id] = UnfulfilledPushRequest{std::move(timer), pull_recv_ns};
    }
  }
}

void ObjectManager::PushLocalObject(const ObjectID &object_id,
                                    const NodeID &node_id,
                                    int64_t pull_recv_ns) {
  const ObjectInfo &object_info = local_objects_[object_id].object_info;
  uint64_t data_size = static_cast<uint64_t>(object_info.data_size);
  uint64_t metadata_size = static_cast<uint64_t>(object_info.metadata_size);

  rpc::Address owner_address;
  owner_address.set_node_id(object_info.owner_node_id.Binary());
  owner_address.set_ip_address(object_info.owner_ip_address);
  owner_address.set_port(object_info.owner_port);
  owner_address.set_worker_id(object_info.owner_worker_id.Binary());

  std::pair<std::shared_ptr<MemoryObjectReader>, ray::Status> reader_status =
      buffer_pool_.CreateObjectReader(object_id, owner_address);
  Status status = reader_status.second;
  if (!status.ok()) {
    // We took the PushLocalObject branch because local_objects_ (the raylet's
    // in-plasma mirror) said this object is in plasma, but reading it failed.
    // This mirror lags plasma: spilling evicts the primary copy from plasma
    // (spill protocol step 5) and only afterwards posts the delete notification
    // that updates local_objects_ on the main thread. A pull that lands inside
    // that window reads a stale mirror and fails here. A non-empty local spill
    // URL is authoritative proof that the object was spilled to this node's disk
    // (reported in step 4, before the step-5 evict), so serve it from the spill
    // file instead of silently dropping the push -- which would strand the puller
    // until its 10s pull_timeout fires and re-pulls. If there is no spill URL,
    // this is a genuine unavailable read; keep the original behavior.
    auto spill_url = get_spilled_object_url_(object_id);
    if (!spill_url.empty() && RayConfig::instance().is_external_storage_type_fs()) {
      return PushFromFilesystem(object_id, node_id, spill_url, pull_recv_ns);
    }
    RAY_LOG_EVERY_N_OR_DEBUG(INFO, 100)
        << "Ignoring stale read request for already deleted object: " << object_id;
    return;
  }

  auto object_reader = std::move(reader_status.first);
  RAY_CHECK(object_reader) << "object_reader can't be null";

  if (object_reader->GetDataSize() != data_size ||
      object_reader->GetMetadataSize() != metadata_size) {
    // TODO(scv119): handle object size changes in a more graceful way.
    RAY_LOG(WARNING) << "Object id:" << object_id
                     << "'s size mismatches our record. Expected data size: " << data_size
                     << ", expected metadata size: " << metadata_size
                     << ", actual data size: " << object_reader->GetDataSize()
                     << ", actual metadata size: " << object_reader->GetMetadataSize()
                     << ". This is likely due to a race condition."
                     << " We will update the object size and proceed sending the object.";
    local_objects_[object_id].object_info.data_size = 0;
    local_objects_[object_id].object_info.metadata_size = 1;
  }

  PushObjectInternal(object_id,
                     node_id,
                     std::make_shared<ChunkObjectReader>(std::move(object_reader),
                                                         config_.object_chunk_size),
                     /*from_disk=*/false,
                     pull_recv_ns);
}

void ObjectManager::PushFromFilesystem(const ObjectID &object_id,
                                       const NodeID &node_id,
                                       const std::string &spilled_url,
                                       int64_t pull_recv_ns) {
  // SpilledObjectReader::CreateSpilledObjectReader does synchronous IO; schedule it off
  // main thread.
  rpc_service_.post(
      [this,
       object_id,
       node_id,
       spilled_url,
       pull_recv_ns,
       chunk_size = config_.object_chunk_size]() {
        auto optional_spilled_object =
            SpilledObjectReader::CreateSpilledObjectReader(spilled_url);
        if (!optional_spilled_object.has_value()) {
          RAY_LOG_EVERY_N_OR_DEBUG(INFO, 100)
              << "Ignoring stale read request for already deleted object: " << object_id;
          return;
        }
        auto chunk_object_reader = std::make_shared<ChunkObjectReader>(
            std::make_shared<SpilledObjectReader>(
                std::move(optional_spilled_object.value())),
            chunk_size);

        // Schedule PushObjectInternal back to main_service as PushObjectInternal access
        // thread unsafe datastructure.
        main_service_->post(
            [this,
             object_id,
             node_id,
             pull_recv_ns,
             chunk_object_reader = std::move(chunk_object_reader)]() {
              PushObjectInternal(object_id,
                                 node_id,
                                 std::move(chunk_object_reader),
                                 /*from_disk=*/true,
                                 pull_recv_ns);
            },
            "ObjectManager.PushLocalSpilledObjectInternal");
      },
      "ObjectManager.CreateSpilledObject");
}

void ObjectManager::PushObjectInternal(const ObjectID &object_id,
                                       const NodeID &node_id,
                                       std::shared_ptr<ChunkObjectReader> chunk_reader,
                                       bool from_disk,
                                       int64_t pull_recv_ns) {
  auto rpc_client = GetRpcClient(node_id);
  if (!rpc_client) {
    // Push is best effort, so do nothing here.
    RAY_LOG(INFO)
        << "Failed to establish connection for Push with remote object manager.";
    return;
  }

  // Emit `phase=push_started` as soon as we've committed to actually calling
  // StartPush. `pull_to_start_ms` measures the time from HandlePull to here.
  // `waited_for_object=1` means the pull was originally queued into
  // `unfulfilled_push_requests_` and only unblocked after HandleObjectAdded;
  // =0 means Pull found the object immediately in local plasma or spilled.
  // `pull_recv_ns=0` guard covers synthetic re-Push callsites that don't
  // carry an origin timestamp (SpreadFreeObjectsRequest et al).
  if (pull_recv_ns != 0) {
    const int64_t now_ns = absl::GetCurrentTimeNanos();
    // The requester originally sat in unfulfilled_push_requests_ iff the
    // object was neither plasma-local nor on-disk when HandlePull first
    // ran. We approximate this using elapsed time crossing a threshold —
    // any real "waited for HandleObjectAdded" path takes ≥ several ms in
    // practice, whereas the fast paths are single-digit microseconds. We
    // also read the current path (from_disk / local) to help disambiguate.
    // A more precise signal would require threading a bool through Push();
    // gap size + from_disk together give sufficient decomposition for the
    // ~3s tail investigation.
    EmitPullEvent(
        "phase=push_started object_id={} bytes={} pull_to_start_ms={} "
        "from_disk={} dest_node={}",
        object_id.Hex(),
        chunk_reader->GetObject().GetObjectSize(),
        (now_ns - pull_recv_ns) / 1e6,
        from_disk ? 1 : 0,
        node_id.Hex());
  }

  RAY_LOG(DEBUG).WithField(object_id).WithField(node_id)
      << "Sending object chunks of object to node, number of chunks: "
      << chunk_reader->GetNumChunks()
      << ", total data size: " << chunk_reader->GetObject().GetObjectSize();

  auto push_id = UniqueID::FromRandom();

  // Sender-side push timing. Captured by-value into the chunk closures so
  // each chunk's send callback can decrement the shared counter; the last
  // chunk to complete emits `phase=object_pushed`. This is the *only*
  // place we measure the wall time of cross-node spill-file reads
  // (from_disk=1) — see PULL_EVENTS_LOG_FORMAT.md §3 for why this lives in
  // pull_events rather than spill_events: PushObjectInternal is exclusively
  // triggered by a remote Pull RPC (HandlePull or queued HandleObjectAdded
  // path), so the byte volume here is by definition pull-driven traffic.
  // Counter is heap-allocated + ref-counted so the value survives across
  // the rpc_service_ -> SendObjectChunk -> on_complete bounce.
  const auto push_start_time = absl::Now();
  const auto chunks_remaining =
      std::make_shared<std::atomic<int64_t>>(chunk_reader->GetNumChunks());
  // Cumulative ns spent inside chunk_reader->GetChunk across this push,
  // accumulated by each rpc_service_ worker that runs SendObjectChunk.
  // Reported as get_chunk_ms_total in `phase=object_pushed` so drain_ms
  // can be split: drain_ms - get_chunk_ms_total is the network + receiver
  // share (and any remaining rpc_service_ scheduling slack).
  const auto get_chunk_ns_total = std::make_shared<std::atomic<int64_t>>(0);
  // Cumulative lag from "rpc_service_ posts OnChunkComplete to main_service_"
  // to "main_service_ thread actually runs the lambda".  Measures contention
  // on the single main_service_ thread that serializes all PushManager state
  // updates.  Reported as bounce_lag_ms_total in `phase=object_pushed`;
  // bounce_lag_ms_total / network_recv_ms localizes the bottleneck inside
  // drain_ms.
  //
  // Accuracy under contention: the emit lambda is itself posted onto
  // main_service_ after the last chunk's bounce lambda (asio io_context is
  // FIFO: same-thread posts run in order), so by the time emit dispatches,
  // every prior bounce fetch_add — including the last chunk's own — has
  // completed. Reading `bounce_ns_total` there is authoritative, not a
  // "skip the last chunk" approximation. This matters precisely in the
  // regime we care about (main_service_ backlogged) where the earlier
  // rpc_service_-side emit could omit an unbounded tail of unfired
  // bounces.
  const auto bounce_ns_total = std::make_shared<std::atomic<int64_t>>(0);
  const auto object_size = chunk_reader->GetObject().GetObjectSize();
  const std::string dest_node_hex = node_id.Hex();
  const std::string object_id_hex = object_id.Hex();

  push_manager_->StartPush(
      node_id, object_id, chunk_reader->GetNumChunks(),
      [=](int64_t chunk_id) {
        rpc_service_.post(
            [=]() {
              // Post to the multithreaded RPC event loop so that data is copied
              // off of the main thread.
              SendObjectChunk(
                  push_id,
                  object_id,
                  node_id,
                  chunk_id,
                  rpc_client,
                  [=](const Status &status) {
                    // Post back to the main event loop because the
                    // PushManager is not thread-safe.  Time the bounce:
                    // stamp before post(), have the lambda subtract on entry.
                    const int64_t bounce_post_ns = absl::GetCurrentTimeNanos();
                    main_service_->post(
                        [this, bounce_ns_total, bounce_post_ns]() {
                          bounce_ns_total->fetch_add(
                              absl::GetCurrentTimeNanos() - bounce_post_ns,
                              std::memory_order_relaxed);
                          push_manager_->OnChunkComplete();
                        },
                        "ObjectManager.Push");
                    // Last completed chunk emits the timing line. fetch_sub
                    // returns the pre-decrement value, so == 1 means we
                    // just drove the counter to 0.
                    if (chunks_remaining->fetch_sub(1) == 1) {
                      // Freeze the "data on the wire" wall time now, on the
                      // rpc_service_ thread, so push_wall_ms reflects
                      // send-complete rather than emit-dispatch (which can
                      // trail arbitrarily under main_service_ contention).
                      const auto push_end_time = absl::Now();
                      // Post emit onto main_service_ after the last chunk's
                      // bounce lambda (which we just posted above). asio
                      // io_context FIFO guarantees the emit runs strictly
                      // after every bounce for this push, so
                      // bounce_ns_total is authoritative when read here —
                      // no more "skips the last chunk" undercount, and
                      // under real main_service_ contention (the regime
                      // this metric exists to detect) we no longer omit an
                      // unbounded tail of unfired bounces.
                      main_service_->post(
                          [=]() {
                            EmitPullEvent(
                                "phase=object_pushed object_id={} bytes={} "
                                "push_wall_ms={} get_chunk_ms_total={} "
                                "bounce_lag_ms_total={} from_disk={} dest_node={}",
                                object_id_hex,
                                object_size,
                                absl::ToDoubleMilliseconds(push_end_time -
                                                           push_start_time),
                                get_chunk_ns_total->load(
                                    std::memory_order_relaxed) /
                                    1e6,
                                bounce_ns_total->load(
                                    std::memory_order_relaxed) /
                                    1e6,
                                from_disk ? 1 : 0,
                                dest_node_hex);
                          },
                          "ObjectManager.PushEmit");
                    }
                  },
                  chunk_reader,
                  from_disk,
                  get_chunk_ns_total);
            },
            "ObjectManager.Push");
      });
}

void ObjectManager::SendObjectChunk(
    const UniqueID &push_id,
    const ObjectID &object_id,
    const NodeID &node_id,
    uint64_t chunk_index,
    std::shared_ptr<rpc::ObjectManagerClientInterface> rpc_client,
    std::function<void(const Status &)> on_complete,
    std::shared_ptr<ChunkObjectReader> chunk_reader,
    bool from_disk,
    std::shared_ptr<std::atomic<int64_t>> get_chunk_ns_total) {
  double start_time = absl::GetCurrentTimeNanos() / 1e9;
  rpc::PushRequest push_request;
  // Set request header
  push_request.set_push_id(push_id.Binary());
  push_request.set_object_id(object_id.Binary());
  push_request.mutable_owner_address()->CopyFrom(
      chunk_reader->GetObject().GetOwnerAddress());
  push_request.set_node_id(self_node_id_.Binary());
  push_request.set_data_size(chunk_reader->GetObject().GetObjectSize());
  push_request.set_metadata_size(chunk_reader->GetObject().GetMetadataSize());
  push_request.set_chunk_index(chunk_index);

  // read a chunk into push_request and handle errors.  Time the call so
  // PushObjectInternal can attribute disk-read latency (the dominant
  // component when from_disk=1) inside the otherwise-opaque drain_ms.
  const int64_t get_chunk_start_ns = absl::GetCurrentTimeNanos();
  auto optional_chunk = chunk_reader->GetChunk(chunk_index);
  const int64_t get_chunk_end_ns = absl::GetCurrentTimeNanos();
  if (get_chunk_ns_total) {
    get_chunk_ns_total->fetch_add(get_chunk_end_ns - get_chunk_start_ns,
                                  std::memory_order_relaxed);
  }
  // Sender per-chunk read latency (part 1 of the per-chunk timeline).
  // `object_pushed.get_chunk_ms_total` was an aggregate — this exposes the
  // distribution so we can spot a single tail-slow chunk vs uniform slow.
  // Emits regardless of read outcome; `ok=0` catches the "chunk evicted
  // between StartPush and now" case which otherwise disappears.
  EmitPullEvent(
      "phase=chunk_read object_id={} chunk_id={} read_ms={} from_disk={} ok={}",
      object_id.Hex(),
      chunk_index,
      (get_chunk_end_ns - get_chunk_start_ns) / 1e6,
      from_disk ? 1 : 0,
      optional_chunk.has_value() ? 1 : 0);
  if (!optional_chunk.has_value()) {
    RAY_LOG(DEBUG) << "Read chunk " << chunk_index << " of object " << object_id
                   << " failed. It may have been evicted.";
    on_complete(Status::IOError("Failed to read spilled object"));
    return;
  }
  push_request.set_data(std::move(optional_chunk.value()));
  if (from_disk) {
    num_bytes_pushed_from_disk_ += push_request.data().length();
  } else {
    num_bytes_pushed_from_plasma_ += push_request.data().length();
  }

  // Stamp wall-clock right before the gRPC send fires. `chunk_sent`
  // measures Push→ACK RTT: gRPC serialize + socket write + network +
  // receiver HandlePush → send_reply_callback. Anomalously large values
  // (relative to plasma_write_wait on the receiver) suggest network /
  // gRPC framing back-pressure rather than plasma admission.
  const int64_t rpc_send_start_ns = absl::GetCurrentTimeNanos();
  const uint64_t chunk_bytes = push_request.data().length();

  // record the time cost between send chunk and receive reply
  rpc::ClientCallback<rpc::PushReply> callback =
      [this,
       start_time,
       object_id,
       node_id,
       chunk_index,
       rpc_send_start_ns,
       chunk_bytes,
       on_complete](const Status &status, const rpc::PushReply &reply) {
        // TODO(Eric Liang): Just print warning here, should we try to resend this chunk?
        if (!status.ok()) {
          RAY_LOG(WARNING).WithField(object_id).WithField(node_id)
              << "Send object chunk to node failed due to" << status
              << ", chunk index: " << chunk_index;
        }
        const int64_t rpc_send_end_ns = absl::GetCurrentTimeNanos();
        // Emit before the on_complete bounce so the timing reflects pure
        // gRPC RTT, not gRPC RTT + main_service_ bounce lag (which is
        // covered separately by bounce_lag_ms_total).
        EmitPullEvent(
            "phase=chunk_sent object_id={} chunk_id={} send_ms={} bytes={} "
            "ok={} dest_node={}",
            object_id.Hex(),
            chunk_index,
            (rpc_send_end_ns - rpc_send_start_ns) / 1e6,
            chunk_bytes,
            status.ok() ? 1 : 0,
            node_id.Hex());
        double end_time = rpc_send_end_ns / 1e9;
        HandleSendFinished(object_id, node_id, chunk_index, start_time, end_time, status);
        on_complete(status);
      };

  rpc_client->Push(push_request, callback);
}

/// Implementation of ObjectManagerServiceHandler
void ObjectManager::HandlePush(rpc::PushRequest request,
                               rpc::PushReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID node_id = NodeID::FromBinary(request.node_id());

  // Serialize.
  uint64_t chunk_index = request.chunk_index();
  uint64_t metadata_size = request.metadata_size();
  uint64_t data_size = request.data_size();
  const rpc::Address &owner_address = request.owner_address();
  const std::string &data = request.data();

  // T1 for `phase=object_first_byte`: the *first* chunk arrival for an
  // object that we previously fired a Pull RPC for. Chunks may not arrive
  // in order, so use the presence of a pull_sent_time_ entry as the "first"
  // sentinel rather than chunk_index == 0. Erase the entry to ensure this
  // event fires exactly once per pull cycle. Same-process pulls (no Pull
  // RPC fired) won't have an entry and are silently skipped.
  absl::Time first_byte_start;
  int64_t pull_attempt = 0;
  bool emit_first_byte = false;
  {
    absl::MutexLock lock(&pull_sent_time_mu_);
    auto it = pull_sent_time_.find(object_id);
    if (it != pull_sent_time_.end()) {
      first_byte_start = it->second;
      pull_sent_time_.erase(it);
      // pull_attempt_count_ is incremented in SendPullRequest, so the
      // current value is the attempt index for THIS pull cycle. Read it
      // here (still under lock) before we lose the synchronization point.
      auto cnt_it = pull_attempt_count_.find(object_id);
      pull_attempt = (cnt_it != pull_attempt_count_.end()) ? cnt_it->second : 1;
      emit_first_byte = true;
    }
  }
  if (emit_first_byte) {
    EmitPullEvent(
        "phase=object_first_byte object_id={} bytes={} first_byte_ms={} "
        "attempt={} from_node={}",
        object_id.Hex(),
        data_size,
        absl::ToDoubleMilliseconds(absl::Now() - first_byte_start),
        pull_attempt,
        node_id.Hex());
  }

  // Per-chunk receiver arrival timing. `gap_from_prev_ms` measures the
  // gap between successive chunks of the same object landing here —
  // large gaps at random chunk indices (not just chunk 0) point at
  // sender-side stalls in the middle of a push, or intermittent
  // network back-pressure. -1 indicates first chunk observed for the
  // object (no prev to diff against). last_chunk_recv_ns_ shares the
  // pull_sent_time_ mutex; both maps are accessed only from HandlePush
  // (gRPC handler) and cleared / read from the main thread's
  // SendPullRequest path.
  int64_t gap_ns = -1;
  const int64_t chunk_recv_ns = absl::GetCurrentTimeNanos();
  {
    absl::MutexLock lock(&pull_sent_time_mu_);
    auto it = last_chunk_recv_ns_.find(object_id);
    if (it != last_chunk_recv_ns_.end()) {
      gap_ns = chunk_recv_ns - it->second;
    }
    last_chunk_recv_ns_[object_id] = chunk_recv_ns;
  }
  EmitPullEvent(
      "phase=chunk_received object_id={} chunk_id={} bytes={} gap_from_prev_ms={} "
      "from_node={}",
      object_id.Hex(),
      chunk_index,
      data.size(),
      gap_ns < 0 ? -1.0 : gap_ns / 1e6,
      node_id.Hex());

  bool success = ReceiveObjectChunk(
      node_id, object_id, owner_address, data_size, metadata_size, chunk_index, data);
  num_chunks_received_total_++;
  if (!success) {
    num_chunks_received_total_failed_++;
    RAY_LOG(INFO) << "Received duplicate or cancelled chunk at index " << chunk_index
                  << " of object " << object_id << ": overall "
                  << num_chunks_received_total_failed_ << "/"
                  << num_chunks_received_total_ << " failed";
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

bool ObjectManager::ReceiveObjectChunk(const NodeID &node_id,
                                       const ObjectID &object_id,
                                       const rpc::Address &owner_address,
                                       uint64_t data_size,
                                       uint64_t metadata_size,
                                       uint64_t chunk_index,
                                       const std::string &data) {
  num_bytes_received_total_ += data.size();
  RAY_LOG(DEBUG).WithField(object_id)
      << "ReceiveObjectChunk on " << self_node_id_ << " from " << node_id
      << " of object, chunk index: " << chunk_index
      << ", chunk data size: " << data.size() << ", object size: " << data_size;

  if (!pull_manager_->IsObjectActive(object_id)) {
    num_chunks_received_cancelled_++;
    // This object is no longer being actively pulled. Do not create the object.
    return false;
  }
  // Time spent inside `buffer_pool_.CreateChunk` is the receiver-side plasma
  // admission wait — includes any plasma eviction / spill we have to drive
  // synchronously before there's room. For OOC shuffle GET path (reduce
  // ray.get), this is the dominant hidden component of
  // `bundle_complete.transfer_ms` (GET bundles never get PullManager-level
  // dx_memory churn, so the wait shows up here instead).
  //
  // We now emit for EVERY chunk, not just chunk_index==0. Rationale:
  // multi-chunk objects can also stall on later chunks if the buffer pool
  // wasn't fully pre-allocated (some plasma implementations lazy-alloc per
  // chunk), and the previous chunk_0-only signal missed those. For 1-chunk
  // objects (small shard case, typical in OOC shuffle) behavior is
  // unchanged.
  const absl::Time create_start_time = absl::Now();
  auto chunk_status = buffer_pool_.CreateChunk(
      object_id, owner_address, data_size, metadata_size, chunk_index);
  EmitPullEvent(
      "phase=plasma_create_wait object_id={} chunk_id={} bytes={} wait_ms={} ok={}",
      object_id.Hex(),
      chunk_index,
      data_size,
      absl::ToDoubleMilliseconds(absl::Now() - create_start_time),
      chunk_status.ok() ? 1 : 0);
  if (!pull_manager_->IsObjectActive(object_id)) {
    num_chunks_received_cancelled_++;
    // This object is no longer being actively pulled. Abort the object. We
    // have to check again here because the pull manager runs in a different
    // thread and the object may have been deactivated right before creating
    // the chunk.
    RAY_LOG(INFO) << "Aborting object creation because it is no longer actively pulled: "
                  << object_id;
    buffer_pool_.AbortCreate(object_id);
    return false;
  }

  if (chunk_status.ok()) {
    // Avoid handling this chunk if it's already being handled by another process.
    buffer_pool_.WriteChunk(object_id, data_size, metadata_size, chunk_index, data);
    return true;
  } else {
    num_chunks_received_failed_due_to_plasma_++;
    RAY_LOG(INFO) << "Error receiving chunk:" << chunk_status;
    if (chunk_status.IsOutOfDisk()) {
      pull_manager_->SetOutOfDisk(object_id);
    }
    return false;
  }
}

void ObjectManager::HandlePull(rpc::PullRequest request,
                               rpc::PullReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  // Stamp the wall-clock at which this Pull arrived so the downstream
  // Push -> StartPush -> PushObjectInternal chain can compute
  // pull_to_start_ms and emit `phase=push_started`. This closes the last
  // observability hole between "receiver's PullManager fires SendPullRequest"
  // and "sender starts pushing chunks" — the ~3s tail we saw where
  // push_wall is tiny but first_byte is 3s lives entirely in this gap
  // (specifically, in unfulfilled_push_requests_ waiting for
  // HandleObjectAdded). All objects in a batched Pull share the same
  // arrival stamp.
  const int64_t pull_recv_ns = absl::GetCurrentTimeNanos();
  for (const auto &binary : request.object_ids()) {
    ObjectID object_id = ObjectID::FromBinary(binary);
    RAY_LOG(DEBUG).WithField(node_id).WithField(object_id)
        << "Received pull request from node for object";
    main_service_->post(
        [this, object_id, node_id, pull_recv_ns]() {
          Push(object_id, node_id, pull_recv_ns);
        },
        "ObjectManager.HandlePull");
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ObjectManager::FreeObjects(const std::vector<ObjectID> &object_ids) {
  buffer_pool_.FreeObjects(object_ids);
}

std::shared_ptr<rpc::ObjectManagerClientInterface> ObjectManager::GetRpcClient(
    const NodeID &node_id) {
  auto it = remote_object_manager_clients_.find(node_id);
  if (it != remote_object_manager_clients_.end()) {
    return it->second;
  }
  auto node_info =
      gcs_client_.Nodes().GetNodeAddressAndLiveness(node_id, /*filter_dead_nodes=*/true);
  if (!node_info) {
    return nullptr;
  }
  auto object_manager_client =
      object_manager_client_factory_(node_info->node_manager_address(),
                                     node_info->object_manager_port(),
                                     client_call_manager_);

  RAY_LOG(DEBUG) << "Get rpc client, address: " << node_info->node_manager_address()
                 << ", port: " << node_info->object_manager_port()
                 << ", local port: " << GetServerPort();

  it = remote_object_manager_clients_.emplace(node_id, std::move(object_manager_client))
           .first;
  return it->second;
}

void ObjectManager::HandleNodeRemoved(const NodeID &node_id) {
  push_manager_->HandleNodeRemoved(node_id);
  remote_object_manager_clients_.erase(node_id);
}

std::vector<ObjectID> ObjectManager::GetLocalObjectsOwnedBy(
    const WorkerID &worker_id) const {
  return GetLocalObjectsFilteredBy(local_objects_,
                                   [&worker_id](const LocalObjectInfo &info) {
                                     return info.object_info.owner_worker_id == worker_id;
                                   });
}

std::vector<ObjectID> ObjectManager::GetLocalObjectsOwnedByOwnersOn(
    const NodeID &node_id) const {
  return GetLocalObjectsFilteredBy(local_objects_,
                                   [&node_id](const LocalObjectInfo &info) {
                                     return info.object_info.owner_node_id == node_id;
                                   });
}

std::string ObjectManager::DebugString() const {
  std::stringstream result;
  result << "ObjectManager:";
  result << "\n- num local objects: " << local_objects_.size();
  result << "\n- num unfulfilled push requests: " << unfulfilled_push_requests_.size();
  result << "\n- num object pull requests: " << pull_manager_->NumObjectPullRequests();
  result << "\n- num chunks received total: " << num_chunks_received_total_;
  result << "\n- num chunks received failed (all): " << num_chunks_received_total_failed_;
  result << "\n- num chunks received failed / cancelled: "
         << num_chunks_received_cancelled_;
  result << "\n- num chunks received failed / plasma error: "
         << num_chunks_received_failed_due_to_plasma_;
  result << "\nEvent stats:" << rpc_service_.stats()->StatsString();
  result << "\n" << push_manager_->DebugString();
  result << "\n" << object_directory_->DebugString();
  result << "\n" << buffer_pool_.DebugString();
  result << "\n" << pull_manager_->DebugString();
  return result.str();
}

void ObjectManager::RecordMetrics() {
  pull_manager_->RecordMetrics();
  push_manager_->RecordMetrics();
  // used_memory_ includes the fallback allocation, so we should add it again here
  // to calculate the exact available memory.
  object_store_available_memory_gauge_.Record(
      config_.object_store_memory - used_memory_ +
      plasma::plasma_store_runner->GetFallbackAllocated());
  // Subtract fallback allocated memory. It is tracked separately by
  // `ObjectStoreFallbackMemory`.
  object_store_used_memory_gauge_.Record(
      used_memory_ - plasma::plasma_store_runner->GetFallbackAllocated());
  object_store_fallback_memory_gauge_.Record(
      plasma::plasma_store_runner->GetFallbackAllocated());
  object_store_local_objects_gauge_.Record(local_objects_.size());
  object_manager_pull_requests_gauge_.Record(pull_manager_->NumObjectPullRequests());

  object_manager_bytes_gauge_.Record(num_bytes_pushed_from_plasma_,
                                     {{"Type", "PushedFromLocalPlasma"}});
  object_manager_bytes_gauge_.Record(num_bytes_pushed_from_disk_,
                                     {{"Type", "PushedFromLocalDisk"}});
  object_manager_bytes_gauge_.Record(num_bytes_received_total_, {{"Type", "Received"}});
  object_manager_received_chunks_gauge_.Record(num_chunks_received_total_,
                                               {{"Type", "Total"}});
  object_manager_received_chunks_gauge_.Record(num_chunks_received_total_failed_,
                                               {{"Type", "FailedTotal"}});
  object_manager_received_chunks_gauge_.Record(num_chunks_received_cancelled_,
                                               {{"Type", "FailedCancelled"}});
  object_manager_received_chunks_gauge_.Record(num_chunks_received_failed_due_to_plasma_,
                                               {{"Type", "FailedPlasmaFull"}});
}

void ObjectManager::FillObjectStoreStats(rpc::GetNodeStatsReply *reply) const {
  auto stats = reply->mutable_store_stats();
  stats->set_object_store_bytes_used(used_memory_);
  stats->set_object_store_bytes_fallback(
      plasma::plasma_store_runner->GetFallbackAllocated());
  stats->set_object_store_bytes_avail(config_.object_store_memory);
  stats->set_num_local_objects(local_objects_.size());
  stats->set_cumulative_created_objects(
      plasma::plasma_store_runner->GetCumulativeCreatedObjects());
  stats->set_cumulative_created_bytes(
      plasma::plasma_store_runner->GetCumulativeCreatedBytes());
  stats->set_object_pulls_queued(pull_manager_->HasPullsQueued());
}

void ObjectManager::Tick(const boost::system::error_code &e) {
  RAY_CHECK(!e) << "The raylet's object manager has failed unexpectedly with error: " << e
                << ". Please file a bug report on here: "
                   "https://github.com/ray-project/ray/issues";

  // Periodic raylet-side snapshot at the object_manager tick cadence
  // (config_.timer_freq_ms, default 100 ms). Provides context for per-object
  // stall events emitted between ticks — e.g. a `push_started` with
  // pull_to_start_ms=3000 becomes much easier to explain if the surrounding
  // raylet_snapshot lines show unfulfilled_push_count sustained at hundreds.
  //
  // Kept intentionally small: only counters accessible without threading
  // through new accessors across modules. Fields to add later if we still
  // need more context: PullManager active/inactive by priority,
  // PushManager in-flight chunks, main_service_ queue length.
  EmitPullEvent(
      "phase=raylet_snapshot node={} unfulfilled_push_count={} "
      "local_objects={} used_memory_bytes={}",
      self_node_id_.Hex(),
      unfulfilled_push_requests_.size(),
      local_objects_.size(),
      used_memory_);

  // Request the current available memory from the object
  // store.
  plasma::plasma_store_runner->GetAvailableMemoryAsync([this](size_t available_memory) {
    main_service_->post(
        [this, available_memory]() {
          pull_manager_->UpdatePullsBasedOnAvailableMemory(available_memory);
        },
        "ObjectManager.UpdateAvailableMemory");
  });

  pull_manager_->Tick();

  auto interval = boost::posix_time::milliseconds(config_.timer_freq_ms);
  pull_retry_timer_.expires_from_now(interval);
  pull_retry_timer_.async_wait(
      [this](const boost::system::error_code &err) { Tick(err); });
}

}  // namespace ray
