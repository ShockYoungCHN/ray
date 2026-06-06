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

#include "ray/object_manager/spilled_object_reader.h"

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <regex>
#include <string>
#include <utility>

#include "absl/strings/internal/resize_uninitialized.h"
#include "ray/util/logging.h"

namespace ray {
namespace {
const size_t UINT64_size = sizeof(uint64_t);

// Append `size` bytes read at `offset` from `fd` to `output`. Uses pread() so
// it is thread-safe without external synchronization (no shared file offset)
// and amortizes the open()/seek() syscalls across multiple chunk reads of the
// same spilled object — the previous implementation opened std::ifstream on
// every call, paying ~3 syscalls per chunk.
bool PreadAppend(int fd, uint64_t offset, uint64_t size, std::string &output) {
  if (fd < 0) {
    return false;
  }
  const size_t old_size = output.size();
  // Grows size to old_size+size reusing reserved capacity; does NOT init the
  // new bytes (unlike std::string::resize, which value-inits to '\0').
  // todo: use std::string::resize_uninitialized when we can require C++23
  absl::strings_internal::STLStringResizeUninitialized(&output, old_size + size);
  ssize_t n = ::pread(fd, &output[old_size], size, static_cast<off_t>(offset));
  if (n < 0 || static_cast<uint64_t>(n) != size) {
    output.resize(old_size);  // roll back: leave `output` exactly as we found it
    return false;
  }
  return true;
}
}  // namespace

/* static */ std::optional<SpilledObjectReader>
SpilledObjectReader::CreateSpilledObjectReader(const std::string &object_url) {
  std::string file_path;
  uint64_t object_offset = 0;
  uint64_t object_size = 0;

  if (!SpilledObjectReader::ParseObjectURL(
          object_url, file_path, object_offset, object_size)) {
    RAY_LOG(WARNING) << "Failed to parse spilled object url: " << object_url;
    return std::optional<SpilledObjectReader>();
  }

  uint64_t data_offset = 0;
  uint64_t data_size = 0;
  uint64_t metadata_offset = 0;
  uint64_t metadata_size = 0;
  rpc::Address owner_address;

  std::ifstream is(file_path, std::ios::binary);
  if (!is || !SpilledObjectReader::ParseObjectHeader(is,
                                                     object_offset,
                                                     data_offset,
                                                     data_size,
                                                     metadata_offset,
                                                     metadata_size,
                                                     owner_address)) {
    RAY_LOG(WARNING) << "Failed to parse object header for spilled object " << object_url;
    return std::optional<SpilledObjectReader>();
  }

  // Open a persistent fd for the data-read path. Header parsing above already
  // succeeded via std::ifstream (one-shot, perf-irrelevant); from here on the
  // chunked reads share this single fd via pread().
  int fd = ::open(file_path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    RAY_LOG(WARNING) << "Failed to open spilled object file " << file_path
                     << " for read: " << std::strerror(errno);
    return std::optional<SpilledObjectReader>();
  }
#if defined(__linux__)
  // Hint the kernel: reads will be sequential within this object's window;
  // start prefetching the object payload immediately. WILLNEED is advisory —
  // safe to ignore failure. Range [object_offset, object_offset+object_size).
  ::posix_fadvise(fd,
                  static_cast<off_t>(object_offset),
                  static_cast<off_t>(object_size),
                  POSIX_FADV_SEQUENTIAL);
  ::posix_fadvise(fd,
                  static_cast<off_t>(object_offset),
                  static_cast<off_t>(object_size),
                  POSIX_FADV_WILLNEED);
#endif

  return std::optional<SpilledObjectReader>(
      SpilledObjectReader(std::move(file_path),
                          object_size,
                          data_offset,
                          data_size,
                          metadata_offset,
                          metadata_size,
                          std::move(owner_address),
                          fd));
}

uint64_t SpilledObjectReader::GetDataSize() const { return data_size_; }

uint64_t SpilledObjectReader::GetMetadataSize() const { return metadata_size_; }

const rpc::Address &SpilledObjectReader::GetOwnerAddress() const {
  return owner_address_;
}

SpilledObjectReader::SpilledObjectReader(std::string file_path,
                                         uint64_t object_size,
                                         uint64_t data_offset,
                                         uint64_t data_size,
                                         uint64_t metadata_offset,
                                         uint64_t metadata_size,
                                         rpc::Address owner_address,
                                         int fd)
    : file_path_(std::move(file_path)),
      object_size_(object_size),
      data_offset_(data_offset),
      data_size_(data_size),
      metadata_offset_(metadata_offset),
      metadata_size_(metadata_size),
      owner_address_(std::move(owner_address)),
      fd_(fd) {}

SpilledObjectReader::~SpilledObjectReader() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

SpilledObjectReader::SpilledObjectReader(SpilledObjectReader &&other) noexcept
    // const members can't be moved-from; copy is equivalent to the implicit
    // move ctor we used to rely on.
    : file_path_(other.file_path_),
      object_size_(other.object_size_),
      data_offset_(other.data_offset_),
      data_size_(other.data_size_),
      metadata_offset_(other.metadata_offset_),
      metadata_size_(other.metadata_size_),
      owner_address_(other.owner_address_),
      fd_(other.fd_) {
  other.fd_ = -1;  // transfer fd ownership; source becomes a no-op on destruction
}

/* static */ bool SpilledObjectReader::ParseObjectURL(const std::string &object_url,
                                                      std::string &file_path,
                                                      uint64_t &object_offset,
                                                      uint64_t &object_size) {
  static const std::regex object_url_pattern("^(.*)\\?offset=(\\d+)&size=(\\d+)$");
  std::smatch match_groups;
  if (!std::regex_match(object_url, match_groups, object_url_pattern) ||
      match_groups.size() != 4) {
    return false;
  }
  file_path = match_groups[1].str();
  try {
    auto offset = std::stoll(match_groups[2].str());
    auto size = std::stoll(match_groups[3].str());
    if (offset < 0 || size < 0) {
      RAY_LOG(ERROR) << "Offset and size can't be negative. offset: " << offset
                     << ", size: " << size;
      return false;
    }
    object_offset = offset;
    object_size = size;
  } catch (...) {
    RAY_LOG(ERROR) << "Failed to parse offset: " << match_groups[2].str()
                   << " and size: " << match_groups[3].str();
    return false;
  }
  return true;
}

/* static */
bool SpilledObjectReader::ParseObjectHeader(std::istream &is,
                                            uint64_t object_offset,
                                            uint64_t &data_offset,
                                            uint64_t &data_size,
                                            uint64_t &metadata_offset,
                                            uint64_t &metadata_size,
                                            rpc::Address &owner_address) {
  if (!is.seekg(object_offset)) {
    return false;
  }

  uint64_t address_size = 0;
  if (!ReadUINT64(is, address_size) || !ReadUINT64(is, metadata_size) ||
      !ReadUINT64(is, data_size)) {
    return false;
  }

  std::string address_str(address_size, '\0');
  if (!is.read(&address_str[0], address_size) ||
      !owner_address.ParseFromString(address_str)) {
    return false;
  }

  metadata_offset = object_offset + UINT64_size * 3 + address_size;
  data_offset = metadata_offset + metadata_size;
  return true;
}

/* static */
bool SpilledObjectReader::ReadUINT64(std::istream &is, uint64_t &output) {
  std::string buff(UINT64_size, '\0');
  if (!is.read(&buff[0], UINT64_size)) {
    return false;
  }
  output = SpilledObjectReader::ToUINT64(buff);
  return true;
}

/* static */
uint64_t SpilledObjectReader::ToUINT64(const std::string &s) {
  RAY_CHECK(s.size() == UINT64_size);
  uint64_t result = 0;
  for (size_t i = 0; i < s.size(); i++) {
    result = result << 8;
    result += static_cast<unsigned char>(s.at(s.size() - i - 1));
  }
  return result;
}

bool SpilledObjectReader::ReadFromDataSection(uint64_t offset,
                                              uint64_t size,
                                              std::string &output) const {
  return PreadAppend(fd_, data_offset_ + offset, size, output);
}

bool SpilledObjectReader::ReadFromMetadataSection(uint64_t offset,
                                                  uint64_t size,
                                                  std::string &output) const {
  return PreadAppend(fd_, metadata_offset_ + offset, size, output);
}
}  // namespace ray
