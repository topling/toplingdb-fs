// Copyright (c) 2021-present, Topling, Inc.  All rights reserved.
// Created by leipeng at 2024-10-09
//  Copyright (c) Topling, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <terark/num_to_str.hpp>
#include <nfsc/libnfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <rocksdb/file_system.h>
#include <topling/side_plugin_factory.h>
#include "logging/env_logger.h"

const char* git_version_hash_info_toplingdb_fs();

namespace ROCKSDB_NAMESPACE {

// use libnfs to speed up tailing read performance.
// linux kernel nfs tailing needs to set mount option `noac`,
// because linux kernel nfs needs to getattr before tailing.
// if there is no option `noac`, tailing will wait until attribute
// cache ttl(3 seconds by default).
// even with `noac`, linux kernel nfs has an extra getattr before
// tailing read, this is not needed. libnfs will not send getattr
// before (tailing) read.
class TailingNFS : public FileSystem {
 public:
  TailingNFS(const json&, const SidePluginRepo&);
  ~TailingNFS();
  void Update(const json&, const json& js, const SidePluginRepo&);
  std::string ToString(const json& d, const SidePluginRepo&) const;
//-----------------------------------------------------------------

  const char* Name() const override { return "TailingNFS"; }
  static const char* kClassName() { return "TailingNFS"; }
  bool IsInstanceOf(const std::string& id) const override;

  Status RegisterDbPaths(const std::vector<std::string>& paths) override;
  Status UnregisterDbPaths(const std::vector<std::string>& paths) override;

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;

  IOStatus ReopenWritableFile(const std::string& /*fname*/, const FileOptions&,
                              std::unique_ptr<FSWritableFile>*, IODebugContext*);

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override;

  IOStatus NewRandomRWFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override;

  IOStatus NewDirectory(const std::string& dir, const IOOptions& options,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;

  IOStatus FileExists(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override;

  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& options,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* dbg) override;

  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus CreateDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus CreateDirIfMissing(const std::string& dirname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;

  IOStatus DeleteDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override;

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override;

  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dbg) override;

  IOStatus RenameFile(const std::string& src, const std::string& dest,
                      const IOOptions& options, IODebugContext* dbg) override;

  IOStatus LinkFile(const std::string& src, const std::string& dest,
                    const IOOptions& options, IODebugContext* dbg) override;

  IOStatus LockFile(const std::string& fname, const IOOptions& options,
                    FileLock** lock, IODebugContext* dbg) override;

  IOStatus NewLogger(const std::string& fname, const IOOptions& options,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override;

  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override;

  IOStatus GetTestDirectory(const IOOptions&, std::string* path, IODebugContext*);
  IOStatus UnlockFile(FileLock*, const IOOptions&, IODebugContext*);

  bool use_osfs_write = false; // mutable dynamicallay
  std::string osfs_mount_root; // required when use_osfs_write
  std::shared_ptr<FileSystem> osfs;

  int nfs_rpc_timeout_ms = 60 * 100; // nfs default rpc timeout 60 seconds
  int nfs_poll_timeout_ms = 100;
  std::string nfs_server; // nfs://server.host.name
  std::string nfs_export; // /path/to/export

  nfs_context* m_nfs;
};

TailingNFS::TailingNFS(const json& js, const SidePluginRepo& repo) {
  ROCKSDB_JSON_OPT_FACT(js, osfs);
  ROCKSDB_JSON_OPT_PROP(js, use_osfs_write);
  ROCKSDB_JSON_OPT_PROP(js, osfs_mount_root);
  if (use_osfs_write) {
    if (!osfs) {
      THROW_InvalidArgument("osfs must be set when use_osfs_write");
    }
    if (osfs_mount_root.empty()) {
      THROW_InvalidArgument("osfs_mount_root must be set when use_osfs_write");
    }
  }
  else if ((osfs != nullptr) ^ osfs_mount_root.empty()) {
    THROW_InvalidArgument("osfs and osfs_mount_root must be set both or neither");
  }
  ROCKSDB_JSON_OPT_PROP(js, nfs_rpc_timeout_ms);
  ROCKSDB_JSON_OPT_PROP(js, nfs_poll_timeout_ms);
  ROCKSDB_JSON_REQ_PROP(js, nfs_server);
  ROCKSDB_JSON_REQ_PROP(js, nfs_export);
  // just very basic validity check
  if (!Slice(nfs_server).starts_with("nfs://")) {
    nfs_server = "nfs://" + nfs_server;
  }
  if (!Slice(nfs_export).starts_with("/")) {
    THROW_InvalidArgument("Bad param: nfs_export: " + nfs_export);
  }
  m_nfs = nfs_init_context();
  nfs_set_timeout(m_nfs, nfs_rpc_timeout_ms);
  nfs_set_poll_timeout(m_nfs, nfs_poll_timeout_ms);
  int err = nfs_mount(m_nfs, nfs_server.c_str(), nfs_export.c_str());
  if (err) {
    throw Status::IOError(
        Slice("nfs_mount fail: ") + nfs_get_error(m_nfs),
        "server: " + nfs_server + ", "
        "export: " + nfs_export);
  }
}

TailingNFS::~TailingNFS() {
  if (m_nfs)
    nfs_destroy_context(m_nfs);
}

bool TailingNFS::IsInstanceOf(const std::string& id) const {
  if (id == kClassName())
    return true;
  else
    return FileSystem::IsInstanceOf(id);
}

Status TailingNFS::RegisterDbPaths(const std::vector<std::string>& paths) {
  return Status::OK();
}

Status TailingNFS::UnregisterDbPaths(const std::vector<std::string>& paths) {
  return Status::OK();
}

// Now File->GetUniqueId() is not used in RocksDB,
// it had been used by BlockBasedTable
static size_t
NFSGetUniqueId(nfs_context* nfs, nfsfh* fh, char* id, size_t max_size) {
  if (max_size < kMaxVarint64Length * 3) {
    return 0;
  }
  struct nfs_stat_64 st;
  int result = nfs_fstat64(nfs, fh, &st);
  if (result == -1) {
    return 0;
  }
  char* rid = id;
  rid = EncodeVarint64(rid, st.nfs_dev);
  rid = EncodeVarint64(rid, st.nfs_ino);
  rid = EncodeVarint64(rid, st.nfs_rdev); // use st_gen on MacOS
  assert(rid >= id);
  return static_cast<size_t>(rid - id);
}

struct TailingNFSReaderFile : FSRandomAccessFile, FSSequentialFile {
  nfs_context* m_nfs = nullptr;
  nfsfh* m_fh = nullptr;
  std::string m_fname;
  bool m_is_random_access; // intentional not init

  ~TailingNFSReaderFile() override {
    if (m_fh)
      nfs_close(m_nfs, m_fh);
  }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    int len = nfs_pread(m_nfs, m_fh, scratch, n, offset);
    if (len < 0) {
      return IOStatus::IOError("TailingNFSReaderFile::Read nfs_pread",
        m_fname + nfs_get_error(m_nfs));
    }
    result->data_ = scratch;
    result->size_ = len;
    return IOStatus::OK();
  }

  // use FSRandomAccessFile::Prefetch
  // use FSRandomAccessFile::MultiRead

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return NFSGetUniqueId(m_nfs, m_fh, id, max_size);
  }

  // use FSRandomAccessFile::Hint

  bool use_direct_io() const override { return false; }
  size_t GetRequiredBufferAlignment() const override { return kDefaultPageSize; }

  // use FSRandomAccessFile::InvalidateCache
  // use FSRandomAccessFile::ReadAsync

  Temperature GetTemperature() const override { return Temperature::kUnknown; }
  intptr_t FileDescriptor() const override { return -1; }

  //-------------------------------------------------------------------------
  // FSSequentialFile methods:

  IOStatus Read(size_t n, const IOOptions&, Slice* result,
                char* scratch, IODebugContext*) {
    // there is m_fh->offset internal, it is equal to m_offset
    int len = nfs_read(m_nfs, m_fh, scratch, n);
    if (len < 0) {
      return IOStatus::IOError("TailingNFSReaderFile::Read nfs_read",
        m_fname + nfs_get_error(m_nfs));
    }
    result->data_ = scratch;
    result->size_ = len;
    return IOStatus::OK();
  }

  IOStatus Skip(uint64_t n) {
    uint64_t cur_offset; // SEEK_CUR need not nfs rpc, just SEEK_END need
    int err = nfs_lseek(m_nfs, m_fh, n, SEEK_CUR, &cur_offset);
    if (err) {
      return IOStatus::IOError("TailingNFSReaderFile::Skip nfs_lseek",
        m_fname + nfs_get_error(m_nfs));
    }
    return IOStatus::OK();
  }

  // FSSequentialFile::use_direct_io
  // FSSequentialFile::GetRequiredBufferAlignment
  // FSSequentialFile::InvalidateCache

  IOStatus PositionedRead(uint64_t offset, size_t n,
                          const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* /*dbg*/) override {
    int len = nfs_pread(m_nfs, m_fh, scratch, n, offset);
    if (len < 0) {
      return IOStatus::IOError("TailingNFSReaderFile::PositionedRead nfs_pread",
        m_fname + nfs_get_error(m_nfs));
    }
    result->data_ = scratch;
    result->size_ = len;
    return IOStatus::OK();
  }

  // Temperature FSSequentialFile::GetTemperature
};

IOStatus TailingNFS::NewSequentialFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  auto f = new TailingNFSReaderFile;
  int err = nfs_open(m_nfs, fname.c_str(), O_RDONLY, &f->m_fh);
  if (err) {
    delete f;
    return IOStatus::IOError("TailingNFS::NewSequentialFile nfs_open",
        fname + " : " + nfs_get_error(m_nfs));
  }
  f->m_fname = fname;
  f->m_is_random_access = false;
  result->reset(f);
  return IOStatus::OK();
}

IOStatus TailingNFS::NewRandomAccessFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  auto f = new TailingNFSReaderFile;
  int err = nfs_open(m_nfs, fname.c_str(), O_RDONLY, &f->m_fh);
  if (err) {
    delete f;
    return IOStatus::IOError("TailingNFS::NewRandomAccessFile nfs_open",
        fname + " : " + nfs_get_error(m_nfs));
  }
  f->m_fname = fname;
  f->m_is_random_access = true;
  result->reset(f);
  return IOStatus::OK();
}

struct TailingNFSWritableFile : FSWritableFile {
  nfs_context* m_nfs = nullptr;
  nfsfh* m_fh = nullptr;
  size_t m_offset = 0;
  std::string m_fname;

  ~TailingNFSWritableFile() {
    if (m_fh)
      nfs_close(m_nfs, m_fh);
  }
  IOStatus Append(const Slice& data, const IOOptions&,
                  IODebugContext*) override {
    int len = nfs_pwrite(m_nfs, m_fh, data.data_, data.size_, m_offset);
    if (len < 0) {
      return IOStatus::IOError("TailingNFSWritableFile::Append nfs_pwrite",
          m_fname + " : " + nfs_get_error(m_nfs));
    }
    m_offset += len;
    return IOStatus::OK();
  }
  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& verification_info,
                  IODebugContext* dbg) override {
    int len = nfs_pwrite(m_nfs, m_fh, data.data_, data.size_, m_offset);
    if (len < 0) {
      return IOStatus::IOError("TailingNFSWritableFile::Append nfs_pwrite",
          m_fname + " : " + nfs_get_error(m_nfs));
    }
    m_offset += len;
    return IOStatus::OK();
  }
  // Now PositionedAppend is not used in rocksdb, it is just a legacy api
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override {
    int len = nfs_pwrite(m_nfs, m_fh, data.data_, data.size_, offset);
    if (len < 0) {
      return IOStatus::IOError(
          "TailingNFSWritableFile::PositionedAppend nfs_pwrite",
          m_fname + " : " + nfs_get_error(m_nfs));
    }
    return IOStatus::OK();
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& verification_info,
                            IODebugContext* dbg) override {
    int len = nfs_pwrite(m_nfs, m_fh, data.data_, data.size_, offset);
    if (len < 0) {
      return IOStatus::IOError(
          "TailingNFSWritableFile::PositionedAppend nfs_pwrite",
          m_fname + " : " + nfs_get_error(m_nfs));
    }
    return IOStatus::OK();
  }
  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override {
    int err = nfs_ftruncate(m_nfs, m_fh, size);
    if (err) {
      return IOStatus::IOError(
          "TailingNFSWritableFile::Truncate nfs_ftruncate",
          m_fname + " : " + nfs_get_error(m_nfs));
    }
    return IOStatus::OK();
  }
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    int err = nfs_close(m_nfs, m_fh);
    if (err) {
      return IOStatus::IOError("TailingNFSWritableFile::Close nfs_close",
          m_fname + " : " + nfs_get_error(m_nfs));
    }
    return IOStatus::OK();
  }
  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    return IOStatus::OK();
  }
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    return IOStatus::OK();
  }
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return IOStatus::OK();
  }
  bool IsSyncThreadSafe() const override { return true; }

  bool use_direct_io() const override { return true; }

  size_t GetRequiredBufferAlignment() const override { return 0; }

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
  }
  Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return Env::WriteLifeTimeHint::WLTH_NONE;
  }
  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override {
    return m_offset;
  }
  void SetPreallocationBlockSize(size_t size) override {}
  void GetPreallocationStatus(size_t* block_size,
                              size_t* last_allocated_block) override {}

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return NFSGetUniqueId(m_nfs, m_fh, id, max_size);
  }

  IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    return IOStatus::OK();
  }

  IOStatus RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/,
                     const IOOptions&, IODebugContext*) override {
    return IOStatus::OK();
  }

  void PrepareWrite(size_t /*offset*/, size_t /*len*/, const IOOptions&,
                    IODebugContext*) override {
  }

  IOStatus Allocate(uint64_t /*offset*/, uint64_t /*len*/, const IOOptions&,
                    IODebugContext*) override {
    return IOStatus::OK();
  }

  intptr_t FileDescriptor() const final { return -1; }

  // The semantic is seek to fsize for later writes
  void SetFileSize(uint64_t fsize) final { m_offset = fsize; }
};

IOStatus TailingNFS::NewWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  auto f = new TailingNFSWritableFile;
  // nfs append write needs getattr for file size, never use O_APPEND
  int flags = O_WRONLY|O_CREAT|O_TRUNC;
  int err = nfs_open(m_nfs, fname.c_str(), flags, &f->m_fh);
  if (err) {
    delete f;
    return IOStatus::IOError("TailingNFS::NewWritableFile: nfs_open",
                             fname + ": " + nfs_get_error(m_nfs));
  }
  f->m_fname = fname;
  result->reset(f);
  return IOStatus::OK();
}

IOStatus TailingNFS::ReopenWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  struct stat st;
  int err = nfs_stat(m_nfs, fname.c_str(), &st);
  if (err) {
    return IOStatus::IOError("TailingNFS::ReopenWritableFile: nfs_stat",
                             fname + ": " + nfs_get_error(m_nfs));
  }
  auto f = new TailingNFSWritableFile;
  // nfs append write needs getattr for file size, never use O_APPEND!
  // instead we get file size for init m_offset, thus avoid getattr on
  // each append write.
  f->m_offset = st.st_size;
  int flags = O_WRONLY|O_CREAT; //|O_APPEND;
  err = nfs_open(m_nfs, fname.c_str(), flags, &f->m_fh);
  if (err) {
    delete f;
    return IOStatus::IOError("TailingNFS::ReopenWritableFile: nfs_open",
                             fname + ": " + nfs_get_error(m_nfs));
  }
  f->m_fname = fname;
  result->reset(f);
  return IOStatus::OK();
}

IOStatus TailingNFS::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    const FileOptions& options, std::unique_ptr<FSWritableFile>* result,
    IODebugContext* dbg) {
  ROCKSDB_DIE("Not supported: DBOptions::recycle_log_file_num must be 0");
}

IOStatus TailingNFS::NewRandomRWFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSRandomRWFile>* result, IODebugContext* dbg) {
  ROCKSDB_DIE(
"Not supported: IngestExternalFileOptions::allow_global_seqno must be false");
}

struct TailingNFSDirectory : public FSDirectory {
  IOStatus Fsync(const IOOptions&, IODebugContext*) override {
    return IOStatus::OK();
  }
  IOStatus FsyncWithDirOptions(const IOOptions&, IODebugContext*,
                               const DirFsyncOptions&) override {
    return IOStatus::OK();
  }
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    return IOStatus::OK();
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return 0;
  }
};
IOStatus TailingNFS::NewDirectory(const std::string& dir,
                                  const IOOptions& options,
                                  std::unique_ptr<FSDirectory>* result,
                                  IODebugContext* dbg) {
  int err = nfs_mkdir(m_nfs, dir.c_str());
  if (err) {
    return IOStatus::IOError("TailingNFS::NewDirectory: nfs_mkdir",
        dir + " : " + nfs_get_error(m_nfs));
  }
  result->reset(new TailingNFSDirectory);
  return IOStatus::OK();
}

IOStatus TailingNFS::FileExists(const std::string& fname,
                                const IOOptions& options,
                                IODebugContext* dbg) {
  int err = nfs_access(m_nfs, fname.c_str(), 0111);
  if (err) {
    return IOStatus::IOError("TailingNFS::NewDirectory: nfs_mkdir",
        fname + " : " + nfs_get_error(m_nfs));
  }
  return IOStatus::OK();
}

IOStatus TailingNFS::GetChildren(const std::string& dir,
                                 const IOOptions& options,
                                 std::vector<std::string>* result,
                                 IODebugContext* dbg) {
  result->clear();
  nfsdir* dh = nullptr;
  int err = nfs_opendir(m_nfs, dir.c_str(), &dh);
  if (err) {
    return IOStatus::IOError("TailingNFS::GetChildren: nfs_opendir",
        dir + " : " + nfs_get_error(m_nfs));
  }
  while (auto ent = nfs_readdir(m_nfs, dh)) {
    result->emplace_back(ent->name);
  }
  nfs_closedir(m_nfs, dh);
  return IOStatus::OK();
}

IOStatus TailingNFS::GetChildrenFileAttributes(
    const std::string& dir, const IOOptions& options,
    std::vector<FileAttributes>* result, IODebugContext* dbg) {
  result->clear();
  nfsdir* dh = nullptr;
  int err = nfs_opendir(m_nfs, dir.c_str(), &dh);
  if (err) {
    return IOStatus::IOError("TailingNFS::GetChildren: nfs_opendir",
        dir + " : " + nfs_get_error(m_nfs));
  }
  while (auto ent = nfs_readdir(m_nfs, dh)) {
    result->push_back({ent->name, ent->size});
  }
  nfs_closedir(m_nfs, dh);
  return IOStatus::OK();
}

IOStatus TailingNFS::DeleteFile(const std::string& fname,
                                const IOOptions& options,
                                IODebugContext* dbg) {
  int err = nfs_unlink(m_nfs, fname.c_str());
  if (err) {
    return IOStatus::IOError("TailingNFS::DeleteFile: nfs_unlink",
        fname + " : " + nfs_get_error(m_nfs));
  }
  return IOStatus::OK();
}

IOStatus TailingNFS::CreateDir(const std::string& dirname,
                               const IOOptions& options,
                               IODebugContext* dbg) {
  int err = nfs_mkdir(m_nfs, dirname.c_str());
  if (err) {
    return IOStatus::IOError("TailingNFS::CreateDir: nfs_mkdir",
        dirname + " : " + nfs_get_error(m_nfs));
  }
  return IOStatus::OK();
}

IOStatus TailingNFS::CreateDirIfMissing(const std::string& dir,
                                        const IOOptions& options,
                                        IODebugContext* dbg) {
  int err = nfs_mkdir(m_nfs, dir.c_str());
  if (err) {
    return IOStatus::IOError("TailingNFS::CreateDirIfMissing: nfs_mkdir",
        dir + " : " + nfs_get_error(m_nfs));
  }
  return IOStatus::OK();
}

IOStatus TailingNFS::DeleteDir(const std::string& dir,
                               const IOOptions& options,
                               IODebugContext* dbg) {
  int err = nfs_rmdir(m_nfs, dir.c_str());
  if (err) {
    return IOStatus::IOError("TailingNFS::CreateDirIfMissing: nfs_rmdir",
        dir + " : " + nfs_get_error(m_nfs));
  }
  return IOStatus::OK();
}

IOStatus TailingNFS::GetFileSize(const std::string& fname,
                                 const IOOptions& options,
                                 uint64_t* file_size,
                                 IODebugContext* dbg) {
  struct nfs_stat_64 st{};
  int err = nfs_stat64(m_nfs, fname.c_str(), &st);
  if (err) {
    return IOStatus::IOError("TailingNFS::CreateDirIfMissing: nfs_stat64",
        fname + " : " + nfs_get_error(m_nfs));
  }
  *file_size = st.nfs_size;
  return IOStatus::OK();
}

// unlikely be called
IOStatus TailingNFS::GetFileModificationTime(const std::string& fname,
                                             const IOOptions& options,
                                             uint64_t* file_mtime,
                                             IODebugContext* dbg) {
  struct nfs_stat_64 st{};
  int err = nfs_stat64(m_nfs, fname.c_str(), &st);
  if (err) {
    return IOStatus::IOError("TailingNFS::CreateDirIfMissing: nfs_stat64",
        fname + " : " + nfs_get_error(m_nfs));
  }
  *file_mtime = st.nfs_mtime;
  return IOStatus::OK();
}

IOStatus TailingNFS::IsDirectory(const std::string& path,
                                 const IOOptions& options, bool* is_dir,
                                 IODebugContext* dbg) {
  struct nfs_stat_64 st{};
  int err = nfs_stat64(m_nfs, path.c_str(), &st);
  if (ENOENT == err) {
    return IOStatus::NotFound(path);
  }
  if (err) {
    return IOStatus::IOError("TailingNFS::IsDirectory: nfs_stat64",
        path + " : " + nfs_get_error(m_nfs));
  }
  *is_dir = S_ISDIR(st.nfs_mode);
  return IOStatus::OK();
}

IOStatus TailingNFS::RenameFile(const std::string& src,
                                const std::string& dest,
                                const IOOptions& options,
                                IODebugContext* dbg) {
  int err = nfs_rename(m_nfs, src.c_str(), dest.c_str());
  if (err) {
    return IOStatus::IOError("TailingNFS::RenameFile: nfs_rename",
        src + " => " + dest + " : " + nfs_get_error(m_nfs));
  }
  return IOStatus::OK();
}

IOStatus TailingNFS::LinkFile(const std::string& src,
                              const std::string& dest,
                              const IOOptions& options,
                              IODebugContext* dbg) {
  int err = nfs_link(m_nfs, src.c_str(), dest.c_str());
  if (err) {
    return IOStatus::IOError("TailingNFS::LinkFile: nfs_link",
        src + " -> " + dest + " : " + nfs_get_error(m_nfs));
  }
  return IOStatus::OK();
}

struct TailingNFSLock : public FileLock {
  ~TailingNFSLock() {
    if (locked) {
      nfs_lockf(nfs, fh, NFS4_F_ULOCK, 0);
    }
    nfs_close(nfs, fh);
  }
  std::string fname;
  nfs_context* nfs = nullptr;
  nfsfh* fh = nullptr;
  bool locked = false;
};

IOStatus TailingNFS::LockFile(const std::string& fname,
                              const IOOptions& options, FileLock** lock,
                              IODebugContext* dbg) {
  *lock = nullptr;
  auto lk = new TailingNFSLock;
  int err = nfs_open2(m_nfs, fname.c_str(), O_CREAT|O_WRONLY, 0644, &lk->fh);
  if (err) {
    delete lk;
    return IOStatus::IOError("TailingNFS::LockFile: nfs_open2",
        fname + nfs_get_error(m_nfs));
  }
  err = nfs_lockf(m_nfs, lk->fh, NFS4_F_LOCK, 0);
  if (err) {
    delete lk;
    return IOStatus::IOError("TailingNFS::LockFile: nfs_lockf",
        fname + nfs_get_error(m_nfs));
  }
  lk->fname = fname;
  lk->locked = true;
  *lock = lk;
  return IOStatus::OK();
}

IOStatus TailingNFS::UnlockFile(FileLock* flock,
                                const IOOptions& options,
                                IODebugContext* dbg) {
  auto lk = dynamic_cast<TailingNFSLock*>(flock);
  int err = nfs_lockf(m_nfs, lk->fh, NFS4_F_ULOCK, 0);
  lk->locked = false; // always treat as unlocked
  auto fname = std::move(lk->fname);
  delete lk;
  if (err) {
    return IOStatus::IOError("TailingNFS::UnlockFile: nfs_lockf NFS4_F_ULOCK",
        fname + nfs_get_error(m_nfs));
  }
  return IOStatus::OK();
}

IOStatus TailingNFS::NewLogger(const std::string& fname,
                               const IOOptions& io_opts,
                               std::shared_ptr<Logger>* result,
                               IODebugContext* dbg) {
  FileOptions options;
  options.io_options = io_opts;
  options.writable_file_max_buffer_size = 64 * 1024;
  std::unique_ptr<FSWritableFile> writable_file;
  const IOStatus status = NewWritableFile(fname, options, &writable_file, dbg);
  if (!status.ok()) {
    return status;
  }
  // EnvLogger use env just for clock
  *result = std::make_shared<EnvLogger>(std::move(writable_file), fname,
                                        options, Env::Default());
  return IOStatus::OK();
}

IOStatus TailingNFS::GetAbsolutePath(const std::string& db_path,
                                     const IOOptions& options,
                                     std::string* output_path,
                                     IODebugContext* dbg) {
  output_path->clear();
  output_path->append(nfs_server);
  output_path->append(nfs_export);
  if (!db_path.empty() && db_path[0] != '/') {
    output_path->append("/");
  }
  output_path->append(db_path);
  return IOStatus::OK();
}

IOStatus TailingNFS::GetTestDirectory(const IOOptions& options,
                                      std::string* path,
                                      IODebugContext* dbg) {
  // copy from ChrootFileSystem::GetTestDirectory
  char buf[256];
  snprintf(buf, sizeof(buf), "/rocksdbtest-%d", static_cast<int>(geteuid()));
  *path = buf;
  return CreateDirIfMissing(*path, options, dbg);
}

//-----------------------------------------------------------------
void JS_ToplingDB_FS_AddVersion(json& djs, bool html) {
  auto& ver = djs["toplingdb-fs"];
  const char* git_ver = git_version_hash_info_toplingdb_fs();
  if (html) {
    std::string topling_rocks = HtmlEscapeMin(strstr(git_ver, "commit ") + strlen("commit "));
    auto headstr = [](const std::string& s, auto pos) {
      return terark::fstring(s.data(), pos - s.begin());
    };
    auto tailstr = [](const std::string& s, auto pos) {
      return terark::fstring(&*pos, s.end() - pos);
    };
    auto topling_rocks_sha_end = std::find_if(topling_rocks.begin(), topling_rocks.end(), &isspace);
    using namespace terark;
    terark::string_appender<> oss_rocks(valvec_reserve(), 512);
    oss_rocks|"<pre>"
             |"<a href='https://github.com/topling/toplingdb-fs/commit/"
             |headstr(topling_rocks, topling_rocks_sha_end)|"'>"
             |headstr(topling_rocks, topling_rocks_sha_end)|"</a>"
             |tailstr(topling_rocks, topling_rocks_sha_end)
             |"</pre>";
    ver = static_cast<std::string&&>(oss_rocks);
  } else {
    ver = git_ver;
  }
}

void TailingNFS::Update(const json&, const json& js, const SidePluginRepo&) {
  if (osfs && !osfs_mount_root.empty()) {
    ROCKSDB_JSON_OPT_PROP(js, use_osfs_write);
  }
}
std::string TailingNFS::ToString(const json& d, const SidePluginRepo& repo) const {
  bool html = JsonSmartBool(d, "html");
  json djs;
  ROCKSDB_JSON_SET_PROP(djs, use_osfs_write);
  ROCKSDB_JSON_SET_FACX(djs, osfs, file_system);
  ROCKSDB_JSON_SET_PROP(djs, osfs_mount_root);
  return JsonToString(djs, d);
}

ROCKSDB_REG_Plugin(TailingNFS, FileSystem);
ROCKSDB_REG_EasyProxyManip(TailingNFS, FileSystem);


}  // namespace ROCKSDB_NAMESPACE


