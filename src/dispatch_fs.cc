// Copyright (c) 2021-present, Topling, Inc.  All rights reserved.
// Created by leipeng at 2024-10-09
//  Copyright (c) Topling, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <rocksdb/file_system.h>
#include <rocksdb/transaction_log.h>
#include <file/filename.h>
#include <topling/side_plugin_factory.h>

namespace ROCKSDB_NAMESPACE {

class DispatchFS : public FileSystem {
 public:
  DispatchFS(const json&, const SidePluginRepo&);
  void Update(const json&, const json& js, const SidePluginRepo&);
  std::string ToString(const json& d, const SidePluginRepo&) const;
//-----------------------------------------------------------------

  const char* Name() const override { return "DispatchFS"; }
  static const char* kClassName() { return "DispatchFS"; }
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

  FileSystem* MapParentToFS(const std::string& path) const;
  FileSystem* MapDirToFS(const std::string& dir) const;
  FileSystem* GetFS(const std::string& fname) const;

  std::shared_ptr<FileSystem> others;
  std::shared_ptr<FileSystem> manifest;
  std::shared_ptr<FileSystem> info_log;
  std::shared_ptr<FileSystem> wal_alive;
  std::shared_ptr<FileSystem> wal_archive;
  std::shared_ptr<FileSystem> sst;
  std::shared_ptr<FileSystem> dir_fs_default;
  terark::hash_strmap<std::shared_ptr<FileSystem> > dir_fs_map;
};

bool DispatchFS::IsInstanceOf(const std::string& id) const {
  if (id == kClassName()) {
    return true;
  } else {
    return FileSystem::IsInstanceOf(id);
  }
}

DispatchFS::DispatchFS(const json& js, const SidePluginRepo& repo) {
  ROCKSDB_JSON_OPT_FACT(js, others);
  ROCKSDB_JSON_OPT_FACT(js, manifest);
  ROCKSDB_JSON_OPT_FACT(js, info_log);
  ROCKSDB_JSON_OPT_FACT(js, wal_alive);
  ROCKSDB_JSON_OPT_FACT(js, wal_archive);
  ROCKSDB_JSON_OPT_FACT(js, sst);

  auto iter = js.find("dir_fs_map");
  if (iter != js.end()) {
    const json& mapjs = iter.value();
    for (iter = mapjs.begin(); iter != mapjs.end(); ++iter) {
      const std::string& dir = iter.key();
      const json& fsjs = iter.value();
      if (!fsjs.is_string()) {
        THROW_InvalidArgument("dir_fs_map sub obj value must be string(ref to FileSystem)");
      }
      std::string varname;
      varname.resize(dir.size() + 64);
      int len = snprintf(varname.data(), varname.size(), "dir_fs_map['%s']", dir.c_str());
      varname.resize(len);
      std::shared_ptr<FileSystem> fs;
      fs = PluginFactory<decltype(fs)>::ObtainPlugin(
              varname.c_str(), ROCKSDB_FUNC, fsjs, repo);
      dir_fs_map[dir] = fs;
    }
  }
  ROCKSDB_JSON_OPT_FACT(js, dir_fs_default); // on dir fs map fail
}

Status DispatchFS::RegisterDbPaths(const std::vector<std::string>& paths) {
  return Status::OK();
}

Status DispatchFS::UnregisterDbPaths(const std::vector<std::string>& paths) {
  return Status::OK();
}

FileSystem* DispatchFS::MapParentToFS(const std::string& path) const {
  const char* begin = path.c_str();
  const char* slash = strrchr(begin, '/');
  std::string parent(begin, slash);
  return MapDirToFS(parent);
}

FileSystem* DispatchFS::MapDirToFS(const std::string& dir) const {
  auto iter = dir_fs_map.find(dir);
  if (dir_fs_map.end() != iter)
    return iter->second.get();
  else
    return dir_fs_default.get();
}

FileSystem* DispatchFS::GetFS(const std::string& fname) const {
  uint64_t fno;
  FileType ft;
  WalFileType wft;
  if (!ParseFileName(fname, &fno, &ft, &wft)) {
    return nullptr;
  }
  FileSystem* fs = nullptr;
  switch (ft) {
  case kWalFile:
    if (kAliveLogFile == wft)
      fs = wal_alive.get();
    else
      fs = wal_archive.get();
    break;
  case kDBLockFile:
    fs = others.get();
    break;
  case kTableFile: // sst
    fs = sst.get();
    break;
  case kDescriptorFile:
  case kCurrentFile:
    fs = manifest.get();
    break;
  case kTempFile:
    fs = others.get();
    break;
  case kInfoLogFile:
    fs = info_log.get();
    break;
  case kMetaDatabase:
    fs = manifest.get();
    break;
  case kIdentityFile:
    fs = manifest.get();
    break;
  case kOptionsFile:
    fs = others.get();
    break;
  case kBlobFile:
    fs = sst.get();
    break;
  }
  if (fs)
    return fs;
  else
    return others.get();
}

IOStatus DispatchFS::NewSequentialFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  if (FileSystem* fs = GetFS(fname)) {
    return fs->NewSequentialFile(fname, options, result, dbg);
  } else {
    return IOStatus::InvalidArgument(
      "DispatchFS::NewSequentialFile: ParseFileName", fname);
  }
}

IOStatus DispatchFS::NewRandomAccessFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  if (FileSystem* fs = GetFS(fname)) {
    return fs->NewRandomAccessFile(fname, options, result, dbg);
  } else {
    return IOStatus::InvalidArgument(
      "DispatchFS::NewRandomAccessFile: ParseFileName", fname);
  }
}

IOStatus DispatchFS::NewWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  if (FileSystem* fs = GetFS(fname)) {
    return fs->NewWritableFile(fname, options, result, dbg);
  } else {
    return IOStatus::InvalidArgument(
      "DispatchFS::NewWritableFile: ParseFileName", fname);
  }
}

IOStatus DispatchFS::ReopenWritableFile(
      const std::string& fname, const FileOptions& options,
      std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  if (FileSystem* fs = GetFS(fname)) {
    return fs->ReopenWritableFile(fname, options, result, dbg);
  } else {
    return IOStatus::InvalidArgument(
      "DispatchFS::ReopenWritableFile: ParseFileName", fname);
  }
}

IOStatus DispatchFS::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    const FileOptions& options, std::unique_ptr<FSWritableFile>* result,
    IODebugContext* dbg) {
  if (FileSystem* fs = GetFS(fname)) {
    return fs->ReuseWritableFile(fname, old_fname, options, result, dbg);
  } else {
    return IOStatus::InvalidArgument(
      "DispatchFS::ReuseWritableFile: ParseFileName", fname);
  }
}

IOStatus DispatchFS::NewRandomRWFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSRandomRWFile>* result, IODebugContext* dbg) {
  ROCKSDB_DIE(
"Not supported: IngestExternalFileOptions::allow_global_seqno must be false");
}

IOStatus DispatchFS::NewDirectory(const std::string& dir, const IOOptions& options,
                      std::unique_ptr<FSDirectory>* result,
                      IODebugContext* dbg) {
  auto fs = MapDirToFS(dir);
  if (!fs) {
    fs = MapParentToFS(dir);
  }
  if (fs) {
    return fs->NewDirectory(dir, options, result, dbg);
  } else {
    return IOStatus::InvalidArgument(
      "DispatchFS::NewDirectory: Map dir failed", dir);
  }
}

IOStatus DispatchFS::FileExists(const std::string& fname,
                                const IOOptions& options,
                                IODebugContext* dbg) {
  if (FileSystem* fs = GetFS(fname)) {
    return fs->FileExists(fname, options, dbg);
  } else {
    return IOStatus::InvalidArgument(
      "DispatchFS::FileExists: ParseFileName", fname);
  }
}

IOStatus DispatchFS::GetChildren(const std::string& dir,
                                 const IOOptions& options,
                                 std::vector<std::string>* result,
                                 IODebugContext* dbg) {
  result->clear();
  auto fs = MapDirToFS(dir);
  return fs->GetChildren(dir, options, result, dbg);
}

IOStatus DispatchFS::GetChildrenFileAttributes(
    const std::string& dir, const IOOptions& options,
    std::vector<FileAttributes>* result, IODebugContext* dbg) {
  result->clear();
  auto fs = MapDirToFS(dir);
  return fs->GetChildrenFileAttributes(dir, options, result, dbg);
}

IOStatus DispatchFS::DeleteFile(const std::string& fname,
                                const IOOptions& options,
                                IODebugContext* dbg) {
  if (FileSystem* fs = GetFS(fname)) {
    return fs->DeleteFile(fname, options, dbg);
  } else {
    return IOStatus::InvalidArgument(
      "DispatchFS::DeleteFile: ParseFileName", fname);
  }
}

IOStatus DispatchFS::CreateDir(const std::string& dirname,
                               const IOOptions& options,
                               IODebugContext* dbg) {
  auto fs = MapParentToFS(dirname);
  return fs->CreateDir(dirname, options, dbg);
}

IOStatus DispatchFS::CreateDirIfMissing(const std::string& dirname,
                                           const IOOptions& options,
                                           IODebugContext* dbg) {
  auto fs = MapParentToFS(dirname);
  return fs->CreateDirIfMissing(dirname, options, dbg);
}

IOStatus DispatchFS::DeleteDir(const std::string& dirname,
                                  const IOOptions& options,
                                  IODebugContext* dbg) {
  auto fs = MapDirToFS(dirname);
  return fs->DeleteDir(dirname, options, dbg);
}

IOStatus DispatchFS::GetFileSize(const std::string& fname,
                                      const IOOptions& options,
                                      uint64_t* file_size,
                                      IODebugContext* dbg) {
  if (FileSystem* fs = GetFS(fname)) {
    return fs->GetFileSize(fname, options, file_size, dbg);
  } else {
    return IOStatus::InvalidArgument(
      "DispatchFS::GetFileSize: ParseFileName", fname);
  }
}

IOStatus DispatchFS::GetFileModificationTime(const std::string& fname,
                                                  const IOOptions& options,
                                                  uint64_t* file_mtime,
                                                  IODebugContext* dbg) {
  if (FileSystem* fs = GetFS(fname)) {
    return fs->GetFileModificationTime(fname, options, file_mtime, dbg);
  } else {
    return IOStatus::InvalidArgument(
      "DispatchFS::GetFileModificationTime: ParseFileName", fname);
  }
}

IOStatus DispatchFS::IsDirectory(const std::string& path,
                                 const IOOptions& options, bool* is_dir,
                                 IODebugContext* dbg) {
  auto iter = dir_fs_map.find(path);
  if (iter != dir_fs_map.end()) {
    *is_dir = true;
    return IOStatus::OK();
  }
  auto fs = MapParentToFS(path);
  return fs->IsDirectory(path, options, is_dir, dbg);
}

IOStatus DispatchFS::RenameFile(const std::string& src,
                                const std::string& dest,
                                const IOOptions& options,
                                IODebugContext* dbg) {
  auto fs1 = GetFS(src), fs2 = GetFS(dest);
  if (!fs1 || !fs2) {
    return IOStatus::InvalidArgument(
        "DispatchFS::RenameFile: ParseFileName", src + " => " + dest);
  }
  if (fs1 == fs2) {
    return fs1->RenameFile(src, dest, options, dbg);
  }
  return IOStatus::InvalidArgument(
      "DispatchFS::RenameFile: ParseFileName, different fs", src + " => " + dest);
}

IOStatus DispatchFS::LinkFile(const std::string& src,
                              const std::string& dest,
                              const IOOptions& options,
                              IODebugContext* dbg) {
  auto fs1 = GetFS(src), fs2 = GetFS(dest);
  if (!fs1 || !fs2) {
    return IOStatus::InvalidArgument(
        "DispatchFS::LinkFile: ParseFileName", src + " => " + dest);
  }
  if (fs1 == fs2) {
    return fs1->LinkFile(src, dest, options, dbg);
  }
  return IOStatus::InvalidArgument(
      "DispatchFS::LinkFile: ParseFileName, different fs", src + " => " + dest);
}

struct DelegateFileLock : public FileLock {
  FileLock* lock = nullptr;
  FileSystem* fs = nullptr;
  std::string filename;
  ~DelegateFileLock() { delete lock; }
};
IOStatus DispatchFS::LockFile(const std::string& fname,
                              const IOOptions& options, FileLock** lock,
                              IODebugContext* dbg) {
  if (FileSystem* fs = GetFS(fname)) {
    auto dlk = new DelegateFileLock;
    auto res = fs->LockFile(fname, options, &dlk->lock, dbg);
    if (res.ok()) {
      dlk->filename = fname;
      dlk->fs = fs;
      *lock = dlk;
    } else {
      delete lock;
    }
    return res;
  } else {
    return IOStatus::InvalidArgument(
        "DispatchFS::LockFile::ParseFileName", fname);
  }
}

IOStatus DispatchFS::NewLogger(const std::string& fname,
                               const IOOptions& options,
                               std::shared_ptr<Logger>* result,
                               IODebugContext* dbg) {
  if (FileSystem* fs = GetFS(fname)) {
    return fs->NewLogger(fname, options, result, dbg);
  } else if (info_log) {
    return info_log->NewLogger(fname, options, result, dbg);
  } else {
    return IOStatus::InvalidArgument(
        "DispatchFS::NewLogger::ParseFileName", fname);
  }
}

IOStatus DispatchFS::GetAbsolutePath(const std::string& db_path,
                                     const IOOptions& options,
                                     std::string* output_path,
                                     IODebugContext* dbg) {
  auto iter = dir_fs_map.find(db_path);
  auto fs = (dir_fs_map.end() != iter ? iter->second : dir_fs_default).get();
  return fs->GetAbsolutePath(db_path, options, output_path, dbg);
}

IOStatus DispatchFS::UnlockFile(FileLock* flock,
                                const IOOptions& options,
                                IODebugContext* dbg) {
  ROCKSDB_VERIFY(flock != nullptr);
  auto dlk = dynamic_cast<DelegateFileLock*>(flock);
  ROCKSDB_VERIFY(dlk != nullptr);
  return dlk->fs->UnlockFile(dlk->lock, options, dbg);
}

IOStatus DispatchFS::GetTestDirectory(const IOOptions& options,
                                      std::string* path,
                                      IODebugContext* dbg) {
  // copy from ChrootFileSystem::GetTestDirectory
  char buf[256];
  snprintf(buf, sizeof(buf), "/rocksdbtest-%d", static_cast<int>(geteuid()));
  *path = buf;
  return CreateDirIfMissing(*path, options, dbg);
}

//-----------------------------------------------------------------
void DispatchFS::Update(const json&, const json& js, const SidePluginRepo&) {
  // do nothing, do not support dynamic update
}
std::string DispatchFS::ToString(const json& d, const SidePluginRepo& repo) const {
  bool html = JsonSmartBool(d, "html");
  json djs;
  ROCKSDB_JSON_SET_FACX(djs, others     , file_system);
  ROCKSDB_JSON_SET_FACX(djs, manifest   , file_system);
  ROCKSDB_JSON_SET_FACX(djs, info_log   , file_system);
  ROCKSDB_JSON_SET_FACX(djs, wal_alive  , file_system);
  ROCKSDB_JSON_SET_FACX(djs, wal_archive, file_system);
  ROCKSDB_JSON_SET_FACX(djs, sst        , file_system);
  ROCKSDB_JSON_SET_FACX(djs, dir_fs_default, file_system);
  json& jsfsmap = djs["dir_fs_map"];
  for (const auto& kv : dir_fs_map) {
    const auto& k = kv.first;
    const auto& fs = kv.second;
    const std::string dir(k.data(), k.size());
    ROCKSDB_JSON_SET_FACT_INNER(jsfsmap[dir], fs, file_system);
  }
  return JsonToString(djs, d);
}

ROCKSDB_REG_Plugin(DispatchFS, FileSystem);
ROCKSDB_REG_EasyProxyManip(DispatchFS, FileSystem);

}  // namespace ROCKSDB_NAMESPACE


