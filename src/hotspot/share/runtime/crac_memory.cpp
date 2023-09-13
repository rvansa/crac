/*
 * Copyright (c) 2023, Azul Systems, Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */
#include <limits.h>
#include <zlib.h>

#include "memory/resourceArea.hpp"
#include "runtime/crac.hpp"
#include "runtime/os.hpp"

const char crac::MemoryPersister::MEMORY_IMG[11];
const char crac::MemoryPersister::MEMORY_INDEX[13];
GrowableArray<struct crac::MemoryPersister::record> crac::MemoryPersister::_index(256, mtInternal);
GrowableArray<const void *> *crac::MemoryPersister::_load_profile = nullptr;
crac::MemoryWriter *crac::MemoryPersister::_writer = nullptr;
int crac::MemoryPersister::_uffd = -1;
bool crac::MemoryPersister::_is_compressed;

class FileMemoryWriter: public crac::MemoryWriter {
private:
  size_t _alignment;
public:
  FileMemoryWriter(const char *filename, size_t alignment): MemoryWriter(filename), _alignment(alignment) {}

  size_t write(void *addr, size_t size) override {
    if (!os::write(_fd, addr, size)) {
      tty->print_cr("Cannot store persisted memory: %s", os::strerror(errno));
      return BAD_OFFSET;
    }
    size_t prev_offset = _offset_curr;
    _offset_curr += size;
    if (_alignment) {
      size_t off = align_up(_offset_curr, _alignment);
      if (off > _offset_curr) {
        if (os::seek_to_file_offset(_fd, off) < 0) {
          tty->print_cr("Cannot seek: %s", os::strerror(errno));
          return false;
        }
        _offset_curr = off;
      }
    }
    return prev_offset;
  }
};

crac::MemoryWriter::MemoryWriter(const char *filename): _offset_curr(0) {
  char path[PATH_MAX];
  snprintf(path, PATH_MAX, "%s%s%s", CRaCCheckpointTo, os::file_separator(), filename);
  _fd = os::open(path, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (_fd < 0) {
    fatal("Cannot open persisted memory file %s: %s", path, os::strerror(errno));
  }
  _offset_curr = 0;
}

#define LOOKUP_FUNC(var, name) \
      _##var = CAST_TO_FN_PTR(var##_func, os::dll_lookup(_lib, name)); \
      if (_##var == nullptr) { \
        tty->print_cr("Cannot load zlib function " name); \
        return; \
      }

class ZlibUtil {
protected:
  void *_lib;

  ZlibUtil(): _lib(nullptr) {
    char path[PATH_MAX];
    char ebuf[1024];
    if (os::dll_locate_lib(path, sizeof(path), Arguments::get_dll_dir(), "zip")) {
      _lib = os::dll_load(path, ebuf, sizeof(ebuf));
    }
    if (_lib == nullptr) {
      tty->print_cr("Cannot open zlib! %s", ebuf);
      return;
    }
  }

  ~ZlibUtil() {
    os::dll_unload(_lib);
  }
};

class CompressingWriter: public crac::MemoryWriter, ZlibUtil {
typedef int (*deflate_init_func)(z_streamp, int, const char *, int);
typedef int (*deflate_func)(z_streamp, int);
private:
  deflate_init_func _deflate_init;
  deflate_func _deflate;
  z_stream _stream;
  size_t _offset;
public:
  CompressingWriter(const char *filename, bool *success): MemoryWriter(filename), _offset(0) {
    *success = false;
    if (_lib == nullptr) {
      return;
    }
    LOOKUP_FUNC(deflate_init, "deflateInit_");
    LOOKUP_FUNC(deflate, "deflate");
    int compression_level = -1; // 1 - 9, -1 is default
    if (strcmp(CRPersistMemoryCompression, "enabled")) {
      compression_level = atoi(CRPersistMemoryCompression);
      if (compression_level < Z_BEST_SPEED || compression_level > Z_BEST_COMPRESSION) {
        tty->print_cr("Invalid compression level %s: use value between 1 (best speed) and 2 (best compression)",
        CRPersistMemoryCompression);
      }
    }
    memset(&_stream, 0, sizeof(z_stream));
    int retval = _deflate_init(&_stream, compression_level, ZLIB_VERSION, sizeof(z_stream));
    if (retval != Z_OK) {
      tty->print_cr("Failed to init deflate: %d", retval);
      return;
    }
    *success = true;
  }

  size_t write(void *addr, size_t size) override {
    char buf[4096];
    while (size > 0) {
      _stream.next_in = (Bytef *) addr;
      _stream.avail_in = size;
      _stream.next_out = (Bytef *)buf;
      _stream.avail_out = sizeof(buf);

      size_t used;
      int retval = _deflate(&_stream, Z_NO_FLUSH);
      switch (retval) {
        case Z_STREAM_END:
          assert(_stream.avail_in == 0, "Should have no available input");
          /* fall through */
        case Z_OK:
        case Z_BUF_ERROR:
          if (!os::write(_fd, buf, sizeof(buf) - _stream.avail_out)) {
            fatal("Error writing compressed memory: %s", os::strerror(errno));
          }
          used = size - _stream.avail_in;
          addr = (char *) addr + used;
          size -= used;
          break;
        default:
          fatal("Error compressing memory: %d - %s", retval, _stream.msg);
      }
    }
    // Here the offset will be just order, not offset to the file
    return _offset++;
  };

  ~CompressingWriter() {
    char buf[4096];
    bool finished = false;
    while (!finished) {
      _stream.next_in = nullptr;
      _stream.avail_in = 0;
      _stream.next_out = (Bytef *)buf;
      _stream.avail_out = sizeof(buf);

      int retval = _deflate(&_stream, Z_FINISH);
      switch (retval) {
        case Z_STREAM_END:
          finished = true;
          /* fall through */
        case Z_OK:
        case Z_BUF_ERROR:
          if (!os::write(_fd, buf, sizeof(buf) - _stream.avail_out)) {
            fatal("Error writing compressed memory: %s", os::strerror(errno));
          }
          break;
        default:
          fatal("Error compressing memory: %d - %s", retval, _stream.msg);
      }
    }
  }
};

class DecompressingReader: public crac::MemoryReader, ZlibUtil {
typedef int (*inflate_init_func)(z_streamp strm, const char *version, int stream_size);
typedef int (*inflate_func)(z_streamp, int);
private:
  inflate_init_func _inflate_init;
  inflate_func _inflate;
  z_stream _stream;
  char _buf[4096];
public:
  DecompressingReader(const char *filename): MemoryReader(filename) {
    if (_lib == nullptr) {
      fatal("Cannot load zlib, decompression is not possible.");
    }
    LOOKUP_FUNC(inflate_init, "inflateInit_");
    LOOKUP_FUNC(inflate, "inflate");
    memset(&_stream, 0, sizeof(z_stream));
    int retval = _inflate_init(&_stream, ZLIB_VERSION, sizeof(z_stream));
    if (retval != Z_OK) {
      fatal("Failed to initialize decompression stream: %d", retval);
    }
  }

  void read(size_t offset, void *addr, size_t size, bool executable) {
    while (size > 0) {
      if (_stream.avail_in == 0) {
        int read = ::read(_fd, _buf, sizeof(_buf));
        if (read < 0) {
          if (errno == EINTR) {
            continue;
          }
          fatal("Cannot read persisted memory file: %s", os::strerror(errno));
        } else if (read == 0) {
          if (size != 0) {
            fatal("Encountered end of persisted memory file too early");
          }
          return;
        }
        _stream.next_in = (Bytef *) _buf;
        _stream.avail_in = read;
      }

      _stream.next_out = (Bytef *) addr;
      _stream.avail_out = size;

      size_t decompressed;
      int retval = _inflate(&_stream, Z_NO_FLUSH);
      switch (retval) {
        case Z_STREAM_END:
          assert(_stream.avail_out == 0, "Should have no space left");
          /* fall through */
        case Z_OK:
        case Z_BUF_ERROR:
          decompressed = size - _stream.avail_out;
          addr = (char *) addr + decompressed;
          size -= decompressed;
          break;
        default:
          fatal("Error decompressing memory: %d - %s", retval, _stream.msg);
      }
    }
  }
};

#undef LOOKUP_FUNC

crac::MemoryReader::MemoryReader(const char *filename) {
  char path[PATH_MAX];
  snprintf(path, PATH_MAX, "%s%s%s", CRaCRestoreFrom, os::file_separator(), filename);
  _fd = os::open(path, O_RDONLY, S_IRUSR | S_IWUSR);
  if (_fd < 0) {
    fatal("Cannot open persisted memory file %s: %s", path, os::strerror(errno));
  }
}

void crac::FileMemoryReader::read(size_t offset, void *addr, size_t length, bool executable) {
  assert(_fd >= 0, "File descriptor not open");
  if (os::seek_to_file_offset(_fd, offset) < 0) {
    fatal("Cannot seek in persisted memory file: %d, 0x%zx: %s", _fd, offset, os::strerror(errno));
  }
  if (!read_all(_fd, (char *) addr, length)) {
    fatal("Cannot read persisted memory file: %s", os::strerror(errno));
  }
};

void crac::MemoryPersister::init() {
  _is_compressed = CRPersistMemoryCompression != nullptr && strcmp(CRPersistMemoryCompression, "disabled");
  // When the memory is going to be optimized we don't need to compress
  // it and decompress aftwards; just use regular writer.
  if (CRAutoOptimizeMemory == nullptr || !strcmp(CRAutoOptimizeMemory, "enabled")) {
    _is_compressed = false;
  } else {
    guarantee(!strcmp(CRAutoOptimizeMemory, "disabled"),
      "CRAutoOptimizeMemory can be 'enabled' or 'disabled', current value: '%s'", CRAutoOptimizeMemory);
  }
  if (_is_compressed) {
    bool success;
    _writer = new CompressingWriter(MEMORY_IMG, &success);
    if (!success) {
      warning("Cannot persist memory compressed, falling back to uncompressed.");
      _writer = new FileMemoryWriter(MEMORY_IMG, os::vm_page_size());
      _is_compressed = false;
    }
  } else {
    _writer = new FileMemoryWriter(MEMORY_IMG, os::vm_page_size());
  }
}

static bool is_all_zeroes(void *addr, size_t page_size) {
  unsigned long long *ptr = (unsigned long long *) addr;
  unsigned long long *end = (unsigned long long *)((char *) addr + page_size);
  while (ptr < end && *ptr == 0) ++ptr;
  return ptr == end;
}

bool crac::MemoryPersister::store(void *addr, size_t length, size_t mapped_length, bool executable) {
  if (mapped_length == 0) {
    return true;
  }

  size_t page_size = os::vm_page_size();
  assert(((u_int64_t) addr & (page_size - 1)) == 0, "Unaligned address %p", addr);
  assert(length <= mapped_length, "Useful length %lx longer than mapped %lx", length, mapped_length);
  assert((mapped_length & (page_size - 1)) == 0, "Unaligned length %lx at %p (page size %lx)", mapped_length, addr, page_size);

  int execFlag = (executable ? Flags::EXECUTABLE : 0);
  char *curr = (char *) addr;
  char *end = curr + length;
  bool do_zeroes = is_all_zeroes(addr, page_size);
  while (curr < end) {
    char *start = curr;
    if (do_zeroes) {
      do {
        curr += page_size;
      } while (curr < end && is_all_zeroes(curr, page_size));
      _index.append({
        .addr = (u_int64_t) start,
        .length = (u_int64_t) (curr - start),
        .offset = BAD_OFFSET,
        .flags = Flags::ACCESSIBLE | execFlag
      });
      do_zeroes = false;
    } else {
      do {
        curr += page_size;
      } while (curr < end && !is_all_zeroes(curr, page_size));
      size_t to_write = (curr > end ? end : curr) - start;
      size_t offset = _writer->write(start, to_write);
      _index.append({
        .addr = (u_int64_t) start,
        .length = (u_int64_t) to_write,
        .offset = (u_int64_t) offset,
        .flags = Flags::DATA | Flags::ACCESSIBLE | execFlag
      });
      do_zeroes = true;
    }
  }

  size_t aligned_length = align_up(length, page_size);
  if (aligned_length < mapped_length) {
    _index.append({
      .addr = (u_int64_t) addr + aligned_length,
      .length = (u_int64_t) mapped_length - aligned_length,
      .offset = BAD_OFFSET,
      .flags = Flags::ACCESSIBLE | execFlag
    });
  }
  return unmap(addr, mapped_length);
}

bool crac::MemoryPersister::store_gap(void *addr, size_t length) {
  assert(((u_int64_t) addr & (os::vm_page_size() - 1)) == 0, "Unaligned address");
  assert((length & (os::vm_page_size() - 1)) == 0, "Unaligned length");
  if (length == 0) {
    return true;
  }
  _index.append({
    .addr = (u_int64_t) addr,
    .length = (u_int64_t) length,
    .offset = BAD_OFFSET,
    .flags = 0
  });
  return unmap(addr, length);
}

void crac::MemoryPersister::load_on_restore() {
  const GrowableArray<struct record> *records;
  ResourceMark rm;
  GrowableArray<struct record> by_offset(0);
  MemoryReader *reader;
  bool should_map = false;;
  if (_is_compressed) {
    by_offset.reserve(_index.length());
    by_offset.appendAll(&_index);
    by_offset.sort([](struct record *a, struct record *b) {
      if (a->offset < b->offset) return -1;
      if (a->offset > b->offset) return 1;
      return 0;
    });
    records = &by_offset;
    reader = new DecompressingReader(MEMORY_IMG);
    should_map = true;
  } else {
    records = &_index;
    if (CRRestoreMemoryNoWait) {
      reader = new FileMemoryReader(MEMORY_IMG);
    } else {
      reader = new MmappingMemoryReader(MEMORY_IMG);
    }
  }
  size_t page_size = os::vm_page_size();
  for (int i = 0; i < records->length(); ++i) {
    const struct record &r = records->at(i);
    size_t aligned_length = align_up(r.length, page_size);
    bool executable = r.flags & Flags::EXECUTABLE;
    if (r.flags & Flags::ACCESSIBLE) {
      if ((r.flags & Flags::DATA) == 0) {
        if (!map((void *) r.addr, aligned_length, executable)) {
          fatal("Cannot remap memory at %p-%p", (void *) r.addr, (void *)(r.addr + aligned_length));
        }
      } else {
        ResourceMark rm;
        char *data = (char *) r.addr;
        if (CRRestoreMemoryNoWait) {
          // src for ioctl_copy does not need to be page-aligned. Length must be but
          // if we copy some bogus past the allocation buffer we don't care
          data = resource_allocate_bytes(r.length, AllocFailStrategy::EXIT_OOM);
        } else if (should_map && !map(data, aligned_length, executable)) {
          fatal("Cannot remap memory at %p-%p", (void *) r.addr, (void *)(r.addr + aligned_length));
        }
        reader->read(r.offset, data, r.length, r.flags & Flags::EXECUTABLE);
        if (CRRestoreMemoryNoWait) {
          if (!copy((void *) r.addr, data, aligned_length)) {
            fatal("Cannot copy memory %lx -> %p (0x%zx bytes) to final destination: %s", r.addr, data, aligned_length, os::strerror(errno));
          }
        }
      }
    } else if (!CRRestoreMemoryNoWait) {
      // In case of RestoreMemoryNoWait the gaps are already mapped in init_userfault()
      if (!map_gap((void *) r.addr, aligned_length)) {
        fatal("Cannot remap non-accessible memory at %p-%p", (void *) r.addr, (void *)(r.addr + aligned_length));
      }
    }
  }
  if (CRRestoreMemoryNoWait) {
    ::close(_uffd);
    _uffd = -1;
  }
}

#ifdef ASSERT
void crac::MemoryPersister::assert_mem(void *addr, size_t used, size_t total) {
  assert(((u_int64_t) addr & (os::vm_page_size() - 1)) == 0, "Unaligned address %p", addr);
  assert((total & (os::vm_page_size() - 1)) == 0, "Unaligned length %lx", total);

  size_t aligned = align_up(used, os::vm_page_size());
  size_t unused = total - aligned;
  void *gap_addr = (char *) addr + aligned;

  SearchInIndex comparator;
  bool found;
  int at = _index.find_sorted<struct record>(&comparator, { .addr = (u_int64_t) addr }, found);
  assert(found, "Cannot find region with address %p (%d records)", addr, _index.length());
  while (used > 0) {
    assert(at < _index.length(), "Overrunning index with 0x%zx used", used);
    const record &r = _index.at(at);
    // fprintf(stderr, "R %d %lx %lx %lx %x\n", at, r.addr, r.length, r.offset, r.flags);
    assert((void   *) r.addr == addr, "Unexpected address %lx, expected %p", r.addr, addr);
    assert(r.flags & Flags::ACCESSIBLE, "Bad flags for %lx: 0x%x", r.addr, r.flags);
    assert(r.length <= used, "Persisted memory region length does not match at %p: %lu vs. %lu", addr, used, r.length);
    if (r.flags & Flags::DATA) {
      assert(r.offset != BAD_OFFSET, "Invalid offset at %lx", r.addr);
    } else {
      assert(r.offset == BAD_OFFSET, "Invalid offset at %lx: %lx", r.addr, r.offset);
    }
    used -= r.length;
    addr = (char *) addr + r.length;
    at++;
  }
  if (unused > 0) {
    const record &g = _index.at(at);
    assert((void *) g.addr == gap_addr, "Invalid address for the gap region: %lx vs. %p", g.addr, gap_addr);
    assert(g.length == unused, "Persisted gap length does not match at %p: %lu vs. %lu", gap_addr, unused, g.length);
    assert((g.flags & (Flags::DATA | Flags::ACCESSIBLE)) == Flags::ACCESSIBLE, "Bad flags for gap %p: 0x%x", gap_addr, g.flags);
    assert(g.offset == BAD_OFFSET, "Invalid offset at %p: %lx", gap_addr, g.offset);
  }
}

void crac::MemoryPersister::assert_gap(void *addr, size_t length) {
  assert(((u_int64_t) addr & (os::vm_page_size() - 1)) == 0, "Unaligned address %p", addr);
  assert((length & (os::vm_page_size() - 1)) == 0, "Unaligned length %lx", length);
  if (length > 0) {
    SearchInIndex comparator;
    bool found;
    int at = _index.find_sorted<struct record>(&comparator, { .addr = (u_int64_t) addr }, found);
    assert(found, "Cannot find region with address %p (%d records)", addr, _index.length());
    const record &r = _index.at(at);
    assert(r.length == length, "Persisted memory region length does not match at %p: %lu vs. %lu", addr, length, r.length);
    assert((r.flags & (Flags::DATA | Flags::ACCESSIBLE)) == 0, "Bad flags for %p: 0x%x", addr, r.flags);
    assert(r.offset == BAD_OFFSET, "Invalid offset at %p: %lx", addr, r.offset);
  }
}
#endif // ASSERT

void crac::MemoryPersister::finalize() {
  delete _writer;
  _writer = nullptr;

  // In some cases we could keep the index in-memory, but this would
  // make optimization more complex.
  persist_index(_index);
}

enum IndexHeaderFlags {
  COMPRESSED = 1 << 0
};

struct index_header {
  __u32 length; // ATM used only for assertion
  __u32 flags;
};

void crac::MemoryPersister::persist_index(GrowableArray<struct record> &index) {
  index.sort([](struct record *a, struct record *b) {
    // simple cast to int doesn't work, let compiler figure it out with cmovs
    if (a->addr < b->addr) return -1;
    if (a->addr > b->addr) return 1;
    return 0;
  });

  char path[PATH_MAX];
  snprintf(path, PATH_MAX, "%s%s%s", CRaCCheckpointTo, os::file_separator(), MEMORY_INDEX);
  int fd = ::open(path, O_CREAT | O_TRUNC | O_WRONLY, 0600);
  if (fd < 0) {
    fatal("Cannot persist memory index to %s: %s", path, os::strerror(errno));
  }
  struct index_header header = {
    .length = (__u32) index.length(),
    .flags = (__u32) (_is_compressed ? IndexHeaderFlags::COMPRESSED : 0)
  };
  if (!os::write(fd, &header, sizeof(struct index_header))) {
    fatal("Cannot write memory index header: %s", os::strerror(errno));
  }
  for (int i = 0; i < index.length(); ++i) {
    const struct record &r = index.at(i);
    if (!os::write(fd, &r, sizeof(struct record))) {
      fatal("Cannot write to memory index: %s", os::strerror(errno));
    }
  }
  if (::close(fd)) {
    warning("Cannot close memory index: %s", os::strerror(errno));
  }
  // We are not deallocating the index as most likely this wouldn't reduce
  // the native memory footprint and we would have to reallocate on restore
  index.clear();
}

void crac::MemoryPersister::load_index() {
  char path[PATH_MAX];
  snprintf(path, PATH_MAX, "%s%s%s", CRaCCheckpointTo, os::file_separator(), MEMORY_INDEX);
  int fd = ::open(path, O_RDONLY);
  if (fd < 0) {
    fatal("Cannot load memory index from %s: %s", path, os::strerror(errno));
  }
  struct index_header header;
  if (!read_all(fd, (char *) &header, sizeof(struct index_header))) {
    fatal("Cannot read memory index header: %s", os::strerror(errno));
  }
  _is_compressed = header.flags & COMPRESSED;

  struct record buf[256];
  size_t offset = 0;
  for (;;) {
    ssize_t read = ::read(fd, (char *) buf + offset, sizeof(buf) - offset);
    if (read == 0) {
      break;
    } else if (read < 0) {
      if (errno == EINTR) continue;
      fatal("Cannot read memory index: %s", os::strerror(errno));
    }
    size_t num = (offset + read) / sizeof(struct record);
    for (size_t i = 0; i < num; ++i) {
      _index.append(buf[i]);
    }
    offset = read - num * sizeof(struct record);
    if (offset > 0) {
      memcpy(buf, (char *) buf + read - offset, offset);
    }
  }
  assert(offset == 0, "Leftover bytes for index? %zu", offset);
  if (::close(fd)) {
    warning("Cannot close memory index file: %s", os::strerror(errno));
  }
  assert(header.length == (__u32) _index.length(), "Unexpected index length %u (read only %d)", header.length, _index.length());
}

const struct crac::MemoryPersister::record &crac::MemoryPersister::find_record(const void *addr) {
  SearchInIndex comparator;
  bool found; // ignored
  int at = _index.find_sorted<struct record>(&comparator, { .addr = (u_int64_t) addr }, found);
  if (at >= _index.length()) {
    --at;
  }
  const record &f = _index.at(at);
  const record &r = at > 0 && addr < (void *) f.addr ? _index.at(--at) : f;

  void *end = (void *) (r.addr + align_up(r.length, os::vm_page_size()));
  if (addr < (void *) r.addr || addr >= end) {
    fatal("Lookup for address %p but closest region is %p-%p", addr, (void *) r.addr, end);
  }
  assert(r.flags & Flags::DATA, "Bad flags (missing DATA) for %p: 0x%x", addr, r.flags);
  assert(r.flags & Flags::ACCESSIBLE, "Bad flags (missing ACCESSIBLE) for %p: 0x%x", addr, r.flags);
  assert(r.offset != BAD_OFFSET, "Invalid offset at %p", addr);
  return r;
}

static int compare_address(const char * const &addr1, const void * const &addr2) {
  if (addr1 < addr2) return -1;
  if (addr1 > addr2) return 1;
  return 0;
}

void crac::MemoryPersister::add_record_to_new_index(const char *from, const char *to,
    crac::MemoryReader &reader, const struct record &old, crac::MemoryWriter *writer,
    GrowableArray<struct record> &new_index) {
  size_t first_offset = BAD_OFFSET;
  const char *curr = from;
  const size_t page_size = os::vm_page_size();
  char buf[page_size];
  while (curr < to) {
    reader.read(old.offset + (curr - (const char *) old.addr), buf, page_size, false);
    size_t length = curr + page_size < to ? page_size : to - curr;
    size_t offset = writer->write(buf, length);
    if (first_offset == BAD_OFFSET) {
      first_offset = offset;
    }
    curr += page_size;
  }
  new_index.append({
    .addr = (u_int64_t) from,
    .length = (u_int64_t) ((curr > to ? to : curr) - from),
    .offset = first_offset,
    .flags = old.flags
  });
}

void crac::MemoryPersister::optimize() {
  guarantee(!_is_compressed, "Optimization of compressed image not implemented");
  tty->print_cr("Starting persisted memory optimization...");
  FileMemoryReader reader(MEMORY_IMG);
  MemoryWriter *writer;
  if (CRPersistMemoryCompression == nullptr || !strcmp(CRPersistMemoryCompression, "disabled")) {
    // TODO: since we're optimizing order we might expect the data to be streamed;
    // it might make sense to not align the writes and use FileMemoryReader on restore.
    writer = new FileMemoryWriter("memory.tmp", os::vm_page_size());
  } else {
    bool success;
    writer = new CompressingWriter("memory.tmp", &success);
    if (!success) {
      fatal("Cannot start compression in persisted memory optimization.");
    }
    _is_compressed = true;
  }
  ResourceMark rm;
  GrowableArray<struct record> new_index(_load_profile->length());
  const size_t page_size = os::vm_page_size();
  char buf[page_size];
  for (int i = 0; i < _load_profile->length(); ++i) {
    const void *addr = _load_profile->at(i);
    const struct record &r = find_record(addr);
    const char *from = align_down((char *) addr, page_size);
    const char *to = from + page_size;
    const char *end = (char *)(r.addr + r.length);
    if (to > end) {
      to = end;
    }
    add_record_to_new_index(from, to, reader, r, writer, new_index);
  }
  // We won't need the order of loads anymore so we just sort inplace
  // for presence lookup
  _load_profile->sort([](const void **paddr1, const void **paddr2) {
    const void *addr1 = *paddr1;
    const void *addr2 = *paddr2;
    if (addr1 < addr2) return -1;
    if (addr1 > addr2) return 1;
    return 0;
  });
  size_t old_total = 0;
  for (int i = 0; i < _index.length(); ++i) {
    const struct record &r = _index.at(i);
    old_total += r.length;

    if (!(r.flags & Flags::DATA)) {
      // copy as-is
      new_index.append(r);
      continue;
    }

    const char *begin = (const char *) r.addr;
    const char *end = (const char *) (r.addr + r.length);
    bool found;
    int index = _load_profile->find_sorted<const char *, compare_address>(begin, found);
    if (index >= _load_profile->length()) {
      --index;
    }
    const void *addr = _load_profile->at(index);
    while (addr < begin && index < _load_profile->length() - 1) {
      addr = _load_profile->at(++index);
    }
    const char *last = begin;
    while (addr < end) {
      const char *page_start = (const char *) align_down(addr, page_size);
      if (page_start > last) {
        add_record_to_new_index(last, page_start, reader, r, writer, new_index);
      }
      last = page_start + page_size;
      if (++index < _load_profile->length()) {
        addr = _load_profile->at(index);
      } else {
        break;
      }
    }
    if (last < end) {
      add_record_to_new_index(last, end, reader, r, writer, new_index);
    }
  }
  delete writer; // closes memory.tmp
#ifdef ASSERT
  size_t new_total = 0;
  for (int i = 0; i < new_index.length(); ++i) {
    new_total += new_index.at(i).length;
  }
  assert(new_total == old_total, "Index size does not match: old 0x%zx, new 0x%zx", old_total, new_total);
#endif // ASSERT
  int dirfd = open(CRaCCheckpointTo, O_RDONLY | O_DIRECTORY);
  if (dirfd < 0) {
    fatal("Cannot open checkpoint directory: %s", os::strerror(errno));
  }
  if (renameat(dirfd, "memory.tmp", dirfd, MEMORY_IMG)) {
    fatal("Cannot replace memory image with optimized version: %s", os::strerror(errno));
  }
  persist_index(new_index);
  tty->print_cr("Optimization complete, %d pages moved ahead.", _load_profile->length());

  const char *opt_fifo = "opt.fifo";
  if (mkfifoat(dirfd, opt_fifo, 0600) && errno != EEXIST) {
      warning("Cannot create fifo for synchronization of optimization: %s", os::strerror(errno));
  }
  int fifo_fd = openat(dirfd, opt_fifo, O_WRONLY);
  if (fifo_fd < 0) {
      warning("Cannot open (write) fifo used for synchronization of optimization: %s", os::strerror(errno));
  } else {
      close(fifo_fd);
  }
  if (close(dirfd)) {
    warning("Cannot close checkpoint directory: %s", os::strerror(errno));
  }
}
