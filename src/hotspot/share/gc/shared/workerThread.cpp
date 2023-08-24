/*
 * Copyright (c) 2001, 2023, Oracle and/or its affiliates. All rights reserved.
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
 *
 */

#include "precompiled.hpp"
#include "gc/shared/gc_globals.hpp"
#include "gc/shared/workerThread.hpp"
#include "logging/log.hpp"
#include "memory/iterator.hpp"
#include "runtime/atomic.hpp"
#include "runtime/init.hpp"
#include "runtime/java.hpp"
#include "runtime/os.hpp"
#include "runtime/osThread.hpp"

WorkerTaskDispatcher::WorkerTaskDispatcher() :
    _task(nullptr),
    _started(0),
    _not_finished(0),
    _start_semaphore(),
    _end_semaphore() {}

void WorkerTaskDispatcher::coordinator_distribute_task(WorkerTask* task, uint num_workers) {
  // No workers are allowed to read the state variables until they have been signaled.
  _task = task;
  _not_finished = num_workers;

  // Dispatch 'num_workers' number of tasks.
  _start_semaphore.signal(num_workers);

  // Wait for the last worker to signal the coordinator.
  _end_semaphore.wait();

  // No workers are allowed to read the state variables after the coordinator has been signaled.
  assert(_not_finished == 0, "%d not finished workers?", _not_finished);
  _task = nullptr;
  _started = 0;
}

void WorkerTaskDispatcher::worker_run_task() {
  // Wait for the coordinator to dispatch a task.
  _start_semaphore.wait();

  // Get and set worker id.
  const uint worker_id = Atomic::fetch_then_add(&_started, 1u);
  WorkerThread::set_worker_id(worker_id);

  // Run task.
  GCIdMark gc_id_mark(_task->gc_id());
  _task->work(worker_id);

  // Mark that the worker is done with the task.
  // The worker is not allowed to read the state variables after this line.
  const uint not_finished = Atomic::sub(&_not_finished, 1u);

  // The last worker signals to the coordinator that all work is completed.
  if (not_finished == 0) {
    _end_semaphore.signal();
  }
}

WorkerThreads::WorkerThreads(const char* name, uint max_workers) :
    _name(name),
    _workers(NEW_C_HEAP_ARRAY(WorkerThread*, max_workers, mtInternal)),
    _max_workers(max_workers),
    _created_workers(0),
    _active_workers(0),
    _dispatcher() {}

void WorkerThreads::initialize_workers() {
  const uint initial_active_workers = UseDynamicNumberOfGCThreads ? 1 : _max_workers;
  if (set_active_workers(initial_active_workers) != initial_active_workers) {
    vm_exit_during_initialization();
  }
}

WorkerThread* WorkerThreads::create_worker(uint name_suffix) {
  if (is_init_completed() && InjectGCWorkerCreationFailure) {
    return nullptr;
  }

  WorkerThread* const worker = new WorkerThread(_name, name_suffix, &_dispatcher);

  size_t stack_size = os::get_thread_stack_size(os::gc_thread);
  char *stack_addr = os::reserve_memory(stack_size, false, mtThreadStack);
  bool success = stack_addr != nullptr && os::commit_memory(stack_addr, stack_size, false);
  success = success && os::create_thread(worker, os::gc_thread, 0, false, stack_addr, stack_size);

  if (!success) {
    delete worker;
    if (stack_addr != nullptr) {
      os::release_memory(stack_addr, stack_size);
    }
    return nullptr;
  }

  on_create_worker(worker);

  os::start_thread(worker);

  return worker;
}

uint WorkerThreads::set_active_workers(uint num_workers) {
  assert(num_workers > 0 && num_workers <= _max_workers,
         "Invalid number of active workers %u (should be 1-%u)",
         num_workers, _max_workers);

  while (_created_workers < num_workers) {
    WorkerThread* const worker = create_worker(_created_workers);
    if (worker == nullptr) {
      log_error(gc, task)("Failed to create worker thread");
      break;
    }

    _workers[_created_workers] = worker;
    _created_workers++;
  }

  _active_workers = MIN2(_created_workers, num_workers);

  log_trace(gc, task)("%s: using %d out of %d workers", _name, _active_workers, _max_workers);

  return _active_workers;
}

void WorkerThreads::threads_do(ThreadClosure* tc) const {
  for (uint i = 0; i < _created_workers; i++) {
    tc->do_thread(_workers[i]);
  }
}

void WorkerThreads::set_indirectly_suspendible_threads() {
#ifdef ASSERT
  class SetIndirectlySuspendibleThreadClosure : public ThreadClosure {
    virtual void do_thread(Thread* thread) {
      thread->set_indirectly_suspendible_thread();
    }
  };

  if (Thread::current()->is_suspendible_thread()) {
    SetIndirectlySuspendibleThreadClosure cl;
    threads_do(&cl);
  }
#endif
}

void WorkerThreads::clear_indirectly_suspendible_threads() {
#ifdef ASSERT
  class ClearIndirectlySuspendibleThreadClosure : public ThreadClosure {
    virtual void do_thread(Thread* thread) {
      thread->clear_indirectly_suspendible_thread();
    }
  };

  if (Thread::current()->is_suspendible_thread()) {
    ClearIndirectlySuspendibleThreadClosure cl;
    threads_do(&cl);
  }
#endif
}

void WorkerThreads::run_task(WorkerTask* task) {
  set_indirectly_suspendible_threads();
  _dispatcher.coordinator_distribute_task(task, _active_workers);
  clear_indirectly_suspendible_threads();
}

void WorkerThreads::run_task(WorkerTask* task, uint num_workers) {
  WithActiveWorkers with_active_workers(this, num_workers);
  run_task(task);
}

class TerminateWorkerTask: public WorkerTask {
public:
  TerminateWorkerTask(): WorkerTask("terminate-worker") {}

  void work(uint worker_id) {
    Thread *t = Thread::current();
    assert(t->is_Worker_thread(), "Must be worker");
    static_cast<WorkerThread *>(t)->stop();
  }
};

void WorkerThreads::suspend_workers() {
  log_trace(gc, task)("Suspending work gang %s with %u/%u threads",
    name(), _created_workers, _max_workers);

  ResourceMark rm;
  struct threadinfo {
    void *_os_id;
    char *_stack_end;
    size_t _stack_size;
  };
  struct threadinfo *worker_info = NEW_RESOURCE_ARRAY(struct threadinfo, _created_workers);
  // When we send the terminate task the Thread instance is deallocated,
  // hence we need to copy everything ahead;
  for (uint i = 0; i < _created_workers; ++i) {
    Thread *t = _workers[i];
    worker_info[i] = {
#ifdef LINUX
      ._os_id = reinterpret_cast<void *>(t->osthread()->pthread_id()),
#endif
      ._stack_end = (char *) t->stack_end(),
      ._stack_size = t->stack_size()
    };
  }

  TerminateWorkerTask task;
  _dispatcher.coordinator_distribute_task(&task, _created_workers);
  for (uint i = 0; i < _created_workers; ++i) {
    os::join_thread(worker_info[i]._os_id);
    os::release_memory(worker_info[i]._stack_end, worker_info[i]._stack_size);
    _workers[i] = NULL;
  }

  FREE_RESOURCE_ARRAY(struct threadinfo, worker_info, _created_workers);
}

void WorkerThreads::resume_workers() {
  log_trace(gc, task)("Resuming work gang %s with %u threads", name(), _created_workers);
  uint num_workers = _created_workers;
  _created_workers = 0;
  set_active_workers(num_workers);
}


THREAD_LOCAL uint WorkerThread::_worker_id = UINT_MAX;

WorkerThread::WorkerThread(const char* name_prefix, uint name_suffix, WorkerTaskDispatcher* dispatcher) :
    _dispatcher(dispatcher), _stop(false) {
  set_name("%s#%u", name_prefix, name_suffix);
}

void WorkerThread::run() {
  os::set_priority(this, NearMaxPriority);

  while (!_stop) {
    _dispatcher->worker_run_task();
  }
}
