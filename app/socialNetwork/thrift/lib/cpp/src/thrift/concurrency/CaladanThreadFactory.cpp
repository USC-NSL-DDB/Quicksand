/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <thrift/thrift-config.h>

#if USE_CALADAN_THREAD

#include <thrift/concurrency/CaladanThreadFactory.h>
#include <thrift/concurrency/Exception.h>
#include <thrift/concurrency/Monitor.h>
#include <thrift/stdcxx.h>

#include <cassert>
#include <thread>

namespace apache {
namespace thrift {
namespace concurrency {

/**
 * The C++11 thread class.
 *
 * Note that we use boost shared_ptr rather than std shared_ptrs here
 * because the Thread/Runnable classes use those and we don't want to
 * mix them.
 *
 * @version $Id:$
 */
class CaladanThread : public Thread, public stdcxx::enable_shared_from_this<CaladanThread> {
public:
  enum STATE { uninitialized, starting, started, stopping, stopped };

  static void threadMain(stdcxx::shared_ptr<CaladanThread> thread);

private:
  std::unique_ptr<rt::Thread> thread_;
  Monitor monitor_;
  STATE state_;
  bool detached_;

public:
  CaladanThread(bool detached, stdcxx::shared_ptr<Runnable> runnable)
    : state_(uninitialized), detached_(detached) {
    this->Thread::runnable(runnable);
  }

  ~CaladanThread() {
    if (!detached_ && thread_->Joinable()) {
      try {
        join();
      } catch (...) {
        // We're really hosed.
      }
    }
  }

  STATE getState() const
  {
    Synchronized sync(monitor_);
    return state_;
  }

  void setState(STATE newState)
  {
    Synchronized sync(monitor_);
    state_ = newState;

    // unblock start() with the knowledge that the thread has actually
    // started running, which avoids a race in detached threads.
    if (newState == started) {
	  monitor_.notify();
    }
  }

  void start() {
    if (getState() != uninitialized) {
      return;
    }

    stdcxx::shared_ptr<CaladanThread> selfRef = shared_from_this();
    setState(starting);

    Synchronized sync(monitor_);
    thread_ = std::unique_ptr<rt::Thread>(new rt::Thread([=] { threadMain(selfRef); }));

    if (detached_)
      thread_->Detach();

    // Wait for the thread to start and get far enough to grab everything
    // that it needs from the calling context, thus absolving the caller
    // from being required to hold on to runnable indefinitely.
    monitor_.wait();
  }

  void join() {
    if (!detached_ && state_ != uninitialized) {
      thread_->Join();
    }
  }

  Thread::id_t getId() { return thread_.get() ? thread_->GetId() : rt::Thread::id(); }

  stdcxx::shared_ptr<Runnable> runnable() const { return Thread::runnable(); }

  void runnable(stdcxx::shared_ptr<Runnable> value) { Thread::runnable(value); }
};

void CaladanThread::threadMain(stdcxx::shared_ptr<CaladanThread> thread) {
#if GOOGLE_PERFTOOLS_REGISTER_THREAD
  ProfilerRegisterThread();
#endif

  thread->setState(started);
  thread->runnable()->run();

  if (thread->getState() != stopping && thread->getState() != stopped) {
    thread->setState(stopping);
  }
}

CaladanThreadFactory::CaladanThreadFactory(bool detached) : ThreadFactory(detached) {
}

stdcxx::shared_ptr<Thread> CaladanThreadFactory::newThread(stdcxx::shared_ptr<Runnable> runnable) const {
  stdcxx::shared_ptr<CaladanThread> result
      = stdcxx::shared_ptr<CaladanThread>(new CaladanThread(isDetached(), runnable));
  runnable->thread(result);
  return result;
}

Thread::id_t CaladanThreadFactory::getCurrentThreadId() const {
  return rt::Thread::GetCurrentId();
}
}
}
} // apache::thrift::concurrency

#endif // USE_CALADAN_THREAD
