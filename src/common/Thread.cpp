#include "Ctx.h"
#include "Thread.h"

#include <utility>
#include "exception/RuntimeException.h"

namespace OpenLogReplicator {
    Thread::Thread(Ctx* newCtx, std::string newAlias) :
            ctx(newCtx),
            alias(std::move(newAlias)) {
    }

    void Thread::wakeUp() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "thread (" + ss.str() + ") wake up");
        }
    }

    void* Thread::runStatic(void* voidThread) {
        auto* thread = reinterpret_cast<Thread*>(voidThread);
        thread->contextRun();
        thread->finished = true;
        return nullptr;
    }
}
