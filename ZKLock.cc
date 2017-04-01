#include <vector>
#include <polaris/polaris.h>
#include "./ZKLock.h"

namespace sandsea {

ZKLock::ZKLock(zhandle_t *zh) 
{
    zh_ = zh;
}

ZKLock::~ZKLock() 
{
}

void ZKLock::defaultWatcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
}

void ZKLock::lockWatcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) 
{
    ZKLockWrapper * wrp = static_cast<ZKLockWrapper*>(watcherCtx);
    if (!wrp) return;
    std::shared_ptr<ZKLock> zklock = wrp->getZKLock();
    if (zklock.get()) {
        zklock->notifyAll();
    } else {
        LOG_WARN("ZKLock is expired, path:%s\n", path);
    }
}


void ZKLock::notifyAll()
{
    cond_.notify_all();
}

bool ZKLock::init(const char *zkHost) {
    int count = 0;
    do {
        count++;
        zh_ = zookeeper_init(zkHost, &ZKLock::defaultWatcher, 10000, 0, NULL, 0);
    } while (!zh_ && count < 3);

    if (!zh_) {
        LOG_ERROR("zookeeper_init failed!!!\n");
        return false;
    } else {
        LOG_INFO("zookeeper_init successfully.\n");
    }
    return true;
}

void ZKLock::copyResult(struct String_vector *vec, std::vector<std::string> *copy) 
{
    if (!vec || !vec->data) return;
    for (int i = 0; i < vec->count; i++) {
        const char *path = *(vec->data + i);
        if (path) {
            copy->push_back(path);
        }
    }
    deallocate_String_vector(vec);
}

bool ZKLock::isGotLock(const char *lockPath, std::vector<std::string> *childrens) 
{
    std::vector<std::string> pathSeq;
    for (auto& path:(*childrens)) {
        size_t ln = path.find_last_of("_");
        std::string seq = path.substr(ln + 1, path.size());
        pathSeq.push_back(seq);
        LOG_DEBUG("path:%s, seq: %s.\n", path.c_str(), seq.c_str());
    }
    std::sort(pathSeq.begin(), pathSeq.end());

    size_t ln = myPath_.find_last_of("_");
    std::string mySeq = myPath_.substr(ln + 1, myPath_.size());
    if (mySeq.compare(pathSeq.front()) == 0) {
        return true;
    }
    return false;
}

bool ZKLock::isLockAlreadyCreated(const char *lockPath, const char *uniqueString, std::string *createdPath) 
{
    struct String_vector vec;
    std::vector<std::string> childrens;
    int rc = zoo_get_children(zh_, lockPath, 0, &vec);
    if (rc != ZOK) {
        LOG_ERROR("zoo_get_children failed with return code:%d, path:%s, uniqueString:%s!!!\n", rc, lockPath, uniqueString);
        return false;
    }
    copyResult(&vec, &childrens);

    for (auto& path : childrens) {
        size_t fn = path.find_first_of("_");
        size_t ln = path.find_last_of("_");
        std::string unique = path.substr(fn + 1, ln - fn - 1);
        LOG_DEBUG("path:%s, unique: %s.\n", path.c_str(), unique.c_str());
        if (unique.compare(uniqueString) == 0) {
            *createdPath = std::string(lockPath) + "/" + path;
            LOG_INFO("already created, path:%s, full path: %s.\n", path.c_str(), createdPath->c_str());
            return true;
        }
    } 
    return false;  
}

bool ZKLock::lock(const char *lockPath, const char *uniqueString) 
{
    if (!isLockAlreadyCreated(lockPath, uniqueString, &myPath_)) {
        char buf[256];
        std::string lockSeq(lockPath);
        lockSeq.append("/lock_");
        lockSeq.append(uniqueString);
        lockSeq.append("_");
        int rc = zoo_create(zh_, lockSeq.c_str(), NULL, -1, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL|ZOO_SEQUENCE, buf, 256);
        if (rc != ZOK) {
            LOG_ERROR("zoo_create failed with return code:%d!!!\n", rc);
            return false;
        }
        myPath_.assign(buf);
    }

    std::unique_lock<std::mutex> mtxLock(mtx_);
    while (true) {
        struct String_vector vec;
        std::vector<std::string> childrens;
        ZKLockWrapper *wrp = new ZKLockWrapper(getPtr());
        int rc = zoo_wget_children(zh_, lockPath, &ZKLock::lockWatcher, wrp, &vec);
        if (rc != ZOK) {
            LOG_ERROR("zoo_wget_children2 failed with return code:%d!!!\n", rc);
            return false;
        }
        copyResult(&vec, &childrens);
        if (isGotLock(lockPath, &childrens)) {
            LOG_DEBUG("got the lock.\n");
            break;
        } else {
            LOG_DEBUG("didn't got the lock, wait watcher event.\n");
            cond_.wait(mtxLock);
        }
    }
    return true;
}

void ZKLock::unLock() 
{
    int rc = zoo_delete(zh_, myPath_.c_str(), -1);
    if (rc != ZOK) {
        LOG_WARN("zoo_delete failed with return code:%d!!!\n", rc);
    }
}


ZKLockGuard::ZKLockGuard(zhandle_t *zh, const char *lockPath, const char *uniqueString)
{
    zkLock_ = std::make_shared<ZKLock>(zh);
    zkLock_->lock(lockPath, uniqueString);
}

ZKLockGuard::~ZKLockGuard()
{
    if (zkLock_.get()) {
        zkLock_->unLock();
    }
}


}


