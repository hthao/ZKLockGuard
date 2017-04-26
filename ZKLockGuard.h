//Author: HaoHongting<haohongting@live.com>
// -initial create

#ifndef __SANDSEA_ZKLOCKGUARD_H
#define __SANDSEA_ZKLOCKGUARD_H

#include <string>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <zookeeper/zookeeper.h>

namespace sandsea {

class ZKLock : public std::enable_shared_from_this<ZKLock> {
public:
    ZKLock(zhandle_t *zh);
    ~ZKLock();
    static void defaultWatcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx); 
    static void lockWatcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx); 
    std::shared_ptr<ZKLock> getPtr() { return shared_from_this(); }
    bool lock(const char *lockPath, const char *uniqueString);
    void unLock(); 

private:
    void notifyAll();
    bool init(const char *zkHost);
    void copyResult(struct String_vector *vec, std::vector<std::string> *copy);
    bool isGotLock(const char *lockPath, std::vector<std::string> *childrens);
    bool isLockAlreadyCreated(const char *lockPath, const char *uniqueString, std::string *createdPath);
private:
    zhandle_t *zh_;
    std::string myPath_;
    std::mutex mtx_;
    std::condition_variable cond_;
};

class ZKLockWrapper {
public:
    ZKLockWrapper(std::shared_ptr<ZKLock> zklock):
        zklock_(zklock) {}
    ~ZKLockWrapper() {}
    std::shared_ptr<ZKLock> getZKLock() { return zklock_.lock(); }
private:
    std::weak_ptr<ZKLock> zklock_;
};

class ZKLockGuard {
public:
    ZKLockGuard(zhandle_t *zh, const char *lockPath, const char *uniqueString);
    ~ZKLockGuard();
private:
    std::shared_ptr<ZKLock>  zkLock_;

};


}

#endif

