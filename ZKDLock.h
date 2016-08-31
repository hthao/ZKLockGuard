//http://github.com/hthao/ZKDLock
//Author: Hao Hongting (haohongting@live.com)

#ifndef __ZKDLOCK_H
#define __ZKDLOCK_H

#include <string>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <zookeeper/zookeeper.h>

//implementation of Distributed Lock with Zookeeper.
class ZKDLock {
    public:
        ZKDLock(zhandle_t *zh, const char *zkLockPath, const char *uniqueString) {
            zh_ = zh;
            if (!lock(zkLockPath, uniqueString)) {
                fprintf(stderr, "error happened to lock!!!\n");
                return;
            }
        }

        ~ZKDLock() {
            unLock();
        }


        static void defaultWatcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
            //do nothing.
        }

        static void lockWatcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
            ZKDLock * zl = static_cast<ZKDLock*>(watcherCtx);
            if (zl) {
                zl->notifyAll();
            } else {
                fprintf(stderr, "%s, failed to get context!!!\n", __func__);
            }
        }

    private:

        void notifyAll()
        {
            cond_.notify_all();
        }

        void copyResult(struct String_vector *vec, std::vector<std::string> *copy) {
            if (!vec || !vec->data) return;
            for (int i = 0; i < vec->count; i++) {
                const char *path = *(vec->data + i);
                if (path) {
                    copy->push_back(path);
                    delete path;
                }
            }     
            delete vec->data;
        }

        bool isGotLock(const char *lockPath, std::vector<std::string> *childrens) {
            std::vector<std::string> pathSeq;
            for (auto& path : (*childrens)) {
                size_t ln = path.find_last_of("_");
                std::string seq = path.substr(ln + 1, path.size());
                pathSeq.push_back(seq);
            }
            std::sort(pathSeq.begin(), pathSeq.end());

            size_t ln = zkLockPath_.find_last_of("_");
            std::string mySeq = zkLockPath_.substr(ln + 1, zkLockPath_.size()); 
            return mySeq == pathSeq.front();
        }

        bool isLockPathCreated(const char *lockPath, const char *uniqueString, std::string *createdPath) {
            struct String_vector vec;
            std::vector<std::string> childrens;
            int rc = zoo_get_children(zh_, lockPath, 0, &vec);
            if (rc != ZOK) {
                fprintf(stderr, "%s, zoo_get_children failed with return code:%d, path:%s, uniqueString:%s!!!\n", __func__, rc, lockPath, uniqueString);
                return false;
            }
            copyResult(&vec, &childrens);

            for (auto& path : childrens) {
                size_t fn = path.find_first_of("_");
                size_t ln = path.find_last_of("_");
                std::string unique = path.substr(fn + 1, ln - fn - 1);
                fprintf(stdout, "%s, path:%s, unique: %s.\n", __func__, path.c_str(), unique.c_str());
                if (unique.compare(uniqueString) == 0) {
                    *createdPath = std::string(lockPath) + "/" + path;
                    fprintf(stdout, "%s, already created, path:%s, full path: %s.\n", __func__, path.c_str(), createdPath->c_str());
                    return true;
                }
            } 
            return false;           
        }

        bool lock(const char *lockPath, const char *uniqueString) {
            if (!isLockPathCreated(lockPath, uniqueString, &zkLockPath_)) {
                char buf[512] = {0};
                std::string lockSeqPath = std::string(lockPath) + "/lock_" + uniqueString + "_";
                int rc = zoo_create(zh_, lockSeqPath.c_str(), NULL, -1, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL|ZOO_SEQUENCE, buf, 512);
                if (rc != ZOK) {
                    fprintf(stderr, "%s, zoo_create failed with return code:%d, path:%s!!!\n", __func__, rc, lockSeqPath.c_str());
                    return false; 
                }
                zkLockPath_.assign(buf);
            } 

            std::unique_lock<std::mutex> mtxLock(mtx_);
            while (true) {
                struct String_vector vec;
                std::vector<std::string> childrens;
                int rc = zoo_wget_children(zh_, lockPath, &ZKDLock::lockWatcher, (void*)this, &vec);
                if (rc != ZOK) {
                    fprintf(stderr, "%s, zoo_wget_children failed with return code:%d, path:%s!!!\n", __func__, rc, lockPath);
                    return false; 
                }
                copyResult(&vec, &childrens);                
                if (isGotLock(lockPath, &childrens)) {
                    fprintf(stdout, "%s, got the lock.\n", __func__);
                    break;
                } else {
                    fprintf(stdout, "%s, wait for the lock.\n", __func__);
                    cond_.wait(mtxLock);
                }
            }
            return true;
        }

        void unLock() {
            int rc = zoo_delete(zh_, zkLockPath_.c_str(), -1);
            if (rc != ZOK) {
                fprintf(stderr, "zoo_delete failed with return code:%d!!!\n", rc);
            }
        }

    private:
        zhandle_t *zh_;
        std::string zkLockPath_;
        std::mutex mtx_;
        std::condition_variable cond_;
};


#endif


