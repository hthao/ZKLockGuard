# ZKLockGuard
An implementation of distributed lock with zookeeper.

## Instruction
	class ZKLockGuard;
(constructor) 

	ZKLockGuard(zhandle_t *zh, const char *zkLockPath, const char *uniqueString);
When a `ZKLockGuard` object is created, it attempts to create a zookeeper node with type `ZOO_EPHEMERAL|ZOO_SEQUENCE` under path `zkLockPath`, for example, 


> {zkLockPath}/lock_{uniqueString}_{zookeeper_generated_sequence_number}.

if the {zookeeper_generated_sequence_number} is the smallest one, the distributed lock is acquired, otherwise it will wait.

When control leaves the scope in which the `ZKLockGuard` object was created, the `ZKLockGuard` is destructed, the zookeeper node is removed, and the distributed lock is released.

## Example

    #include <stdio.h>
    #include <stdlib.h>
    #include <unistd.h>
    #include "ZKLockGuard.h"
    
    void defaultWatcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    	//do nothing.
    }
    
    zhandle_t *zh = NULL;
    
    bool init(const char *zkHost) 
	{
	    int count = 0;
	    do {
		    count++;
		    zh = zookeeper_init(zkHost, &defaultWatcher, 10000, 0, NULL, 0);
    	} while (!zh && count < 3);
    
	    if (!zh) {
		    printf("zookeeper_init failed!!!\n");
		    return false;
	    } else {
	    	printf("zookeeper_init successfully.\n");
	    }
	    return true;
    }
    
    int main()
    {
		if (!init("192.168.7.171:2181,192.168.7.172:2181,192.168.7.173:2181")) {
			return -1;
		}

		{
			//attemp to lock
			ZKLockGuard lock(zh, "/dlock/test", "myUniqueString");
			//got the lock, 
			//do something
			sleep(10);
		}
		//lock is released.

		return 0;
    }
    
## Dependecy
- -lzookeeper_mt
- -lpthread
- c++11
