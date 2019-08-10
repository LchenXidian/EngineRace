// Copyright [2018] Alibaba Cloud All rights reserved

#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <string>
#include "engine.h"
#include <pthread.h>
#include <map>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <cassert>
#include <functional>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string.h>
#include <string>
#include <atomic>
#include <stdlib.h>
#include <stdio.h>


namespace polar_race {

using namespace std;

#define IL __attribute__((always_inline)) inline
#define likely(x) __builtin_expect((x),1)
#define unlikely(x) __builtin_expect((x),0)

#define HASH(x) hash<string>()(x)

struct IndexItem{
    IndexItem(size_t k,int32_t p,int32_t cur):key(k),page(p),pos(cur){}
    size_t key;
    int32_t page;
    int32_t pos;
    IndexItem& operator=(const IndexItem& index){
        this->key = index.key;
        this->page = index.page;
        this->pos = index.pos;
        return *this;
    }
};
struct Index{
    size_t key;
    int32_t page;
    int32_t pos;
};
struct FileItem{
    FileItem(int n,int p,char* cur,char* b)
           :page(n),pos(p),curr_write(cur),base(b){}
    int page;
    size_t pos;
    char* curr_write;
    char* base;
};
struct Range_Thread{
    Range_Thread(int n):num(n){}
    int num;
};
//struct Range_Thread{
//    Range_Thread(int n,char* b):num(n),base(b){}
//    int num;
//    char* base;
//};
struct RebuildIndex{
    size_t pos;
    IndexItem* base;
};
struct logItem{
    int page;
    int index_pos;
    int index_num;
    RebuildIndex index;
};
struct Read_count{
    int _count;
};
int _files[2][37768];
struct Re{
    char* base;
}ReBuf[128];

class EngineRace : public Engine  {
    public:
        const int APPEND_INDEX = 64001000;  //985MB
        const int HASH_INDEX = 67100009;
        const int SEQUENCE_INDEX = 64001000; //985MB

        const int POS_BITS = 23; // 8MB
        const int VALUE_LEN = 4096;
        const int SEG_SIZE = 1 << POS_BITS;

        //const int BUF_SIZE = 1<<30;
        //const int index_number = 262144; //一次跳跃的索引记录数目
        //const int BLOCK_SIZE = 1<<24; //16777216
        //const int NUMBER_IN_BLOCK = 4096;
        //string str_value[262144];

        const int BUF_SIZE = 1<<29;
        const int index_number = 131072; //一次跳跃的索引记录数目
        const int BLOCK_SIZE = 1<<23;    //8MB
        const int NUMBER_IN_BLOCK = 2048;
        string str_value[131072];

    public:
        static RetCode Open(const std::string& name, Engine** eptr);

        explicit EngineRace(const std::string& dir)
            : mu_1(PTHREAD_MUTEX_INITIALIZER),
              mu_2(PTHREAD_MUTEX_INITIALIZER),
              mu_3(PTHREAD_MUTEX_INITIALIZER)
              {}

        ~EngineRace();

        RetCode Write(const PolarString& key,
            const PolarString& value) override;

        RetCode Read(const PolarString& key,
            std::string* value) override;

        RetCode Range(const PolarString& lower,
            const PolarString& upper,
            Visitor &visitor) override;

    public:
        pthread_mutex_t mu_1,mu_2,mu_3;
        std::string dir_;

        int8_t _threadNum;
        size_t _n;

        std::map<pthread_t,FileItem*> _threadFileID;
        std::map<pthread_t,Range_Thread*> M;
        int _fds[37768];

        int not_null_fd;
        logItem *baseNot;

        int _fd;
        int append_size;
        IndexItem* _begin;
        IndexItem* _end;

        int _fd_rebuild;
        int rebuild_size;
        IndexItem* rebuild_begin;
        IndexItem* rebuild_end;

        int _fd_range;
        int range_size;

        IndexItem* range_begin;
        IndexItem* range_end;
        bool range_flag;

        char* buf_base;
        int range_thread_num;
        volatile int tongji_count;
        volatile int read_count;

        int read_count_fd;
        Read_count* r_begin;
        //std::atomic<int> tongji_count;
        //std::atomic<int> read_count;

        char buf_conut[64];
    private:
        void openFile();
};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_