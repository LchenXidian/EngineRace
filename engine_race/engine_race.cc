// Copyright [2018] Alibaba Cloud All rights reserved
#include <sys/types.h>
#include <sys/time.h>
#include "util.h"
#include "engine_race.h"
#include <dirent.h>
#include <iostream>
#include <sys/mman.h>
#include <algorithm>
#include <thread>

namespace polar_race {


IL bool operator == (const IndexItem& left, const IndexItem& rigt){
     return left.key == rigt.key;
}
IL bool operator < (const IndexItem& left, const IndexItem& rigt){
	return left.key < rigt.key;
}
IL bool operator <= (const IndexItem& left, const IndexItem& rigt){
	return left.key <= rigt.key;
}
IL bool operator > (const IndexItem& left, const IndexItem& rigt){
	return left.key > rigt.key;
}
IL bool operator >= (const IndexItem& left, const IndexItem& rigt){
	return left.key >= rigt.key;
}
IL bool operator != (const IndexItem& left, const IndexItem& rigt){
	return left.key != rigt.key;
}
IL bool cmp(const IndexItem& p,const IndexItem& q){
    return p.key < q.key;
}
IL size_t StringToSize_t(const std::string &str){
    size_t M1 = 0, M2  = 0, M3 = 0, M4 = 0, M5 = 0, M6 = 0, M7 = 0, M8 = 0;
    switch(str.length()){
        case 8: M8 = (size_t((unsigned char )str[7]));
        case 7: M7 = (size_t((unsigned char )str[6])<<8);
        case 6: M6 = (size_t((unsigned char )str[5])<<16);
        case 5: M5 = (size_t((unsigned char )str[4])<<24);
        case 4: M4 = (size_t((unsigned char )str[3])<<32);
        case 3: M3 = (size_t((unsigned char )str[2])<<40);
        case 2: M2 = (size_t((unsigned char )str[1])<<48);
        case 1: M1 = (size_t((unsigned char )str[0])<<56);
        default: break;
    }
    return M1 | M2 | M3 | M4 | M5 | M6 | M7 | M8;
}
IL std::string Size_tToString(const size_t &num){
    std::string result(8,'\0');
    result[0] = (unsigned char)(num>>56);
    result[1] = (unsigned char)(num>>48);
    result[2] = (unsigned char)(num>>40);
    result[3] = (unsigned char)(num>>32);
    result[4] = (unsigned char)(num>>24);
    result[5] = (unsigned char)(num>>16);
    result[6] = (unsigned char)(num>>8);
    result[7] = (unsigned char)(num);
    return result;
}

static void rebuildIndex(EngineRace &engine, int n){
    engine.baseNot[n].index.pos = 0;

    size_t num(0);
    size_t i(0);
    //engine.baseNot[0].index_pos存放顺序写索引总数
    for(; i != engine.baseNot[0].index_pos; ++i){
        if(engine._begin[i].key >= ((size_t)(n*4)<<56)
           && engine._begin[i].key <= ((((size_t)(n*4+3))<<56)|((((size_t)1)<<56)-1))){
            memcpy(engine.baseNot[n].index.base + num, engine._begin + i, sizeof(IndexItem));
            ++num;
            if(n==0) std::cout<<(engine._begin + i)->key<<"\t";
        }
    }
    if(n==0) std::cout<<"-----------------"<<std::endl;
    if(num==0) return;
    stable_sort(engine.baseNot[n].index.base, engine.baseNot[n].index.base + num, cmp);

    IndexItem* item;
    for(i = 0; i < num - 1; ++i){
        item = engine.baseNot[n].index.base + i;
        if(item->key != (item+1)->key){
            if(i != engine.baseNot[n].index.pos)
                memmove(engine.baseNot[n].index.base+engine.baseNot[n].index.pos, item, sizeof(IndexItem));
            ++engine.baseNot[n].index.pos;
        }
    }

    memmove(engine.baseNot[n].index.base + engine.baseNot[n].index.pos, engine.baseNot[n].index.base + num - 1, sizeof(IndexItem));
    ++engine.baseNot[n].index.pos;


    /*IndexItem* item;
    for(item = engine.baseNot[n].index.base; item != engine.baseNot[n].index.base+num-1; ++item){
        if(item->key != (item+1)->key){
            memmove(engine.baseNot[n].index.base+engine.baseNot[n].index.pos, item, sizeof(IndexItem));
            ++engine.baseNot[n].index.pos;
            if(n==0) std::cout<<item->key<<"\t";
        }
    }

    memmove(engine.baseNot[n].index.base + engine.baseNot[n].index.pos, item, sizeof(IndexItem));
    ++engine.baseNot[n].index.pos;
    if(n==0) std::cout<<item->key<<"\n";*/

    std::cout<<"sort end "<< n << " " << engine.baseNot[n].index_num << " " << num << " " << engine.baseNot[n].index.pos <<std::endl;
}

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
    *eptr = NULL;
    EngineRace *engine_example = new EngineRace(name);

    if (!FileExists(name) && 0 != mkdir(name.c_str(), 0755)) {
        return kIOError;
    }
    engine_example->tongji_count = 0;
    engine_example->read_count = 0;
    engine_example->dir_ = name;

    engine_example->_threadNum = 0;
    engine_example->range_thread_num = 0;

    engine_example->range_flag = true;

    engine_example->rebuild_begin = NULL;
    engine_example->_begin = NULL;
    engine_example->range_begin = NULL;
    for(int i = 0;i<128;i++){
        ReBuf[i].base = NULL;
    }

    struct stat sb;

    engine_example->append_size = sizeof(IndexItem) * (engine_example->APPEND_INDEX+1);
    engine_example->_fd = open((name + "/META").c_str(), O_RDWR|O_CREAT|O_NOATIME, S_IRUSR|S_IWUSR);
    fstat(engine_example->_fd, &sb);
    if (sb.st_size == 0 && ftruncate(engine_example->_fd, engine_example->append_size)!=0)
       return kIOError;
    engine_example->_begin=(IndexItem*)mmap(NULL,engine_example->append_size,PROT_READ|PROT_WRITE,MAP_SHARED|MAP_POPULATE,engine_example->_fd,0);

    /*
    int read_count_size = sizeof(Read_count) * (37768+1);
    engine_example->read_count_fd = open((name + "/Read_count").c_str(), O_RDWR|O_CREAT|O_NOATIME, S_IRUSR|S_IWUSR);
    fstat(engine_example->read_count_fd, &sb);
    if (sb.st_size==0&&ftruncate(engine_example->read_count_fd,read_count_size)!=0)
       return kIOError;
    engine_example->r_begin=(Read_count*)mmap(NULL,read_count_size,PROT_READ|PROT_WRITE,MAP_SHARED|MAP_POPULATE,engine_example->read_count_fd,0);
    */
    setvbuf(stdout,NULL,_IOFBF,16*1024*1024);

    int result = stat((name + "/is_not_null").c_str(), &sb);
    int not_null_fd_size = sizeof(logItem)<<6;
    if(!result){
        int i = 0;
        engine_example->not_null_fd = open((name + "/is_not_null").c_str(), O_RDWR|O_CREAT|O_NOATIME, S_IRUSR|S_IWUSR);
        engine_example->baseNot = (logItem*)mmap(NULL, not_null_fd_size, PROT_READ|PROT_WRITE,MAP_SHARED|MAP_POPULATE,engine_example->not_null_fd,0);

        int max_fd = -1;
        for(i=0;i!=64;++i){
            if(engine_example->baseNot[i].page == -1)   continue;
            if(max_fd<engine_example->baseNot[i].page)  max_fd = engine_example->baseNot[i].page;
        }

        //printf("----------------------------------begin\n");
        //gettimeofday(&rebuild_timeStart, NULL );

        if(engine_example->baseNot[3].index_pos < 50000000){
            //cout<<"read through"<<endl;
            engine_example->rebuild_size = sizeof(IndexItem)*(engine_example->baseNot[0].index_pos+1);
            engine_example->_fd_rebuild = open((name + "/RebuildIndex").c_str(), O_RDWR|O_CREAT|O_NOATIME, S_IRUSR|S_IWUSR);
            if (ftruncate(engine_example->_fd_rebuild, engine_example->rebuild_size)!=0)
                return kIOError;
            engine_example->rebuild_begin = (IndexItem*)mmap(NULL, engine_example->rebuild_size, PROT_READ|PROT_WRITE, MAP_SHARED | MAP_POPULATE, engine_example->_fd_rebuild, 0);

            size_t num = 0;
            for(i = 0; i < 64; ++i){
                engine_example->baseNot[i].index.base = engine_example->rebuild_begin + num;
                num += engine_example->baseNot[i].index_num;
            }

            stable_sort(engine_example->_begin, engine_example->_begin + engine_example->baseNot[0].index_pos, cmp);

            IndexItem* item;
            num = 0;
            for(i = 0; i < engine_example->baseNot[0].index_pos - 1; ++i){
                item = engine_example->_begin + i;
                if(item->key != (item+1)->key){
                    memcpy(engine_example->rebuild_begin + num, item, sizeof(IndexItem));
                    ++num;
                }
            }
            memcpy(engine_example->rebuild_begin + num, engine_example->_begin + engine_example->baseNot[0].index_pos - 1, sizeof(IndexItem));
            ++num;
            engine_example->baseNot[9].index_pos = num;
            engine_example->baseNot[3].index_pos = engine_example->baseNot[0].index_pos;

            munmap(engine_example->_begin, engine_example->append_size);
            engine_example->_begin = NULL;
        }
        //gettimeofday( &rebuild_timeEnd, NULL );
        //runTime = (rebuild_timeEnd.tv_sec-rebuild_timeStart.tv_sec)+(double)(rebuild_timeEnd.tv_usec-rebuild_timeStart.tv_usec)/1000000;
        //printf("rebuild index table cost time is %lf\n", runTime);
        //printf("----------------------------------end\n");

        engine_example->openFile();
        engine_example->_n = max_fd + 1;

        for(int i = 0;i<128;i++){
            _files[0][1000 + i] = -1;  //文件
            _files[1][1000 + i] = i;           //第几个缓存
            ReBuf[i].base = (char*)mmap(NULL,engine_example->SEG_SIZE,PROT_READ|PROT_WRITE,MAP_SHARED,engine_example->_fds[1000 + i],0);
        }
    }
    else{
        engine_example->_n = 0;
        engine_example->not_null_fd = open((name + "/is_not_null").c_str(), O_RDWR|O_CREAT|O_NOATIME, S_IRUSR|S_IWUSR);
        ftruncate(engine_example->not_null_fd, not_null_fd_size);
        engine_example->baseNot = (logItem*)mmap(NULL,not_null_fd_size,PROT_READ|PROT_WRITE,MAP_SHARED|MAP_POPULATE,engine_example->not_null_fd,0);
        memset(engine_example->baseNot,-1,not_null_fd_size);

        for(int i=0;i<64;++i){
            engine_example->baseNot[i].index_num = 0;
        }
    }

    *eptr = engine_example;

    //gettimeofday(&timeStart, NULL );
    return kSucc;
}

// 2. Close engine
EngineRace::~EngineRace() {
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
    pthread_t thread_id= pthread_self();

    pthread_mutex_lock(&mu_1);
    if(unlikely(_threadNum!=64)){
        auto iter = _threadFileID.find(thread_id);
        if(iter == _threadFileID.end()){
            if(range_begin != NULL){
                munmap(range_begin, rebuild_size);
                range_begin = NULL;
                rename((dir_+"/RebuildIndex").c_str(),(dir_+"/RebuildIndexOld").c_str());
            }
            
            for(int i = 0;i<128;i++){
                if(ReBuf[i].base != NULL){
                    munmap(ReBuf[i].base, SEG_SIZE);
                    ReBuf[i].base = NULL;
                }
            }
            if(_begin == NULL){
                _begin = (IndexItem*)mmap(NULL,append_size,PROT_READ|PROT_WRITE,MAP_SHARED|MAP_POPULATE,_fd,0);
            }

            _fds[_n] = open((dir_ + "/" + std::to_string(_n)).c_str(), O_RDWR|O_CREAT|O_NOATIME, S_IRUSR|S_IWUSR);
            if(ftruncate(_fds[_n], SEG_SIZE)!=0)
                return kIOError;

            char* base = (char*)mmap(NULL, SEG_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, _fds[_n], 0);
            madvise(base, SEG_SIZE, MADV_SEQUENTIAL);

            baseNot[_threadNum].page = _n;
            if(baseNot[0].index_pos==-1) baseNot[0].index_pos=0;

            _threadFileID.insert(make_pair(thread_id,new FileItem(_n,0,base,base)));
            _threadNum++;
            _n++;
        }
    }
    pthread_mutex_unlock(&mu_1);

    std::string value_str = value.ToString();
    string key_str = key.ToString();
    size_t KEY = StringToSize_t(key_str);

    if (unlikely(_threadFileID[thread_id]->pos + VALUE_LEN > (unsigned int)SEG_SIZE)) {
        size_t n = _threadFileID[thread_id]->page + 64;
        if (_threadFileID[thread_id]->base != NULL) {
            munmap(_threadFileID[thread_id]->base, SEG_SIZE);
        }
        _fds[n] = open((dir_ + "/" + std::to_string(n)).c_str(), O_RDWR|O_CREAT|O_NOATIME, S_IRUSR|S_IWUSR);
        if(ftruncate(_fds[n], SEG_SIZE)!=0)
            return kIOError;
        baseNot[n & 63].page = n;
        _threadFileID[thread_id]->base = (char*)mmap(NULL, SEG_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, _fds[n], 0);
        madvise(_threadFileID[thread_id]->base, SEG_SIZE, MADV_SEQUENTIAL);
        _threadFileID[thread_id]->curr_write = _threadFileID[thread_id]->base;
        _threadFileID[thread_id]->pos = 0;
        _threadFileID[thread_id]->page = n;
    }
    memcpy(_threadFileID[thread_id]->curr_write, value_str.c_str(), VALUE_LEN);

    int pos = __sync_fetch_and_add(&(baseNot[0].index_pos),1);
    __sync_add_and_fetch(&(baseNot[((uint8_t)key_str[0])>>2].index_num),1);

    IndexItem* item = _begin + pos;
    item->key = KEY;
    item->page = _threadFileID[thread_id]->page;
    item->pos = _threadFileID[thread_id]->pos;
/*pthread_mutex_lock(&mu_1);
    cout << KEY << " " << item->page << " " << item->pos << " " << HASH(value_str) << endl;
pthread_mutex_unlock(&mu_1);*/
    _threadFileID[thread_id]->pos += VALUE_LEN;
    _threadFileID[thread_id]->curr_write += VALUE_LEN;

    return kSucc;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {

    string key_str(key.ToString());
    size_t k = StringToSize_t(key_str);
    IndexItem comp(k,0,0);
    IndexItem* item = lower_bound(rebuild_begin,
                            rebuild_begin + baseNot[9].index_pos,
                            comp);

    if(likely((item != rebuild_begin + baseNot[9].index_pos) && (item->key==k))){
        value->resize(VALUE_LEN);
        if(_files[0][item->page] == -1){ //已经映射到内存
            memcpy((void*)(value->c_str()), ReBuf[_files[1][item->page]].base + item->pos, VALUE_LEN);
        }
        else{
            pread(_fds[item->page], (void*)(value->c_str()), VALUE_LEN, item->pos);
        }
    }
    else{
        return kNotFound;
    }

    return kSucc;
}
static size_t if_count = 0, if1_count = 0;
RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,Visitor &visitor) {
    pthread_t thread_id = pthread_self();

    pthread_mutex_lock(&mu_3);
    if(range_flag){
        //gettimeofday(&rebuild_timeStart, NULL );
        if(_begin != NULL){
            munmap(_begin, append_size);
            _begin = NULL;
        }
        for(int i = 64;i<128;i++){
            if(ReBuf[i].base != NULL){
                munmap(ReBuf[i].base, SEG_SIZE);
                ReBuf[i].base = NULL;
            }
            _files[0][1000 + i] = 0;   //文件
            _files[1][1000 + i] = 0;   //第几个缓存
        }

        if(rebuild_begin == NULL){
            rebuild_size = sizeof(IndexItem)*(baseNot[0].index_pos+1);
            _fd_rebuild = open((dir_ + "/RebuildIndex").c_str(), O_RDWR|O_CREAT|O_NOATIME, S_IRUSR|S_IWUSR);

            rebuild_begin = (IndexItem*)mmap(NULL, rebuild_size, PROT_READ|PROT_WRITE, MAP_SHARED | MAP_POPULATE, _fd_rebuild, 0);
            rebuild_end = rebuild_begin + baseNot[0].index_pos;
        }
        range_begin = rebuild_begin;
        range_end = rebuild_end;

        memset((void*)buf_conut,64,64);

       // buf_base = (char*)mmap(NULL, BUF_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0);
        for(int i = 0; i < index_number; ++i){
            str_value[i].resize(VALUE_LEN);
        }

        if(baseNot[9].index_pos > 10000000){
            baseNot[10].index_pos = 1;
        }
        range_flag = false;
    }
    if(range_thread_num != 64){
        M.insert(make_pair(thread_id, new Range_Thread(range_thread_num)));
        ++range_thread_num;
    }
    pthread_mutex_unlock(&mu_3);

    int INDEX_END = baseNot[9].index_pos;

    std::string value(VALUE_LEN,'\0');
    std::string key;

    if(baseNot[10].index_pos == 1){
        int j(0), k;
        int m = M[thread_id]->num*NUMBER_IN_BLOCK;
        int i = m;    int s, e;        //s——索引起始   e——索引结束
        int a = INDEX_END/index_number + 1;    int END = a*index_number;
        int base_num;
        for(; i < END; i = i + index_number){  //遍历索引
            if(buf_conut[M[thread_id]->num]==64){
                for(j = 0;j<NUMBER_IN_BLOCK;j++){
                    if(unlikely((i + j) >= INDEX_END)){
                        break;
                    }
                    if(_files[0][range_begin[i+j].page] == -1){ //已经映射到内存
                        memcpy((void*)str_value[i%index_number+j].c_str(), ReBuf[_files[1][range_begin[i+j].page]].base + range_begin[i+j].pos, VALUE_LEN);
                    }
                    else{
                        pread(_fds[range_begin[i+j].page], (void*)str_value[i%index_number+j].c_str(), VALUE_LEN, range_begin[i+j].pos);
                    }
                    //pread(_fds[range_begin[i+j].page], M[thread_id]->base + VALUE_LEN*j, VALUE_LEN, range_begin[i+j].pos);
                    //pthread_mutex_lock(&mu_1);
                    //cout << "Pread: " << i + j << " " << range_begin[i+j].key << " " << range_begin[i+j].page << " " << range_begin[i+j].pos << " " << HASH(str_value[i%index_number+j]) << endl;
                    //pthread_mutex_unlock(&mu_1);
                }
                __sync_and_and_fetch (buf_conut+M[thread_id]->num, 0);
                //cout << M[thread_id]->num << " : " << (int)buf_conut[M[thread_id]->num] << endl;
            }
            else{
                i -= index_number;
                //usleep(1);
                //++if_count;
                //cout << "if1----------------" <<endl;
                continue;
            }

            //s = i-m;
            //e = ((s+index_number)>=INDEX_END)?INDEX_END:(s+index_number);
            base_num = i - m;
            for(j=0; j<64; ++j){
                //cout<<"j:"<<j<<endl;
                if(buf_conut[j]<64){
                    s = base_num + j*NUMBER_IN_BLOCK;
                    e = ((s+NUMBER_IN_BLOCK)>=INDEX_END)?INDEX_END:(s+NUMBER_IN_BLOCK);
                    //cout<< "---" << j << " " << s << " " << e <<endl;
                    //if(s>=e) break;
                    for(k = s; k < e;++k){
                        //memcpy((void*)value.c_str(), buf_base+(k-base_num)*VALUE_LEN, VALUE_LEN);
                        key = Size_tToString(range_begin[k].key);
                       // cout << k << endl;
                        //pthread_mutex_lock(&mu_1);
                        //cout << "MEMORY: " << k << " " << range_begin[k].key << " " << HASH(str_value[k%index_number]) << endl;
                        //pthread_mutex_unlock(&mu_1);
                        //visitor.Visit(key, value);
                        visitor.Visit(key, str_value[k%index_number]);
                    }
                    __sync_fetch_and_add(buf_conut+j,1);

                }
                else{
                    --j;
                    //cout << "if2--------------" << endl;
                    //usleep(1);
                    //++if1_count;
                    continue;
                }
            }
        }
    }
    else{
        for(int i = 0; i < INDEX_END; ++i) {
            pread(_fds[range_begin[i].page], (void*)(value.c_str()), VALUE_LEN, range_begin[i].pos);
            key = Size_tToString(range_begin[i].key);
            visitor.Visit(key, value);
        }
    }
    //cout << "if_count " << if_count << endl;
    //cout << "if1_count " << if1_count << endl;
    return kSucc;
}

void EngineRace::openFile(){
    int tmp;
    DIR *d = opendir((dir_ + "/").c_str());
    struct dirent *dir_entry = NULL;
    while((dir_entry = readdir(d)) != NULL){
        if(dir_entry->d_name[0]>='0'&&dir_entry->d_name[0]<='9'){
            tmp = atoi(dir_entry->d_name);
            _fds[tmp] = open((dir_ + "/" + dir_entry->d_name).c_str(),O_RDWR|O_NOATIME,S_IRUSR|S_IWUSR);
        }
    }
    closedir(d);
}

}  // namespace polar_race