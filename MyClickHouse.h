/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   MyClickHouse.h
 * Author: Administrator
 *
 * Created on 2020年6月5日, 下午5:01
 */

#ifndef MYCLICKHOUSE_H
#define MYCLICKHOUSE_H
#include <stdexcept>
#include <iostream>
#include <stdlib.h>  
#include <stdio.h>
#include<time.h>
#include <stdio.h>
#include <sys/time.h>
#include <stdlib.h>
#include <vector>
#include <iostream>

#include <cstdlib>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <cassert>
#include <string.h>
#include <memory>
#include <list>

#include <clickhouse/client.h>
#include <clickhouse/error_codes.h>
#include <clickhouse/types/type_parser.h>

#include "x/x.h"
#include "QTick_generated.h"

#define random(x) (rand()%x)

#if defined(_MSC_VER)
#   pragma warning(disable : 4996)
#endif
using namespace clickhouse;
using namespace std;


static long GetLocalTime() {
    struct timeval _t;
    gettimeofday(&_t, NULL);
    long time = long(_t.tv_sec)*1000000 + long(_t.tv_usec);
    return time;
}

static int64_t GetTime(int64_t endTime) {
    endTime = endTime / 1000;
    struct tm _time_1; // 开始时间
    struct tm _time_2; // 结束时间
    memset(&_time_1, 0, sizeof (_time_1));
    memset(&_time_2, 0, sizeof (_time_2));
    long startTime = 19700101000000;
    //long endTime = 20190101080000;
    // 现在的日期　　c++时间是从1900年１月１日开始
    _time_1.tm_year = startTime / (10000000000) - 1900;
    _time_1.tm_mon = startTime / (100000000) % 100 - 1;
    _time_1.tm_mday = startTime / (1000000) % 100 - 1;
    _time_1.tm_hour = startTime / (10000) % 100;
    _time_1.tm_min = startTime / (100) % 100;
    _time_1.tm_sec = startTime % 100;
    // 结束时间
    _time_2.tm_year = endTime / (10000000000) - 1900;
    _time_2.tm_mon = endTime / (100000000) % 100 - 1;
    _time_2.tm_mday = endTime / (1000000) % 100 - 1;
    _time_2.tm_hour = endTime / (10000) % 100;
    _time_2.tm_min = endTime / (100) % 100;
    _time_2.tm_sec = endTime % 100;

    time_t time_1 = mktime(&_time_1);
    time_t time_2 = mktime(&_time_2);
    //long remainSec = difftime(time_2,time_1) - 8 * 3600;
    int64_t remainSec = difftime(time_2, time_1) + 7 * 3600;
    return remainSec;
    //printf("<%ld>\n", remainSec);
}

static int64_t GetStamp(long endTime) {
    endTime = endTime / 1000;
    int64_t stamp = endTime % 1000000;
}

class MyClickHouse {
public:
    MyClickHouse();
    MyClickHouse(const MyClickHouse& orig);
    virtual ~MyClickHouse();
    void ReadQTick(string file);
    void InsertQTick();
    void ReadQTick();    
    void GenericExample();
    
private:    
    Client* client_;    
    std::list<string> list_qtick;
};

#endif /* MYCLICKHOUSE_H */

