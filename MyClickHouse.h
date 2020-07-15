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

struct QTickT {
    int8_t src;
    int8_t dtype;
    int64_t timestamp;
    char code[16];
    char name[16];
    int8_t market;
    double pre_close;
    double upper_limit;
    double lower_limit;
    double bp[10];
    int64_t bv[10];
    double ap[10];
    int64_t av[10];
    int8_t status;
    double new_price;
    int64_t new_volume;
    double new_amount;
    int64_t sum_volume;
    double sum_amount;
    double open;
    double high;
    double low;
    double avg_bid_price;
    double avg_ask_price;
    int64_t new_bid_volume;
    double new_bid_amount;
    int64_t new_ask_volume;
    double new_ask_amount;
    int64_t open_interest;
    double pre_settle;
    int64_t pre_open_interest;
    double close;
    double settle;
    int64_t multiple;
    double price_step;
    int32_t create_date;
    int32_t list_date;
    int32_t expire_date;
    int32_t start_settle_date;
    int32_t end_settle_date;
    int32_t exercise_date;
    double exercise_price;
    int8_t cp_flag;
    char underlying_code[8];
    int64_t sum_bid_volume;
    double sum_bid_amount;
    int64_t sum_ask_volume;
    double sum_ask_amount;
    int64_t bid_order_volume;
    double bid_order_amount;
    int64_t bid_cancel_volume;
    double bid_cancel_amount;
    int64_t ask_order_volume;
    double ask_order_amount;
    int64_t ask_cancel_volume;
    double ask_cancel_amount;
    int64_t new_knock_count;
    int64_t sum_knock_count;
    int64_t local_time;
};


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
    int64_t remainSec = difftime(time_2, time_1) + 0 * 3600;
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
    void ReadPBQTick(string file);
    void InsertQTick();
    void InsertPBQTick();
    void ReadClickHouse(string date, string code);    
    void GenericExample();
    bool HandleLine(const string& line);    
    void GetFileNum(const string& dirname, const string& date);
    void ReadAllPBQTick();
    void DeleteTable();
    
    void ReadStructTick(string file);
    void ConvertStruct();
    void InsertStructQTick();
    
private:    
    Client* client_;    
    std::list<string> list_qtick;    
    std::list<string> list_pb;
    std::list<QTickT> list_ticks_;
    std::map<int, string> files_;
    
};

#endif /* MYCLICKHOUSE_H */

