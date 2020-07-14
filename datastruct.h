/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   datastruct.h
 * Author: Administrator
 *
 * Created on 2020年7月14日, 上午9:56
 */

#ifndef DATASTRUCT_H
#define DATASTRUCT_H

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


#endif /* DATASTRUCT_H */

