/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   MyClickHouse.cpp
 * Author: Administrator
 * 
 * Created on 2020年6月5日, 下午5:01
 */

#include "MyClickHouse.h"
#include "coral/quote.pb.h"
#include <dirent.h>
#include "datastruct.h"

MyClickHouse::MyClickHouse() {
//    client_ = new Client (ClientOptions()
//                .SetHost("localhost")
//                .SetUser("test")
//                .SetPassword("abc123")
//                .SetDefaultDatabase("yujiwei")
//                .SetPingBeforeQuery(true)
//                .SetCompressionMethod(CompressionMethod::LZ4));
    
    client_ = new Client (ClientOptions()
                .SetHost("192.168.101.236")
                .SetPingBeforeQuery(true)
                .SetCompressionMethod(CompressionMethod::LZ4));
}

MyClickHouse::MyClickHouse(const MyClickHouse& orig) {
}

MyClickHouse::~MyClickHouse() {
}

void MyClickHouse::ReadQTick(string file) {
    ifstream infile; 
    infile.open(file.data()); 
    assert(infile.is_open());

    string line;
    while(getline(infile,line))
    {        
        string security_type = line.substr(0, 1);
        if(security_type != "a") {
            continue;
        }
        line = line.substr(12);    
        //cout << line << endl;
        list_qtick.push_back(move(line));
    }
    infile.close();    
    cout << "list_qtick " << list_qtick.size() << endl;
}

void MyClickHouse::InsertQTick() {
    client_->Execute("CREATE TABLE IF NOT EXISTS QTick (date Date, datetime DateTime, stamp Int64, src Int8, dtype Int8, timestamp Int64, code String, name String, market Int8, pre_close Float64, upper_limit Float64, lower_limit Float64, bp Array(Float64), bv Array(Int64), ap Array(Float64), av Array(Int64), status Int8, new_price Float64, new_volume Int64, new_amount Float64, sum_volume Int64, sum_amount Float64, open Float64, high Float64, low Float64, avg_bid_price Float64, avg_ask_price Float64, new_bid_volume Int64, new_bid_amount Float64, new_ask_volume Int64, new_ask_amount Float64, open_interest Int64, pre_settle Float64, pre_open_interest Int64, close Float64, settle Float64, multiple Int64, price_step Float64, create_date Int32, list_date Int32, expire_date Int32, start_settle_date Int32, end_settle_date Int32, exercise_date Int32, exercise_price Float64, cp_flag Int8, underlying_code String, sum_bid_volume Int64, sum_bid_amount Float64, sum_ask_volume Int64, sum_ask_amount Float64, bid_order_volume Int64, bid_order_amount Float64, bid_cancel_volume Int64, bid_cancel_amount Float64, ask_order_volume Int64, ask_order_amount Float64, ask_cancel_volume Int64, ask_cancel_amount Float64, new_knock_count Int64, sum_knock_count Int64) ENGINE=MergeTree ORDER BY(date, timestamp, code) PARTITION BY (date)");
    long time_use = 0;
    struct timeval start;
    struct timeval end;
    gettimeofday(&start, NULL);

    Block block;
    
    auto date = std::make_shared<ColumnDate>();
    auto datetime = std::make_shared<ColumnDateTime>();
    auto stamp = std::make_shared<ColumnInt64>();
    
    auto src = std::make_shared<ColumnInt8>();
    auto dtype = std::make_shared<ColumnInt8>();
    auto timestamp = std::make_shared<ColumnInt64>();
    auto code = std::make_shared<ColumnString>();
    auto name = std::make_shared<ColumnString>();
    auto market = std::make_shared<ColumnInt8>();
    auto pre_close = std::make_shared<ColumnFloat64>();
    auto upper_limit = std::make_shared<ColumnFloat64>();
    auto lower_limit = std::make_shared<ColumnFloat64>();
    
    auto bp = std::make_shared<ColumnArray>(std::make_shared<ColumnFloat64>()); 
    auto bp_price = std::make_shared<ColumnFloat64>();
    auto bv = std::make_shared<ColumnArray>(std::make_shared<ColumnInt64>());   
    auto bv_volume = std::make_shared<ColumnInt64>();
    auto ap = std::make_shared<ColumnArray>(std::make_shared<ColumnFloat64>());
    auto ap_price = std::make_shared<ColumnFloat64>();    
    auto av = std::make_shared<ColumnArray>(std::make_shared<ColumnInt64>());    
    auto av_volume = std::make_shared<ColumnInt64>();    
   
    auto status = std::make_shared<ColumnInt8>();
    auto new_price = std::make_shared<ColumnFloat64>();
    auto new_volume = std::make_shared<ColumnInt64>();
    auto new_amount = std::make_shared<ColumnFloat64>();
    auto sum_volume = std::make_shared<ColumnInt64>();
    auto sum_amount = std::make_shared<ColumnFloat64>();
    auto open = std::make_shared<ColumnFloat64>();
    auto high = std::make_shared<ColumnFloat64>();
    auto low = std::make_shared<ColumnFloat64>();
    auto avg_bid_price = std::make_shared<ColumnFloat64>();
    auto avg_ask_price = std::make_shared<ColumnFloat64>();
    auto new_bid_volume = std::make_shared<ColumnInt64>();
    auto new_bid_amount = std::make_shared<ColumnFloat64>();
    auto new_ask_volume = std::make_shared<ColumnInt64>();
    auto new_ask_amount = std::make_shared<ColumnFloat64>();
    auto open_interest = std::make_shared<ColumnInt64>();
    auto pre_settle = std::make_shared<ColumnFloat64>();
    auto pre_open_interest = std::make_shared<ColumnInt64>();
    auto close = std::make_shared<ColumnFloat64>();
    auto settle = std::make_shared<ColumnFloat64>();
    auto multiple = std::make_shared<ColumnInt64>();
    auto price_step = std::make_shared<ColumnFloat64>();
    auto create_date = std::make_shared<ColumnInt32>();
    auto list_date = std::make_shared<ColumnInt32>();
    auto expire_date = std::make_shared<ColumnInt32>();
    auto start_settle_date = std::make_shared<ColumnInt32>();
    auto end_settle_date = std::make_shared<ColumnInt32>();
    auto exercise_date = std::make_shared<ColumnInt32>();
    auto exercise_price = std::make_shared<ColumnFloat64>();
    auto cp_flag = std::make_shared<ColumnInt8>();
    auto underlying_code = std::make_shared<ColumnString>();
    auto sum_bid_volume = std::make_shared<ColumnInt64>();
    auto sum_bid_amount = std::make_shared<ColumnFloat64>();
    auto sum_ask_volume = std::make_shared<ColumnInt64>();
    auto sum_ask_amount = std::make_shared<ColumnFloat64>();
    auto bid_order_volume = std::make_shared<ColumnInt64>();
    auto bid_order_amount = std::make_shared<ColumnFloat64>();
    auto bid_cancel_volume = std::make_shared<ColumnInt64>();
    auto bid_cancel_amount = std::make_shared<ColumnFloat64>();
    auto ask_order_volume = std::make_shared<ColumnInt64>();
    auto ask_order_amount = std::make_shared<ColumnFloat64>();
    auto ask_cancel_volume = std::make_shared<ColumnInt64>();
    auto ask_cancel_amount = std::make_shared<ColumnFloat64>();
    auto new_knock_count = std::make_shared<ColumnInt64>();
    auto sum_knock_count = std::make_shared<ColumnInt64>();


    for (auto &itor : list_qtick) {
        auto temp = x::Base64Decode(itor);
        auto it = flatbuffers::GetRoot<QTick>(temp.data());
        
        if (it->code() && it->name()) {
            cout << it->code()->str() << " " << it->timestamp() << " " << it->name()->str() << " " << it->pre_close() << " "
                << it->upper_limit() << " " << it->lower_limit() << " " << it->new_price() << " " << it->new_volume() << endl;
        }
        
        date->Append(GetTime(it->timestamp()));
        datetime->Append(GetTime(it->timestamp()));
        stamp->Append(GetStamp(it->timestamp()));     
                
        src->Append(it->src());
        dtype->Append(it->dtype());
        timestamp->Append(it->timestamp());
        code->Append(it->code() ? it->code()->str() : "");
        name->Append(it->name() ? it->name()->str() : "");
        market->Append(it->market());
        pre_close->Append(it->pre_close());
        upper_limit->Append(it->upper_limit());
        lower_limit->Append(it->lower_limit());
        
        bp_price->Clear();
        auto tick_bp = it->bp();
        for(int Index = 0; Index < tick_bp->size(); ++Index) {
            bp_price->Append(tick_bp->Get(Index));
        }
        bp->AppendAsColumn(bp_price);
        
        bv_volume->Clear();
        auto tick_bv = it->bv();
        for(int Index = 0; Index < tick_bv->size(); ++Index) {
            bv_volume->Append(tick_bv->Get(Index));
        }
        bv->AppendAsColumn(bv_volume);       

        
        ap_price->Clear();
        auto tick_ap = it->ap();
        for(int Index = 0; Index < tick_ap->size(); ++Index) {
            ap_price->Append(tick_ap->Get(Index));
        }
        ap->AppendAsColumn(ap_price);
        
        av_volume->Clear();
        auto tick_av = it->av();
        for(int Index = 0; Index < tick_av->size(); ++Index) {
            av_volume->Append(tick_av->Get(Index));
        }
        av->AppendAsColumn(av_volume);       
        
        status->Append(it->status());
        new_price->Append(it->new_price());
        new_volume->Append(it->new_volume());
        new_amount->Append(it->new_amount());
        sum_volume->Append(it->sum_volume());
        sum_amount->Append(it->sum_amount());
        open->Append(it->open());
        high->Append(it->high());
        low->Append(it->low());
        avg_bid_price->Append(it->avg_bid_price());
        avg_ask_price->Append(it->avg_ask_price());
        new_bid_volume->Append(it->new_bid_volume());
        new_bid_amount->Append(it->new_bid_amount());
        new_ask_volume->Append(it->new_ask_volume());
        new_ask_amount->Append(it->new_ask_amount());
        open_interest->Append(it->open_interest());
        pre_settle->Append(it->pre_settle());
        pre_open_interest->Append(it->pre_open_interest());
        close->Append(it->close());
        settle->Append(it->settle());
        multiple->Append(it->multiple());
        price_step->Append(it->price_step());
        create_date->Append(it->create_date());
        list_date->Append(it->list_date());
        expire_date->Append(it->expire_date());
        start_settle_date->Append(it->start_settle_date());
        end_settle_date->Append(it->end_settle_date());
        exercise_date->Append(it->exercise_date());
        exercise_price->Append(it->exercise_price());
        cp_flag->Append(it->cp_flag());
        underlying_code->Append(it->underlying_code() ? it->underlying_code()->str() : "");
        sum_bid_volume->Append(it->sum_bid_volume());
        sum_bid_amount->Append(it->sum_bid_amount());
        sum_ask_volume->Append(it->sum_ask_volume());
        sum_ask_amount->Append(it->sum_amount());
        bid_order_volume->Append(it->bid_order_volume());
        bid_order_amount->Append(it->bid_order_amount());
        bid_cancel_volume->Append(it->bid_cancel_volume());
        bid_cancel_amount->Append(it->bid_cancel_amount());
        ask_order_volume->Append(it->ask_order_volume());
        ask_order_amount->Append(it->ask_order_amount());
        ask_cancel_volume->Append(it->ask_cancel_volume());
        ask_cancel_amount->Append(it->ask_cancel_amount());
        new_knock_count->Append(it->new_knock_count());
        sum_knock_count->Append(it->sum_knock_count());
    }
    
    block.AppendColumn("date", date);
    block.AppendColumn("datetime", datetime);
    block.AppendColumn("stamp", stamp);
    
    block.AppendColumn("src", src);
    block.AppendColumn("dtype", dtype);
    block.AppendColumn("timestamp", timestamp);
    block.AppendColumn("code", code);
    block.AppendColumn("name", name);
    block.AppendColumn("market", market);
    block.AppendColumn("pre_close", pre_close);
    block.AppendColumn("upper_limit", upper_limit);
    block.AppendColumn("lower_limit", lower_limit);
    
    block.AppendColumn("bp", bp);
    block.AppendColumn("bv", bv);
    block.AppendColumn("ap", ap);
    block.AppendColumn("av", av);
    
    block.AppendColumn("status", status);
    block.AppendColumn("new_price", new_price);
    block.AppendColumn("new_volume", new_volume);
    block.AppendColumn("new_amount", new_amount);
    block.AppendColumn("sum_volume", sum_volume);
    block.AppendColumn("sum_amount", sum_amount);
    block.AppendColumn("open", open);
    block.AppendColumn("high", high);
    block.AppendColumn("low", low);
    block.AppendColumn("avg_bid_price", avg_bid_price);
    block.AppendColumn("avg_ask_price", avg_ask_price);
    block.AppendColumn("new_bid_volume", new_bid_volume);
    block.AppendColumn("new_bid_amount", new_bid_amount);
    block.AppendColumn("new_ask_volume", new_ask_volume);
    block.AppendColumn("new_ask_amount", new_ask_amount);
    block.AppendColumn("open_interest", open_interest);
    block.AppendColumn("pre_settle", pre_settle);
    block.AppendColumn("pre_open_interest", pre_open_interest);
    block.AppendColumn("close", close);
    block.AppendColumn("settle", settle);
    block.AppendColumn("multiple", multiple);
    block.AppendColumn("price_step", price_step);
    block.AppendColumn("create_date", create_date);
    block.AppendColumn("list_date", list_date);
    block.AppendColumn("expire_date", expire_date);
    block.AppendColumn("start_settle_date", start_settle_date);
    block.AppendColumn("end_settle_date", end_settle_date);
    block.AppendColumn("exercise_date", exercise_date);
    block.AppendColumn("exercise_price", exercise_price);
    block.AppendColumn("cp_flag", cp_flag);
    block.AppendColumn("underlying_code", underlying_code);
    block.AppendColumn("sum_bid_volume", sum_bid_volume);
    block.AppendColumn("sum_bid_amount", sum_bid_amount);
    block.AppendColumn("sum_ask_volume", sum_ask_volume);
    block.AppendColumn("sum_ask_amount", sum_ask_amount);
    block.AppendColumn("bid_order_volume", bid_order_volume);
    block.AppendColumn("bid_order_amount", bid_order_amount);
    block.AppendColumn("bid_cancel_volume", bid_cancel_volume);
    block.AppendColumn("bid_cancel_amount", bid_cancel_amount);
    block.AppendColumn("ask_order_volume", ask_order_volume);
    block.AppendColumn("ask_order_amount", ask_order_amount);
    block.AppendColumn("ask_cancel_volume", ask_cancel_volume);
    block.AppendColumn("ask_cancel_amount", ask_cancel_amount);
    block.AppendColumn("new_knock_count", new_knock_count);
    block.AppendColumn("sum_knock_count", sum_knock_count);

    client_->Insert("QTick", block);
    gettimeofday(&end, NULL);
    time_use = (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec); //微秒
    printf("time_use is %.10f\n", time_use);
}

void MyClickHouse::ReadClickHouse(string date, string code) {   
    string sql = "";
    if(code.length() == 9) {
        sql = "SELECT * FROM QTick where date = '" + date + "' and code = '" + code + "';";
    } else {
        sql = "SELECT * FROM QTick where date = '" + date + "'";
    }
    LOG_INFO << sql;
    
    long time_use = 0;
    struct timeval start;
    struct timeval end;
    gettimeofday(&start, NULL);
    
    long Total_num = 0;
    flatbuffers::FlatBufferBuilder builder;     

    client_->Select(sql.c_str(), [&](const Block & block) { 
        for (size_t i = 0; i < block.GetRowCount(); ++i) {     
              Total_num++;
            builder.Clear();            
            char code[16] = "";
            char name[16] = "";
            char underlying_code[16] = "";
            int Index = 0;
            for (char ch : (*block[6]->As<ColumnString>())[i])	
                    code[Index++] = ch;
            code[Index] = '\0';
            
            Index = 0;
            for (char ch : (*block[7]->As<ColumnString>())[i])	
                    name[Index++] = ch;
            name[Index] = '\0';
            
            Index = 0;
            for (char ch : (*block[82]->As<ColumnString>())[i])	
                    underlying_code[Index++] = ch;
            underlying_code[Index] = '\0';
      
            flatbuffers::Offset<flatbuffers::String> content_code;
            content_code = builder.CreateString(code, strlen(code) + 1);
   
            flatbuffers::Offset<flatbuffers::String> content_name;
            content_name = builder.CreateString(name, strlen(name) + 1);
            
            flatbuffers::Offset<flatbuffers::String> content_underlying_code;
            content_underlying_code = builder.CreateString(underlying_code, strlen(underlying_code) + 1);
            
            //long timestamp = (*block[5]->As<ColumnInt64>())[i];   

//            cout << "bp:"
//                << (*block[12]->As<ColumnInt64>())[i] << " "
//                << (*block[13]->As<ColumnInt64>())[i] << " "
//                << (*block[14]->As<ColumnInt64>())[i] << " "
//                << (*block[15]->As<ColumnInt64>())[i] << " "
//                << (*block[16]->As<ColumnInt64>())[i] << " "
//                << (*block[17]->As<ColumnInt64>())[i] << " "
//                << (*block[18]->As<ColumnInt64>())[i] << " "
//                << (*block[19]->As<ColumnInt64>())[i] << " "
//                << (*block[20]->As<ColumnInt64>())[i] << " "
//                << (*block[21]->As<ColumnInt64>())[i] << " "
//                << "ap:"
//                << (*block[32]->As<ColumnInt64>())[i] << " "
//                << (*block[33]->As<ColumnInt64>())[i] << " "
//                << (*block[34]->As<ColumnInt64>())[i] << " "
//                << (*block[35]->As<ColumnInt64>())[i] << " "
//                << (*block[36]->As<ColumnInt64>())[i] << " "
//                << (*block[37]->As<ColumnInt64>())[i] << " "
//                << (*block[38]->As<ColumnInt64>())[i] << " "
//                << (*block[39]->As<ColumnInt64>())[i] << " "
//                << (*block[40]->As<ColumnInt64>())[i] << " "
//                << (*block[41]->As<ColumnInt64>())[i] << " "
//                << endl;
                        
            int length = 10;
            double bp_value[length];
            for (size_t j = 0; j < length; ++j) {                
                bp_value[j] = (*block[12 + j]->As<ColumnInt64>())[i]/ 10000.0;
            }
            auto bp_vec = builder.CreateVector(bp_value, length);           
            
  
            int64_t bv_value[length];
            for (size_t j = 0; j < length; ++j) {
                bv_value[j] = (*block[22 + j]->As<ColumnInt64>())[i];
            }    
            auto bv_vec = builder.CreateVector(bv_value, length);            
                        
     
            double ap_value[length];
            for (size_t j = 0; j < length; ++j) {
                ap_value[j] = (*block[32 + j]->As<ColumnInt64>())[i] / 10000.0;
            } 
            auto ap_vec = builder.CreateVector(ap_value, length);        
     
            int64_t av_value[length];
            for (size_t j = 0; j < length; ++j) {  
                av_value[j] = (*block[42 + j]->As<ColumnInt64>())[i];
            }    
            auto av_vec = builder.CreateVector(av_value, length);
   
            QTickBuilder msgData(builder);
            msgData.add_src((*block[3]->As<ColumnInt8>())[i]);
            msgData.add_dtype((*block[4]->As<ColumnInt8>())[i]);
            msgData.add_timestamp((*block[5]->As<ColumnInt64>())[i]);
            msgData.add_code(content_code);
            msgData.add_name(content_name);  
            msgData.add_market((*block[8]->As<ColumnInt8>())[i]);
            msgData.add_pre_close((*block[9]->As<ColumnInt64>())[i]);
            msgData.add_upper_limit((*block[10]->As<ColumnInt64>())[i]);
            msgData.add_lower_limit((*block[11]->As<ColumnInt64>())[i]);     
                   
            
            
            msgData.add_bp(bp_vec);    
            msgData.add_bv(bv_vec);
            msgData.add_bp(ap_vec);
            msgData.add_bv(av_vec);        

            int offset = 36;
            msgData.add_status((*block[16 + offset]->As<ColumnInt8>())[i]);
            msgData.add_new_price((*block[17 + offset]->As<ColumnInt64>())[i]);
            msgData.add_new_volume((*block[18 + offset]->As<ColumnInt64>())[i]);
            msgData.add_new_amount((*block[19 + offset]->As<ColumnFloat64>())[i]);
            msgData.add_sum_volume((*block[20 + offset]->As<ColumnInt64>())[i]);
            msgData.add_sum_amount((*block[21 + offset]->As<ColumnFloat64>())[i]);
            msgData.add_open((*block[22 + offset]->As<ColumnInt64>())[i]);
            msgData.add_high((*block[23 + offset]->As<ColumnInt64>())[i]);
            msgData.add_low((*block[24 + offset]->As<ColumnInt64>())[i]);
            msgData.add_avg_bid_price((*block[25 + offset]->As<ColumnInt64>())[i]);
            msgData.add_avg_ask_price((*block[26 + offset]->As<ColumnInt64>())[i]);
            msgData.add_new_bid_volume((*block[27 + offset]->As<ColumnInt64>())[i]);
            msgData.add_new_bid_amount((*block[28 + offset]->As<ColumnFloat64>())[i]);
            msgData.add_new_ask_volume((*block[29 + offset]->As<ColumnInt64>())[i]);
            msgData.add_new_ask_amount((*block[30 + offset]->As<ColumnFloat64>())[i]);
            msgData.add_open_interest((*block[31 + offset]->As<ColumnInt64>())[i]);
            msgData.add_pre_settle((*block[32 + offset]->As<ColumnInt64>())[i]);
            msgData.add_pre_open_interest((*block[33 + offset]->As<ColumnInt64>())[i]);
            msgData.add_close((*block[34 + offset]->As<ColumnInt64>())[i]);
            msgData.add_settle((*block[35 + offset]->As<ColumnInt64>())[i]);
            msgData.add_multiple((*block[36 + offset]->As<ColumnInt64>())[i]);
            msgData.add_price_step((*block[37 + offset]->As<ColumnInt64>())[i]);
            msgData.add_create_date((*block[38 + offset]->As<ColumnInt32>())[i]);
            msgData.add_list_date((*block[39 + offset]->As<ColumnInt32>())[i]);
            msgData.add_expire_date((*block[40 + offset]->As<ColumnInt32>())[i]);
            msgData.add_start_settle_date((*block[41 + offset]->As<ColumnInt32>())[i]);
            msgData.add_end_settle_date((*block[42 + offset]->As<ColumnInt32>())[i]);
            msgData.add_exercise_date((*block[43 + offset]->As<ColumnInt32>())[i]);
            msgData.add_exercise_price((*block[44 + offset]->As<ColumnInt64>())[i]);
            msgData.add_cp_flag((*block[45 + offset]->As<ColumnInt8>())[i]);
            msgData.add_underlying_code(content_underlying_code);
            msgData.add_sum_bid_volume((*block[47 + offset]->As<ColumnInt64>())[i]);
            msgData.add_sum_bid_amount((*block[48 + offset]->As<ColumnFloat64>())[i]);
            msgData.add_sum_ask_volume((*block[49 + offset]->As<ColumnInt64>())[i]);
            msgData.add_sum_ask_amount((*block[50 + offset]->As<ColumnFloat64>())[i]);
            msgData.add_bid_order_volume((*block[51 + offset]->As<ColumnInt64>())[i]);
            msgData.add_bid_order_amount((*block[52 + offset]->As<ColumnFloat64>())[i]);
            msgData.add_bid_cancel_volume((*block[53 + offset]->As<ColumnInt64>())[i]);
            msgData.add_bid_cancel_amount((*block[54 + offset]->As<ColumnFloat64>())[i]);
            msgData.add_ask_order_volume((*block[55 + offset]->As<ColumnInt64>())[i]);
            msgData.add_ask_order_amount((*block[56 + offset]->As<ColumnFloat64>())[i]);
            msgData.add_ask_cancel_volume((*block[57 + offset]->As<ColumnInt64>())[i]);
            msgData.add_ask_cancel_amount((*block[58 + offset]->As<ColumnFloat64>())[i]);
            msgData.add_new_knock_count((*block[59 + offset]->As<ColumnInt64>())[i]);
            msgData.add_sum_knock_count((*block[60 + offset]->As<ColumnInt64>())[i]);

            
//            flatbuffers::Offset<QTick> tickData;
//            tickData = msgData.Finish();
//            builder.Finish(tickData);
          

            if(Total_num % 100000 == 0) {
                struct timeval pre_end = end;
                gettimeofday(&end, NULL);
                time_use = (end.tv_sec - pre_end.tv_sec)*1000000 + (end.tv_usec - pre_end.tv_usec); //微秒
                LOG_INFO << "read clickhouse time: " << time_use << " Index: " << Total_num << "  " << (*block[5]->As<ColumnInt64>())[i];
            }
        }
    }
    );
    gettimeofday(&end, NULL);
    time_use = (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec); //微秒
    LOG_INFO << "last read clickhouse time: " << time_use << "(us), total num: " << Total_num;
    
    //// Delete table.
    //client_->Execute("DROP TABLE QTick");
}

void MyClickHouse::GenericExample() {
client_->Execute("CREATE TABLE IF NOT EXISTS test.client (id UInt64, name String, date Date, datetime DateTime) ENGINE = Memory");
    {
        Block block;
        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        id->Append(7);
        auto name = std::make_shared<ColumnString>();
        name->Append("one");
        name->Append("sev");        
        block.AppendColumn("id"  , id);
        block.AppendColumn("name", name);
        std::cout << std::time(nullptr) << " std::time(nullptr)" << endl;
        long _time123 = std::time(nullptr);
        {
            auto date = std::make_shared<ColumnDate>();            
            date->Append(1586967567);   //1546300800
            date->Append(1586967567);
            block.AppendColumn("date", date);
        }            
        {
            auto datetime = std::make_shared<ColumnDateTime>();
            datetime->Append(1586967567);
            datetime->Append(1586967567);
            block.AppendColumn("datetime", datetime);
        }
        client_->Insert("test.client", block);
    }

client_->Select("SELECT * FROM test.client", [](const Block& block)
        {
            //PrintBlock(block);
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                std::cout << (*block[0]->As<ColumnUInt64>())[i] << " "
                        << (*block[1]->As<ColumnString>())[i] <<  " "
                        << block[2]->As<ColumnDate>()->At(i) << " "  
                        << block[3]->As<ColumnDateTime>()->At(i) << " "      
                                "\n";
                  
                std::time_t t = block[3]->As<ColumnDateTime>()->At(i);
                std::cerr << std::asctime(std::localtime(&t)) << " " << std::endl;
            }     
        }
    );

    /// Delete table.
    //client_->Execute("DROP TABLE test.client");
}

void MyClickHouse::ReadPBQTick(string file) {
    cout << "read file: " << file << endl;
    list_pb.clear();
    ifstream infile; 
    infile.open(file.data()); 
    assert(infile.is_open());
    string line;
    while(getline(infile,line)) {    
        if(HandleLine(line)) {
            list_pb.push_back(move(line));
            if(list_pb.size() > 100000) {
                InsertPBQTick();
            }
        }
    }
    InsertPBQTick();
    infile.close();    
    cout << "list_pb " << list_pb.size() << endl;
}

void MyClickHouse::InsertPBQTick() {
    client_->Execute("CREATE TABLE IF NOT EXISTS QTick (date Date, datetime DateTime, stamp Int64, src Int8, dtype Int8, timestamp Int64, code String, name String, market Int8, pre_close Float64, upper_limit Float64, lower_limit Float64, bp Array(Float64), bv Array(Int64), ap Array(Float64), av Array(Int64), status Int8, new_price Float64, new_volume Int64, new_amount Float64, sum_volume Int64, sum_amount Float64, open Float64, high Float64, low Float64, avg_bid_price Float64, avg_ask_price Float64, new_bid_volume Int64, new_bid_amount Float64, new_ask_volume Int64, new_ask_amount Float64, open_interest Int64, pre_settle Float64, pre_open_interest Int64, close Float64, settle Float64, multiple Int64, price_step Float64, create_date Int32, list_date Int32, expire_date Int32, start_settle_date Int32, end_settle_date Int32, exercise_date Int32, exercise_price Float64, cp_flag Int8, underlying_code String, sum_bid_volume Int64, sum_bid_amount Float64, sum_ask_volume Int64, sum_ask_amount Float64, bid_order_volume Int64, bid_order_amount Float64, bid_cancel_volume Int64, bid_cancel_amount Float64, ask_order_volume Int64, ask_order_amount Float64, ask_cancel_volume Int64, ask_cancel_amount Float64, new_knock_count Int64, sum_knock_count Int64) ENGINE = MergeTree ORDER BY(timestamp, code, name) PARTITION BY (date)");
    long time_use = 0;
    struct timeval start;
    struct timeval end;
    gettimeofday(&start, NULL);

    Block block;
    
    auto date = std::make_shared<ColumnDate>();
    auto datetime = std::make_shared<ColumnDateTime>();
    auto stamp = std::make_shared<ColumnInt64>();
    
    auto src = std::make_shared<ColumnInt8>();
    auto dtype = std::make_shared<ColumnInt8>();
    auto timestamp = std::make_shared<ColumnInt64>();
    auto code = std::make_shared<ColumnString>();
    auto name = std::make_shared<ColumnString>();
    auto market = std::make_shared<ColumnInt8>();
    auto pre_close = std::make_shared<ColumnFloat64>();
    auto upper_limit = std::make_shared<ColumnFloat64>();
    auto lower_limit = std::make_shared<ColumnFloat64>();
    
    auto bp = std::make_shared<ColumnArray>(std::make_shared<ColumnFloat64>()); 
    auto bp_price = std::make_shared<ColumnFloat64>();
    auto bv = std::make_shared<ColumnArray>(std::make_shared<ColumnInt64>());   
    auto bv_volume = std::make_shared<ColumnInt64>();
    auto ap = std::make_shared<ColumnArray>(std::make_shared<ColumnFloat64>());
    auto ap_price = std::make_shared<ColumnFloat64>();    
    auto av = std::make_shared<ColumnArray>(std::make_shared<ColumnInt64>());    
    auto av_volume = std::make_shared<ColumnInt64>();    
   
    auto status = std::make_shared<ColumnInt8>();
    auto new_price = std::make_shared<ColumnFloat64>();
    auto new_volume = std::make_shared<ColumnInt64>();
    auto new_amount = std::make_shared<ColumnFloat64>();
    auto sum_volume = std::make_shared<ColumnInt64>();
    auto sum_amount = std::make_shared<ColumnFloat64>();
    auto open = std::make_shared<ColumnFloat64>();
    auto high = std::make_shared<ColumnFloat64>();
    auto low = std::make_shared<ColumnFloat64>();
    auto avg_bid_price = std::make_shared<ColumnFloat64>();
    auto avg_ask_price = std::make_shared<ColumnFloat64>();
    auto new_bid_volume = std::make_shared<ColumnInt64>();
    auto new_bid_amount = std::make_shared<ColumnFloat64>();
    auto new_ask_volume = std::make_shared<ColumnInt64>();
    auto new_ask_amount = std::make_shared<ColumnFloat64>();
    auto open_interest = std::make_shared<ColumnInt64>();
    auto pre_settle = std::make_shared<ColumnFloat64>();
    auto pre_open_interest = std::make_shared<ColumnInt64>();
    auto close = std::make_shared<ColumnFloat64>();
    auto settle = std::make_shared<ColumnFloat64>();
    auto multiple = std::make_shared<ColumnInt64>();
    auto price_step = std::make_shared<ColumnFloat64>();
    auto create_date = std::make_shared<ColumnInt32>();
    auto list_date = std::make_shared<ColumnInt32>();
    auto expire_date = std::make_shared<ColumnInt32>();
    auto start_settle_date = std::make_shared<ColumnInt32>();
    auto end_settle_date = std::make_shared<ColumnInt32>();
    auto exercise_date = std::make_shared<ColumnInt32>();
    auto exercise_price = std::make_shared<ColumnFloat64>();
    auto cp_flag = std::make_shared<ColumnInt8>();
    auto underlying_code = std::make_shared<ColumnString>();
    auto sum_bid_volume = std::make_shared<ColumnInt64>();
    auto sum_bid_amount = std::make_shared<ColumnFloat64>();
    auto sum_ask_volume = std::make_shared<ColumnInt64>();
    auto sum_ask_amount = std::make_shared<ColumnFloat64>();
    auto bid_order_volume = std::make_shared<ColumnInt64>();
    auto bid_order_amount = std::make_shared<ColumnFloat64>();
    auto bid_cancel_volume = std::make_shared<ColumnInt64>();
    auto bid_cancel_amount = std::make_shared<ColumnFloat64>();
    auto ask_order_volume = std::make_shared<ColumnInt64>();
    auto ask_order_amount = std::make_shared<ColumnFloat64>();
    auto ask_cancel_volume = std::make_shared<ColumnInt64>();
    auto ask_cancel_amount = std::make_shared<ColumnFloat64>();
    auto new_knock_count = std::make_shared<ColumnInt64>();
    auto sum_knock_count = std::make_shared<ColumnInt64>();


    for (auto &itor : list_pb) {        
        auto temp = x::Base64Decode(itor);
        auto it = flatbuffers::GetRoot<QTick>(temp.data());        
        string _line;
        _line = x::Base64Decode(itor);

        int pos1 = (int)_line.find("\t");
        if (pos1 < 0) {
            continue;
        }
        int pos2 = (int)_line.find("\t", pos1 + 1);
        if (pos2 < 0) {
            continue;
        }
        string s_prefix = _line.substr(0, pos1);
        string raw = _line.substr(pos2 + 1);
        if (s_prefix == "S") {
            co::QStock it;
            if (!it.ParseFromString(raw)) {
                continue;
            }
        
//        if (it.code().length() > 0 && it.name().length() > 0) {
//            cout << it.code()<< " " << it.date() << " " << it.stamp() << " " << it.name() << " " << it.pre_close() << " "
//                << it.upper_limit() << " " << it.lower_limit() << " " << it.new_price() << " " << it.new_volume() << endl;
//        }
        int64_t stamp_value = it.date() * 1000000000LL + it.stamp() % 1000000000LL;
          
        date->Append(GetTime(stamp_value));
        datetime->Append(GetTime(stamp_value));
        stamp->Append(GetStamp(stamp_value));     
                
        src->Append(atoi(it.src().c_str()));
        dtype->Append(it.type());
        timestamp->Append(stamp_value);
        code->Append(it.code());
        name->Append(it.name());
        market->Append(it.market());
        pre_close->Append(it.pre_close());
        upper_limit->Append(it.upper_limit());
        lower_limit->Append(it.lower_limit());
        
        bp_price->Clear();
        if (it.bp1() > 0) {
                bp_price->Append(it.bp1());
                if (it.bp2() > 0) {
                    bp_price->Append(it.bp2());
                    if (it.bp3() > 0) {
                        bp_price->Append(it.bp3());
                        if (it.bp4() > 0) {
                            bp_price->Append(it.bp4());
                            if (it.bp5() > 0) {
                                bp_price->Append(it.bp5());
                                if (it.bp6() > 0) {
                                    bp_price->Append(it.bp6());
                                    if (it.bp7() > 0) {
                                        bp_price->Append(it.bp7());
                                        if (it.bp8() > 0) {
                                            bp_price->Append(it.bp8());
                                            if (it.bp9() > 0) {
                                                bp_price->Append(it.bp9());
                                                if (it.bp10() > 0) {
                                                    bp_price->Append(it.bp10());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }   
        bp->AppendAsColumn(bp_price);
        
        bv_volume->Clear();
        if (it.bv1() > 0) {
                bv_volume->Append(it.bv1());
                if (it.bv2() > 0) {
                    bv_volume->Append(it.bv2());
                    if (it.bv3() > 0) {
                        bv_volume->Append(it.bv3());
                        if (it.bv4() > 0) {
                            bv_volume->Append(it.bv4());
                            if (it.bv5() > 0) {
                                bv_volume->Append(it.bv5());
                                if (it.bv6() > 0) {
                                    bv_volume->Append(it.bv6());
                                    if (it.bv7() > 0) {
                                        bv_volume->Append(it.bv7());
                                        if (it.bv8() > 0) {
                                            bv_volume->Append(it.bv8());
                                            if (it.bv9() > 0) {
                                                bv_volume->Append(it.bv9());
                                                if (it.bv10() > 0) {
                                                    bv_volume->Append(it.bv10());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }   
        bv->AppendAsColumn(bv_volume);
        
        ap_price->Clear();
        if (it.ap1() > 0) {
                ap_price->Append(it.ap1());
                if (it.ap2() > 0) {
                    ap_price->Append(it.ap2());
                    if (it.ap3() > 0) {
                        ap_price->Append(it.ap3());
                        if (it.ap4() > 0) {
                            ap_price->Append(it.ap4());
                            if (it.ap5() > 0) {
                                ap_price->Append(it.ap5());
                                if (it.ap6() > 0) {
                                    ap_price->Append(it.ap6());
                                    if (it.ap7() > 0) {
                                        ap_price->Append(it.ap7());
                                        if (it.ap8() > 0) {
                                            ap_price->Append(it.ap8());
                                            if (it.ap9() > 0) {
                                                ap_price->Append(it.ap9());
                                                if (it.ap10() > 0) {
                                                    ap_price->Append(it.ap10());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }   
        ap->AppendAsColumn(ap_price); 
        
        av_volume->Clear();
        if (it.av1() > 0) {
                av_volume->Append(it.av1());
                if (it.av2() > 0) {
                    av_volume->Append(it.av2());
                    if (it.av3() > 0) {
                        av_volume->Append(it.av3());
                        if (it.av4() > 0) {
                            av_volume->Append(it.av4());
                            if (it.av5() > 0) {
                                av_volume->Append(it.av5());
                                if (it.av6() > 0) {
                                    av_volume->Append(it.av6());
                                    if (it.av7() > 0) {
                                        av_volume->Append(it.av7());
                                        if (it.av8() > 0) {
                                            av_volume->Append(it.av8());
                                            if (it.av9() > 0) {
                                                av_volume->Append(it.av9());
                                                if (it.av10() > 0) {
                                                    av_volume->Append(it.av10());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }   
        av->AppendAsColumn(av_volume);
        
        status->Append(it.status());
        new_price->Append(it.new_price());
        new_volume->Append(it.new_volume());
        new_amount->Append(it.new_amount());
        sum_volume->Append(it.sum_volume());
        sum_amount->Append(it.sum_amount());
        open->Append(it.open());
        high->Append(it.high());
        low->Append(it.low());
        avg_bid_price->Append(it.weighted_avg_bid_price());
        avg_ask_price->Append(it.weighted_avg_ask_price());
        new_bid_volume->Append(it.new_bid_volume());
        new_bid_amount->Append(it.new_bid_amount());
        new_ask_volume->Append(it.new_ask_volume());
        new_ask_amount->Append(it.new_ask_amount());
        open_interest->Append(0);
        pre_settle->Append(0);
        pre_open_interest->Append(0);
        close->Append(0);
        settle->Append(0);
        multiple->Append(0);
        price_step->Append(0);
        create_date->Append(0);
        list_date->Append(0);
        expire_date->Append(0);
        start_settle_date->Append(0);
        end_settle_date->Append(0);
        exercise_date->Append(0);
        exercise_price->Append(0);
        cp_flag->Append(0);
        underlying_code->Append("");
        sum_bid_volume->Append(it.sum_bid_volume());
        sum_bid_amount->Append(it.sum_bid_amount());
        sum_ask_volume->Append(it.sum_ask_volume());
        sum_ask_amount->Append(it.sum_amount());
        bid_order_volume->Append(it.bid_order_volume());
        bid_order_amount->Append(it.bid_order_amount());
        bid_cancel_volume->Append(it.bid_cancel_volume());
        bid_cancel_amount->Append(it.bid_cancel_amount());
        ask_order_volume->Append(it.ask_order_volume());
        ask_order_amount->Append(it.ask_order_amount());
        ask_cancel_volume->Append(it.ask_cancel_volume());
        ask_cancel_amount->Append(it.ask_cancel_amount());
        new_knock_count->Append(it.new_knock_count());
        sum_knock_count->Append(it.sum_knock_count());
        }
    }
    
    block.AppendColumn("date", date);
    block.AppendColumn("datetime", datetime);
    block.AppendColumn("stamp", stamp);
    
    block.AppendColumn("src", src);
    block.AppendColumn("dtype", dtype);
    block.AppendColumn("timestamp", timestamp);
    block.AppendColumn("code", code);
    block.AppendColumn("name", name);
    block.AppendColumn("market", market);
    block.AppendColumn("pre_close", pre_close);
    block.AppendColumn("upper_limit", upper_limit);
    block.AppendColumn("lower_limit", lower_limit);
    
    block.AppendColumn("bp", bp);
    block.AppendColumn("bv", bv);
    block.AppendColumn("ap", ap);
    block.AppendColumn("av", av);
    
    block.AppendColumn("status", status);
    block.AppendColumn("new_price", new_price);
    block.AppendColumn("new_volume", new_volume);
    block.AppendColumn("new_amount", new_amount);
    block.AppendColumn("sum_volume", sum_volume);
    block.AppendColumn("sum_amount", sum_amount);
    block.AppendColumn("open", open);
    block.AppendColumn("high", high);
    block.AppendColumn("low", low);
    block.AppendColumn("avg_bid_price", avg_bid_price);
    block.AppendColumn("avg_ask_price", avg_ask_price);
    block.AppendColumn("new_bid_volume", new_bid_volume);
    block.AppendColumn("new_bid_amount", new_bid_amount);
    block.AppendColumn("new_ask_volume", new_ask_volume);
    block.AppendColumn("new_ask_amount", new_ask_amount);
    block.AppendColumn("open_interest", open_interest);
    block.AppendColumn("pre_settle", pre_settle);
    block.AppendColumn("pre_open_interest", pre_open_interest);
    block.AppendColumn("close", close);
    block.AppendColumn("settle", settle);
    block.AppendColumn("multiple", multiple);
    block.AppendColumn("price_step", price_step);
    block.AppendColumn("create_date", create_date);
    block.AppendColumn("list_date", list_date);
    block.AppendColumn("expire_date", expire_date);
    block.AppendColumn("start_settle_date", start_settle_date);
    block.AppendColumn("end_settle_date", end_settle_date);
    block.AppendColumn("exercise_date", exercise_date);
    block.AppendColumn("exercise_price", exercise_price);
    block.AppendColumn("cp_flag", cp_flag);
    block.AppendColumn("underlying_code", underlying_code);
    block.AppendColumn("sum_bid_volume", sum_bid_volume);
    block.AppendColumn("sum_bid_amount", sum_bid_amount);
    block.AppendColumn("sum_ask_volume", sum_ask_volume);
    block.AppendColumn("sum_ask_amount", sum_ask_amount);
    block.AppendColumn("bid_order_volume", bid_order_volume);
    block.AppendColumn("bid_order_amount", bid_order_amount);
    block.AppendColumn("bid_cancel_volume", bid_cancel_volume);
    block.AppendColumn("bid_cancel_amount", bid_cancel_amount);
    block.AppendColumn("ask_order_volume", ask_order_volume);
    block.AppendColumn("ask_order_amount", ask_order_amount);
    block.AppendColumn("ask_cancel_volume", ask_cancel_volume);
    block.AppendColumn("ask_cancel_amount", ask_cancel_amount);
    block.AppendColumn("new_knock_count", new_knock_count);
    block.AppendColumn("sum_knock_count", sum_knock_count);

    client_->Insert("QTick", block);
    gettimeofday(&end, NULL);
    time_use = (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec); //微秒
    printf("list_pb size <%d> time_use is <%ld>\n", list_pb.size(), time_use);
    list_pb.clear();
}

bool MyClickHouse::HandleLine(const string& line) {
    string _line;
    try {
        _line = x::Base64Decode(line);
    } catch (std::exception& e) {
        /*x::write_file("error/error.b64", line);
        throw e;*/
        return false;
    }
    int pos1 = (int) _line.find("\t");
    if (pos1 < 0) {
        return false;
        throw runtime_error("illegal line: b64_line = " + line);
    }
    int pos2 = (int) _line.find("\t", pos1 + 1);
    if (pos2 < 0) {
        return false;
        throw runtime_error("illegal line: b64_line = " + line);
    }
    string s_prefix = _line.substr(0, pos1);
    string raw = _line.substr(pos2 + 1);
    if (s_prefix == "S") {
        if (true) {
            co::QStock m;
            if (!m.ParseFromString(raw)) {
                return false;
                throw runtime_error("parse pb failed: b64_line = " + line);
            }
            return true;
            string code = m.code();
            string name = m.name();
            if (m.code().length() > 0 && m.name().length() > 0) {
                cout << m.code() << " " << m.date() << " " << m.stamp() << " " << m.name() << " " << m.pre_close() << " "
                        << m.upper_limit() << " " << m.lower_limit() << " " << m.new_price() << " " << m.new_volume() << endl;
            }
            
        }
    }
    return false;
}

void MyClickHouse::GetFileNum(const string& dirname, const string& date) {
    struct dirent *ptr;
    DIR *dir;
    dir = opendir(dirname.c_str());
    int begin_date = atoi(date.c_str());
    while ((ptr = readdir(dir)) != NULL) {
        //printf("%s\n", ptr->d_name);
        string file_name = string(ptr->d_name);
        string::size_type position;
        position = file_name.find("co_stock_");
        if (position != file_name.npos && file_name.length() == 17)  {
            string temp = file_name.substr(9);            
            if (atoi(temp.c_str()) >= begin_date) {
                string full_name = dirname + "/" + file_name;                
                //files_.push_back(std::move(full_name));
                files_[atoi(temp.c_str())] = full_name;
            }            
        }
    }
    closedir(dir);
}

void MyClickHouse::ReadAllPBQTick() {
    for(auto it = files_.begin(); it != files_.end(); ++it) {
        printf("%s\n", it->second.c_str());
    }
    
    for(auto it = files_.begin(); it != files_.end(); ++it) {
        printf("begin %s\n", it->second.c_str());
        ReadStructTick(it->second);
    }   
}

void MyClickHouse::DeleteTable() {
    for (int month = 1; month <= 12; month++) {
        for (int day = 1; day <= 31; day++) {
            char Temp[256] = "";
            sprintf(Temp, "alter table QTick DROP PARTITION '2019-%02d-%02d';", month, day);
            printf("%s\n", Temp);    
            client_->Execute(Temp);
            sleep(1);
        }        
    }    
}

void MyClickHouse::ConvertStruct() {
    for (auto &itor : list_pb) {        
        auto temp = x::Base64Decode(itor);
        auto it = flatbuffers::GetRoot<QTick>(temp.data());        
        string _line;
        _line = x::Base64Decode(itor);

        int pos1 = (int)_line.find("\t");
        if (pos1 < 0) {
            continue;
        }
        int pos2 = (int)_line.find("\t", pos1 + 1);
        if (pos2 < 0) {
            continue;
        }
        string s_prefix = _line.substr(0, pos1);
        string raw = _line.substr(pos2 + 1);
        if (s_prefix == "S") {
            co::QStock it;
            if (!it.ParseFromString(raw)) {
                continue;
            }
        
//        if (it.code().length() > 0 && it.name().length() > 0) {
//            cout << it.code()<< " " << it.date() << " " << it.stamp() << " " << it.name() << " " << it.pre_close() << " "
//                << it.upper_limit() << " " << it.lower_limit() << " " << it.new_price() << " " << it.new_volume() << endl;
//        }
            int64_t stamp_value = it.date() * 1000000000LL + it.stamp() % 1000000000LL;
            QTickT tick;
            memset(&tick, 0, sizeof(tick));
            tick.src = atoi(it.src().c_str());
            tick.dtype = it.type();
            tick.timestamp = stamp_value;
            strcpy(tick.code, it.code().c_str());
            strcpy(tick.name, it.name().c_str());
            tick.market = it.market();
            tick.pre_close = it.pre_close();
            tick.upper_limit = it.upper_limit();
            tick.lower_limit = it.lower_limit();
            
            if (it.bp1() > 0) {
                tick.bp1 = it.bp1();
                if (it.bp2() > 0) {
                    tick.bp2 = it.bp2();
                    if (it.bp3() > 0) {
                        tick.bp3 = it.bp3();
                        if (it.bp4() > 0) {
                            tick.bp4 = it.bp4();
                            if (it.bp5() > 0) {
                                tick.bp5 = it.bp5();
                                if (it.bp6() > 0) {
                                   tick.bp6 = it.bp6();
                                    if (it.bp7() > 0) {
                                        tick.bp7 = it.bp7();
                                        if (it.bp8() > 0) {
                                            tick.bp8= it.bp8();
                                            if (it.bp9() > 0) {
                                                tick.bp9 = it.bp9();
                                                if (it.bp10() > 0) {
                                                    tick.bp10 = it.bp10();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }  
            
            if (it.bv1() > 0) {
                tick.bv1 = it.bv1();
                if (it.bv2() > 0) {
                    tick.bv2 = it.bv2();
                    if (it.bv3() > 0) {
                        tick.bv3 = it.bv3();
                        if (it.bv4() > 0) {
                            tick.bv4 = it.bv4();
                            if (it.bv5() > 0) {
                                tick.bv5 = it.bv5();
                                if (it.bv6() > 0) {
                                   tick.bv6 = it.bv6();
                                    if (it.bv7() > 0) {
                                        tick.bv7 = it.bv7();
                                        if (it.bv8() > 0) {
                                            tick.bv8 = it.bv8();
                                            if (it.bv9() > 0) {
                                                tick.bv9 = it.bv9();
                                                if (it.bv10() > 0) {
                                                    tick.bv10 = it.bv10();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (it.ap1() > 0) {
                tick.ap1 = it.ap1();
                if (it.ap2() > 0) {
                    tick.ap2 = it.ap2();
                    if (it.ap3() > 0) {
                        tick.ap3 = it.ap3();
                        if (it.ap4() > 0) {
                            tick.ap4 = it.ap4();
                            if (it.ap5() > 0) {
                                tick.ap5 = it.ap5();
                                if (it.ap6() > 0) {
                                   tick.ap6 = it.ap6();
                                    if (it.ap7() > 0) {
                                        tick.ap7 = it.ap7();
                                        if (it.ap8() > 0) {
                                            tick.ap8 = it.ap8();
                                            if (it.ap9() > 0) {
                                                tick.ap9 = it.ap9();
                                                if (it.ap10() > 0) {
                                                    tick.ap10 = it.ap10();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }   
            
            if (it.av1() > 0) {
                tick.av1 = it.av1();
                if (it.av2() > 0) {
                    tick.av2 = it.av2();
                    if (it.av3() > 0) {
                        tick.av3 = it.av3();
                        if (it.av4() > 0) {
                            tick.av4 = it.av4();
                            if (it.av5() > 0) {
                                tick.av5 = it.av5();
                                if (it.av6() > 0) {
                                   tick.av6 = it.av6();
                                    if (it.av7() > 0) {
                                        tick.av7 = it.av7();
                                        if (it.av8() > 0) {
                                            tick.av8 = it.av8();
                                            if (it.av9() > 0) {
                                                tick.av9 = it.av9();
                                                if (it.av10() > 0) {
                                                    tick.av10 = it.av10();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            tick.status = it.status();
            tick.new_price = it.new_price();
            tick.new_volume = it.new_volume();
            tick.new_amount = it.new_amount();
            tick.sum_volume = it.sum_volume();
            tick.sum_amount = it.sum_amount();
            tick.open = it.open();
            tick.high = it.high();
            tick.low = it.low();
            tick.avg_bid_price = it.weighted_avg_bid_price();
            tick.avg_ask_price = it.weighted_avg_ask_price();
            tick.new_bid_volume = it.new_bid_volume();
            tick.new_bid_amount = it.new_bid_amount();
            tick.new_ask_volume = it.new_ask_volume();
            tick.new_ask_amount = it.new_ask_amount();
            tick.open_interest = 0;
            tick.pre_settle = 0;
            tick.pre_open_interest = 0;
            tick.close = 0;
            tick.settle = 0;
            tick.multiple = 0;
            tick.price_step = 0;
            tick.create_date = 0;
            tick.list_date = 0;
            tick.expire_date = 0;
            tick.start_settle_date = 0;
            tick.end_settle_date = 0;
            tick.exercise_date = 0;
            tick.exercise_price = 0;
            tick.cp_flag = 0;
            // tick.underlying_code = it.underlying_code();
            tick.sum_bid_volume = it.sum_bid_volume();
            tick.sum_bid_amount = it.sum_bid_amount();
            tick.sum_ask_volume = it.sum_ask_volume();
            tick.sum_ask_amount = it.sum_ask_amount();
            tick.bid_order_volume = it.bid_order_volume();
            tick.bid_order_amount = it.bid_order_amount();
            tick.bid_cancel_volume = it.bid_cancel_volume();
            tick.bid_cancel_amount = it.bid_cancel_amount();
            tick.ask_order_volume = it.ask_order_volume();
            tick.ask_order_amount = it.ask_order_amount();
            tick.ask_cancel_volume = it.ask_cancel_volume();
            tick.ask_cancel_amount = it.ask_cancel_amount();
            tick.new_knock_count = it.new_knock_count();
            tick.sum_knock_count = it.sum_knock_count();
            list_ticks_.push_back(tick);
        }
    }
    InsertStructQTick();
    list_pb.clear();
}

void MyClickHouse::InsertStructQTick() {
    client_->Execute("CREATE TABLE IF NOT EXISTS QTick (date Date, datetime DateTime, stamp Int64, src Int8, dtype Int8, timestamp Int64, code String, name String, market Int8, pre_close Int64, upper_limit Int64, lower_limit Int64, bp1 Int64, bp2 Int64, bp3 Int64, bp4 Int64, bp5 Int64, bp6 Int64, bp7 Int64, bp8 Int64, bp9 Int64, bp10 Int64, bv1 Int64, bv2 Int64, bv3 Int64, bv4 Int64, bv5 Int64, bv6 Int64, bv7 Int64, bv8 Int64, bv9 Int64, bv10 Int64, ap1 Int64, ap2 Int64, ap3 Int64, ap4 Int64, ap5 Int64, ap6 Int64, ap7 Int64, ap8 Int64, ap9 Int64, ap10 Int64, av1 Int64, av2 Int64, av3 Int64, av4 Int64, av5 Int64, av6 Int64, av7 Int64, av8 Int64, av9 Int64, av10 Int64,status Int8, new_price Int64, new_volume Int64, new_amount Float64, sum_volume Int64, sum_amount Float64, open Int64, high Int64, low Int64, avg_bid_price Int64, avg_ask_price Int64, new_bid_volume Int64, new_bid_amount Float64, new_ask_volume Int64, new_ask_amount Float64, open_interest Int64, pre_settle Int64, pre_open_interest Int64, close Int64, settle Int64, multiple Int64, price_step Int64, create_date Int32, list_date Int32, expire_date Int32, start_settle_date Int32, end_settle_date Int32, exercise_date Int32, exercise_price Int64, cp_flag Int8, underlying_code String, sum_bid_volume Int64, sum_bid_amount Float64, sum_ask_volume Int64, sum_ask_amount Float64, bid_order_volume Int64, bid_order_amount Float64, bid_cancel_volume Int64, bid_cancel_amount Float64, ask_order_volume Int64, ask_order_amount Float64, ask_cancel_volume Int64, ask_cancel_amount Float64, new_knock_count Int64, sum_knock_count Int64) ENGINE = MergeTree PARTITION BY date ORDER BY code SETTINGS index_granularity = 8192");
    long time_use = 0;
    struct timeval start;
    struct timeval mid;
    struct timeval end;
    gettimeofday(&start, NULL);

    Block block;
    
    auto date = std::make_shared<ColumnDate>();
    auto datetime = std::make_shared<ColumnDateTime>();
    auto stamp = std::make_shared<ColumnInt64>();
    
    auto src = std::make_shared<ColumnInt8>();
    auto dtype = std::make_shared<ColumnInt8>();
    auto timestamp = std::make_shared<ColumnInt64>();
    auto code = std::make_shared<ColumnString>();
    auto name = std::make_shared<ColumnString>();
    auto market = std::make_shared<ColumnInt8>();
    auto pre_close = std::make_shared<ColumnInt64>();
    auto upper_limit = std::make_shared<ColumnInt64>();
    auto lower_limit = std::make_shared<ColumnInt64>();
    
    auto bp1 = std::make_shared<ColumnInt64>();
    auto bp2 = std::make_shared<ColumnInt64>();
    auto bp3 = std::make_shared<ColumnInt64>();
    auto bp4 = std::make_shared<ColumnInt64>();
    auto bp5 = std::make_shared<ColumnInt64>();
    auto bp6 = std::make_shared<ColumnInt64>();
    auto bp7 = std::make_shared<ColumnInt64>();
    auto bp8 = std::make_shared<ColumnInt64>();
    auto bp9 = std::make_shared<ColumnInt64>();
    auto bp10 = std::make_shared<ColumnInt64>();
    
    auto bv1 = std::make_shared<ColumnInt64>();
    auto bv2 = std::make_shared<ColumnInt64>();
    auto bv3 = std::make_shared<ColumnInt64>();
    auto bv4 = std::make_shared<ColumnInt64>();
    auto bv5 = std::make_shared<ColumnInt64>();
    auto bv6 = std::make_shared<ColumnInt64>();
    auto bv7 = std::make_shared<ColumnInt64>();
    auto bv8 = std::make_shared<ColumnInt64>();
    auto bv9 = std::make_shared<ColumnInt64>();
    auto bv10 = std::make_shared<ColumnInt64>();
    
    auto ap1 = std::make_shared<ColumnInt64>();
    auto ap2 = std::make_shared<ColumnInt64>();
    auto ap3 = std::make_shared<ColumnInt64>();
    auto ap4 = std::make_shared<ColumnInt64>();
    auto ap5 = std::make_shared<ColumnInt64>();
    auto ap6 = std::make_shared<ColumnInt64>();
    auto ap7 = std::make_shared<ColumnInt64>();
    auto ap8 = std::make_shared<ColumnInt64>();
    auto ap9 = std::make_shared<ColumnInt64>();
    auto ap10 = std::make_shared<ColumnInt64>();
    
    auto av1 = std::make_shared<ColumnInt64>();
    auto av2 = std::make_shared<ColumnInt64>();
    auto av3 = std::make_shared<ColumnInt64>();
    auto av4 = std::make_shared<ColumnInt64>();
    auto av5 = std::make_shared<ColumnInt64>();
    auto av6 = std::make_shared<ColumnInt64>();
    auto av7 = std::make_shared<ColumnInt64>();
    auto av8 = std::make_shared<ColumnInt64>();
    auto av9 = std::make_shared<ColumnInt64>();
    auto av10 = std::make_shared<ColumnInt64>();   
   
    auto status = std::make_shared<ColumnInt8>();
    auto new_price = std::make_shared<ColumnInt64>();
    auto new_volume = std::make_shared<ColumnInt64>();
    auto new_amount = std::make_shared<ColumnFloat64>();
    auto sum_volume = std::make_shared<ColumnInt64>();
    auto sum_amount = std::make_shared<ColumnFloat64>();
    auto open = std::make_shared<ColumnInt64>();
    auto high = std::make_shared<ColumnInt64>();
    auto low = std::make_shared<ColumnInt64>();
    auto avg_bid_price = std::make_shared<ColumnInt64>();
    auto avg_ask_price = std::make_shared<ColumnInt64>();
    auto new_bid_volume = std::make_shared<ColumnInt64>();
    auto new_bid_amount = std::make_shared<ColumnFloat64>();
    auto new_ask_volume = std::make_shared<ColumnInt64>();
    auto new_ask_amount = std::make_shared<ColumnFloat64>();
    auto open_interest = std::make_shared<ColumnInt64>();
    auto pre_settle = std::make_shared<ColumnInt64>();
    auto pre_open_interest = std::make_shared<ColumnInt64>();
    auto close = std::make_shared<ColumnInt64>();
    auto settle = std::make_shared<ColumnInt64>();
    auto multiple = std::make_shared<ColumnInt64>();
    auto price_step = std::make_shared<ColumnInt64>();
    auto create_date = std::make_shared<ColumnInt32>();
    auto list_date = std::make_shared<ColumnInt32>();
    auto expire_date = std::make_shared<ColumnInt32>();
    auto start_settle_date = std::make_shared<ColumnInt32>();
    auto end_settle_date = std::make_shared<ColumnInt32>();
    auto exercise_date = std::make_shared<ColumnInt32>();
    auto exercise_price = std::make_shared<ColumnInt64>();
    auto cp_flag = std::make_shared<ColumnInt8>();
    auto underlying_code = std::make_shared<ColumnString>();
    auto sum_bid_volume = std::make_shared<ColumnInt64>();
    auto sum_bid_amount = std::make_shared<ColumnFloat64>();
    auto sum_ask_volume = std::make_shared<ColumnInt64>();
    auto sum_ask_amount = std::make_shared<ColumnFloat64>();
    auto bid_order_volume = std::make_shared<ColumnInt64>();
    auto bid_order_amount = std::make_shared<ColumnFloat64>();
    auto bid_cancel_volume = std::make_shared<ColumnInt64>();
    auto bid_cancel_amount = std::make_shared<ColumnFloat64>();
    auto ask_order_volume = std::make_shared<ColumnInt64>();
    auto ask_order_amount = std::make_shared<ColumnFloat64>();
    auto ask_cancel_volume = std::make_shared<ColumnInt64>();
    auto ask_cancel_amount = std::make_shared<ColumnFloat64>();
    auto new_knock_count = std::make_shared<ColumnInt64>();
    auto sum_knock_count = std::make_shared<ColumnInt64>();  


    for (auto& it : list_ticks_) {   
//         cout << it.code<< " " << it.timestamp << " " << it.code << " " << it.pre_close << " "
//              << it.upper_limit << " " << it.lower_limit << " " << it.new_price << " " << it.new_volume << endl;
                 
        int64_t stamp_value = it.timestamp;
          
        date->Append(GetTime(stamp_value));
        datetime->Append(GetTime(stamp_value));
        stamp->Append(GetStamp(stamp_value));     
                
        src->Append(it.src);
        dtype->Append(it.dtype);
        timestamp->Append(stamp_value);
        code->Append(it.code);
        name->Append(it.name);
        market->Append(it.market);
        pre_close->Append(it.pre_close);
        upper_limit->Append(it.upper_limit);
        lower_limit->Append(it.lower_limit);        
        
        bp1->Append(it.bp1);
        bp2->Append(it.bp2);
        bp3->Append(it.bp3);
        bp4->Append(it.bp4);
        bp5->Append(it.bp5);
        bp6->Append(it.bp6);
        bp7->Append(it.bp7);
        bp8->Append(it.bp8);
        bp9->Append(it.bp9);
        bp10->Append(it.bp10);    
        
        av1->Append(it.av1);
        av2->Append(it.av2);
        av3->Append(it.av3);
        av4->Append(it.av4);
        av5->Append(it.av5);
        av6->Append(it.av6);
        av7->Append(it.av7);
        av8->Append(it.av8);
        av9->Append(it.av9);
        av10->Append(it.av10);  
             
        ap1->Append(it.ap1);
        ap2->Append(it.ap2);
        ap3->Append(it.ap3);
        ap4->Append(it.ap4);
        ap5->Append(it.ap5);
        ap6->Append(it.ap6);
        ap7->Append(it.ap7);
        ap8->Append(it.ap8);
        ap9->Append(it.ap9);
        ap10->Append(it.ap10);         

        bv1->Append(it.bv1);
        bv2->Append(it.bv2);
        bv3->Append(it.bv3);
        bv4->Append(it.bv4);
        bv5->Append(it.bv5);
        bv6->Append(it.bv6);
        bv7->Append(it.bv7);
        bv8->Append(it.bv8);
        bv9->Append(it.bv9);
        bv10->Append(it.bv10);         
        
        status->Append(it.status);
        new_price->Append(it.new_price);
        new_volume->Append(it.new_volume);
        new_amount->Append(it.new_amount);
        sum_volume->Append(it.sum_volume);
        sum_amount->Append(it.sum_amount);
        open->Append(it.open);
        high->Append(it.high);
        low->Append(it.low);
        avg_bid_price->Append(it.avg_bid_price);
        avg_ask_price->Append(it.avg_ask_price);
        new_bid_volume->Append(it.new_bid_volume);
        new_bid_amount->Append(it.new_bid_amount);
        new_ask_volume->Append(it.new_ask_volume);
        new_ask_amount->Append(it.new_ask_amount);
        open_interest->Append(0);
        pre_settle->Append(0);
        pre_open_interest->Append(0);
        close->Append(0);
        settle->Append(0);
        multiple->Append(0);
        price_step->Append(0);
        create_date->Append(0);
        list_date->Append(0);
        expire_date->Append(0);
        start_settle_date->Append(0);
        end_settle_date->Append(0);
        exercise_date->Append(0);
        exercise_price->Append(0);
        cp_flag->Append(0);
        underlying_code->Append("");
        sum_bid_volume->Append(it.sum_bid_volume);
        sum_bid_amount->Append(it.sum_bid_amount);
        sum_ask_volume->Append(it.sum_ask_volume);
        sum_ask_amount->Append(it.sum_amount);
        bid_order_volume->Append(it.bid_order_volume);
        bid_order_amount->Append(it.bid_order_amount);
        bid_cancel_volume->Append(it.bid_cancel_volume);
        bid_cancel_amount->Append(it.bid_cancel_amount);
        ask_order_volume->Append(it.ask_order_volume);
        ask_order_amount->Append(it.ask_order_amount);
        ask_cancel_volume->Append(it.ask_cancel_volume);
        ask_cancel_amount->Append(it.ask_cancel_amount);
        new_knock_count->Append(it.new_knock_count);
        sum_knock_count->Append(it.sum_knock_count);
    }
    
    block.AppendColumn("date", date);
    block.AppendColumn("datetime", datetime);
    block.AppendColumn("stamp", stamp);
    
    block.AppendColumn("src", src);
    block.AppendColumn("dtype", dtype);
    block.AppendColumn("timestamp", timestamp);
    block.AppendColumn("code", code);
    block.AppendColumn("name", name);
    block.AppendColumn("market", market);
    block.AppendColumn("pre_close", pre_close);
    block.AppendColumn("upper_limit", upper_limit);
    block.AppendColumn("lower_limit", lower_limit);
    
    block.AppendColumn("bp1", bp1);
    block.AppendColumn("bp2", bp2);
    block.AppendColumn("bp3", bp3);
    block.AppendColumn("bp4", bp4);
    block.AppendColumn("bp5", bp5);
    block.AppendColumn("bp6", bp6);
    block.AppendColumn("bp7", bp7);
    block.AppendColumn("bp8", bp8);
    block.AppendColumn("bp9", bp9);
    block.AppendColumn("bp10", bp10);
    
    block.AppendColumn("bv1", bv1);
    block.AppendColumn("bv2", bv2);
    block.AppendColumn("bv3", bv3);
    block.AppendColumn("bv4", bv4);
    block.AppendColumn("bv5", bv5);
    block.AppendColumn("bv6", bv6);
    block.AppendColumn("bv7", bv7);
    block.AppendColumn("bv8", bv8);
    block.AppendColumn("bv9", bv9);
    block.AppendColumn("bv10", bv10);
    
    block.AppendColumn("ap1", ap1);
    block.AppendColumn("ap2", ap2);
    block.AppendColumn("ap3", ap3);
    block.AppendColumn("ap4", ap4);
    block.AppendColumn("ap5", ap5);
    block.AppendColumn("ap6", ap6);
    block.AppendColumn("ap7", ap7);
    block.AppendColumn("ap8", ap8);
    block.AppendColumn("ap9", ap9);
    block.AppendColumn("ap10", ap10);
    
    block.AppendColumn("av1", av1);
    block.AppendColumn("av2", av2);
    block.AppendColumn("av3", av3);
    block.AppendColumn("av4", av4);
    block.AppendColumn("av5", av5);
    block.AppendColumn("av6", av6);
    block.AppendColumn("av7", av7);
    block.AppendColumn("av8", av8);
    block.AppendColumn("av9", av9);
    block.AppendColumn("av10", av10);    
   
    block.AppendColumn("status", status);
    block.AppendColumn("new_price", new_price);
    block.AppendColumn("new_volume", new_volume);
    block.AppendColumn("new_amount", new_amount);
    block.AppendColumn("sum_volume", sum_volume);
    block.AppendColumn("sum_amount", sum_amount);
    block.AppendColumn("open", open);
    block.AppendColumn("high", high);
    block.AppendColumn("low", low);
    block.AppendColumn("avg_bid_price", avg_bid_price);
    block.AppendColumn("avg_ask_price", avg_ask_price);
    block.AppendColumn("new_bid_volume", new_bid_volume);
    block.AppendColumn("new_bid_amount", new_bid_amount);
    block.AppendColumn("new_ask_volume", new_ask_volume);
    block.AppendColumn("new_ask_amount", new_ask_amount);
    block.AppendColumn("open_interest", open_interest);
    block.AppendColumn("pre_settle", pre_settle);
    block.AppendColumn("pre_open_interest", pre_open_interest);
    block.AppendColumn("close", close);
    block.AppendColumn("settle", settle);
    block.AppendColumn("multiple", multiple);
    block.AppendColumn("price_step", price_step);
    block.AppendColumn("create_date", create_date);
    block.AppendColumn("list_date", list_date);
    block.AppendColumn("expire_date", expire_date);
    block.AppendColumn("start_settle_date", start_settle_date);
    block.AppendColumn("end_settle_date", end_settle_date);
    block.AppendColumn("exercise_date", exercise_date);
    block.AppendColumn("exercise_price", exercise_price);
    block.AppendColumn("cp_flag", cp_flag);
    block.AppendColumn("underlying_code", underlying_code);
    block.AppendColumn("sum_bid_volume", sum_bid_volume);
    block.AppendColumn("sum_bid_amount", sum_bid_amount);
    block.AppendColumn("sum_ask_volume", sum_ask_volume);
    block.AppendColumn("sum_ask_amount", sum_ask_amount);
    block.AppendColumn("bid_order_volume", bid_order_volume);
    block.AppendColumn("bid_order_amount", bid_order_amount);
    block.AppendColumn("bid_cancel_volume", bid_cancel_volume);
    block.AppendColumn("bid_cancel_amount", bid_cancel_amount);
    block.AppendColumn("ask_order_volume", ask_order_volume);
    block.AppendColumn("ask_order_amount", ask_order_amount);
    block.AppendColumn("ask_cancel_volume", ask_cancel_volume);
    block.AppendColumn("ask_cancel_amount", ask_cancel_amount);
    block.AppendColumn("new_knock_count", new_knock_count);
    block.AppendColumn("sum_knock_count", sum_knock_count);
    
    gettimeofday(&mid, NULL);
    client_->Insert("QTick", block);    
    gettimeofday(&end, NULL);    
    long insert_time_use = (end.tv_sec - mid.tv_sec)*1000000 + (end.tv_usec - mid.tv_usec); 
    //LOG_INFO << "only insert time use" << insert_time_use;
    time_use = (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec); //微秒
    LOG_INFO << "Inset ClickHouse, size: " << list_ticks_.size() << ", total time use:" << time_use
             << ", insert time use:" << insert_time_use << ", genetate struct time use:" << time_use - insert_time_use;
    list_ticks_.clear();
}

void MyClickHouse::ReadStructTick(string file) {
    LOG_INFO << "read file: " <<  file;
    list_pb.clear();
    ifstream infile; 
    infile.open(file.data()); 
    assert(infile.is_open());
    string line;
    while(getline(infile,line)) {    
        if(HandleLine(line)) {
            list_pb.push_back(move(line));
            if(list_pb.size() > 100000) {
                ConvertStruct();
            }
        }
    }
    ConvertStruct();
    infile.close(); 
}








