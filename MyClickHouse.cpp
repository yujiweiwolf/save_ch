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
    long time_use = 0;
    struct timeval start;
    struct timeval end;
    gettimeofday(&start, NULL);
    
    long Total_num = 0;
    flatbuffers::FlatBufferBuilder builder;  
    string sql = "";
    if(code.length() == 9) {
        sql = "SELECT * FROM QTick where date = '" + date + "' and code = '" + code + "';";
    } else {
        sql = "SELECT * FROM QTick where date = '" + date + "'";
    }
    printf("%s\n", sql.c_str());
    client_->Select(sql.c_str(), [&](const Block & block) { 
//        for (size_t i = 0; i < block.GetRowCount(); ++i) {            
//            builder.Clear();            
//            char code[16] = "";
//            char name[16] = "";
//            char underlying_code[16] = "";
//            int Index = 0;
//            for (char ch : (*block[6]->As<ColumnString>())[i])	
//                    code[Index++] = ch;
//            code[Index] = '\0';
//            
//            Index = 0;
//            for (char ch : (*block[7]->As<ColumnString>())[i])	
//                    name[Index++] = ch;
//            name[Index] = '\0';
//            
//            Index = 0;
//            for (char ch : (*block[46]->As<ColumnString>())[i])	
//                    underlying_code[Index++] = ch;
//            underlying_code[Index] = '\0';
//      
//            flatbuffers::Offset<flatbuffers::String> content_code;
//            content_code = builder.CreateString(code, strlen(code) + 1);
//   
//            flatbuffers::Offset<flatbuffers::String> content_name;
//            content_name = builder.CreateString(name, strlen(name) + 1);
//            
//            flatbuffers::Offset<flatbuffers::String> content_underlying_code;
//            content_underlying_code = builder.CreateString(underlying_code, strlen(underlying_code) + 1);
//            
//            long timestamp = (*block[5]->As<ColumnInt64>())[i];
//            //printf("<%ld> <%s> <%s> <%ld>\n", (*block[5]->As<ColumnInt64>())[i], code, name, timestamp);
//            
//            auto bp = block[12]->As<ColumnArray>()->GetAsColumn(i);
//            cout << "bp ";
//            double bp_value[bp->Size()];
//            for (size_t j = 0; j < bp->Size(); ++j) {
//                std::cerr << (double)(*bp->As<ColumnFloat64>())[j] << " ";
//                bp_value[j] = (double)(*bp->As<ColumnFloat64>())[j];
//            }
//            auto bp_vec = builder.CreateVector(bp_value, bp->Size());
//            
//            cout << "bv ";            
//            auto bv = block[13]->As<ColumnArray>()->GetAsColumn(i);
//            int64_t bv_value[bv->Size()];
//            for (size_t j = 0; j < bv->Size(); ++j) {
//                std::cerr << (int64_t)(*bv->As<ColumnInt64>())[j] << " ";
//                bv_value[j] = (int64_t)(*bv->As<ColumnInt64>())[j];
//            }    
//            auto bv_vec = builder.CreateVector(bv_value, bv->Size());            
//                        
//            //cout << "ap ";            
//            auto ap = block[14]->As<ColumnArray>()->GetAsColumn(i);
//            double ap_value[ap->Size()];
//            for (size_t j = 0; j < ap->Size(); ++j) {
//                //std::cerr << (double)(*ap->As<ColumnFloat64>())[j] << " ";
//                ap_value[j] = (double)(*ap->As<ColumnFloat64>())[j];
//            }    
//            auto ap_vec = builder.CreateVector(ap_value, ap->Size());            
//                        
//            //cout << "av ";            
//            auto av = block[15]->As<ColumnArray>()->GetAsColumn(i);
//            int64_t av_value[av->Size()];
//            for (size_t j = 0; j < av->Size(); ++j) {
//                //std::cerr << (int64_t)(*av->As<ColumnInt64>())[j] << " ";
//                av_value[j] = (int64_t)(*av->As<ColumnInt64>())[j];
//            }    
//            auto av_vec = builder.CreateVector(av_value, av->Size());
//            
//            cout << endl;
//
//            
//            QTickBuilder msgData(builder);
//            msgData.add_src((*block[3]->As<ColumnInt8>())[i]);
//            msgData.add_dtype((*block[4]->As<ColumnInt8>())[i]);
//            msgData.add_timestamp((*block[5]->As<ColumnInt64>())[i]);
//            msgData.add_code(content_code);
//            msgData.add_name(content_name);  
//            msgData.add_market((*block[8]->As<ColumnInt8>())[i]);
//            msgData.add_pre_close((*block[9]->As<ColumnFloat64>())[i]);
//            msgData.add_upper_limit((*block[10]->As<ColumnFloat64>())[i]);
//            msgData.add_lower_limit((*block[11]->As<ColumnFloat64>())[i]);          
//                      
//            
//            msgData.add_bp(bp_vec);    
//            msgData.add_bv(bv_vec);
//            msgData.add_bp(ap_vec);
//            msgData.add_bv(av_vec);
//            
//            msgData.add_status((*block[16]->As<ColumnInt8>())[i]);
//            msgData.add_new_price((*block[17]->As<ColumnFloat64>())[i]);
//            msgData.add_new_volume((*block[18]->As<ColumnInt64>())[i]);
//            msgData.add_new_amount((*block[19]->As<ColumnFloat64>())[i]);
//            msgData.add_sum_volume((*block[20]->As<ColumnInt64>())[i]);
//            msgData.add_sum_amount((*block[21]->As<ColumnFloat64>())[i]);
//            msgData.add_open((*block[22]->As<ColumnFloat64>())[i]);
//            msgData.add_high((*block[23]->As<ColumnFloat64>())[i]);
//            msgData.add_low((*block[24]->As<ColumnFloat64>())[i]);
//            msgData.add_avg_bid_price((*block[25]->As<ColumnFloat64>())[i]);
//            msgData.add_avg_ask_price((*block[26]->As<ColumnFloat64>())[i]);
//            msgData.add_new_bid_volume((*block[27]->As<ColumnInt64>())[i]);
//            msgData.add_new_bid_amount((*block[28]->As<ColumnFloat64>())[i]);
//            msgData.add_new_ask_volume((*block[29]->As<ColumnInt64>())[i]);
//            msgData.add_new_ask_amount((*block[30]->As<ColumnFloat64>())[i]);
//            msgData.add_open_interest((*block[31]->As<ColumnInt64>())[i]);
//            msgData.add_pre_settle((*block[32]->As<ColumnFloat64>())[i]);
//            msgData.add_pre_open_interest((*block[33]->As<ColumnInt64>())[i]);
//            msgData.add_close((*block[34]->As<ColumnFloat64>())[i]);
//            msgData.add_settle((*block[35]->As<ColumnFloat64>())[i]);
//            msgData.add_multiple((*block[36]->As<ColumnInt64>())[i]);
//            msgData.add_price_step((*block[37]->As<ColumnFloat64>())[i]);
//            msgData.add_create_date((*block[38]->As<ColumnInt32>())[i]);
//            msgData.add_list_date((*block[39]->As<ColumnInt32>())[i]);
//            msgData.add_expire_date((*block[40]->As<ColumnInt32>())[i]);
//            msgData.add_start_settle_date((*block[41]->As<ColumnInt32>())[i]);
//            msgData.add_end_settle_date((*block[42]->As<ColumnInt32>())[i]);
//            msgData.add_exercise_date((*block[43]->As<ColumnInt32>())[i]);
//            msgData.add_exercise_price((*block[44]->As<ColumnFloat64>())[i]);
//            msgData.add_cp_flag((*block[45]->As<ColumnInt8>())[i]);
//            msgData.add_underlying_code(content_underlying_code);
//            msgData.add_sum_bid_volume((*block[47]->As<ColumnInt64>())[i]);
//            msgData.add_sum_bid_amount((*block[48]->As<ColumnFloat64>())[i]);
//            msgData.add_sum_ask_volume((*block[49]->As<ColumnInt64>())[i]);
//            msgData.add_sum_ask_amount((*block[50]->As<ColumnFloat64>())[i]);
//            msgData.add_bid_order_volume((*block[51]->As<ColumnInt64>())[i]);
//            msgData.add_bid_order_amount((*block[52]->As<ColumnFloat64>())[i]);
//            msgData.add_bid_cancel_volume((*block[53]->As<ColumnInt64>())[i]);
//            msgData.add_bid_cancel_amount((*block[54]->As<ColumnFloat64>())[i]);
//            msgData.add_ask_order_volume((*block[55]->As<ColumnInt64>())[i]);
//            msgData.add_ask_order_amount((*block[56]->As<ColumnFloat64>())[i]);
//            msgData.add_ask_cancel_volume((*block[57]->As<ColumnInt64>())[i]);
//            msgData.add_ask_cancel_amount((*block[58]->As<ColumnFloat64>())[i]);
//            msgData.add_new_knock_count((*block[59]->As<ColumnInt64>())[i]);
//            msgData.add_sum_knock_count((*block[60]->As<ColumnInt64>())[i]);
//
//            
//            flatbuffers::Offset<QTick> tickData;
////            tickData = msgData.Finish();
////            builder.Finish(tickData);
//            Total_num++;
//            if(Total_num % 100000 == 0) {
//                gettimeofday(&end, NULL);
//                time_use = (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec); //微秒
//                printf("read time_use <%ld> Total_num<%ld> <%ld>\n", time_use, Total_num, (*block[5]->As<ColumnInt64>())[i]);
//            }
//        }
    }
    );
    gettimeofday(&end, NULL);
    time_use = (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec); //微秒
    printf("read time_use <%ld> Total_num<%ld>\n", time_use, Total_num);
    //cout << "Total_num " << Total_num << endl;
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
    client_->Execute("CREATE TABLE IF NOT EXISTS QTick (date Date, datetime DateTime, stamp Int64, src Int8, dtype Int8, timestamp Int64, code String, name String, market Int8, pre_close Float64, upper_limit Float64, lower_limit Float64, bp Array(Float64), bv Array(Int64), ap Array(Float64), av Array(Int64), status Int8, new_price Float64, new_volume Int64, new_amount Float64, sum_volume Int64, sum_amount Float64, open Float64, high Float64, low Float64, avg_bid_price Float64, avg_ask_price Float64, new_bid_volume Int64, new_bid_amount Float64, new_ask_volume Int64, new_ask_amount Float64, open_interest Int64, pre_settle Float64, pre_open_interest Int64, close Float64, settle Float64, multiple Int64, price_step Float64, create_date Int32, list_date Int32, expire_date Int32, start_settle_date Int32, end_settle_date Int32, exercise_date Int32, exercise_price Float64, cp_flag Int8, underlying_code String, sum_bid_volume Int64, sum_bid_amount Float64, sum_ask_volume Int64, sum_ask_amount Float64, bid_order_volume Int64, bid_order_amount Float64, bid_cancel_volume Int64, bid_cancel_amount Float64, ask_order_volume Int64, ask_order_amount Float64, ask_cancel_volume Int64, ask_cancel_amount Float64, new_knock_count Int64, sum_knock_count Int64) ENGINE=MergeTree ORDER BY(timestamp, code, name) PARTITION BY (date)");
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
        ReadPBQTick(it->second);
        //InsertPBQTick();
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
                tick.bp[0] = it.bp1();
                if (it.bp2() > 0) {
                    tick.bp[1] = it.bp2();
                    if (it.bp3() > 0) {
                        tick.bp[2] = it.bp3();
                        if (it.bp4() > 0) {
                            tick.bp[3] = it.bp4();
                            if (it.bp5() > 0) {
                                tick.bp[4] = it.bp5();
                                if (it.bp6() > 0) {
                                   tick.bp[5] = it.bp6();
                                    if (it.bp7() > 0) {
                                        tick.bp[6] = it.bp7();
                                        if (it.bp8() > 0) {
                                            tick.bp[7] = it.bp8();
                                            if (it.bp9() > 0) {
                                                tick.bp[8] = it.bp9();
                                                if (it.bp10() > 0) {
                                                    tick.bp[9] = it.bp10();
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
                tick.bv[0] = it.bv1();
                if (it.bv2() > 0) {
                    tick.bv[1] = it.bv2();
                    if (it.bv3() > 0) {
                        tick.bv[2] = it.bv3();
                        if (it.bv4() > 0) {
                            tick.bv[3] = it.bv4();
                            if (it.bv5() > 0) {
                                tick.bv[4] = it.bv5();
                                if (it.bv6() > 0) {
                                   tick.bv[5] = it.bv6();
                                    if (it.bv7() > 0) {
                                        tick.bv[6] = it.bv7();
                                        if (it.bv8() > 0) {
                                            tick.bv[7] = it.bv8();
                                            if (it.bv9() > 0) {
                                                tick.bv[8] = it.bv9();
                                                if (it.bv10() > 0) {
                                                    tick.bv[9] = it.bv10();
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
                tick.ap[0] = it.ap1();
                if (it.ap2() > 0) {
                    tick.ap[1] = it.ap2();
                    if (it.ap3() > 0) {
                        tick.ap[2] = it.ap3();
                        if (it.ap4() > 0) {
                            tick.ap[3] = it.ap4();
                            if (it.ap5() > 0) {
                                tick.ap[4] = it.ap5();
                                if (it.ap6() > 0) {
                                   tick.ap[5] = it.ap6();
                                    if (it.ap7() > 0) {
                                        tick.ap[6] = it.ap7();
                                        if (it.ap8() > 0) {
                                            tick.ap[7] = it.ap8();
                                            if (it.ap9() > 0) {
                                                tick.ap[8] = it.ap9();
                                                if (it.ap10() > 0) {
                                                    tick.ap[9] = it.ap10();
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
                tick.av[0] = it.av1();
                if (it.av2() > 0) {
                    tick.av[1] = it.av2();
                    if (it.av3() > 0) {
                        tick.av[2] = it.av3();
                        if (it.av4() > 0) {
                            tick.av[3] = it.av4();
                            if (it.av5() > 0) {
                                tick.av[4] = it.av5();
                                if (it.av6() > 0) {
                                   tick.av[5] = it.av6();
                                    if (it.av7() > 0) {
                                        tick.av[6] = it.av7();
                                        if (it.av8() > 0) {
                                            tick.av[7] = it.av8();
                                            if (it.av9() > 0) {
                                                tick.av[8] = it.av9();
                                                if (it.av10() > 0) {
                                                    tick.av[9] = it.av10();
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
}

void MyClickHouse::InsertStructQTick() {
    client_->Execute("CREATE TABLE IF NOT EXISTS QTick (date Date, datetime DateTime, stamp Int64, src Int8, dtype Int8, timestamp Int64, code String, name String, market Int8, pre_close Float64, upper_limit Float64, lower_limit Float64, bp Array(Float64), bv Array(Int64), ap Array(Float64), av Array(Int64), status Int8, new_price Float64, new_volume Int64, new_amount Float64, sum_volume Int64, sum_amount Float64, open Float64, high Float64, low Float64, avg_bid_price Float64, avg_ask_price Float64, new_bid_volume Int64, new_bid_amount Float64, new_ask_volume Int64, new_ask_amount Float64, open_interest Int64, pre_settle Float64, pre_open_interest Int64, close Float64, settle Float64, multiple Int64, price_step Float64, create_date Int32, list_date Int32, expire_date Int32, start_settle_date Int32, end_settle_date Int32, exercise_date Int32, exercise_price Float64, cp_flag Int8, underlying_code String, sum_bid_volume Int64, sum_bid_amount Float64, sum_ask_volume Int64, sum_ask_amount Float64, bid_order_volume Int64, bid_order_amount Float64, bid_cancel_volume Int64, bid_cancel_amount Float64, ask_order_volume Int64, ask_order_amount Float64, ask_cancel_volume Int64, ask_cancel_amount Float64, new_knock_count Int64, sum_knock_count Int64) ENGINE=MergeTree ORDER BY(timestamp, code, name) PARTITION BY (date)");
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
    
    //        if (it.code.length > 0 && it.name.length > 0) {
//            cout << it.code<< " " << it.date << " " << it.stamp << " " << it.name << " " << it.pre_close << " "
//                << it.upper_limit << " " << it.lower_limit << " " << it.new_price << " " << it.new_volume << endl;
//        }

    for (auto& it : list_ticks_) {   
         cout << it.code<< " " << it.timestamp << " " << it.code << " " << it.pre_close << " "
              << it.upper_limit << " " << it.lower_limit << " " << it.new_price << " " << it.new_volume << endl;
                 
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
        
        bp_price->Clear();
        if (it.bp[0] > 0) {
            bp_price->Append(it.bp[0]);
            if (it.bp[1] > 0) {
                bp_price->Append(it.bp[1]);
                if (it.bp[2] > 0) {
                    bp_price->Append(it.bp[2]);
                    if (it.bp[3] > 0) {
                        bp_price->Append(it.bp[3]);
                        if (it.bp[4] > 0) {
                            bp_price->Append(it.bp[4]);
                            if (it.bp[5] > 0) {
                                bp_price->Append(it.bp[5]);
                                if (it.bp[6] > 0) {
                                    bp_price->Append(it.bp[6]);
                                    if (it.bp[7] > 0) {
                                        bp_price->Append(it.bp[7]);
                                        if (it.bp[8] > 0) {
                                            bp_price->Append(it.bp[8]);
                                            if (it.bp[9] > 0) {
                                                bp_price->Append(it.bp[9]);
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
        if (it.bv[0] > 0) {
            bv_volume->Append(it.bv[0]);
            if (it.bv[1] > 0) {
                bv_volume->Append(it.bv[1]);
                if (it.bv[2] > 0) {
                    bv_volume->Append(it.bv[2]);
                    if (it.bv[3] > 0) {
                        bv_volume->Append(it.bv[3]);
                        if (it.bv[4] > 0) {
                            bv_volume->Append(it.bv[4]);
                            if (it.bv[5] > 0) {
                                bv_volume->Append(it.bv[5]);
                                if (it.bv[6] > 0) {
                                    bv_volume->Append(it.bv[6]);
                                    if (it.bv[7] > 0) {
                                        bv_volume->Append(it.bv[7]);
                                        if (it.bv[8] > 0) {
                                            bv_volume->Append(it.bv[8]);
                                            if (it.bv[9] > 0) {
                                                bv_volume->Append(it.bv[9]);
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

        if (it.ap[0] > 0) {
            ap_price->Append(it.ap[0]);
            if (it.ap[1] > 0) {
                ap_price->Append(it.ap[1]);
                if (it.ap[2] > 0) {
                    ap_price->Append(it.ap[2]);
                    if (it.ap[3] > 0) {
                        ap_price->Append(it.ap[3]);
                        if (it.ap[4] > 0) {
                            ap_price->Append(it.ap[4]);
                            if (it.ap[5] > 0) {
                                ap_price->Append(it.ap[5]);
                                if (it.ap[6] > 0) {
                                    ap_price->Append(it.ap[6]);
                                    if (it.ap[7] > 0) {
                                        ap_price->Append(it.ap[7]);
                                        if (it.ap[8] > 0) {
                                            ap_price->Append(it.ap[8]);
                                            if (it.ap[9] > 0) {
                                                ap_price->Append(it.ap[9]);
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
        if (it.av[0] > 0) {
            av_volume->Append(it.av[0]);
            if (it.av[1] > 0) {
                av_volume->Append(it.av[1]);
                if (it.av[2] > 0) {
                    av_volume->Append(it.av[2]);
                    if (it.av[3] > 0) {
                        av_volume->Append(it.av[3]);
                        if (it.av[4] > 0) {
                            av_volume->Append(it.av[4]);
                            if (it.av[5] > 0) {
                                av_volume->Append(it.av[5]);
                                if (it.av[6] > 0) {
                                    av_volume->Append(it.av[6]);
                                    if (it.av[7] > 0) {
                                        av_volume->Append(it.av[7]);
                                        if (it.av[8] > 0) {
                                            av_volume->Append(it.av[8]);
                                            if (it.av[9] > 0) {
                                                av_volume->Append(it.av[9]);
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
    gettimeofday(&end, NULL);
    time_use = (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec); //微秒
    printf("list_ticks_ size <%d> time_use is <%ld>\n", list_ticks_.size(), time_use);
    list_ticks_.clear();
}

void MyClickHouse::ReadStructTick(string file) {
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
                ConvertStruct();
            }
        }
    }
    ConvertStruct();
    infile.close();    
    cout << "list_pb " << list_pb.size() << endl;
}








