#include "MyClickHouse.h"

int main(int argc, char** argv) {  
    nanolog::initialize(nanolog::NonGuaranteedLogger(10), "./", "clickhouse.log", 1);
    LOG_INFO << "nanolog::initialize";
    try {             
        MyClickHouse* my = new MyClickHouse();
        string file_name = argv[1];   
        string date = argv[2]; 
        
//        my->ReadQTick(file_name);
//        my->InsertQTick();
        //my->ReadQTick();
        
//        my->ReadPBQTick(file_name);
        
        //my->DeleteTable();
        
        // 读某个文件夹下所有
        my->GetFileNum(file_name, date);
        my->ReadAllPBQTick();  
        
        ////单独读取某个文件
        //my->ReadStructTick(file_name);
        
//        //// 读数据库
//        my->ReadClickHouse(file_name.c_str(), date.c_str());
        
    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }
    return 0;
}
