#include "MyClickHouse.h"

int main(int argc, char** argv) {     
    try {             
        MyClickHouse* my = new MyClickHouse();
//        string file_name = argv[1];   
//        string date = argv[2]; 
//        my->ReadQTick(file_name);
//        my->InsertQTick();
        //my->ReadQTick();
        
       // my->ReadPBQTick(file_name);
        
        my->DeleteTable();

//        my->GetFileNum(file_name, date);
//        my->ReadAllPBQTick();
        
//        string date = argv[1];   
//        string code = argv[2]; 
//        my->ReadClickHouse(date, code);
        
    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }
    return 0;
}
