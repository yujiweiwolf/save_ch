#include "MyClickHouse.h"

int main(int argc, char** argv) {
    try {
        string file_name = argv[1];        
        MyClickHouse* my = new MyClickHouse();
        my->ReadQTick(file_name);
        my->InsertQTick();
        my->ReadQTick();
    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }
    return 0;
}
