#include <zmq.h>
#include <iostream>

int main() {
    char public_key[41];
    char secret_key[41];
    int rc = zmq_curve_keypair(public_key, secret_key);
    if (rc == 0) {
        std::cout << "CURVE is supported!" << std::endl;
        return 0;
    } else {
        std::cout << "CURVE is NOT supported!" << std::endl;
        return 1;
    }
}
