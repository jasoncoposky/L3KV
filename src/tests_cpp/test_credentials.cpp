#include "../engine/credential_manager.hpp"
#include <cassert>
#include <iostream>

using namespace l3kv;

void test_user_registration() {
    CredentialManager cm(nullptr); // No engine needed for basic tests
    
    uint32_t uid = 101;
    std::string name = "US-Cluster-1";
    std::string pubkey = "49p!v*T8$vD=f(O7(S?%1p!v*T8$vD=f(O7(S?%1"; // 40 chars
    
    assert(cm.register_user(uid, name, pubkey));
    assert(!cm.register_user(uid, "Duplicate", pubkey)); // Fail on duplicate UID
    
    auto user = cm.get_user_by_key(pubkey);
    assert(user.has_value());
    assert(user->uid == uid);
    assert(user->name == name);
    
    std::cout << "[PASS] User Registration & Key Lookup" << std::endl;
}

void test_prefix_acl() {
    CredentialManager cm(nullptr);
    uint32_t uid = 101;
    cm.register_user(uid, "User1", "key1");
    
    // Set ACL: User 101 can write to "n:{us:"
    cm.set_acl(uid, "n:{us:", Permission::WRITE);
    
    assert(cm.check_permission(uid, "n:{us:123}") == Permission::WRITE);
    assert(cm.check_permission(uid, "e:{us:456}") == Permission::NONE);
    
    // Longest prefix match test
    cm.set_acl(uid, "n:{us:admin:", Permission::ADMIN);
    assert(cm.check_permission(uid, "n:{us:admin:keys}") == Permission::ADMIN);
    assert(cm.check_permission(uid, "n:{us:user1}") == Permission::WRITE);
    
    // Wildcard test
    cm.set_acl(uid, "*", Permission::READ);
    assert(cm.check_permission(uid, "any_other_key") == Permission::READ);
    // Longer prefix still wins if it matches
    assert(cm.check_permission(uid, "n:{us:123}") == Permission::WRITE);
    
    std::cout << "[PASS] Prefix ACL Enforcement" << std::endl;
}

int main() {
    std::cout << "Starting test_credentials..." << std::endl;
    try {
        test_user_registration();
        test_prefix_acl();
        std::cout << "All Credential Tests Passed!" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Test Failed: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
