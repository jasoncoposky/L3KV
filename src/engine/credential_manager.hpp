#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <cstdint>
#include <optional>
#include <algorithm>
#include <mutex>

namespace l3kv {

class Engine;

static constexpr uint32_t INTERNAL_UID = 0xFFFFFFFF;
static constexpr uint32_t ADMIN_UID = 0;

enum class Permission : uint8_t {
    NONE = 0,
    READ = 1 << 0,
    WRITE = 1 << 1,
    ADMIN = 1 << 2
};

inline Permission operator|(Permission a, Permission b) {
    return static_cast<Permission>(static_cast<uint8_t>(a) | static_cast<uint8_t>(b));
}

inline bool operator&(Permission a, Permission b) {
    return static_cast<uint8_t>(a) & static_cast<uint8_t>(b);
}

struct UserIdentity {
    uint32_t uid;
    std::string name;
    std::string public_key;
};

class CredentialManager {
public:
    explicit CredentialManager(Engine* engine) : engine_(engine) {}
    
    bool register_user(uint32_t uid, const std::string& name, const std::string& public_key) {
        std::unique_lock lock(mutex_);
        if (users_.contains(uid)) return false;
        
        UserIdentity user{uid, name, public_key};
        users_[uid] = user;
        key_to_uid_[public_key] = uid;
        return true;
    }

    std::optional<UserIdentity> get_user_by_key(const std::string& public_key) const {
        std::shared_lock lock(mutex_);
        auto it = key_to_uid_.find(public_key);
        if (it == key_to_uid_.end()) return std::nullopt;
        return users_.at(it->second);
    }
    
    void set_acl(uint32_t uid, const std::string& prefix, Permission perm) {
        std::unique_lock lock(mutex_);
        auto& acls = user_acls_[uid];
        
        // Remove existing rule for same prefix
        acls.erase(std::remove_if(acls.begin(), acls.end(), 
                   [&](const ACLRule& r) { return r.prefix == prefix; }), acls.end());
        
        acls.push_back({prefix, perm});
        
        // Sort by prefix length descending for longest-prefix match
        std::sort(acls.begin(), acls.end(), [](const ACLRule& a, const ACLRule& b) {
            return a.prefix.length() > b.prefix.length();
        });
    }

    Permission check_permission(uint32_t uid, const std::string& key) const {
        if (uid == INTERNAL_UID || uid == ADMIN_UID) return Permission::ADMIN;
        
        std::shared_lock lock(mutex_);
        auto it = user_acls_.find(uid);
        if (it == user_acls_.end()) return Permission::NONE;
        
        for (const auto& rule : it->second) {
            if (rule.prefix == "*" || key.starts_with(rule.prefix)) {
                return rule.perm;
            }
        }
        return Permission::NONE;
    }

private:
    struct ACLRule {
        std::string prefix;
        Permission perm;
    };

    Engine* engine_;
    std::unordered_map<std::string, uint32_t> key_to_uid_;
    std::unordered_map<uint32_t, UserIdentity> users_;
    std::unordered_map<uint32_t, std::vector<ACLRule>> user_acls_;
    mutable std::shared_mutex mutex_;
};

} // namespace l3kv
