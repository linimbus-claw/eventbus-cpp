#ifndef EVENTBUS_HPP
#define EVENTBUS_HPP

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

// ============================================================
// Message struct - 基础消息结构
// ============================================================
struct Message {
    std::string type;                              // 消息类型/名称
    std::map<std::string, std::string> data;     // 消息数据 key-value

    Message() = default;
    explicit Message(const std::string& t) : type(t) {}

    // 设置数据 - 注意：const char* 必须在 bool 之前声明，避免被 bool 优先级更高
    Message& set(const std::string& key, const char* value) {
        data[key] = std::string(value);
        return *this;
    }

    Message& set(const std::string& key, const std::string& value) {
        data[key] = value;
        return *this;
    }

    Message& set(const std::string& key, int value) {
        data[key] = std::to_string(value);
        return *this;
    }

    Message& set(const std::string& key, double value) {
        data[key] = std::to_string(value);
        return *this;
    }

    Message& set(const std::string& key, bool value) {
        data[key] = value ? "true" : "false";
        return *this;
    }

    // 获取数据
    template<typename T>
    T get(const std::string& key, const T& defaultValue = T{}) const {
        auto it = data.find(key);
        if (it == data.end()) return defaultValue;
        
        std::istringstream iss(it->second);
        T value;
        if (iss >> value) return value;
        return defaultValue;
    }

    // bool 特化版本
    template<>
    bool get(const std::string& key, const bool& defaultValue) const {
        auto it = data.find(key);
        if (it == data.end()) return defaultValue;
        
        if (it->second == "true") return true;
        if (it->second == "false") return false;
        return defaultValue;
    }

    std::string getString(const std::string& key, const std::string& defaultValue = "") const {
        auto it = data.find(key);
        return it != data.end() ? it->second : defaultValue;
    }

    // 检查是否包含指定key
    bool has(const std::string& key) const {
        return data.find(key) != data.end();
    }

    // 消息匹配 - 检查此消息是否匹配另一个消息的条件
    bool matches(const Message& other) const {
        // 如果订阅消息有type，必须匹配
        if (!type.empty() && type != other.type) {
            return false;
        }
        
        // 如果订阅消息有数据条件，必须全部匹配
        for (const auto& kv : data) {
            auto it = other.data.find(kv.first);
            if (it == other.data.end() || it->second != kv.second) {
                return false;
            }
        }
        return true;
    }

    // 比较运算符
    bool operator==(const Message& other) const {
        return type == other.type && data == other.data;
    }

    bool operator!=(const Message& other) const {
        return !(*this == other);
    }

    std::string toString() const {
        std::ostringstream oss;
        oss << "Message{type=\"" << type << "\", data={";
        bool first = true;
        for (const auto& kv : data) {
            if (!first) oss << ", ";
            oss << "\"" << kv.first << "\":\"" << kv.second << "\"";
            first = false;
        }
        oss << "}}";
        return oss.str();
    }
};

// ============================================================
// EventBus - 事件总线
// ============================================================
class EventBus {
public:
    using Callback = std::function<void(const Message&)>;
    using MultiCallback = std::function<void(const std::vector<Message>&)>;
    using SubscriptionId = uint64_t;

private:
    // 单事件订阅信息
    struct SingleSubscription {
        SubscriptionId id;
        Message filter;           // 消息过滤器
        Callback callback;
        bool oneShot;            // 是否只触发一次

        bool operator==(const SingleSubscription& other) const {
            return id == other.id;
        }
    };

    // 多事件订阅信息
    struct MultiSubscription {
        SubscriptionId id;
        std::vector<Message> filters;  // 多个消息过滤器
        MultiCallback callback;
        std::vector<Message> pending; // 待触发的事件
        bool triggered;               // 是否已触发

        bool operator==(const MultiSubscription& other) const {
            return id == other.id;
        }
    };

    mutable std::mutex mutex_;
    std::vector<SingleSubscription> singleSubscriptions_;
    std::vector<MultiSubscription> multiSubscriptions_;
    SubscriptionId nextId_ = 1;

public:
    EventBus() = default;
    ~EventBus() = default;

    // 禁止拷贝
    EventBus(const EventBus&) = delete;
    EventBus& operator=(const EventBus&) = delete;

    // ============================================================
    // 订阅 - 单事件
    // ============================================================
    
    // 使用Message订阅
    SubscriptionId subscribe(const Message& filter, Callback callback, bool oneShot = false) {
        std::lock_guard<std::mutex> lock(mutex_);
        SubscriptionId id = nextId_++;
        singleSubscriptions_.push_back({id, filter, std::move(callback), oneShot});
        return id;
    }

    // 使用字符串type订阅
    SubscriptionId subscribe(const std::string& type, Callback callback, bool oneShot = false) {
        Message filter;
        filter.type = type;
        return subscribe(filter, std::move(callback), oneShot);
    }

    // ============================================================
    // 订阅 - 多事件（需要等待所有事件都发布后才触发）
    // ============================================================
    
    // 使用Message数组订阅
    SubscriptionId subscribeMulti(const std::vector<Message>& filters, MultiCallback callback) {
        std::lock_guard<std::mutex> lock(mutex_);
        SubscriptionId id = nextId_++;
        multiSubscriptions_.push_back({id, filters, std::move(callback), {}, false});
        return id;
    }

    // 使用字符串type数组订阅多个事件
    SubscriptionId subscribeMultiTypes(const std::vector<std::string>& types, MultiCallback callback) {
        std::lock_guard<std::mutex> lock(mutex_);
        SubscriptionId id = nextId_++;
        std::vector<Message> filters;
        for (const auto& t : types) {
            filters.push_back(Message{t});
        }
        multiSubscriptions_.push_back({id, filters, std::move(callback), {}, false});
        return id;
    }

    // ============================================================
    // 发布
    // ============================================================
    void publish(const Message& message) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // 触发单事件订阅
        triggerSingleSubscriptions(message);
        
        // 触发多事件订阅
        triggerMultiSubscriptions(message);
    }

    // 便捷发布方法 - 使用字符串type和可选数据
    void publish(const std::string& type, 
                 const std::map<std::string, std::string>& data = {}) {
        Message msg;
        msg.type = type;
        msg.data = data;
        publish(msg);
    }

    // ============================================================
    // 取消订阅
    // ============================================================
    bool unsubscribe(SubscriptionId id) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // 单事件订阅
        auto it = std::remove_if(singleSubscriptions_.begin(), singleSubscriptions_.end(),
            [id](const SingleSubscription& sub) { return sub.id == id; });
        if (it != singleSubscriptions_.end()) {
            singleSubscriptions_.erase(it, singleSubscriptions_.end());
            return true;
        }
        
        // 多事件订阅
        auto it2 = std::remove_if(multiSubscriptions_.begin(), multiSubscriptions_.end(),
            [id](const MultiSubscription& sub) { return sub.id == id; });
        if (it2 != multiSubscriptions_.end()) {
            multiSubscriptions_.erase(it2, multiSubscriptions_.end());
            return true;
        }
        
        return false;
    }

    // 取消所有订阅
    void unsubscribeAll() {
        std::lock_guard<std::mutex> lock(mutex_);
        singleSubscriptions_.clear();
        multiSubscriptions_.clear();
    }

    // ============================================================
    // 查询
    // ============================================================
    size_t subscriptionCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return singleSubscriptions_.size() + multiSubscriptions_.size();
    }

    void clear() {
        unsubscribeAll();
    }

private:
    // 触发单事件订阅
    void triggerSingleSubscriptions(const Message& message) {
        auto it = singleSubscriptions_.begin();
        while (it != singleSubscriptions_.end()) {
            if (it->filter.matches(message)) {
                // 调用回调
                it->callback(message);
                
                // oneShot 需要删除
                if (it->oneShot) {
                    it = singleSubscriptions_.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }

    // 触发多事件订阅
    void triggerMultiSubscriptions(const Message& message) {
        auto it = multiSubscriptions_.begin();
        while (it != multiSubscriptions_.end()) {
            if (it->triggered) {
                ++it;
                continue;
            }
            
            // 检查消息是否匹配其中一个过滤器
            bool matched = false;
            for (const auto& filter : it->filters) {
                if (filter.matches(message)) {
                    // 检查是否已经收到过这个消息
                    bool alreadyReceived = false;
                    for (const auto& pending : it->pending) {
                        if (pending.matches(message)) {
                            alreadyReceived = true;
                            break;
                        }
                    }
                    if (!alreadyReceived) {
                        it->pending.push_back(message);
                    }
                    matched = true;
                    break;
                }
            }
            
            if (matched && it->pending.size() == it->filters.size()) {
                // 所有事件都收到了，触发回调
                it->callback(it->pending);
                it->triggered = true;
                it = multiSubscriptions_.erase(it);
            } else {
                ++it;
            }
        }
    }
};

// ============================================================
// 便捷宏 - 用于创建特定类型的事件
// ============================================================
#define EVENTBUS_MSG(type) \
    ([]() { \
        Message msg; msg.type = type; return msg; \
    }())

#define EVENTBUS_MSG_WITH_DATA(type, ...) \
    ([&]() { \
        Message msg; msg.type = type; \
        __VA_ARGS__ \
        return msg; \
    }())

#endif // EVENTBUS_HPP
