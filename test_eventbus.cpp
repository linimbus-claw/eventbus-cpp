#include "eventbus.hpp"
#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>
#include <atomic>

// ============================================================
// 测试辅助
// ============================================================
static int testsPassed = 0;
static int testsFailed = 0;

#define TEST(name) void test_##name()
#define RUN_TEST(name) do { \
    std::cout << "Running " << #name << "... "; \
    try { \
        test_##name(); \
        std::cout << "✓ PASSED" << std::endl; \
        testsPassed++; \
    } catch (const std::exception& e) { \
        std::cout << "✗ FAILED: " << e.what() << std::endl; \
        testsFailed++; \
    } \
} while(0)

#define ASSERT(cond) do { \
    if (!(cond)) throw std::runtime_error("Assertion failed: " #cond); \
} while(0)

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) throw std::runtime_error("Assertion failed: " #a " == " #b); \
} while(0)

// ============================================================
// 测试用例
// ============================================================

TEST(message_creation) {
    Message msg("login");
    msg.set("username", "alice")
       .set("age", 25)
       .set("active", true);
    
    ASSERT(msg.type == "login");
    ASSERT(msg.getString("username") == "alice");
    ASSERT(msg.get<int>("age") == 25);
    ASSERT(msg.get<bool>("active") == true);
    ASSERT(msg.has("username"));
    ASSERT(!msg.has("nonexistent"));
}

TEST(message_matching) {
    // 完全匹配
    Message filter1;
    filter1.type = "login";
    filter1.set("username", "alice");
    
    Message msg1;
    msg1.type = "login";
    msg1.set("username", "alice");
    
    ASSERT(filter1.matches(msg1) == true);
    
    // 部分匹配 - filter有更多条件
    Message filter2;
    filter2.type = "login";
    filter2.set("username", "alice");
    
    Message msg2;
    msg2.type = "login";
    msg2.set("username", "alice");
    msg2.set("extra", "data");  // 额外数据不影响匹配
    
    ASSERT(filter2.matches(msg2) == true);
    
    // 不匹配 - type不同
    Message filter3;
    filter3.type = "login";
    
    Message msg3;
    msg3.type = "logout";
    
    ASSERT(filter3.matches(msg3) == false);
    
    // 不匹配 - 数据不匹配
    Message filter4;
    filter4.type = "login";
    filter4.set("username", "alice");
    
    Message msg4;
    msg4.type = "login";
    msg4.set("username", "bob");
    
    ASSERT(filter4.matches(msg4) == false);
    
    // 空filter匹配任意同type
    Message filter5;
    filter5.type = "login";
    
    Message msg5;
    msg5.type = "login";
    msg5.set("any", "data");
    
    ASSERT(filter5.matches(msg5) == true);
}

TEST(single_event_subscribe_basic) {
    EventBus eb;
    int callCount = 0;
    std::string lastUser;
    
    // 订阅login事件
    eb.subscribe("login", [&](const Message& msg) {
        callCount++;
        lastUser = msg.getString("username");
    });
    
    // 发布login消息
    Message msg1;
    msg1.type = "login";
    msg1.set("username", "alice");
    eb.publish(msg1);
    
    ASSERT_EQ(callCount, 1);
    ASSERT_EQ(lastUser, "alice");
    
    // 发布logout消息，不触发
    Message msg2;
    msg2.type = "logout";
    eb.publish(msg2);
    
    ASSERT_EQ(callCount, 1);
}

TEST(single_event_with_data_filter) {
    EventBus eb;
    int callCount = 0;
    
    // 订阅特定username的login
    Message filter;
    filter.type = "login";
    filter.set("username", "alice");
    
    eb.subscribe(filter, [&](const Message& msg) {
        callCount++;
    });
    
    // alice的login，触发
    Message msg1;
    msg1.type = "login";
    msg1.set("username", "alice");
    eb.publish(msg1);
    ASSERT_EQ(callCount, 1);
    
    // bob的login，不触发
    Message msg2;
    msg2.type = "login";
    msg2.set("username", "bob");
    eb.publish(msg2);
    ASSERT_EQ(callCount, 1);
}

TEST(one_shot_subscription) {
    EventBus eb;
    int callCount = 0;
    
    auto id = eb.subscribe("login", [&](const Message&) {
        callCount++;
    }, true);  // oneShot = true
    
    ASSERT_EQ(eb.subscriptionCount(), 1);
    
    // 第一次触发
    eb.publish(Message("login"));
    ASSERT_EQ(callCount, 1);
    
    // 第二次不触发
    eb.publish(Message("login"));
    ASSERT_EQ(callCount, 1);
    
    // 已自动移除
    ASSERT_EQ(eb.subscriptionCount(), 0);
}

TEST(multi_event_subscription_basic) {
    EventBus eb;
    int callCount = 0;
    
    // 订阅login + logout两个事件
    std::vector<std::string> types = {"login", "logout"};
    eb.subscribeMultiTypes(types, [&](const std::vector<Message>& events) {
        callCount++;
    });
    
    // 只发布login，不触发
    eb.publish(Message("login"));
    ASSERT_EQ(callCount, 0);
    
    // 发布logout，触发
    eb.publish(Message("logout"));
    ASSERT_EQ(callCount, 1);
}

TEST(multi_event_with_filters) {
    EventBus eb;
    int callCount = 0;
    
    // 使用Message数组订阅
    std::vector<Message> filters;
    Message f1;
    f1.type = "order_created";
    f1.set("status", "pending");
    
    Message f2;
    f2.type = "payment";
    f2.set("status", "success");
    
    filters.push_back(f1);
    filters.push_back(f2);
    
    eb.subscribeMulti(filters, [&](const std::vector<Message>& events) {
        callCount++;
    });
    
    // 只发布一个事件
    Message e1;
    e1.type = "order_created";
    e1.set("status", "pending");
    eb.publish(e1);
    ASSERT_EQ(callCount, 0);
    
    // 发布另一个事件，触发
    Message e2;
    e2.type = "payment";
    e2.set("status", "success");
    eb.publish(e2);
    ASSERT_EQ(callCount, 1);
}

TEST(multi_event_order_independent) {
    EventBus eb;
    std::vector<std::string> received;
    
    eb.subscribeMultiTypes({"login", "logout"}, [&](const std::vector<Message>& events) {
        received.push_back("both");
    });
    
    // 先发布logout
    eb.publish(Message("logout"));
    ASSERT(received.empty());
    
    // 再发布login，触发
    eb.publish(Message("login"));
    ASSERT(received.size() == 1);
}

TEST(multi_event_content_matching) {
    EventBus eb;
    int callCount = 0;
    
    // 订阅：login的username=alice + logout的username=alice
    // 注意：两个filter都限制username为alice
    std::vector<Message> filters;
    Message f1;
    f1.type = "login";
    f1.set("username", "alice");
    
    Message f2;
    f2.type = "logout";
    f2.set("username", "alice");
    
    filters.push_back(f1);
    filters.push_back(f2);
    
    eb.subscribeMulti(filters, [&](const std::vector<Message>&) {
        callCount++;
    });
    
    // alice的login + bob的logout -> 不匹配 (bob != alice)
    Message m1;
    m1.type = "login";
    m1.set("username", "alice");
    eb.publish(m1);
    
    Message m2;
    m2.type = "logout";
    m2.set("username", "bob");
    eb.publish(m2);
    ASSERT_EQ(callCount, 0);  // 不触发，因为logout的username是bob不匹配
    
    // alice的logout -> 触发
    Message m3;
    m3.type = "logout";
    m3.set("username", "alice");
    eb.publish(m3);
    ASSERT_EQ(callCount, 1);
}

TEST(unsubscribe_single) {
    EventBus eb;
    int callCount = 0;
    
    auto id = eb.subscribe("login", [&](const Message&) {
        callCount++;
    });
    
    eb.publish(Message("login"));
    ASSERT_EQ(callCount, 1);
    
    // 取消订阅
    eb.unsubscribe(id);
    
    eb.publish(Message("login"));
    ASSERT_EQ(callCount, 1);
}

TEST(unsubscribe_multi) {
    EventBus eb;
    int callCount = 0;
    
    auto id = eb.subscribeMultiTypes({"login", "logout"}, [&](const std::vector<Message>&) {
        callCount++;
    });
    
    eb.publish(Message("login"));
    ASSERT_EQ(callCount, 0);
    
    // 取消订阅
    eb.unsubscribe(id);
    
    eb.publish(Message("logout"));
    ASSERT_EQ(callCount, 0);
}

TEST(publish_with_string_type) {
    EventBus eb;
    int callCount = 0;
    
    eb.subscribe("payment", [&](const Message& msg) {
        callCount++;
        // std::to_string(double) produces "100.500000"
        ASSERT(msg.getString("amount") == "100.500000");
        ASSERT(msg.getString("currency") == "USD");
    });
    
    Message msg;
    msg.type = "payment";
    msg.set("amount", 100.5);
    msg.set("currency", "USD");
    eb.publish(msg);
    
    ASSERT_EQ(callCount, 1);
}

TEST(publish_overload) {
    EventBus eb;
    int callCount = 0;
    
    eb.subscribe("test", [&](const Message& msg) {
        callCount++;
    });
    
    // 使用便捷发布方法
    eb.publish("test", {{"key", "value"}});
    ASSERT_EQ(callCount, 1);
}

TEST(thread_safety_basic) {
    EventBus eb;
    std::atomic<int> callCount{0};
    const int iterations = 100;
    const int threadCount = 10;
    const int subscriptionCount = 10;
    
    // 多个线程订阅
    for (int i = 0; i < subscriptionCount; i++) {
        eb.subscribe("login", [&](const Message&) {
            callCount.fetch_add(1);
        });
    }
    
    // 多个线程发布
    std::vector<std::thread> threads;
    for (int i = 0; i < threadCount; i++) {
        threads.emplace_back([&]() {
            for (int j = 0; j < iterations; j++) {
                eb.publish(Message("login"));
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // 每个发布应该触发所有10个订阅
    // 总消息数 = threadCount * iterations = 10 * 100 = 1000
    // 总回调数 = 1000 * subscriptionCount = 1000 * 10 = 10000
    ASSERT_EQ(callCount.load(), threadCount * iterations * subscriptionCount);
}

TEST(mixed_single_and_multi) {
    EventBus eb;
    int singleCount = 0;
    int multiCount = 0;
    
    // 单事件订阅
    eb.subscribe("login", [&](const Message&) {
        singleCount++;
    });
    
    // 多事件订阅
    eb.subscribeMultiTypes({"login", "logout"}, [&](const std::vector<Message>&) {
        multiCount++;
    });
    
    eb.publish(Message("login"));
    // 单事件立即触发，多事件等待
    
    ASSERT_EQ(singleCount, 1);
    ASSERT_EQ(multiCount, 0);
    
    eb.publish(Message("logout"));
    
    ASSERT_EQ(singleCount, 1);
    ASSERT_EQ(multiCount, 1);
}

TEST(message_to_string) {
    Message msg;
    msg.type = "test";
    msg.set("key1", "value1");
    msg.set("key2", "value2");
    
    std::string str = msg.toString();
    
    ASSERT(str.find("test") != std::string::npos);
    ASSERT(str.find("key1") != std::string::npos);
    ASSERT(str.find("value1") != std::string::npos);
}

TEST(empty_filter) {
    EventBus eb;
    int callCount = 0;
    
    // 订阅任意login消息（空的filter）
    Message filter;
    filter.type = "login";
    
    eb.subscribe(filter, [&](const Message&) {
        callCount++;
    });
    
    Message msg1;
    msg1.type = "login";
    msg1.set("any", "data");
    eb.publish(msg1);
    
    Message msg2;
    msg2.type = "login";
    eb.publish(msg2);
    
    ASSERT_EQ(callCount, 2);
}

TEST(clear_all_subscriptions) {
    EventBus eb;
    int callCount = 0;
    
    eb.subscribe("login", [&](const Message&) { callCount++; });
    eb.subscribe("logout", [&](const Message&) { callCount++; });
    eb.subscribeMultiTypes({"login", "logout"}, [&](const std::vector<Message>&) { callCount++; });
    
    ASSERT_EQ(eb.subscriptionCount(), 3);
    
    eb.unsubscribeAll();
    
    ASSERT_EQ(eb.subscriptionCount(), 0);
    
    eb.publish(Message("login"));
    eb.publish(Message("logout"));
    
    ASSERT_EQ(callCount, 0);
}

TEST(multi_event_all_filters_matched) {
    EventBus eb;
    int callCount = 0;
    std::string lastOrderId;
    
    // 订阅 order_created + payment 两个事件，需要特定orderId匹配
    std::vector<Message> filters;
    Message f1;
    f1.type = "order_created";
    f1.set("orderId", "ORDER_123");
    
    Message f2;
    f2.type = "payment";
    f2.set("orderId", "ORDER_123");
    
    filters.push_back(f1);
    filters.push_back(f2);
    
    eb.subscribeMulti(filters, [&](const std::vector<Message>& events) {
        callCount++;
        for (const auto& e : events) {
            if (e.type == "order_created") {
                lastOrderId = e.getString("orderId");
            }
        }
    });
    
    // 发布第一个事件
    Message e1;
    e1.type = "order_created";
    e1.set("orderId", "ORDER_123");
    eb.publish(e1);
    ASSERT_EQ(callCount, 0);
    
    // 发布第二个事件，触发
    Message e2;
    e2.type = "payment";
    e2.set("orderId", "ORDER_123");
    eb.publish(e2);
    ASSERT_EQ(callCount, 1);
    ASSERT_EQ(lastOrderId, "ORDER_123");
}

TEST(multi_event_partial_different_order) {
    EventBus eb;
    int callCount = 0;
    
    // 订阅三个事件
    eb.subscribeMultiTypes({"step1", "step2", "step3"}, [&](const std::vector<Message>&) {
        callCount++;
    });
    
    // 按任意顺序发布
    eb.publish(Message("step2"));
    ASSERT_EQ(callCount, 0);
    
    eb.publish(Message("step3"));
    ASSERT_EQ(callCount, 0);
    
    eb.publish(Message("step1"));
    ASSERT_EQ(callCount, 1);
}

TEST(duplicate_event_same_type) {
    EventBus eb;
    int callCount = 0;
    
    // 订阅同一类型的多个消息（需要都收到）
    std::vector<Message> filters;
    Message f1;
    f1.type = "event";
    f1.set("seq", "1");
    Message f2;
    f2.type = "event";
    f2.set("seq", "2");
    
    filters.push_back(f1);
    filters.push_back(f2);
    
    eb.subscribeMulti(filters, [&](const std::vector<Message>&) {
        callCount++;
    });
    
    // 发送两个消息
    Message m1;
    m1.type = "event";
    m1.set("seq", "1");
    eb.publish(m1);
    
    Message m2;
    m2.type = "event";
    m2.set("seq", "2");
    eb.publish(m2);
    
    ASSERT_EQ(callCount, 1);
}

// ============================================================
// 主函数
// ============================================================
int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "    EventBus Unit Tests" << std::endl;
    std::cout << "========================================" << std::endl << std::endl;
    
    // Message tests
    std::cout << "--- Message Tests ---" << std::endl;
    RUN_TEST(message_creation);
    RUN_TEST(message_matching);
    RUN_TEST(message_to_string);
    RUN_TEST(empty_filter);
    
    // Single event tests
    std::cout << "\n--- Single Event Tests ---" << std::endl;
    RUN_TEST(single_event_subscribe_basic);
    RUN_TEST(single_event_with_data_filter);
    RUN_TEST(one_shot_subscription);
    RUN_TEST(publish_with_string_type);
    RUN_TEST(publish_overload);
    RUN_TEST(unsubscribe_single);
    
    // Multi event tests
    std::cout << "\n--- Multi Event Tests ---" << std::endl;
    RUN_TEST(multi_event_subscription_basic);
    RUN_TEST(multi_event_with_filters);
    RUN_TEST(multi_event_order_independent);
    RUN_TEST(multi_event_content_matching);
    RUN_TEST(multi_event_all_filters_matched);
    RUN_TEST(multi_event_partial_different_order);
    RUN_TEST(duplicate_event_same_type);
    RUN_TEST(unsubscribe_multi);
    
    // Mixed tests
    std::cout << "\n--- Mixed Tests ---" << std::endl;
    RUN_TEST(mixed_single_and_multi);
    
    // Thread safety
    std::cout << "\n--- Thread Safety Tests ---" << std::endl;
    RUN_TEST(thread_safety_basic);
    
    // Cleanup
    std::cout << "\n--- Cleanup Tests ---" << std::endl;
    RUN_TEST(clear_all_subscriptions);
    
    // Summary
    std::cout << "\n========================================" << std::endl;
    std::cout << "Results: " << testsPassed << " passed, " << testsFailed << " failed" << std::endl;
    std::cout << "========================================" << std::endl;
    
    return testsFailed > 0 ? 1 : 0;
}