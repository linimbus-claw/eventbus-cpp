# EventBus - 进程内事件通知机制

## 项目简介

一个灵活的事件通知系统，支持：
- 单事件订阅
- 多事件组合订阅（等待所有事件发布后才触发回调）
- 消息内容匹配
- 线程安全

**GitHub**: https://github.com/linimbus-claw/eventbus-lib

## 构建

```bash
mkdir build && cd build
cmake ..
make
```

## 运行测试

```bash
./eventbus_test
```

## 使用示例

```cpp
#include "eventbus.hpp"

int main() {
    EventBus eb;
    
    // 单事件订阅
    eb.subscribe("login", [](const Message& msg) {
        std::cout << "User: " << msg.getString("username") << std::endl;
    });
    
    // 发布事件
    Message msg;
    msg.type = "login";
    msg.set("username", "Alice");
    eb.publish(msg);
    
    // 多事件订阅（等待 login + logout 都发布后才触发）
    eb.subscribeMultiTypes({"order_created", "payment"}, [](const std::vector<Message>& events) {
        std::cout << "Order completed!" << std::endl;
    });
    
    eb.publish(Message("order_created"));
    eb.publish(Message("payment"));  // 触发回调
    
    return 0;
}
```