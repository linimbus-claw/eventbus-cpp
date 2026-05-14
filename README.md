# EventBus - 进程内事件通知机制

## 项目简介

一个灵活的事件通知系统，支持：
- 单事件订阅
- 多事件组合订阅（等待所有事件发布后才触发回调）
- 消息内容匹配
- 线程安全

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

// 定义消息
struct LoginEvent {
    std::string user;
    int age;
};

struct LogoutEvent {
    std::string user;
};

// 注册消息类型
EVENTBUS_REGISTER(LoginEvent, "login")
EVENTBUS_REGISTER(LogoutEvent, "logout")

int main() {
    EventBus eb;
    
    // 单事件订阅
    eb.subscribe<LoginEvent>([](const LoginEvent& e) {
        std::cout << "User logged in: " << e.user << std::endl;
    });
    
    // 发布事件
    eb.publish(LoginEvent{"Alice", 25});
    
    // 多事件订阅（等待 login + logout 都发布后才触发）
    eb.subscribeMulti<std::tuple<LoginEvent, LogoutEvent>>(
        [](const LoginEvent& login, const LogoutEvent& logout) {
            std::cout << login.user << " logged out after logging in" << std::endl;
        }
    );
    
    eb.publish(LoginEvent{"Bob", 30});
    eb.publish(LogoutEvent{"Bob"});
    
    return 0;
}
```