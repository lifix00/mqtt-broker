#ifndef PERSISTENCE_H
#define PERSISTENCE_H

#include <stdint.h>

// 初始化状态文件并启动后台写入线程；path 为空表示禁用持久化
void persistence_init(const char *path);

// 标记 retained + 持久会话状态已变化。由 persistence_tick() 合并后异步落盘。
void persistence_save(void);

// 主循环定期调用：在合并窗口结束后生成快照并交给后台 writer。
void persistence_tick(uint64_t now_ms);

// 是否有尚未生成快照的修改；主循环据此缩短 epoll_wait 超时。
int persistence_is_dirty(void);

// 停止后台线程并同步写入最终状态；只在 Broker 正常退出时调用。
void persistence_shutdown(void);

// 是否正在从状态文件加载（加载时跳过资源上限，避免淘汰刚恢复的数据）
int persistence_is_loading(void);

#endif
