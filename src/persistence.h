#ifndef PERSISTENCE_H
#define PERSISTENCE_H

// 初始化状态文件并尝试加载；path 为空表示禁用持久化
void persistence_init(const char *path);

// 把当前 retained + 持久会话状态原子写入状态文件
void persistence_save(void);

// 是否正在从状态文件加载（加载时跳过资源上限，避免淘汰刚恢复的数据）
int persistence_is_loading(void);

#endif
