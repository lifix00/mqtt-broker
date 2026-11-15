#ifndef ACL_H
#define ACL_H

#include <stddef.h>

// 加载 ACL 文件；path 为 NULL/空时关闭 ACL（全部允许）。
// 返回 0 成功，-1 文件或语法错误。
int acl_init(const char *path);

// 释放/重置 ACL 状态（静态存储，主要用于测试/退出）
void acl_cleanup(void);

int acl_enabled(void);

// 发布授权：topic 是带长度的原始 topic，不要求 NUL 结尾
int acl_check_publish(const char *username, const char *topic, size_t topic_len);

// 订阅授权：filter 是 NUL 结尾的 topic filter，可能包含 +/#
int acl_check_subscribe(const char *username, const char *filter);

#endif
