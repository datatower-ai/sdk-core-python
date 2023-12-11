# v1.1.0(2022/12/29)
- 新增当当前queue size大于batch size时，立即发送消息的功能

# v1.2.0(2023/02/14)
- 新增user_append, user_uniq_append接口
- 支持列表、对象、对象组数据结构传输

# v2.0.0(2023/03/16)
- 修复SDK获取到服务端异常result code后使用方无感知的问题（考虑到前后代码兼容问题以及SDK设计，通过日志打印方式通知，不向用户raise error）
- 新增非标准Json数据异常判断（Nan、Inf）
- 现在使用logging模块进行日志输出
- 新增MetaDataException错误类型
- 增加数据单元测试