package com.kouyy;

public interface ILogService {
    /**
     * 保存异常消息
     *
     * @param message 消息内容，字符串或json格式字符串，json格式时在es中显示相应的字段
     * @param type    模块类型,通过这个类型获取异常是哪个模块最开始调用的类抛出来的
     * @param topic   发送到kafka的话题
     * @param group   分组，多个站点共用一个topic时，用来标识是哪个站点的
     */
    void error(String message, String type, String topic, String group);

    /**
     * 保存异常消息
     *
     * @param message 消息内容，字符串或json格式字符串，json格式时在es中显示相应的字段
     * @param type    模块类型,通过这个类型获取异常是哪个模块最开始调用的类抛出来的
     * @param topic   发送到kafka的话题
     * @param group   分组，多个站点共用一个topic时，用来标识是哪个站点的
     * @param ex      异常
     */
    void error(String message, String type, String topic, String group, Exception ex);

    /**
     * 记录info日志
     *
     * @param message 消息内容，字符串或json格式字符串，json格式时在es中显示相应的字段
     * @param type    模块类型
     * @param topic   发送到kafka的话题
     * @param group   分组，多个站点共用一个topic时，用来标识是哪个站点的
     */
    void info(String message, String type, String topic, String group);

    /**
     * 记录warn日志
     *
     * @param message 消息内容，字符串或json格式字符串，json格式时在es中显示相应的字段
     * @param type    模块类型
     * @param topic   发送到kafka的话题
     * @param group   分组，多个站点共用一个topic时，用来标识是哪个站点的
     */
    void warn(String message, String type, String topic, String group);

    /**
     * 记录fatal日志
     *
     * @param message 消息内容，字符串或json格式字符串，json格式时在es中显示相应的字段
     * @param type    模块类型
     * @param topic   发送到kafka的话题
     * @param group   分组，多个站点共用一个topic时，用来标识是哪个站点的
     */
    void fatal(String message, String type, String topic, String group);
}
