package com.wl.canal.listener;

/***
 * 监听类
 * @param <T>
 */
public interface CanalListener<T>{

    /***
     * 数据发生新增
     * @param after 新增之后实体对象
     */
    void insert(T after);

    /***
     * 数据发生修改
     * @param before 修改之前实体
     * @param after 修改之后实体
     */
    void update(T before,T after);

    /***
     * 数据发生删除
     * @param before 删除之前的数据
     */
    void delete(T before);
}
